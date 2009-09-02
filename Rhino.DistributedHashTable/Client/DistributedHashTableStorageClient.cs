using System;
using System.Net.Sockets;
using Google.ProtocolBuffers;
using Rhino.DistributedHashTable.Exceptions;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Protocol;
using Rhino.DistributedHashTable.Remote;
using Rhino.DistributedHashTable.Util;
using Rhino.PersistentHashTable;
using NodeEndpoint = Rhino.DistributedHashTable.Internal.NodeEndpoint;
using Value = Rhino.PersistentHashTable.Value;
using System.Linq;
using ReplicationType=Rhino.DistributedHashTable.Internal.ReplicationType;

namespace Rhino.DistributedHashTable.Client
{
	/// <summary>
	/// Thread Safety - This is NOT a thread safe connection
	/// Exception Safety - After an exception is thrown, it should be disposed and not used afterward
	/// Connection Pooling - It is expected that this will be part of a connection pool
	/// </summary>
	public class DistributedHashTableStorageClient : 
		IDistributedHashTableStorage, 
		IDistributedHashTableNodeReplication,
		IDistributedHashTableRemoteNode
	{
		private readonly NodeEndpoint endpoint;
		protected readonly TcpClient Client;
		private readonly NetworkStream stream;
		private readonly MessageStreamWriter<StorageMessageUnion> writer;

		public DistributedHashTableStorageClient(NodeEndpoint endpoint)
		{
			this.endpoint = endpoint;
			Client = new TcpClient(endpoint.Sync.Host, endpoint.Sync.Port);
			stream = Client.GetStream();
			writer = new MessageStreamWriter<StorageMessageUnion>(stream);
		}

		public NodeEndpoint Endpoint
		{
			get { return endpoint; }
		}

		public virtual void Dispose()
		{
			stream.Dispose();
			Client.Close();
		}

		public PutResult[] Put(int topologyVersion,
							   params ExtendedPutRequest[] valuesToAdd)
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.PutRequests,
				TopologyVersion = topologyVersion,
				PutRequestsList =
                	{
                		valuesToAdd.Select(x => x.GetPutRequest())
                	}
			}.Build());
			writer.Flush();
			stream.Flush();

			var union = ReadReply(StorageMessageType.PutResponses);
			return union.PutResponsesList.Select(x => new PutResult
			{
                ConflictExists = x.ConflictExists,
                Version = new PersistentHashTable.ValueVersion
                {
					InstanceId = new Guid(x.Version.InstanceId.ToByteArray()),
                    Number = x.Version.Number
                }
			}).ToArray();
		}

		private StorageMessageUnion ReadReply(StorageMessageType responses)
		{
			var iterator = MessageStreamIterator<StorageMessageUnion>.FromStreamProvider(() => new UndisposableStream(stream));
			var union = iterator.First();

			if(union.Type==StorageMessageType.TopologyChangedError)
				throw new TopologyVersionDoesNotMatchException();
			if(union.Type==StorageMessageType.SeeOtherError)
			{
				throw new SeeOtherException
				{
					Endpoint = union.SeeOtherError.Other.GetNodeEndpoint()
				};
			}
			if (union.Type == StorageMessageType.StorageErrorResult)
				throw new RemoteNodeException(union.Exception.Message);
			if (union.Type != responses)
				throw new UnexpectedReplyException("Got reply " + union.Type + " but expected " + responses);

			return union;
		}

		public bool[] Remove(int topologyVersion,
							 params ExtendedRemoveRequest[] valuesToRemove)
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.RemoveRequests,
				TopologyVersion = topologyVersion,
				RemoveRequestsList = 
                	{
                		valuesToRemove.Select(x=>x.GetRemoveRequest())
                	}
			}.Build());
			writer.Flush();
			stream.Flush();

			var union = ReadReply(StorageMessageType.RemoveResponses);
			return union.RemoveResponesList.Select(x => x.WasRemoved).ToArray();
		}

		public Value[][] Get(int topologyVersion,
							 params ExtendedGetRequest[] valuesToGet)
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.GetRequests,
				TopologyVersion = topologyVersion,
				GetRequestsList = 
                	{
                		valuesToGet.Select(x => CreateGetRequest(x))
                	}
			}.Build());
			writer.Flush();
			stream.Flush();

			var union = ReadReply(StorageMessageType.GetResponses);
			return union.GetResponsesList.Select(x => 
				x.ValuesList.Select(y=> new Value
				{
					Data = y.Data.ToByteArray(),
                    ExpiresAt = y.ExpiresAtAsDouble != null ? DateTime.FromOADate(y.ExpiresAtAsDouble.Value) : (DateTime?)null,
                    Key = y.Key,
                    ParentVersions = y.ParentVersionsList.Select(z => new PersistentHashTable.ValueVersion
                    {
                    	InstanceId = new Guid(z.InstanceId.ToByteArray()),
                        Number = z.Number
                    }).ToArray(),
                    ReadOnly = y.ReadOnly,
                    Sha256Hash = y.Sha256Hash.ToByteArray(),
                    Tag = y.Tag,
					Timestamp = DateTime.FromOADate(y.TimeStampAsDouble),
					Version = new PersistentHashTable.ValueVersion
					{
						InstanceId = new Guid(y.Version.InstanceId.ToByteArray()),
						Number = y.Version.Number
					}
				}).ToArray()
			).ToArray();
		}

		private static GetRequestMessage CreateGetRequest(ExtendedGetRequest x)
		{
			return new GetRequestMessage.Builder
			{
				Key = x.Key,
				Segment = x.Segment,
			}.Build();
		}

		public IDistributedHashTableNodeReplication Replication
		{
			get { return this; }
		}

		public ReplicationResult ReplicateNextPage(NodeEndpoint replicationEndpoint,
			ReplicationType type,
		                                           int segment)
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.ReplicateNextPageRequest,
				ReplicateNextPageRequest = new ReplicateNextPageRequestMessage.Builder
				{
					ReplicationEndpoint = replicationEndpoint.GetNodeEndpoint(),
					Segment = segment,
                    Type = type == ReplicationType.Backup? Protocol.ReplicationType.Backup : Protocol.ReplicationType.Ownership
				}.Build()
			}.Build());
			writer.Flush();
			stream.Flush();

			var union = ReadReply(StorageMessageType.ReplicateNextPageResponse);

			return new ReplicationResult
			{
				Done = union.ReplicateNextPageResponse.Done,
				PutRequests = union.ReplicateNextPageResponse.PutRequestsList.Select(
					x => x.GetPutRequest()
					).ToArray(),
                RemoveRequests = union.ReplicateNextPageResponse.RemoveRequestsList.Select(
					x => x.GetRemoveRequest()
					).ToArray()
			};
		}

		public int[] AssignAllEmptySegments(NodeEndpoint replicationEndpoint,
		                                  ReplicationType type,   int[] segments)
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.AssignAllEmptySegmentsRequest,
				AssignAllEmptySegmentsRequest = new AssignAllEmptySegmentsRequestMessage.Builder
				{
					ReplicationEndpoint = replicationEndpoint.GetNodeEndpoint(),
                    Type = type == ReplicationType.Backup? Protocol.ReplicationType.Backup : Protocol.ReplicationType.Ownership,
                    SegmentsList = { segments }
				}.Build()
			}.Build());
			writer.Flush();
			stream.Flush();

			var union = ReadReply(StorageMessageType.AssignAllEmptySegmentsResponse);

			return union.AssignAllEmptySegmentsResponse.AssignedSegmentsList.ToArray();
		}

		public void UpdateTopology()
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.UpdateTopology,
			}.Build());

			writer.Flush();
			stream.Flush();

			ReadReply(StorageMessageType.TopologyUpdated);
		}
	}
}