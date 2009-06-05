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
using ValueVersion=Rhino.DistributedHashTable.Protocol.ValueVersion;

namespace Rhino.DistributedHashTable.Client
{
	/// <summary>
	/// Thread Safety - This is NOT a thread safe connection
	/// Exception Safety - After an exception is thrown, it should be disposed and not used afterward
	/// Connection Pooling - It is expected that this will be part of a connection pool
	/// </summary>
	public class DistributedHashTableStorageClient : IDistributedHashTableStorage
	{
		private readonly NodeEndpoint endpoint;
		private readonly TcpClient client;
		private readonly NetworkStream stream;
		private readonly MessageStreamWriter<StorageMessageUnion> writer;

		public DistributedHashTableStorageClient(NodeEndpoint endpoint)
		{
			this.endpoint = endpoint;
			client = new TcpClient(endpoint.Sync.Host, endpoint.Sync.Port);
			stream = client.GetStream();
			writer = new MessageStreamWriter<StorageMessageUnion>(stream);
		}

		public NodeEndpoint Endpoint
		{
			get { return endpoint; }
		}

		public void Dispose()
		{
			stream.Dispose();
			client.Close();
		}

		public PutResult[] Put(Guid topologyVersion,
							   params ExtendedPutRequest[] valuesToAdd)
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.PutRequests,
				PutRequestsList =
                	{
                		valuesToAdd.Select(x => CreatePutRequest(x))
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

			if (union.Type == StorageMessageType.StorageErrorResult)
				throw new RemoteNodeException(union.Exception.Message);
			if (union.Type != responses)
				throw new UnexpectedReplyException("Got reply " + union.Type + " but expected " + responses);

			return union;
		}

		private static PutRequestMessage CreatePutRequest(ExtendedPutRequest x)
		{
			var builder = new PutRequestMessage.Builder
			{
				Bytes = ByteString.CopyFrom(x.Bytes),
                IsReadOnly = x.IsReadOnly,
                IsReplicationRequest = x.IsReplicationRequest,
                Key = x.Key,
                OptimisticConcurrency = x.OptimisticConcurrency,
                Segment = x.Segment,
                Tag = x.Tag,
			};
			if (x.ExpiresAt != null)
				builder.ExpiresAtAsDouble = x.ExpiresAt.Value.ToOADate();
			if (x.ReplicationTimeStamp != null)
				builder.ReplicationTimeStampAsDouble = x.ReplicationTimeStamp.Value.ToOADate();
			if (x.ReplicationVersion != null)
			{
				builder.ReplicationVersion = new ValueVersion.Builder
				{
                    InstanceId = ByteString.CopyFrom(x.ReplicationVersion.InstanceId.ToByteArray()),
                    Number = x.ReplicationVersion.Number
				}.Build();
			}
			return builder.Build();
		}

		public bool[] Remove(Guid topologyVersion,
							 params ExtendedRemoveRequest[] valuesToRemove)
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.RemoveRequests,
				RemoveRequestsList = 
                	{
                		valuesToRemove.Select(x => new RemoveRequestMessage.Builder
                		{
                			IsReplicationRequest = x.IsReplicationRequest,
                            Key = x.Key,
                            Segment = x.Segment,
                            SpecificVersion = new ValueVersion.Builder
                            {
                            	InstanceId = ByteString.CopyFrom(x.SpecificVersion.InstanceId.ToByteArray()),
                                Number = x.SpecificVersion.Number
                            }.Build()
                		}.Build())
                	}
			}.Build());
			writer.Flush();
			stream.Flush();

			var union = ReadReply(StorageMessageType.RemoveResponses);
			return union.RemoveResponesList.Select(x => x.WasRemoved).ToArray();
		}

		public Value[][] Get(Guid topologyVersion,
							 params ExtendedGetRequest[] valuesToGet)
		{
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.GetRequests,
				GetRequestsList = 
                	{
                		valuesToGet.Select(x => CreateGetRequest(x))
                	}
			}.Build());
			writer.Flush();
			stream.Flush();

			var union = ReadReply(StorageMessageType.RemoveResponses);
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
			get { throw new NotImplementedException(); }
		}
	}
}