using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Google.ProtocolBuffers;
using log4net;
using Rhino.DistributedHashTable.Client;
using Rhino.DistributedHashTable.Exceptions;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Protocol;
using Rhino.DistributedHashTable.Remote;
using Rhino.DistributedHashTable.Util;
using Rhino.Queues;
using NodeEndpoint = Rhino.DistributedHashTable.Internal.NodeEndpoint;

namespace Rhino.DistributedHashTable.Hosting
{
	public class DistributedHashTableStorageHost : IDisposable
	{
		private readonly ILog log = LogManager.GetLogger(typeof(DistributedHashTableStorageHost));
		private readonly TcpListener listener;
		private readonly IDistributedHashTableNode node;
		private readonly QueueManager queueManager;
		private readonly IDistributedHashTableStorage storage;

		public DistributedHashTableStorageHost(Uri master)
			: this(master, "node", 2201)
		{
		}

		public DistributedHashTableStorageHost(
			Uri master,
			string name,
			int port)
		{
			Endpoint = new NodeEndpoint
			{
				Sync = new Uri("rhino.dht://" + Environment.MachineName + ":" + port + "/"),
				Async = new Uri("rhino.queues://" + Environment.MachineName + ":" + (port + 1) + "/")
			};
			queueManager = new QueueManager(new IPEndPoint(IPAddress.Any, port + 1), name + ".queue.esent");

			node = new DistributedHashTableNode(
				new DistributedHashTableMasterClient(master),
				new ThreadPoolExecuter(),
				new BinaryMessageSerializer(),
				Endpoint,
				queueManager,
				new NonPooledDistributedHashTableNodeFactory()
				);
			storage = new DistributedHashTableStorage(name + ".data.esent", node);
			
			listener = new TcpListener(
				Socket.OSSupportsIPv6 ? IPAddress.IPv6Any : IPAddress.Any, 
				port);
		}

		public NodeEndpoint Endpoint { get; private set; }

		public void Start()
		{
			node.Start();
			listener.Start();
			listener.BeginAcceptTcpClient(OnBeginAcceptTcpClient, null);
		}

		private void OnBeginAcceptTcpClient(IAsyncResult result)
		{
			TcpClient client;
			try
			{
				client = listener.EndAcceptTcpClient(result);
				listener.BeginAcceptTcpClient(OnBeginAcceptTcpClient, null);
			}
			catch (ObjectDisposedException)
			{
				return;
			}
			catch (InvalidOperationException)
			{
				return;
			}
			try
			{
				using (client)
				using (var stream = client.GetStream())
				{
					var writer = new MessageStreamWriter<StorageMessageUnion>(stream);
					try
					{
						foreach (var wrapper in MessageStreamIterator<StorageMessageUnion>.FromStreamProvider(() => new UndisposableStream(stream)))
						{
							log.DebugFormat("Got message {0}", wrapper.Type);
							switch (wrapper.Type)
							{
								case StorageMessageType.GetRequests:
									HandleGet(wrapper, new Guid(wrapper.TopologyVersion.ToByteArray()), writer);
									break;
								case StorageMessageType.PutRequests:
									HandlePut(wrapper, new Guid(wrapper.TopologyVersion.ToByteArray()), writer);
									break;
								case StorageMessageType.RemoveRequests:
									HandleRemove(wrapper, new Guid(wrapper.TopologyVersion.ToByteArray()), writer);
									break;
								case StorageMessageType.AssignAllEmptySegmentsRequest:
									HandleAssignEmpty(wrapper, writer);
									break;
								case StorageMessageType.ReplicateNextPageRequest:
									HandleReplicateNextPage(wrapper, writer);
									break;
								case StorageMessageType.UpdateTopology:
									HandleTopologyUpdate(writer);
									break;
								default:
									throw new InvalidOperationException("Message type was not understood: " + wrapper.Type);
							}
							writer.Flush();
							stream.Flush();
						}
					}
					catch (SeeOtherException e)
					{
						writer.Write(new StorageMessageUnion.Builder
						{
							Type = StorageMessageType.SeeOtherError,
							SeeOtherError = new SeeOtherErrorMessage.Builder
							{
								Other = e.Endpoint.GetNodeEndpoint()
							}.Build()
						}.Build());
						writer.Flush();
						stream.Flush();
					}
					catch (TopologyVersionDoesNotMatchException)
					{
						writer.Write(new StorageMessageUnion.Builder
						{
							Type = StorageMessageType.TopologyChangedError,
						}.Build());
						writer.Flush();
						stream.Flush();
					}
					catch (Exception e)
					{
						log.Warn("Error performing request", e);
						writer.Write(new StorageMessageUnion.Builder
						{
							Type = StorageMessageType.StorageErrorResult,
							Exception = new ErrorMessage.Builder
							{
								Message = e.ToString()
							}.Build()
						}.Build());
						writer.Flush();
						stream.Flush();
					}
				}
			}
			catch (Exception e)
			{
				log.Warn("Error when processing request to storage", e);
			}
		}

		private void HandleTopologyUpdate(MessageStreamWriter<StorageMessageUnion> writer)
		{
			node.UpdateTopology();
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.TopologyUpdated
			}.Build());
		}

		private void HandleReplicateNextPage(StorageMessageUnion wrapper,
											 MessageStreamWriter<StorageMessageUnion> writer)
		{
			var replicationResult = storage.Replication.ReplicateNextPage(
				wrapper.ReplicateNextPageRequest.ReplicationEndpoint.GetNodeEndpoint(),
				wrapper.ReplicateNextPageRequest.Segment
				);
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.ReplicateNextPageResponse,
				ReplicateNextPageResponse = new ReplicateNextPageResponseMessage.Builder()
				{
					Done = replicationResult.Done,
					RemoveRequestsList =
						{
							replicationResult.RemoveRequests.Select(x=>x.GetRemoveRequest()),
						},
					PutRequestsList =
						{
							replicationResult.PutRequests.Select(x=>x.GetPutRequest())
						}
				}.Build()
			}.Build());
		}

		private void HandleAssignEmpty(StorageMessageUnion wrapper,
									   MessageStreamWriter<StorageMessageUnion> writer)
		{
			var segments = storage.Replication.AssignAllEmptySegments(
				wrapper.AssignAllEmptySegmentsRequest.ReplicationEndpoint.GetNodeEndpoint(),
				wrapper.AssignAllEmptySegmentsRequest.SegmentsList.ToArray()
				);
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.AssignAllEmptySegmentsResponse,
				AssignAllEmptySegmentsResponse = new AssignAllEmptySegmentsResponseMessage.Builder
				{
					AssignedSegmentsList = { segments }
				}.Build()
			}.Build());
		}

		private void HandleRemove(StorageMessageUnion wrapper,
								  Guid topologyVersion,
								  MessageStreamWriter<StorageMessageUnion> writer)
		{
			var requests = wrapper.RemoveRequestsList.Select(x => x.GetRemoveRequest()).ToArray();
			var removed = storage.Remove(topologyVersion, requests);
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.RemoveResponses,
				TopologyVersion = ByteString.CopyFrom(topologyVersion.ToByteArray()),
				RemoveResponesList =
					{
						removed.Select(x => new RemoveResponseMessage.Builder
						{
							WasRemoved = x
						}.Build())
					}
			}.Build());
		}

		private void HandlePut(StorageMessageUnion wrapper,
							   Guid topologyVersion,
							   MessageStreamWriter<StorageMessageUnion> writer)
		{
			var puts = wrapper.PutRequestsList.Select(x => x.GetPutRequest()).ToArray();
			var results = storage.Put(topologyVersion, puts);
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.PutResponses,
				TopologyVersion = ByteString.CopyFrom(topologyVersion.ToByteArray()),
				PutResponsesList = 
					{
						results.Select(x=>x.GetPutResponse())
					}
			}.Build());
		}

		private void HandleGet(StorageMessageUnion wrapper,
							   Guid topologyVersion,
			MessageStreamWriter<StorageMessageUnion> writer)
		{
			var values = storage.Get(topologyVersion,
				wrapper.GetRequestsList.Select(x => x.GetGetRequest()).ToArray()
				);
			var reply = new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.GetResponses,
				TopologyVersion = wrapper.TopologyVersion,
				GetResponsesList =
					{
						values.Select(x=> x.GetGetResponse())
					}
			};
			writer.Write(reply.Build());
		}



		public void Dispose()
		{
			listener.Stop();
			node.Dispose();
			storage.Dispose();
			queueManager.Dispose();
		}
	}
}