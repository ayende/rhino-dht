using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Google.ProtocolBuffers;
using log4net;
using Rhino.DistributedHashTable.Client;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Protocol;
using Rhino.Queues;
using NodeEndpoint = Rhino.DistributedHashTable.Internal.NodeEndpoint;
using ValueVersion = Rhino.PersistentHashTable.ValueVersion;

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
				null
				);
			storage = new DistributedHashTableStorage(name + ".data.esent", node);

			listener = new TcpListener(IPAddress.Any, port);
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
			try
			{
				using (client)
				using (var stream = client.GetStream())
				{
					var writer = new MessageStreamWriter<StorageMessageUnion>(stream);
					try
					{
						foreach (var wrapper in MessageStreamIterator<StorageMessageUnion>.FromStreamProvider(() => stream))
						{
							var topologyVersion = new Guid(wrapper.TopologyVersion.ToByteArray());
							switch (wrapper.Type)
							{
								case StorageMessageType.GetRequests:
									HandleGet(wrapper, topologyVersion, writer);
									break;
								case StorageMessageType.PutRequests:
									HandlePut(wrapper, topologyVersion, writer);
									break;
								case StorageMessageType.RemoveRequests:
									HandleRemove(wrapper, topologyVersion, writer);
									break;
								default:
									throw new ArgumentOutOfRangeException();
							}
							writer.Flush();
							stream.Flush();
						}
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
					}
				}
			}
			catch (Exception e)
			{
				log.Warn("Error when processing request to storage", e);
			}
		}

		private void HandleRemove(StorageMessageUnion wrapper,
		                          Guid topologyVersion,
		                          MessageStreamWriter<StorageMessageUnion> writer)
		{
			var requests = wrapper.RemoveRequestsList.Select(x =>
			                                                 new ExtendedRemoveRequest
			                                                 {
			                                                 	TopologyVersion = topologyVersion,
			                                                 	Segment = x.Segment,
			                                                 	Key = x.Key,
			                                                 	SpecificVersion = GetVersion(x.SpecificVersion),
			                                                 	IsReplicationRequest = x.IsReplicationRequest
			                                                 }).ToArray();
			var removed = storage.Remove(topologyVersion, requests);
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.RemoveResponses,
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
			var puts = wrapper.PutRequestsList.Select(x => new ExtendedPutRequest
			{
				Bytes = x.Bytes.ToByteArray(),
				ExpiresAt = x.ExpiresAtAsDouble != null ? DateTime.FromOADate(x.ExpiresAtAsDouble.Value) : (DateTime?)null,
				IsReadOnly = x.IsReadOnly,
				IsReplicationRequest = x.IsReplicationRequest,
				Key = x.Key,
				OptimisticConcurrency = x.OptimisticConcurrency,
				ParentVersions = x.ParentVersionsList.Select(y => GetVersion(y)).ToArray(),
				ReplicationTimeStamp = x.ReplicationTimeStampAsDouble != null ? DateTime.FromOADate(x.ReplicationTimeStampAsDouble.Value) : (DateTime?)null,
				ReplicationVersion = GetVersion(x.ReplicationVersion),
				Segment = x.Segment,
				TopologyVersion = topologyVersion,
				Tag = x.Tag
			}).ToArray();
			var results = storage.Put(topologyVersion, puts);
			writer.Write(new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.PutResponses,
				PutResponsesList = 
					{
						results.Select(x=> new PutResponseMessage.Builder
						{
							Version = GetVersion(x.Version),
							ConflictExists = x.ConflictExists
						}.Build())
					}
			}.Build());
		}

		private void HandleGet(StorageMessageUnion wrapper,
							   Guid topologyVersion,
			MessageStreamWriter<StorageMessageUnion> writer)
		{
			var values = storage.Get(topologyVersion, wrapper.GetRequestsList.Select(x => new ExtendedGetRequest
			{
				Segment = x.Segment,
				Key = x.Key,
				SpecifiedVersion = GetVersion(x.SpecificVersion),
				TopologyVersion = topologyVersion,
				IsReplicationRequest = false
			}).ToArray());
			var reply = new StorageMessageUnion.Builder
			{
				Type = StorageMessageType.GetResponses,
				TopologyVersion = wrapper.TopologyVersion,
				GetResponsesList =
					{
						values.Select(x=> new GetResponseMessage.Builder
						{
							ValuesList =
								{
									x.Select(v=> new Value.Builder
									{
										Data	= ByteString.CopyFrom(v.Data),
										ExpiresAtAsDouble = v.ExpiresAt != null ? v.ExpiresAt.Value.ToOADate() : (double?)null,
										Key = v.Key,
										ReadOnly = v.ReadOnly,
										Sha256Hash = ByteString.CopyFrom(v.Sha256Hash),
										Version = GetVersion(v.Version),
										Tag = v.Tag,
										TimeStampAsDouble = v.Timestamp.ToOADate(),
									}.Build())
								}
						}.Build())
					}
			};
			writer.Write(reply.Build());
		}

		private static ValueVersion GetVersion(Protocol.ValueVersion version)
		{
			if (version == null)
				return null;
			return new ValueVersion
			{
				InstanceId = new Guid(version.InstanceId.ToByteArray()),
				Number = version.Number
			};
		}

		private static Protocol.ValueVersion GetVersion(ValueVersion version)
		{
			if (version == null)
				return Protocol.ValueVersion.DefaultInstance;
			return new Protocol.ValueVersion.Builder
			{
				InstanceId = ByteString.CopyFrom(version.InstanceId.ToByteArray()),
				Number = version.Number
			}.Build();
		}

		public void Dispose()
		{
			listener.Stop();
			storage.Dispose();
			queueManager.Dispose();
		}
	}
}