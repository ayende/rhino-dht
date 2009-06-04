using System;
using System.Linq;
using System.Net.Sockets;
using Google.ProtocolBuffers;
using log4net;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Protocol;
using NodeEndpoint = Rhino.DistributedHashTable.Internal.NodeEndpoint;
using Segment = Rhino.DistributedHashTable.Protocol.Segment;

namespace Rhino.DistributedHashTable.Hosting
{
	public class DistributedHashTableMasterHost : IDisposable
	{
		private readonly ILog log = LogManager.GetLogger(typeof(DistributedHashTableStorageHost));

		private readonly TcpListener listener;
		private readonly DistributedHashTableMaster master = new DistributedHashTableMaster();

		public DistributedHashTableMasterHost()
			: this("master", 2200)
		{
		}

		public DistributedHashTableMasterHost(string name,
											  int port)
		{
		}

		public void Dispose()
		{
			listener.Stop();
		}

		public void Start()
		{
			listener.Start();
			listener.BeginAcceptTcpClient(OnAcceptTcpClient, null);
		}

		private void OnAcceptTcpClient(IAsyncResult result)
		{
			TcpClient client;
			try
			{
				client = listener.EndAcceptTcpClient(result);
			}
			catch (ObjectDisposedException)
			{
				return;
			}

			//this is done intentionally in a single threaded fashion
			//the master is not a hot spot and it drastically simplify our life
			//to avoid having to do multi threaded stuff here
			//all calls to the master are also very short

			try
			{
				using (client)
				using (var stream = client.GetStream())
				{
						var writer = new MessageStreamWriter<MasterMessageUnion>(stream);
					try
					{
						foreach (var wrapper in MessageStreamIterator<MasterMessageUnion>.FromStreamProvider(() => stream))
						{
							log.DebugFormat("Accepting message from {0} - {1}",
							                client.Client.RemoteEndPoint,
							                wrapper.Type);
							switch (wrapper.Type)
							{
								case MasterMessageType.GetTopologyRequest:
									HandleGetToplogy(writer);
									break;
								case MasterMessageType.JoinRequest:
									HandleJoin(wrapper, writer);
									break;
								default:
									throw new ArgumentOutOfRangeException();
							}
						}
						writer.Flush();
						stream.Flush();
					}
					catch (Exception e)
					{
						log.Warn("Error performing request",e );
						writer.Write(new MasterMessageUnion.Builder
						{
							Type = MasterMessageType.MasterErrorResult,
                            Exception = new Error.Builder
                            {
                            	Message = e.ToString()
                            }.Build()
						}.Build());
					}
				}
			}
			catch (Exception e)
			{
				log.Warn("Error when processing request to master, error reporting failed as well!", e);
			}
			finally
			{
				listener.BeginAcceptTcpClient(OnAcceptTcpClient, null);
			}
		}

		private void HandleJoin(MasterMessageUnion wrapper,
								MessageStreamWriter<MasterMessageUnion> writer)
		{
			var endpoint = wrapper.JoinRequest.EndpointJoining;
			var segments = master.Join(new NodeEndpoint
			{
				Async = new Uri(endpoint.Async),
				Sync = new Uri(endpoint.Sync)
			});
			var joinResponse = new JoinResponseMessage.Builder
			{
				SegmentsList = {segments.Select(x => ConvertToProtocolSegment(x))}
			};
			writer.Write(new MasterMessageUnion.Builder
			{
				Type = MasterMessageType.JoinResult,
				JoinResponse = joinResponse.Build()
			}.Build());
		}

		private void HandleGetToplogy(MessageStreamWriter<MasterMessageUnion> writer)
		{
			var topology = master.GetTopology();
			var topologyResultMessage = new TopologyResultMessage.Builder
			{
				Version = ByteString.CopyFrom(topology.Version.ToByteArray()),
				TimestampAsDouble = topology.Timestamp.ToOADate(),
				SegmentsList = { topology.Segments.Select(x => ConvertToProtocolSegment(x)) }
			};
			writer.Write(new MasterMessageUnion.Builder
			{
				Type = MasterMessageType.GetTopologyResult,
				Topology = topologyResultMessage.Build()
			}.Build());
		}

		private static Segment ConvertToProtocolSegment(Internal.Segment segment)
		{
			return new Segment.Builder
			{
				Index = segment.Index,
				AssignedEndpoint = new Protocol.NodeEndpoint.Builder
				{
					Async = segment.AssignedEndpoint.Async.ToString(),
					Sync = segment.AssignedEndpoint.Sync.ToString()
				}.Build(),
				InProcessOfMovingToEndpoint = segment.InProcessOfMovingToEndpoint == null
												?
													Protocol.NodeEndpoint.DefaultInstance
												:
													new Protocol.NodeEndpoint.Builder
													{
														Async = segment.InProcessOfMovingToEndpoint.Async.ToString(),
														Sync = segment.InProcessOfMovingToEndpoint.Sync.ToString(),
													}.Build(),
				Version = ByteString.CopyFrom(segment.Version.ToByteArray()),
			}.Build();
		}
	}
}