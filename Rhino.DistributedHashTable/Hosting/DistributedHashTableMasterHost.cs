using System;
using System.Net.Sockets;
using Google.ProtocolBuffers;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Protocol;
using NodeEndpoint=Rhino.DistributedHashTable.Internal.NodeEndpoint;
using Segment=Rhino.DistributedHashTable.Protocol.Segment;

namespace Rhino.DistributedHashTable.Hosting
{
	public class DistributedHashTableMasterHost : IDisposable
	{
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
					var writer = new MessageStreamWriter<MessageWrapper>(stream);
					foreach (var wrapper in MessageStreamIterator<MessageWrapper>.FromStreamProvider(() => stream))
					{
						switch (wrapper.Type)
						{
							case MessageType.GetTopologyRequest:
								HandleGetToplogy(stream, writer);
								break;
							case MessageType.JoinRequest:
								HandleJoin(wrapper, writer);
								break;
							default:
								throw new ArgumentOutOfRangeException();
						}
					}
					writer.Flush();
					stream.Flush();
				}
			}
			finally
			{
				listener.BeginAcceptTcpClient(OnAcceptTcpClient, null);
			}
		}

		private void HandleJoin(MessageWrapper wrapper,
		                        MessageStreamWriter<MessageWrapper> writer)
		{
			var endpoint = wrapper.JoinRequest.EndpointJoining;
			var segments = master.Join(new NodeEndpoint
			{
				Async = new Uri(endpoint.Async),
				Sync = new Uri(endpoint.Sync)
			});
			var joinResponse = new JoinResponseMessage.Builder();
			foreach (var segment in segments)
			{
				joinResponse.SegmentsList.Add(ConverToProtocolSegment(segment));
			}
			writer.Write(new MessageWrapper.Builder
			{
				Type = MessageType.JoinResult,
				JoinResponse = joinResponse.Build()
			}.Build());
		}

		private void HandleGetToplogy(NetworkStream stream,
		                              MessageStreamWriter<MessageWrapper> writer)
		{
			var topology = master.GetTopology();
			var topologyResultMessage = new TopologyResultMessage.Builder
			{
				Version = ByteString.CopyFrom(topology.Version.ToByteArray()),
				TimestampAsDouble = topology.Timestamp.ToOADate(),
			};
			foreach (var segment in topology.Segments)
			{
				topologyResultMessage.SegmentsList.Add(ConverToProtocolSegment(segment));
			}
			writer.Write(new MessageWrapper.Builder
			{
				Type = MessageType.GetTopologyResult,
				Topology = topologyResultMessage.Build()
			}.Build());
		}

		private static Segment ConverToProtocolSegment(Internal.Segment segment)
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