using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Google.ProtocolBuffers;
using log4net;
using Rhino.DistributedHashTable.Commands;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Protocol;
using Rhino.DistributedHashTable.Remote;
using Rhino.DistributedHashTable.Util;
using NodeEndpoint = Rhino.DistributedHashTable.Internal.NodeEndpoint;

namespace Rhino.DistributedHashTable.Hosting
{
	public class DistributedHashTableMasterHost : IDisposable
	{
		private readonly IExecuter executer;
		private readonly ILog log = LogManager.GetLogger(typeof(DistributedHashTableMasterHost));

		private readonly TcpListener listener;
		private readonly DistributedHashTableMaster master;

		public DistributedHashTableMasterHost()
			: this(new ThreadPoolExecuter(), 2200)
		{
		}

		public DistributedHashTableMasterHost(IExecuter executer, int port)
		{
			this.executer = executer;
			master = new DistributedHashTableMaster();
			master.TopologyChanged += OnTopologyChanged;
			listener = new TcpListener(IPAddress.Any, port);
		}

		private void OnTopologyChanged()
		{
			log.DebugFormat("Topology updated to {0}", master.Topology.Version);
			executer.RegisterForExecution(new NotifyEndpointsAboutTopologyChange(
				master.Endpoints.ToArray(),
				new NonPooledDistributedHashTableNodeFactory()
				));
		}

		public void Dispose()
		{
			listener.Stop();
			executer.Dispose();
		}

		public void Start()
		{
			listener.Start();
			listener.BeginAcceptTcpClient(OnAcceptTcpClient, null);
			OnTopologyChanged();
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
					foreach (var wrapper in MessageStreamIterator<MasterMessageUnion>.FromStreamProvider(() => stream))
					{
						try
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
								case MasterMessageType.CaughtUpRequest:
									HandleCatchUp(wrapper, writer);
									break;
								case MasterMessageType.GaveUpRequest:
									HandleGaveUp(wrapper, writer);
									break;
								default:
									throw new ArgumentOutOfRangeException();
							}
							writer.Flush();
							stream.Flush();
						}
						catch (Exception e)
						{
							log.Warn("Error performing request", e);
							writer.Write(new MasterMessageUnion.Builder
							{
								Type = MasterMessageType.MasterErrorResult,
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
			}
			catch (Exception e)
			{
				log.Warn("Error when dealing with a request (or could not send error details)", e);
			}
			finally
			{
				try
				{
					listener.BeginAcceptTcpClient(OnAcceptTcpClient, null);
				}
				catch (InvalidOperationException)
				{
					//the listener was closed
				}
			}
		}

		private void HandleCatchUp(MasterMessageUnion wrapper,
								   MessageStreamWriter<MasterMessageUnion> writer)
		{
			master.CaughtUp(new NodeEndpoint
			{
				Async = new Uri(wrapper.CaughtUp.Endpoint.Async),
				Sync = new Uri(wrapper.CaughtUp.Endpoint.Sync)
			}, wrapper.CaughtUp.CaughtUpSegmentsList.ToArray());
			writer.Write(new MasterMessageUnion.Builder
			{
				Type = MasterMessageType.CaughtUpResponse
			}.Build());
		}

		private void HandleGaveUp(MasterMessageUnion wrapper,
								   MessageStreamWriter<MasterMessageUnion> writer)
		{
			master.GaveUp(new NodeEndpoint
			{
				Async = new Uri(wrapper.GaveUp.Endpoint.Async),
				Sync = new Uri(wrapper.GaveUp.Endpoint.Sync)
			}, wrapper.GaveUp.GaveUpSegmentsList.ToArray());
			writer.Write(new MasterMessageUnion.Builder
			{
				Type = MasterMessageType.GaveUpResponse
			}.Build());
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
				SegmentsList = { segments.Select(x => x.GetSegment()) }
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
			writer.Write(new MasterMessageUnion.Builder
			{
				Type = MasterMessageType.GetTopologyResult,
				Topology = topology.GetTopology()
			}.Build());
		}
	}
}