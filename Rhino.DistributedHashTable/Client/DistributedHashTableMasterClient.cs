using System;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using Google.ProtocolBuffers;
using Rhino.DistributedHashTable.Exceptions;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Protocol;
using NodeEndpoint = Rhino.DistributedHashTable.Internal.NodeEndpoint;
using Segment = Rhino.DistributedHashTable.Internal.Segment;
using Rhino.DistributedHashTable.Util;

namespace Rhino.DistributedHashTable.Client
{
	public class DistributedHashTableMasterClient : IDistributedHashTableMaster
	{
		private readonly Uri uri;

		public DistributedHashTableMasterClient(Uri uri)
		{
			this.uri = uri;
		}

		private T Execute<T>(Func<MessageStreamWriter<MasterMessageUnion>, MessageStreamIterator<MasterMessageUnion>, Stream, T> func)
		{
			using (var client = new TcpClient(uri.Host, uri.Port))
			using (var stream = client.GetStream())
			{
				var writer = new MessageStreamWriter<MasterMessageUnion>(stream);
				var reader = MessageStreamIterator<MasterMessageUnion>.FromStreamProvider(() => stream);
				return func(writer, reader, stream);
			}
		}

		private void Execute(Action<MessageStreamWriter<MasterMessageUnion>, MessageStreamIterator<MasterMessageUnion>, Stream> func)
		{
			Execute<object>((writer,
							 iterator,
							 stream) =>
			{
				func(writer, iterator, stream);
				return null;
			});
		}

		public Segment[] Join(NodeEndpoint endPoint)
		{
			return Execute((writer, iterator, stream) =>
			{
				writer.Write(new MasterMessageUnion.Builder
				{
					Type = MasterMessageType.JoinRequest,
					JoinRequest = new JoinRequestMessage.Builder
					{
						EndpointJoining = new Protocol.NodeEndpoint.Builder
						{
							Async = endPoint.Async.ToString(),
							Sync = endPoint.Sync.ToString()
						}.Build()
					}.Build()
				}.Build());
				writer.Flush();
				stream.Flush();

				var union = iterator.First();
				if (union.Type == MasterMessageType.MasterErrorResult)
					throw new RemoteNodeException(union.Exception.Message);
				if (union.Type != MasterMessageType.JoinResult)
					throw new UnexpectedReplyException("Got reply " + union.Type + " but expected JoinResult");

				var response = union.JoinResponse;

				return response.SegmentsList.Select(x => ConvertSegment(x)).ToArray();
			});
		}

		public void CaughtUp(NodeEndpoint endPoint,
							 params int[] caughtUpSegments)
		{
			Execute((writer,
							iterator,
							stream) =>
			{
				writer.Write(new MasterMessageUnion.Builder
				{
					Type = MasterMessageType.CaughtUpRequest,
					CaughtUp = new CaughtUpRequestMessage.Builder
					{
						CaughtUpSegmentsList = { caughtUpSegments },
						Endpoint = new Protocol.NodeEndpoint.Builder
						{
							Async = endPoint.Async.ToString(),
							Sync = endPoint.Sync.ToString()
						}.Build()
					}.Build()
				}.Build());
				writer.Flush();
				stream.Flush();

				Thread.Sleep(15000);

				var union = iterator.First();
				if (union.Type == MasterMessageType.MasterErrorResult)
					throw new RemoteNodeException(union.Exception.Message);
				if (union.Type != MasterMessageType.CaughtUpResponse)
					throw new UnexpectedReplyException("Got reply " + union.Type + " but expected CaughtUpResponse");
			});
		}

		public Topology GetTopology()
		{
			return Execute((writer, iterator, stream) =>
			{
				writer.Write(new MasterMessageUnion.Builder
				{
					Type = MasterMessageType.GetTopologyRequest,
				}.Build());
				writer.Flush();
				stream.Flush();

				var union = iterator.First();
				if (union.Type == MasterMessageType.MasterErrorResult)
					throw new RemoteNodeException(union.Exception.Message);
				if (union.Type != MasterMessageType.GetTopologyResult)
					throw new UnexpectedReplyException("Got reply " + union.Type + " but expected GetTopologyResult");

				var topology = union.Topology;
				var segments = topology.SegmentsList.Select(x => ConvertSegment(x));
				return new Topology(segments.ToArray(), new Guid(topology.Version.ToByteArray()))
				{
					Timestamp = DateTime.FromOADate(topology.TimestampAsDouble)
				};
			});
		}

		private static Segment ConvertSegment(Protocol.Segment x)
		{
			return new Segment
			{
				Version = new Guid(x.Version.ToByteArray()),
				AssignedEndpoint = x.AssignedEndpoint != Protocol.NodeEndpoint.DefaultInstance
									? new NodeEndpoint
									{
										Async = new Uri(x.AssignedEndpoint.Async),
										Sync = new Uri(x.AssignedEndpoint.Sync)
									}
									: null,
				InProcessOfMovingToEndpoint = x.InProcessOfMovingToEndpoint != Protocol.NodeEndpoint.DefaultInstance
												? new NodeEndpoint
												{
													Async = new Uri(x.InProcessOfMovingToEndpoint.Async),
													Sync = new Uri(x.InProcessOfMovingToEndpoint.Sync)
												}
												: null,
				Index = x.Index,
				Backups = x.BackupsList.Select(b => new NodeEndpoint
				{
					Async = new Uri(b.Async),
					Sync = new Uri(b.Sync)
				}).ToSet(),
			};
		}

		public void GaveUp(NodeEndpoint endpoint,
						   params int[] rangesGivingUpOn)
		{
			throw new NotImplementedException();
		}
	}
}