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
using ReplicationType=Rhino.DistributedHashTable.Internal.ReplicationType;

namespace Rhino.DistributedHashTable.Client
{
	public class DistributedHashTableMasterClient : IDistributedHashTableMaster
	{
		private readonly Uri uri;

		public DistributedHashTableMasterClient(Uri uri)
		{
			this.uri = uri;
		}

		private T Execute<T>(Func<MessageStreamWriter<MasterMessageUnion>, Stream, T> func)
		{
			using (var client = new TcpClient(uri.Host, uri.Port))
			using (var stream = client.GetStream())
			{
				var writer = new MessageStreamWriter<MasterMessageUnion>(stream);
				return func(writer, stream);
			}
		}

		private void Execute(Action<MessageStreamWriter<MasterMessageUnion>, Stream> func)
		{
			Execute<object>((writer,
							 stream) =>
			{
				func(writer, stream);
				return null;
			});
		}

		public Segment[] Join(NodeEndpoint endpoint)
		{
			return Execute((writer, stream) =>
			{
				writer.Write(new MasterMessageUnion.Builder
				{
					Type = MasterMessageType.JoinRequest,
					JoinRequest = new JoinRequestMessage.Builder
					{
						EndpointJoining = new Protocol.NodeEndpoint.Builder
						{
							Async = endpoint.Async.ToString(),
							Sync = endpoint.Sync.ToString()
						}.Build()
					}.Build()
				}.Build());
				writer.Flush();
				stream.Flush();

				var union = ReadReply(MasterMessageType.JoinResult, stream);

				var response = union.JoinResponse;

				return response.SegmentsList.Select(x => x.GetSegment()).ToArray();
			});
		}

		private static MasterMessageUnion ReadReply(MasterMessageType responses, Stream stream)
		{
			var iterator = MessageStreamIterator<MasterMessageUnion>.FromStreamProvider(() => new UndisposableStream(stream));
			var union = iterator.First();

			if (union.Type == MasterMessageType.MasterErrorResult)
				throw new RemoteNodeException(union.Exception.Message);
			if (union.Type != responses)
				throw new UnexpectedReplyException("Got reply " + union.Type + " but expected " + responses);

			return union;
		}

		public void CaughtUp(NodeEndpoint endpoint,
			ReplicationType type,
							 params int[] caughtUpSegments)
		{
			Execute((writer,
							stream) =>
			{
				writer.Write(new MasterMessageUnion.Builder
				{
					Type = MasterMessageType.CaughtUpRequest,
					CaughtUp = new CaughtUpRequestMessage.Builder
					{
						CaughtUpSegmentsList = { caughtUpSegments },
                        Type = type == ReplicationType.Backup ? Protocol.ReplicationType.Backup : Protocol.ReplicationType.Ownership,
						Endpoint = new Protocol.NodeEndpoint.Builder
						{
							Async = endpoint.Async.ToString(),
							Sync = endpoint.Sync.ToString()
						}.Build()
					}.Build()
				}.Build());
				writer.Flush();
				stream.Flush();

				ReadReply(MasterMessageType.CaughtUpResponse, stream);
			});
		}

		public Topology GetTopology()
		{
			return Execute((writer, stream) =>
			{
				writer.Write(new MasterMessageUnion.Builder
				{
					Type = MasterMessageType.GetTopologyRequest,
				}.Build());
				writer.Flush();
				stream.Flush();

				var union = ReadReply(MasterMessageType.GetTopologyResult, stream);

				return union.Topology.GetTopology();
			});
		}

		public void GaveUp(NodeEndpoint endpoint,
			ReplicationType type,
						   params int[] rangesGivingUpOn)
		{
			Execute((writer,
							stream) =>
			{
				writer.Write(new MasterMessageUnion.Builder
				{
					Type = MasterMessageType.GaveUpRequest,
					GaveUp = new GaveUpRequestMessage.Builder
					{
						GaveUpSegmentsList = { rangesGivingUpOn },
						Type = type == ReplicationType.Backup ? Protocol.ReplicationType.Backup : Protocol.ReplicationType.Ownership,
						Endpoint = new Protocol.NodeEndpoint.Builder
						{
							Async = endpoint.Async.ToString(),
							Sync = endpoint.Sync.ToString()
						}.Build()
					}.Build()
				}.Build());
				writer.Flush();
				stream.Flush();

				ReadReply(MasterMessageType.GaveUpResponse, stream);
			});
		}
	}
}