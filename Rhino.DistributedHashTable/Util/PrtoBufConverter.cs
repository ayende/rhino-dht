using System;
using System.Linq;
using Google.ProtocolBuffers;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Protocol;
using Rhino.PersistentHashTable;
using NodeEndpoint = Rhino.DistributedHashTable.Protocol.NodeEndpoint;
using Segment = Rhino.DistributedHashTable.Internal.Segment;
using Value = Rhino.PersistentHashTable.Value;
using ValueVersion = Rhino.PersistentHashTable.ValueVersion;

namespace Rhino.DistributedHashTable.Util
{
	public static class PrtoBufConverter
	{
		public static Topology GetTopology(this TopologyResultMessage topology)
		{
			var segments = topology.SegmentsList.Select(x => GetSegment(x));
			return new Topology(segments.ToArray(), topology.Version)
			{
				Timestamp = DateTime.FromOADate(topology.TimestampAsDouble)
			};
		}

		public static Segment GetSegment(this Protocol.Segment x)
		{
			return new Segment
			{
				AssignedEndpoint = x.AssignedEndpoint.GetNodeEndpoint(),
				InProcessOfMovingToEndpoint = x.InProcessOfMovingToEndpoint.GetNodeEndpoint(),
				Index = x.Index,
				PendingBackups = x.PendingBackupsList.Select(b => b.GetNodeEndpoint()).ToSet(),
				Backups = x.BackupsList.Select(b => b.GetNodeEndpoint()).ToSet()
			};
		}

		public static ExtendedGetRequest GetGetRequest(this GetRequestMessage x)
		{
			return new ExtendedGetRequest
			{
				Segment = x.Segment,
				Key = x.Key,
				SpecifiedVersion = GetVersion(x.SpecificVersion),
			};
		}

		public static GetResponseMessage GetGetResponse(this Value[] x)
		{
			return new GetResponseMessage.Builder
			{
				ValuesList =
					{
						x.Select(v => new Protocol.Value.Builder
						{
							Data = ByteString.CopyFrom(v.Data),
							ExpiresAtAsDouble = v.ExpiresAt != null ? v.ExpiresAt.Value.ToOADate() : (double?) null,
							Key = v.Key,
							ReadOnly = v.ReadOnly,
							Sha256Hash = ByteString.CopyFrom(v.Sha256Hash),
							Version = GetVersion(v.Version),
							Tag = v.Tag,
							TimeStampAsDouble = v.Timestamp.ToOADate(),
						}.Build())
					}
			}.Build();
		}

		public static PutResponseMessage GetPutResponse(this PutResult x)
		{
			return new PutResponseMessage.Builder
			{
				Version = GetVersion(x.Version),
				ConflictExists = x.ConflictExists
			}.Build();
		}

		public static ExtendedPutRequest GetPutRequest(this PutRequestMessage x)
		{
			return new ExtendedPutRequest
			{
				Bytes = x.Bytes.ToByteArray(),
				ExpiresAt = x.HasExpiresAtAsDouble
								?
									DateTime.FromOADate(x.ExpiresAtAsDouble.Value)
								:
									(DateTime?)null,
				IsReadOnly = x.IsReadOnly,
				IsReplicationRequest = x.IsReplicationRequest,
				Key = x.Key,
				OptimisticConcurrency = x.OptimisticConcurrency,
				ParentVersions = x.ParentVersionsList.Select(y => GetVersion(y)).ToArray(),
				ReplicationTimeStamp = x.HasReplicationTimeStampAsDouble
										?
											DateTime.FromOADate(x.ReplicationTimeStampAsDouble.Value)
										:
											(DateTime?)null,
				ReplicationVersion = GetVersion(x.ReplicationVersion),
				Segment = x.Segment,
				Tag = x.Tag
			};
		}

		public static ExtendedRemoveRequest GetRemoveRequest(this RemoveRequestMessage x)
		{
			return
				new ExtendedRemoveRequest
				{
					Segment = x.Segment,
					Key = x.Key,
					SpecificVersion = GetVersion(x.SpecificVersion),
					IsReplicationRequest = x.IsReplicationRequest
				};
		}

		public static RemoveRequestMessage GetRemoveRequest(this ExtendedRemoveRequest x)
		{
			return new RemoveRequestMessage.Builder
			{
				IsReplicationRequest = x.IsReplicationRequest,
				Key = x.Key,
				Segment = x.Segment,
				SpecificVersion = GetVersion(x.SpecificVersion)
			}.Build();
		}

		public static PutRequestMessage GetPutRequest(this ExtendedPutRequest x)
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
				builder.ReplicationVersion = GetVersion(x.ReplicationVersion);
			}
			return builder.Build();
		}


		public static Internal.NodeEndpoint GetNodeEndpoint(this NodeEndpoint endpoint)
		{
			if (endpoint == null || endpoint == NodeEndpoint.DefaultInstance)
				return null;
			return new Internal.NodeEndpoint()
			{
				Async = new Uri(endpoint.Async),
				Sync = new Uri(endpoint.Sync)
			};
		}


		public static NodeEndpoint GetNodeEndpoint(this Internal.NodeEndpoint endpoint)
		{
			return new NodeEndpoint.Builder
			{
				Async = endpoint.Async.ToString(),
				Sync = endpoint.Sync.ToString()
			}.Build();
		}

		public static ValueVersion GetVersion(Protocol.ValueVersion version)
		{
			if (version == Protocol.ValueVersion.DefaultInstance)
				return null;
			return new ValueVersion
			{
				InstanceId = new Guid(version.InstanceId.ToByteArray()),
				Number = version.Number
			};
		}

		public static Protocol.ValueVersion GetVersion(ValueVersion version)
		{
			if (version == null)
				return Protocol.ValueVersion.DefaultInstance;
			return new Protocol.ValueVersion.Builder
			{
				InstanceId = ByteString.CopyFrom(version.InstanceId.ToByteArray()),
				Number = version.Number
			}.Build();
		}

		public static TopologyResultMessage GetTopology(this Topology topology)
		{
			return new TopologyResultMessage.Builder
			{
				Version = topology.Version,
				TimestampAsDouble = topology.Timestamp.ToOADate(),
				SegmentsList = { topology.Segments.Select(x => x.GetSegment()) }
			}.Build();
		}

		public static Protocol.Segment GetSegment(this Internal.Segment segment)
		{
			var builder = new Protocol.Segment.Builder
			{
				Index = segment.Index,
				BackupsList = { segment.Backups.Select(x => x.GetNodeEndpoint()) },
				PendingBackupsList = { segment.PendingBackups.Select(x => x.GetNodeEndpoint()) }
			};
			if (segment.AssignedEndpoint != null)
			{
				builder.AssignedEndpoint = new Protocol.NodeEndpoint.Builder
				{
					Async = segment.AssignedEndpoint.Async.ToString(),
					Sync = segment.AssignedEndpoint.Sync.ToString()
				}.Build();
			}
			if (segment.InProcessOfMovingToEndpoint != null)
			{
				builder.InProcessOfMovingToEndpoint = new Protocol.NodeEndpoint.Builder
				{
					Async = segment.InProcessOfMovingToEndpoint.Async.ToString(),
					Sync = segment.InProcessOfMovingToEndpoint.Sync.ToString(),
				}.Build();
			}
			return builder.Build();
		}

	}
}