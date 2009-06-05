using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using Rhino.DistributedHashTable.Commands;
using Rhino.DistributedHashTable.Util;

namespace Rhino.DistributedHashTable.Internal
{
	/// <summary>
	/// The master is a SIGNLE THREADED service that manages all
	/// operations in the cluster. 
	/// </summary>
	public class DistributedHashTableMaster : IDistributedHashTableMaster
	{
		private readonly HashSet<NodeEndpoint> endpoints = new HashSet<NodeEndpoint>();
		private readonly ILog log = LogManager.GetLogger(typeof(DistributedHashTableMaster));

		public DistributedHashTableMaster()
		{
			NumberOfBackCopiesToKeep = 2;
			Topology = new Topology(CreateDefaultSegments().ToArray());
		}

		public Topology Topology { get; set; }

		public Segment[] Segments
		{
			get { return Topology.Segments; }
		}

		public int NumberOfBackCopiesToKeep { get; set; }

		public IEnumerable<NodeEndpoint> Endpoints
		{
			get { return endpoints; }
		}

		/// <summary>
		/// This method is called when a new node wants to join the cluster.
		/// The result is the ranges that this node is responsible for, if it is an
		/// existing one, or the list of ranges that it needs to pull from the currently 
		/// assigned node.
		/// Note:
		/// that if it needs to pull date from the currently assigned node, it will
		/// also need to call the <see cref="CaughtUp"/> method to let the master know 
		/// that it is done and that the topology changed.
		/// </summary>
		public Segment[] Join(NodeEndpoint endpoint)
		{
			var newlyAlocatedSegments = JoinInternal(endpoint);
			RearrangeBackups();
			LogCurrentSegmentAssignment();
			return newlyAlocatedSegments;
		}

		/// <summary>
		/// Notify the master that the endpoint has caught up on all the specified ranges
		/// </summary>
		public void CaughtUp(NodeEndpoint endpoint,
							 ReplicationType type,
							 params int[] caughtUpSegments)
		{
			if (type == ReplicationType.Ownership)
				CaughtUpOnOwnership(caughtUpSegments, endpoint);
			LogCurrentSegmentAssignment();
			TopologyChanged();
		}

		private void CaughtUpOnOwnership(int[] caughtUpSegments,
										 NodeEndpoint endpoint)
		{
			var matchingSegments = GetMatchingSegments(caughtUpSegments, endpoint);

			var modifiedSegments = from range in Segments
								   join caughtUpSegment in matchingSegments on range.Index equals caughtUpSegment.Index into
									maybeMatchingSegment
								   select
									new { range, maybeMatchingSegment };

			Topology = new Topology((
										from modifiedSegment in modifiedSegments
										let x = modifiedSegment.maybeMatchingSegment.FirstOrDefault()
										select x == null
												? modifiedSegment.range
												: new Segment
												{
													Index = x.Index,
													InProcessOfMovingToEndpoint = null,
													AssignedEndpoint = endpoint,
													PendingBackups = x.PendingBackups
														.Append(x.AssignedEndpoint)
														.Where(e => e != endpoint)
														.ToSet()
												}).ToArray()
				);
			RearrangeBackups();
		}

		public void GaveUp(NodeEndpoint endpoint,
						   ReplicationType type,
						   params int[] rangesGivingUpOn)
		{
			var matchingSegments = GetMatchingSegments(rangesGivingUpOn, endpoint);
			foreach (var range in matchingSegments)
			{
				range.InProcessOfMovingToEndpoint = null;
			}
		}

		public Topology GetTopology()
		{
			return Topology;
		}

		public event Action<BackupState, NodeEndpoint, Segment> BackupChanged = delegate { };
		public event Action TopologyChanged = delegate { };

		private static IEnumerable<Segment> CreateDefaultSegments()
		{
			for (var i = 0; i < 8192; i++)
			{
				var range = new Segment
				{
					Index = i
				};
				yield return range;
			}
		}

		private void LogCurrentSegmentAssignment()
		{
			if (!log.IsDebugEnabled)
				return;

			var sb = new StringBuilder("Current segment assignments are: ");
			var stats = new Dictionary<NodeEndpoint, NodeEndpointStats>();

			var groupByAssignment = Segments.GroupBy(x => x.AssignedEndpoint ?? new NodeEndpoint());
			var groupByTentative = Segments.GroupBy(x => x.InProcessOfMovingToEndpoint);
			var groupByTentativeBackups = Segments.SelectMany(x => x.PendingBackups).GroupBy(x => x);
			var groupByBackups = Segments.SelectMany(x => x.Backups).GroupBy(x => x);

			foreach (var assignment in groupByAssignment)
			{
				stats[assignment.Key] = new NodeEndpointStats
				{
					AssignmentCount = assignment.Count()
				};
			}
	
			NodeEndpointStats value;

			foreach (var backup in groupByBackups)
			{
				if(backup.Key == null)
					continue;
				if (stats.TryGetValue(backup.Key, out value))
					value.BackupCount = backup.Count();
			}

			foreach (var tentative in groupByTentative)
			{
				if (tentative.Key == null)
					continue;
				if (stats.TryGetValue(tentative.Key, out value))
					value.TentativeCount = tentative.Count();
			}

			foreach (var backup in groupByTentativeBackups)
			{
				if (backup.Key == null)
					continue;
				if (stats.TryGetValue(backup.Key, out value))
					value.TentativeBackupCount = backup.Count();
			}

			foreach (var segment in stats)
			{
				sb.Append("[")
					.Append(segment.Key.Sync.ToString() ?? "NULL")
					.Append(", assignments: ")
					.Append(segment.Value.AssignmentCount)
					.Append(", backups: ")
					.Append(segment.Value.BackupCount)
					.Append(", tentatives: ")
					.Append(segment.Value.TentativeCount)
					.Append(", tentative backups: ")
					.Append(segment.Value.TentativeBackupCount)
					.Append("], ");
			}
			log.Debug(sb.ToString());
		}

		private Segment[] GetMatchingSegments(IEnumerable<int> ranges,
											  NodeEndpoint endpoint)
		{
			var matchingSegments = ranges.Select(i => Segments[i]).ToArray();

			var rangesNotBeloningToThespecifiedEndpoint = matchingSegments
				.Where(x => x.InProcessOfMovingToEndpoint != null)
				.Where(x => endpoint.Equals(x.InProcessOfMovingToEndpoint) == false);

			if (rangesNotBeloningToThespecifiedEndpoint.Count() != 0)
				throw new InvalidOperationException("Could not catch up or give up on ranges that belong to another endpoint");
			return matchingSegments;
		}

		private void RearrangeBackups()
		{
			var rearranger = new RearrangeBackups(Segments, endpoints, NumberOfBackCopiesToKeep);
			var rearranged = rearranger.Rearranging();
			if (rearranged == false)
				return;
			foreach (var backUpAdded in rearranger.Changed)
			{
				BackupChanged(BackupState.Added, backUpAdded.Endpoint, backUpAdded.Segment);
			}
		}

		private Segment[] JoinInternal(NodeEndpoint endpoint)
		{
			log.DebugFormat("Endpoint {0} joining", endpoint.Sync);
			endpoints.Add(endpoint);
			if (Segments.Any(x => x.BelongsTo(endpoint)))
			{
				log.DebugFormat("Endpoint {0} is already registered, probably an end point restart, ignoring", endpoint.Sync);
				return Segments.Where(x => x.BelongsTo(endpoint)).ToArray();
			}

			var rangesThatHadNoOwner = Segments
				.Where(x => x.AssignedEndpoint == null)
				.Apply(x => x.AssignedEndpoint = endpoint)
				.ToArray();
			if (rangesThatHadNoOwner.Length > 0)
			{
				log.DebugFormat("Endpoint {0} was assigned all ranges without owners", endpoint.Sync);
				return rangesThatHadNoOwner;
			}

			log.DebugFormat("New endpoint {0}, allocating ranges for it", endpoint.Sync);

			return RestructureSegmentsFairly(endpoint);
		}

		private Segment[] RestructureSegmentsFairly(NodeEndpoint point)
		{
			var newSegments = new List<Segment>();
			var index = 0;
			foreach (var range in Segments)
			{
				index += 1;

				if (range.InProcessOfMovingToEndpoint != null)
				{
					newSegments.Add(range);
					continue;
				}
				if (index % endpoints.Count == 0)
				{
					newSegments.Add(new Segment
					{
						AssignedEndpoint = range.AssignedEndpoint,
						InProcessOfMovingToEndpoint = point,
						Index = range.Index,
						PendingBackups = range.PendingBackups
					});
				}
				else
				{
					newSegments.Add(range);
				}
			}
			// this does NOT create a new topology version
			Topology = new Topology(newSegments.ToArray(), Topology.Version);
			return Segments.Where(x => x.BelongsTo(point)).ToArray();
		}

		public void Decommision(NodeEndpoint endpoint)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Sync the master with the list of items maintained by a copy of the master
		/// </summary>
		public void SyncUp(Topology topology)
		{
			if (Topology.Version == topology.Version)
				return;
			if (Topology.Timestamp > topology.Timestamp)
				return;
			Topology = topology;
		}

		class NodeEndpointStats
		{
			public int AssignmentCount;
			public int TentativeCount;
			public int BackupCount;
			public int TentativeBackupCount;
		}
	}
}