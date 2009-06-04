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
		public event Action<BackupState, NodeEndpoint, Segment> BackupChanged = delegate { };
		public event Action TopologyChanged = delegate { };

		private readonly ILog log = LogManager.GetLogger(typeof(DistributedHashTableMaster));

		public Topology Topology { get; set; }

		private readonly HashSet<NodeEndpoint> endpoints = new HashSet<NodeEndpoint>();

		public IEnumerable<Segment> Segments
		{
			get { return Topology.Segments; }
		}

		public int NumberOfBackCopiesToKeep
		{
			get;
			set;
		}

		public DistributedHashTableMaster()
		{
			NumberOfBackCopiesToKeep = 2;
			Topology = new Topology(CreateDefaultSegments().ToArray());
		}

		private static IEnumerable<Segment> CreateDefaultSegments()
		{
			for (int i = 0; i < 8192; i++)
			{
				var range = new Segment
				{
					Index = i
				};
				yield return range;
			}
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
		public Segment[] Join(NodeEndpoint endPoint)
		{
			var newlyAlocatedSegments = JoinInternal(endPoint);
			RearrangeBackups();
			if (log.IsDebugEnabled)
			{
				var sb = new StringBuilder()
					.Append("After join of ")
					.Append(endPoint)
					.Append(" range allocation is:")
					.AppendLine();
				foreach (var range in Segments)
				{
					sb.Append("\t").Append(range).AppendLine();
				}
				log.Debug(sb.ToString());
			}
			return newlyAlocatedSegments;
		}

		/// <summary>
		/// Notify the master that the endpoint has caught up on all the specified ranges
		/// </summary>
		public void CaughtUp(NodeEndpoint endPoint, params int[] caughtUpSegments)
		{
			Segment[] matchingSegments = GetMatchingSegments(caughtUpSegments, endPoint);

			var modifiedSegments = from range in Segments
								 join caughtUpSegment in matchingSegments on range.Version equals caughtUpSegment.Version into maybeMatchingSegment
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
			                        	       		AssignedEndpoint = endPoint,
			                        	       		Backups = x.Backups
			                        	       	}).ToArray()
				);
			RearrangeBackups();
			TopologyChanged();
		}

		public void GaveUp(NodeEndpoint endpoint,
		                   params int[] rangesGivingUpOn)
		{
			var matchingSegments = GetMatchingSegments(rangesGivingUpOn, endpoint);
			foreach (var range in matchingSegments)
			{
				range.InProcessOfMovingToEndpoint = null;
			}
		}

		private Segment[] GetMatchingSegments(IEnumerable<int> ranges,
		                                  NodeEndpoint endPoint)
		{
			var matchingSegments = Segments
				.Where(x => ranges.Contains(x.Index))
				.ToArray();
		
			var rangesNotBeloningToThespecifiedEndpoint = matchingSegments
				.Where(x => x.InProcessOfMovingToEndpoint != null)
				.Where(x => endPoint.Equals(x.InProcessOfMovingToEndpoint) == false);

			if (rangesNotBeloningToThespecifiedEndpoint.Count() != 0)
				throw new InvalidOperationException("Could not catch up or give up on ranges that belong to another endpoint");
			return matchingSegments;
		}

		private void RearrangeBackups()
		{
			var rearranger = new RearrangeBackups(Segments, endpoints, NumberOfBackCopiesToKeep);
			var rearranged = rearranger.Rearranging();
			if(rearranged == false)
				return;
			foreach (var backUpAdded in rearranger.Changed)
			{
				BackupChanged(BackupState.Added, backUpAdded.Endpoint, backUpAdded.Segment);
			}
		}

		private Segment[] JoinInternal(NodeEndpoint endPoint)
		{
			log.DebugFormat("Endpoint {0} joining", endPoint);
			endpoints.Add(endPoint);
			if (Segments.Any(x => x.BelongsTo(endPoint)))
			{
				log.DebugFormat("Endpoint {0} is already registered, probably an end point restart, ignoring", endPoint);
				return Segments.Where(x => x.BelongsTo(endPoint)).ToArray();
			}

			var rangesThatHadNoOwner = Segments
				.Where(x => x.AssignedEndpoint == null)
				.Apply(x => x.AssignedEndpoint = endPoint)
				.ToArray();
			if (rangesThatHadNoOwner.Length > 0)
			{
				log.DebugFormat("Endpoint {0} was assigned all ranges without owners", endPoint);
				return rangesThatHadNoOwner;
			}

			log.DebugFormat("New endpoint {0}, allocating ranges for it", endPoint);

			return RestructureSegmentsFairly(endPoint);
		}

		private Segment[] RestructureSegmentsFairly(NodeEndpoint point)
		{
			var newSegments = new List<Segment>();
			int index = 0;
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
						Backups = range.Backups
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

		public void Decommision(NodeEndpoint endPoint)
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

		public Topology GetTopology()
		{
			return Topology;
		}
	}
}