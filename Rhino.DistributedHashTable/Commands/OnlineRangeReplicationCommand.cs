using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Remote;
using Rhino.DistributedHashTable.Util;

namespace Rhino.DistributedHashTable.Commands
{
	public class OnlineSegmentReplicationCommand : ICommand
	{
		private readonly ILog log = LogManager.GetLogger(typeof(OnlineSegmentReplicationCommand));

		private readonly IDistributedHashTableNode node;
		private readonly IDistributedHashTableNodeReplication otherNode;
		private readonly string endpoint;
		private readonly Segment[] ranges;

		public OnlineSegmentReplicationCommand(
			NodeEndpoint endpoint,
			Segment[] ranges,
			IDistributedHashTableNode node,
			IDistributedHashTableNodeReplication otherNode)
		{
			this.endpoint = endpoint.Sync.ToString();
			this.ranges = ranges;
			this.node = node;
			this.otherNode = otherNode;
		}

		public bool Execute()
		{
			if (log.IsDebugEnabled)
			{
				var sb = new StringBuilder("Replicating from ")
					.Append(endpoint)
					.AppendLine(" the following ranges:");

				foreach (var range in ranges)
				{
					sb.Append("\t").Append(range).AppendLine();
				}
				log.Debug(sb);
			}
			var processedSegments = new List<int>();

			try
			{
				var rangesToLoad = AssignAllEmptySegmentsFromEndpoint();

				return ProcessSegmentsWithData(rangesToLoad, processedSegments) == false;
			}
			catch (Exception e)
			{
				log.Warn("Could not replicate ranges", e);
				return false;
			}
			finally
			{
				if (processedSegments.Count != ranges.Length)
				{
					try
					{
						node.GivingUpOn(ranges.Select(x => x.Index).Except(processedSegments).ToArray());
					}
					catch (Exception e)
					{
						log.Error("Could not tell node that we are giving up on values", e);
					}
				}
			}
		}

		private bool ProcessSegmentsWithData(IEnumerable<Segment> rangesToLoad,
										   ICollection<int> processedSegments)
		{
			var someFailed = false;
			int numberOfFailures = 0;
			foreach (var range in rangesToLoad)
			{
				try
				{
					ReplicateSegment(range);
					processedSegments.Add(range.Index);
				}
				catch (Exception e)
				{
					log.Error("Failed to replicate range " + range, e);
					numberOfFailures += 1;
					if (numberOfFailures > 5)
					{
						log.WarnFormat("Failed to replicate {0} times, giving up on all additional ranges",
									   numberOfFailures);
						break;
					}
					node.GivingUpOn(range.Index);
					processedSegments.Add(range.Index);
					someFailed |= true;
				}
			}
			return someFailed;
		}

		private void ReplicateSegment(Segment range)
		{
			while (true)
			{
				log.DebugFormat("Starting replication of range [{0}] from {1}",
								range,
								endpoint);

				var result = otherNode.ReplicateNextPage(node.Endpoint, range.Index);
				log.DebugFormat("Replication of range [{0}] from {1} got {2} puts & {3} removals",
								range,
								endpoint,
								result.PutRequests.Length,
								result.RemoveRequests.Length);

				if (result.PutRequests.Length != 0)
					node.Storage.Put(node.GetTopologyVersion(), result.PutRequests);

				if (result.RemoveRequests.Length != 0)
					node.Storage.Remove(node.GetTopologyVersion(), result.RemoveRequests);

				if (result.Done)
					break;
			}
			node.DoneReplicatingSegments(range.Index);
		}

		private List<Segment> AssignAllEmptySegmentsFromEndpoint()
		{
			var remainingSegments = new List<Segment>();
			foreach (var pagedSegment in ranges.Page(500))
			{
				var assignedSegments = otherNode.AssignAllEmptySegments(
					node.Endpoint, 
					pagedSegment.Select(x=>x.Index).ToArray());
				node.DoneReplicatingSegments(assignedSegments);
				if (log.IsDebugEnabled)
				{
					var sb = new StringBuilder("The following empty ranges has been assigned from ")
						.Append(endpoint).AppendLine(":")
						.Append(" [")
						.Append(string.Join(", ", assignedSegments.Select(x => x.ToString()).ToArray()))
						.Append("]");
					log.Debug(sb);
				}
				remainingSegments.AddRange(
					pagedSegment.Where(x => assignedSegments.Contains(x.Index) == false)
					);
			}
			return remainingSegments;
		}
	}
}