using System;
using System.Collections.Generic;
using System.Linq;
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
		private readonly Segment[] segments;
		private readonly ReplicationType type;
		private bool continueWorking = true;

		public event Action Completed = delegate { };

		public OnlineSegmentReplicationCommand(
			NodeEndpoint endpoint,
			Segment[] segments,
            ReplicationType type,
			IDistributedHashTableNode node,
			IDistributedHashTableNodeReplication otherNode)
		{
			this.endpoint = endpoint.Sync.ToString();
			this.segments = segments;
			this.type = type;
			this.node = node;
			this.otherNode = otherNode;
		}

		public void AbortExecution()
		{
			continueWorking = true;
		}

		public bool Execute()
		{
			log.DebugFormat("Replication from {0} of {1} segments", endpoint, segments.Length);
			var processedSegments = new List<int>();

			if (continueWorking == false)
				return false;
			try
			{
				var rangesToLoad = AssignAllEmptySegmentsFromEndpoint(processedSegments);
				if (continueWorking == false)
					return false;
				return ProcessSegmentsWithData(rangesToLoad, processedSegments) == false;
			}
			catch (Exception e)
			{
				log.Warn("Could not replicate segments", e);
				return false;
			}
			finally
			{
				if (processedSegments.Count != segments.Length)
				{
					try
					{
						var array = segments.Select(x => x.Index).Except(processedSegments).ToArray();
						if (array.Length > 0)
						{
							if (log.IsWarnEnabled)
							{
								log.WarnFormat("Giving up replicating the following segments: [{0}]",
								               string.Join(", ", array.Select(x => x.ToString()).ToArray()));
							}
							node.GivingUpOn(type, array);
						}
					}
					catch (Exception e)
					{
						log.Error("Could not tell node that we are giving up on values", e);
					}
				}
				Completed();
			}
		}

		private bool ProcessSegmentsWithData(IEnumerable<Segment> rangesToLoad,
										   ICollection<int> processedSegments)
		{
			var someFailed = false;
			int numberOfFailures = 0;
			foreach (var range in rangesToLoad)
			{
				if (continueWorking == false)
					return true;
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
						log.WarnFormat("Failed to replicate {0} times, giving up on all additional segments",
									   numberOfFailures);
						break;
					}
					node.GivingUpOn(type, range.Index);
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
			node.DoneReplicatingSegments(type, range.Index);
		}

		private List<Segment> AssignAllEmptySegmentsFromEndpoint(List<int> processedSegments)
		{
			var remainingSegments = new List<Segment>();
			foreach (var pagedSegment in segments.Page(500))
			{
				var assignedSegments = otherNode.AssignAllEmptySegments(
					node.Endpoint, 
					pagedSegment.Select(x=>x.Index).ToArray());
				
				processedSegments.AddRange(assignedSegments);
				node.DoneReplicatingSegments(type,assignedSegments);

				log.DebugFormat("{0} empty segments assigned from {1}", assignedSegments.Length, endpoint);
				remainingSegments.AddRange(
					pagedSegment.Where(x => assignedSegments.Contains(x.Index) == false)
					);
			}
			return remainingSegments;
		}

		public void RaiseCompleted()
		{
			Completed();
		}
	}
}