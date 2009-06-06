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
		private readonly IDistributedHashTableNodeReplicationFactory factory;
		private IDistributedHashTableNodeReplication otherNode;
		private readonly Segment[] segments;
		private readonly NodeEndpoint endpoint;
		private readonly ReplicationType type;
		private bool continueWorking = true;

		public event Action Completed = delegate { };

		public OnlineSegmentReplicationCommand(
			NodeEndpoint endpoint,
			Segment[] segments,
            ReplicationType type,
			IDistributedHashTableNode node,
			IDistributedHashTableNodeReplicationFactory factory)
		{
			this.endpoint = endpoint;
			this.segments = segments;
			this.type = type;
			this.node = node;
			this.factory = factory;
		}

		public void AbortExecution()
		{
			continueWorking = true;
		}

		public bool Execute()
		{
			log.DebugFormat("Replication from {0} of {1} segments for {2}", endpoint.Sync, segments.Length, type);
			var processedSegments = new List<int>();

			if (continueWorking == false)
				return false;
			try
			{
				otherNode = factory.Create(endpoint);
				var segmentsToLoad = AssignAllEmptySegmentsFromEndpoint(processedSegments);
				if (continueWorking == false)
					return false;
				return ProcessSegmentsWithData(segmentsToLoad, processedSegments) == false;
			}
			catch (Exception e)
			{
				log.Warn("Could not replicate segments for " + type, e);
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
								log.WarnFormat("Giving up replicating for {0} the {0} segments",
								               type,
								               array.Length);
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

		private bool ProcessSegmentsWithData(IEnumerable<Segment> segmentsToLoad,
										   ICollection<int> processedSegments)
		{
			var someFailed = false;
			int numberOfFailures = 0;
			foreach (var segment in segmentsToLoad)
			{
				if (continueWorking == false)
					return true;
				try
				{
					ReplicateSegment(segment);
					processedSegments.Add(segment.Index);
				}
				catch (Exception e)
				{
					log.Error("Failed to replicate segment " + segment, e);
					numberOfFailures += 1;
					if (numberOfFailures > 5)
					{
						log.WarnFormat("Failed to replicate {0} times, giving up on all additional segments",
									   numberOfFailures);
						break;
					}
					node.GivingUpOn(type, segment.Index);
					processedSegments.Add(segment.Index);
					someFailed |= true;
				}
			}
			return someFailed;
		}

		private void ReplicateSegment(Segment segment)
		{
			while (true)
			{
				log.DebugFormat("Starting replication of segment [{0}] from {1}",
								segment,
								endpoint.Sync);

				var result = otherNode.ReplicateNextPage(node.Endpoint, type, segment.Index);
				log.DebugFormat("Replication of segment [{0}] from {1} got {2} puts & {3} removals",
								segment,
								endpoint.Sync,
								result.PutRequests.Length,
								result.RemoveRequests.Length);

				if (result.PutRequests.Length != 0)
					node.Storage.Put(node.GetTopologyVersion(), result.PutRequests);

				if (result.RemoveRequests.Length != 0)
					node.Storage.Remove(node.GetTopologyVersion(), result.RemoveRequests);

				if (result.Done)
					break;
			}
			node.DoneReplicatingSegments(type, segment.Index);
		}

		private List<Segment> AssignAllEmptySegmentsFromEndpoint(List<int> processedSegments)
		{
			var remainingSegments = new List<Segment>();
			var assignedSegments = otherNode.AssignAllEmptySegments(
				node.Endpoint,
                type, 
				segments.Select(x => x.Index).ToArray());

			processedSegments.AddRange(assignedSegments);
			node.DoneReplicatingSegments(type, assignedSegments);

			log.DebugFormat("{0} empty segments assigned from {1}", assignedSegments.Length, endpoint.Sync);
			remainingSegments.AddRange(
				segments.Where(x => assignedSegments.Contains(x.Index) == false)
				);
			return remainingSegments;
		}

		public void RaiseCompleted()
		{
			Completed();
		}
	}
}