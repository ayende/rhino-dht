using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Rhino.DistributedHashTable.Commands;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Remote;
using Rhino.DistributedHashTable.Util;
using Rhino.Queues;

namespace Rhino.DistributedHashTable.Internal
{
	public class DistributedHashTableNode : IDistributedHashTableNode
	{
		private readonly NodeEndpoint endpoint;
		private readonly IExecuter executer;
		private readonly IDistributedHashTableMaster master;
		private readonly IMessageSerializer messageSerializer;
		private readonly IQueueManager queueManager;
		private readonly IDistributedHashTableNodeReplicationFactory replicationFactory;

		public DistributedHashTableNode(IDistributedHashTableMaster master,
										IExecuter executer,
										IMessageSerializer messageSerializer,
										NodeEndpoint endpoint,
										IQueueManager queueManager,
										IDistributedHashTableNodeReplicationFactory replicationFactory)
		{
			this.master = master;
			this.executer = executer;
			this.messageSerializer = messageSerializer;
			this.endpoint = endpoint;
			this.queueManager = queueManager;
			this.replicationFactory = replicationFactory;
			State = NodeState.NotStarted;
		}

		public NodeState State { get; set; }

		public Topology Topology { get; private set; }

		public NodeEndpoint Endpoint
		{
			get { return endpoint; }
		}

		public void UpdateTopology()
		{
			executer.RegisterForExecution(new UpdateTopologyCommand(master, this));
		}

		public Guid GetTopologyVersion()
		{
			return Topology.Version;
		}

		public bool IsSegmentOwned(int segment)
		{
			return Topology.IsOwnedBy(endpoint, segment);
		}

		public void SendToOwner(int segment,
								IExtendedRequest[] requests)
		{
			var ownerSegment = Topology.GetSegment(segment);
			if (ownerSegment.AssignedEndpoint == null)
				return;
			queueManager.Send(ownerSegment.AssignedEndpoint.Async,
							  new MessagePayload
							  {
								  Data = messageSerializer.Serialize(requests),
							  });
		}

		public void SendToAllOtherBackups(int segment,
										  IExtendedRequest[] requests)
		{
			var ownerSegment = Topology.GetSegment(segment);
			foreach (var otherBackup in ownerSegment
				.PendingBackups
				.Concat(ownerSegment.Backups)
				.Append(ownerSegment.AssignedEndpoint)
				.Where(x => x != endpoint))
			{
				if (otherBackup == null)
					continue;
				queueManager.Send(otherBackup.Async,
								  new MessagePayload
								  {
									  Data = messageSerializer.Serialize(requests),
								  });
			}
		}

		public void DoneReplicatingSegments(ReplicationType type, int[] replicatedSegments)
		{
			master.CaughtUp(endpoint, type, replicatedSegments);
			State = NodeState.Started;
		}

		public void GivingUpOn(ReplicationType type, params int[] segmentsGivingUpOn)
		{
			master.GaveUp(endpoint, type, segmentsGivingUpOn);
		}

		public IDistributedHashTableStorage Storage { get; set; }

		public void Start()
		{
			var assignedSegments = master.Join(endpoint);
			Topology = master.GetTopology();
			var segmentsThatWeAreCatchingUpOnOwnership = assignedSegments
				.Where(x => x.AssignedEndpoint != endpoint)
				.ToArray();
			foreach (var segmentToReplicate in segmentsThatWeAreCatchingUpOnOwnership
				.GroupBy(x => x.AssignedEndpoint))
			{
				executer.RegisterForExecution(
					new OnlineSegmentReplicationCommand(
						segmentToReplicate.Key,
						segmentToReplicate.ToArray(),
						ReplicationType.Ownership,
						this,
						replicationFactory.Create(segmentToReplicate.Key))
					);
			}

			StartPendingBackupsForCurrentNode(Topology);

			var ownsSegments = assignedSegments.Any(x => x.AssignedEndpoint == endpoint);
			State = ownsSegments ?
				NodeState.Started :
				NodeState.Starting;
		}

		private void StartPendingBackupsForCurrentNode(Topology topology)
		{
			if (Topology == null)
				return;
			if (Interlocked.CompareExchange(ref currentlyReplicatingBackups, 0, 0) != 0)
				return;

			foreach (var segmentToReplicate in topology.Segments
				.Where(x => x.AssignedEndpoint != null)
				.Where(x => x.PendingBackups.Contains(endpoint))
				.GroupBy(x => x.AssignedEndpoint))
			{
				var command = new OnlineSegmentReplicationCommand(
					segmentToReplicate.Key,
					segmentToReplicate.ToArray(),
					ReplicationType.Backup,
					this,
					replicationFactory.Create(segmentToReplicate.Key));

				Interlocked.Increment(ref currentlyReplicatingBackups);

				command.Completed += (() => Interlocked.Decrement(ref currentlyReplicatingBackups));

				executer.RegisterForExecution(command);
			}
		}

		protected int currentlyReplicatingBackups;

		public void Dispose()
		{
			executer.Dispose();
		}

		public void SetTopology(Topology topology)
		{
			Topology = topology;
			StartPendingBackupsForCurrentNode(topology);
		}
	}
}