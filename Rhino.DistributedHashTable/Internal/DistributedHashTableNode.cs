using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Transactions;
using log4net;
using Rhino.DistributedHashTable.Commands;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Remote;
using Rhino.DistributedHashTable.Util;
using Rhino.Queues;
using Rhino.Queues.Model;

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
		private readonly Thread backgroundReplication;
		private readonly ILog log = LogManager.GetLogger(typeof(DistributedHashTableNode));

		private int pendingUpdating;

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
			backgroundReplication = new Thread(BackgroundReplication);
		}

		private void BackgroundReplication()
		{
			var errors = new Dictionary<MessageId, int>();
			while (disposed == false)
			{
				int numOfErrors = 0;
				MessageId id = null;
				try
				{
					using (var tx = new TransactionScope())
					{
						var message = queueManager.Receive("replication");
						id = message.Id;
						if (errors.TryGetValue(id, out numOfErrors) == false)
							numOfErrors = 0;
						if (numOfErrors > 5)
						{
							log.ErrorFormat("Could not process message {0}, failed too many times, discarding", id);
							tx.Complete(); //to make sure that the message is consumed
							continue;
						}

						var requests = messageSerializer.Deserialize(message.Data);
						var puts = requests.OfType<ExtendedPutRequest>().ToArray();
						var removes = requests.OfType<ExtendedRemoveRequest>().ToArray();
						log.DebugFormat("Accepting {0} puts and {1} removes for background replication",
							puts.Length, removes.Length);
						if (puts.Length > 0)
							Storage.Put(GetTopologyVersion(), puts);
						if (removes.Length > 0)
							Storage.Remove(GetTopologyVersion(), removes);

						tx.Complete();
					}
					errors.Remove(id);
				}
				catch (ObjectDisposedException)
				{
					log.Info("Queue manager was disposed, quiting background replication");
					return;
				}
				catch (Exception e)
				{
					log.Error("Could not process message, will retry again", e);
					if (id != null)
						errors[id] = numOfErrors + 1;
				}
			}
		}

		public NodeState State { get; set; }

		public Topology Topology { get; private set; }

		public NodeEndpoint Endpoint
		{
			get { return endpoint; }
		}

		public void UpdateTopology()
		{
			if (Interlocked.CompareExchange(ref pendingUpdating, 0, 0) != 0)
				return;
			Interlocked.Increment(ref pendingUpdating);
			var command = new UpdateTopologyCommand(master, this);
			command.Completed += () => Interlocked.Decrement(ref pendingUpdating);
			executer.RegisterForExecution(command);
		}

		public int GetTopologyVersion()
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
			log.DebugFormat("Sending {0} requests to owner {1}",
				requests.Length,
				ownerSegment.AssignedEndpoint.Async);
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
				log.DebugFormat("Sending {0} requests to backup {1}",
								requests.Length,
								ownerSegment.AssignedEndpoint.Async);
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
						replicationFactory)
					);
			}

			StartPendingBackupsForCurrentNode(Topology);

			var ownsSegments = assignedSegments.Any(x => x.AssignedEndpoint == endpoint);

			backgroundReplication.Start();
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
					replicationFactory);

				Interlocked.Increment(ref currentlyReplicatingBackups);

				command.Completed += (() => Interlocked.Decrement(ref currentlyReplicatingBackups));

				executer.RegisterForExecution(command);
			}
		}

		protected int currentlyReplicatingBackups;
		private volatile bool disposed;

		public void Dispose()
		{
			disposed = true;
			executer.Dispose();
			queueManager.Dispose();
			backgroundReplication.Join();
		}

		public void SetTopology(Topology topology)
		{
			RemoveMoveMarkerForSegmentsThatWeAreNoLongerResponsibleFor(topology);
			Topology = topology;
			StartPendingBackupsForCurrentNode(topology);
		}

		private void RemoveMoveMarkerForSegmentsThatWeAreNoLongerResponsibleFor(Topology topology)
		{
			if (Topology == null || topology == null)
				return;

			var removeSegmentsThatWereMovedFromNode =
				(
					from prev in Topology.Segments
					join current in topology.Segments on prev.Index equals current.Index
					where prev.AssignedEndpoint == endpoint && current.AssignedEndpoint != endpoint
					select new ExtendedRemoveRequest
					{
						Key = Constants.MovedSegment + prev.Index,
						Segment = prev.Index,
						IsLocal = true
					}
				).ToArray();

			if (removeSegmentsThatWereMovedFromNode.Length == 0)
				return;
			Storage.Remove(GetTopologyVersion(), removeSegmentsThatWereMovedFromNode);
		}
	}
}