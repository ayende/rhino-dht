using System.Net;
using Rhino.DistributedHashTable.Commands;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Remote;
using Rhino.Mocks;
using Rhino.Queues;
using Xunit;

namespace Rhino.DistributedHashTable.Tests
{
	public class NodeReplicationBehavior
	{
		public class WhenFinishedReplicatingSegment
		{
			private readonly DistributedHashTableNode node;
			private readonly IDistributedHashTableMaster master;
			private readonly IExecuter executer;
			private readonly NodeEndpoint endPoint;

			public WhenFinishedReplicatingSegment()
			{
				master = MockRepository.GenerateStub<IDistributedHashTableMaster>();
				executer = MockRepository.GenerateStub<IExecuter>();
				endPoint = NodeEndpoint.ForTest(1);
				master.Stub(x => x.Join(Arg.Is(endPoint)))
					.Return(new Segment[0]);
				node = new DistributedHashTableNode(master, executer, new BinaryMessageSerializer(), endPoint, MockRepository.GenerateStub<IQueueManager>(),
					MockRepository.GenerateStub<IDistributedHashTableNodeReplicationFactory>());
			}

			[Fact]
			public void StateWillBeStarted()
			{
				node.DoneReplicatingSegments(ReplicationType.Ownership, new[] { 0 });
				Assert.Equal(NodeState.Started, node.State);
			}

			[Fact]
			public void WillLetMasterKnowItCaughtUp()
			{
				node.DoneReplicatingSegments(ReplicationType.Ownership, new[] { 0 });
				master.AssertWasCalled(x => x.CaughtUp(node.Endpoint, ReplicationType.Ownership, 0));
			}
		}

		public class WhenReplicatingRequestToOwner
		{
			private readonly DistributedHashTableNode node;
			private readonly IDistributedHashTableMaster master;
			private readonly IExecuter executer;
			private readonly NodeEndpoint endPoint;
			private readonly IQueueManager queueManager;
			private readonly Topology topology;
			private static NodeEndpoint backup1;
			private static NodeEndpoint backup2;

			public WhenReplicatingRequestToOwner()
			{
				master = MockRepository.GenerateStub<IDistributedHashTableMaster>();
				endPoint = NodeEndpoint.ForTest(1);
				backup1 = NodeEndpoint.ForTest(2);
				backup2 = NodeEndpoint.ForTest(3);
				topology = new Topology(new[]
				{
					new Segment
					{
						Index = 0,
						AssignedEndpoint = endPoint,
						PendingBackups = 
						{
							backup1,
							backup2,
						}
					},
					new Segment
					{
						Index = 1,
						AssignedEndpoint = backup1,
						PendingBackups = 
						{
							endPoint,
							backup2,
						}
					},
				});
				master.Stub(x => x.GetTopology()).Return(topology);
				executer = MockRepository.GenerateStub<IExecuter>();
				master.Stub(x => x.Join(Arg.Is(endPoint)))
					.Return(new Segment[0]);
				queueManager = MockRepository.GenerateStub<IQueueManager>();

				node = new DistributedHashTableNode(master, executer, new BinaryMessageSerializer(), endPoint,
					queueManager, MockRepository.GenerateStub<IDistributedHashTableNodeReplicationFactory>());
				node.Start();
			}

			[Fact]
			public void WhenSendingToOwnerWillSendItToOwnerUri()
			{
				var request = new ExtendedPutRequest();
				node.SendToOwner(0, new[] { request });
				queueManager.Send(endPoint.Async, Arg<MessagePayload>.Is.TypeOf);
			}

			[Fact]
			public void WhenSendingToOtherBackupsFromOwner()
			{
				var request = new ExtendedPutRequest();
				node.SendToAllOtherBackups(0, new[] { request });
				queueManager.Send(backup1.Async, Arg<MessagePayload>.Is.TypeOf);
				queueManager.Send(backup2.Async, Arg<MessagePayload>.Is.TypeOf);
			}

			[Fact]
			public void WhenSendingToOtherBackupsFromBackupNode()
			{
				var request = new ExtendedPutRequest();
				node.SendToAllOtherBackups(1, new[] { request });
				queueManager.Send(endPoint.Async, Arg<MessagePayload>.Is.TypeOf);
				queueManager.Send(backup2.Async, Arg<MessagePayload>.Is.TypeOf);
			}
		}

		public class WhenTopologyIsUpdated
		{
			private readonly DistributedHashTableNode node;
			private readonly IDistributedHashTableMaster master;
			private readonly IExecuter executer;
			private readonly NodeEndpoint endPoint;

			public WhenTopologyIsUpdated()
			{
				master = MockRepository.GenerateStub<IDistributedHashTableMaster>();
				executer = MockRepository.GenerateStub<IExecuter>();
				endPoint = NodeEndpoint.ForTest(1);
				master.Stub(x => x.Join(Arg.Is(endPoint)))
					.Return(new Segment[0]);
				node = new DistributedHashTableNode(master, executer, new BinaryMessageSerializer(), endPoint, MockRepository.GenerateStub<IQueueManager>(),
					MockRepository.GenerateStub<IDistributedHashTableNodeReplicationFactory>());
			}

			[Fact]
			public void TopologyContainsPendingBackupsForCurrentNodeWillStartsBackupReplication()
			{
				node.SetTopology(new Topology(new[]
				{
					new Segment
					{
                        AssignedEndpoint = NodeEndpoint.ForTest(91),
						PendingBackups = {endPoint}
					},
				}));

				executer.AssertWasCalled(x=>x.RegisterForExecution(Arg<OnlineSegmentReplicationCommand>.Is.TypeOf));
			}

			[Fact]
			public void WillNotStartReplicationIfCurrentlyReplicatingBackups()
			{
				node.SetTopology(new Topology(new[]
				{
					new Segment
					{
						AssignedEndpoint = NodeEndpoint.ForTest(91),
						PendingBackups = {endPoint}
					},
				}));

				node.SetTopology(new Topology(new[]
				{
					new Segment
					{
						AssignedEndpoint = NodeEndpoint.ForTest(91),
						PendingBackups = {endPoint}
					},
				}));

				executer.AssertWasCalled(
					x => x.RegisterForExecution(Arg<OnlineSegmentReplicationCommand>.Is.TypeOf),
					o=>o.Repeat.Once());
			}

			[Fact]
			public void AfterBackupsCompleteWillStartReplicationAgain()
			{
				OnlineSegmentReplicationCommand command = null;
				executer.Stub(x => x.RegisterForExecution(Arg<OnlineSegmentReplicationCommand>.Is.TypeOf))
					.WhenCalled(invocation => command = (OnlineSegmentReplicationCommand) invocation.Arguments[0]);

				node.SetTopology(new Topology(new[]
				{
					new Segment
					{
						AssignedEndpoint = NodeEndpoint.ForTest(91),
						PendingBackups = {endPoint}
					},
				}));

				command.RaiseCompleted();

				node.SetTopology(new Topology(new[]
				{
					new Segment
					{
						AssignedEndpoint = NodeEndpoint.ForTest(91),
						PendingBackups = {endPoint}
					},
				}));

				executer.AssertWasCalled(
					x => x.RegisterForExecution(Arg<OnlineSegmentReplicationCommand>.Is.TypeOf),
					o => o.Repeat.Twice());
			}
		}
	}
}