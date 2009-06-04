using System.Net;
using Rhino.DistributedHashTable.Commands;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Remote;
using Rhino.Mocks;
using Rhino.Queues;
using Xunit;

namespace Rhino.DistributedHashTable.Tests
{
	public class NodeStartupBehavior
	{
		public class JoiningMaster
		{
			private readonly DistributedHashTableNode node;
			private readonly IDistributedHashTableMaster master;
			private readonly IExecuter executer;
			private readonly NodeEndpoint endPoint;

			public JoiningMaster()
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
			public void ShouldJoinMasterOnStart()
			{
				node.Start();
				master.AssertWasCalled(x => x.Join(node.Endpoint));
			}

			[Fact]
			public void BeforeStartWouldBeInStateNotStarted()
			{
				Assert.Equal(NodeState.NotStarted, node.State);
			}
		}

		public class HandlingAssignedSegments
		{
			private readonly DistributedHashTableNode node;
			private readonly IDistributedHashTableMaster master;
			private readonly IExecuter executer;
			private readonly NodeEndpoint endPoint;

			public HandlingAssignedSegments()
			{
				master = MockRepository.GenerateStub<IDistributedHashTableMaster>();
				executer = MockRepository.GenerateStub<IExecuter>();
				endPoint = NodeEndpoint.ForTest(1);
				node = new DistributedHashTableNode(master, executer, new BinaryMessageSerializer(), endPoint, MockRepository.GenerateStub<IQueueManager>(),
					MockRepository.GenerateStub<IDistributedHashTableNodeReplicationFactory>());
			}

			[Fact]
			public void WhenSegmentsAssignedToNodeStateWillBeStarted()
			{
				master.Stub(x => x.Join(Arg.Is(endPoint)))
					.Return(new[]
					{
						new Segment {AssignedEndpoint = endPoint},
					});

				node.Start();

				Assert.Equal(NodeState.Started, node.State);
			}

			[Fact]
			public void WhenNoSegmentIsAssignedToNodeStateWillBeStarting()
			{
				master.Stub(x => x.Join(Arg.Is(endPoint)))
					.Return(new[]
					{
						new Segment {AssignedEndpoint = NodeEndpoint.ForTest(9)},
					});

				node.Start();

				Assert.Equal(NodeState.Starting, node.State);
			}

			[Fact]
			public void WillRegisterSegmentsNotAssignedToMeForReplication()
			{
				master.Stub(x => x.Join(Arg.Is(endPoint)))
					.Return(new[]
					{
						new Segment {AssignedEndpoint = NodeEndpoint.ForTest(9)},
					});

				node.Start();

				executer.AssertWasCalled(x => x.RegisterForExecution(Arg<OnlineSegmentReplicationCommand>.Is.TypeOf));
			}
		}
	}
}