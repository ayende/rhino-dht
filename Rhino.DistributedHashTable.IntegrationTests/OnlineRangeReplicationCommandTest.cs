using System;
using System.IO;
using Rhino.DistributedHashTable.Commands;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Remote;
using Rhino.Mocks;
using Xunit;

namespace Rhino.DistributedHashTable.IntegrationTests
{
	public class OnlineSegmentReplicationCommandTest : EsentTestBase
	{
		private readonly OnlineSegmentReplicationCommand command;
		private readonly IDistributedHashTableNode node;
		private readonly IDistributedHashTableNodeReplication replication;
		private readonly NodeEndpoint endpoint;
		private readonly IDistributedHashTableStorage storage;
		private readonly Guid guid = Guid.NewGuid();

		public OnlineSegmentReplicationCommandTest()
		{
			node = MockRepository.GenerateStub<IDistributedHashTableNode>();
			replication = MockRepository.GenerateStub<IDistributedHashTableNodeReplication>();
			endpoint = NodeEndpoint.ForTest(1);
			node.Stub(x => x.Endpoint).Return(NodeEndpoint.ForTest(2));
			storage = MockRepository.GenerateStub<IDistributedHashTableStorage>();
			node.Storage = storage;
			node.Stub(x => x.GetTopologyVersion()).Return(guid);
			command = new OnlineSegmentReplicationCommand(
				endpoint,
				new[] { new Segment { Index = 0 }, new Segment { Index = 1 }, },
                ReplicationType.Ownership, 
				node,
				replication);
		}

		[Fact]
		public void WillAskForAllEmptySegments()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Return(new int[0]);
			replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
				.Return(new ReplicationResult
				{
					PutRequests = new ExtendedPutRequest[0],
					RemoveRequests = new ExtendedRemoveRequest[0],
					Done = true
				});
			var success = command.Execute();
			Assert.True(success);

			replication.AssertWasCalled(x => x.AssignAllEmptySegments(node.Endpoint, ReplicationType.Ownership, new [] { 0, 1 }));
		}

		[Fact]
		public void WillLetNodeKnowAboutAnyEmptySegmentsAssignedToIt()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Return(new []{0});
			replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
				.Return(new ReplicationResult
				{
					PutRequests = new ExtendedPutRequest[0],
					RemoveRequests = new ExtendedRemoveRequest[0],
					Done = true
				});
			var success = command.Execute();
			Assert.True(success);

			node.AssertWasCalled(x => x.DoneReplicatingSegments(ReplicationType.Ownership, new int[] { 0 }));
		}

		[Fact]
		public void WillNotTryToReplicaterangesThatWereEmptyAndAssigned()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Return(new[] { 0 });
			replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
				.Return(new ReplicationResult
				{
					PutRequests = new ExtendedPutRequest[0],
					RemoveRequests = new ExtendedRemoveRequest[0],
					Done = true
				});
			var success = command.Execute();
			Assert.True(success);

			replication.AssertWasNotCalled(x => x.ReplicateNextPage(node.Endpoint, ReplicationType.Ownership, 0));
		}

		[Fact]
		public void WillTryToReplicaterangesThatWereNotEmpty()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Return(new[] { 0 });
			replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
				.Return(new ReplicationResult
				{
					PutRequests = new ExtendedPutRequest[0],
					RemoveRequests = new ExtendedRemoveRequest[0],
					Done = true
				});
			var success = command.Execute();
			Assert.True(success);

			replication.AssertWasCalled(x => x.ReplicateNextPage(node.Endpoint, ReplicationType.Ownership, 1));
		}

		[Fact]
		public void WillPutReturnedItemsIntoStorage()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Return(new[] { 0 });
			var request = new ExtendedPutRequest
			{
				Bytes = new byte[]{1},
				Key = "a",
			};
			replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
				.Return(new ReplicationResult
				{
					PutRequests = new[]{ request, },
					RemoveRequests = new ExtendedRemoveRequest[0],
					Done = true
				});
			var success = command.Execute();
			Assert.True(success);

			storage.AssertWasCalled(x => x.Put(guid, request));
		}

		[Fact]
		public void WillRemoveReturnedRemovalFromStorage()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Return(new[] { 0 });
			var request = new ExtendedRemoveRequest
			{
				Key = "a",
			};
			replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
				.Return(new ReplicationResult
				{
					PutRequests = new ExtendedPutRequest[0],
					RemoveRequests = new[] { request },
					Done = true
				});
			var success = command.Execute();
			Assert.True(success);

			storage.AssertWasCalled(x => x.Remove(guid, request));
		}

		[Fact]
		public void WhenSegmentReplicationFailsWillGiveUpTheSegment()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Return(new int [0]);

			replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
				.Throw(new IOException());
			var success = command.Execute();
			Assert.False(success);

			node.AssertWasCalled(x => x.GivingUpOn(ReplicationType.Ownership, 0));
			node.AssertWasCalled(x => x.GivingUpOn(ReplicationType.Ownership, 1));
		}

		[Fact]
		public void WhenEmptySegmentReplicationFailsWillGiveEverythingUp()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Throw(new IOException());
			var success = command.Execute();
			Assert.False(success);

			node.AssertWasCalled(x => x.GivingUpOn(ReplicationType.Ownership, 0, 1));
		}

		[Fact]
		public void WillRepeatReplicationUntilGetDone()
		{
			replication.Stub(x => x.AssignAllEmptySegments(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int[]>.Is.Anything))
				.Return(new[] { 0 });
			var request = new ExtendedPutRequest
			{
				Bytes = new byte[] { 1 },
				Key = "a",
			};
			for (int i = 0; i < 5; i++)
			{
				replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
					.Repeat.Once()
					.Return(new ReplicationResult
					{
						PutRequests = new[] {request,},
						RemoveRequests = new ExtendedRemoveRequest[0],
						Done = false
					});
			}
			replication.Stub(x => x.ReplicateNextPage(Arg<NodeEndpoint>.Is.Anything, Arg.Is(ReplicationType.Ownership), Arg<int>.Is.Anything))
				.Repeat.Once()
				.Return(new ReplicationResult
				{
					PutRequests = new[] { request, },
					RemoveRequests = new ExtendedRemoveRequest[0],
					Done = true
				});
			var success = command.Execute();
			Assert.True(success);

			storage.AssertWasCalled(x => x.Put(guid, request), o => o.Repeat.Times(6));
		}
	}
}