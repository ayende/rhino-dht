using System.Linq;
using System.Net;
using Rhino.DistributedHashTable.Internal;
using Xunit;

namespace Rhino.DistributedHashTable.Tests
{
	public class MasterJoinBehavior
	{
		public class OnEmptyMaster
		{
			private readonly NodeEndpoint endPoint = NodeEndpoint.ForTest(0);
			private readonly DistributedHashTableMaster master = new DistributedHashTableMaster();

			[Fact]
			public void AllSegmentsAreDirectlyAllocatedToEndpoint()
			{
				master.Join(endPoint);

				Assert.True(master.Segments.All(x => x.AssignedEndpoint == endPoint));
			}
		}

		public class JoiningTwice
		{
			private readonly NodeEndpoint endPoint = NodeEndpoint.ForTest(0);
			private readonly DistributedHashTableMaster master = new DistributedHashTableMaster();
			
			[Fact]
			public void IsNoOpp()
			{
				var ranges1 = master.Join(endPoint);
				var ranges2 = master.Join(endPoint);

				Assert.Equal(ranges1, ranges2);
			}
		}

		public class NewEndpointJoiningNonEmptyMaster
		{
			private readonly NodeEndpoint endPoint = NodeEndpoint.ForTest(0);
			private readonly DistributedHashTableMaster master = new DistributedHashTableMaster();
			private readonly NodeEndpoint newEndpoint = NodeEndpoint.ForTest(1);

			public NewEndpointJoiningNonEmptyMaster()
			{
				master.Join(endPoint);
				master.Join(newEndpoint);
			}

			[Fact]
			public void SegmentAssignmentsWillNotChange()
			{
				Assert.True(master.Segments.All(x => x.AssignedEndpoint == endPoint));
			}

			[Fact]
			public void WillNotChangeTotalNumberOfSegments()
			{
				Assert.Equal(8192, master.Segments.Count());
			}

			[Fact]
			public void HalfOfTheSegmentsWillBeInTheProcessOfAssigningToNewEndpoint()
			{
				Assert.Equal(master.Segments.Count()/2, 
					master.Segments.Count(x => x.InProcessOfMovingToEndpoint == newEndpoint));				
			}
		}

		public class NewEndpointJoiningMasterWhenAnotherJoinIsInTheProcessOfJoining
		{
			private readonly NodeEndpoint endPoint = NodeEndpoint.ForTest(0);
			private readonly DistributedHashTableMaster master = new DistributedHashTableMaster();
			private readonly NodeEndpoint newEndpoint = NodeEndpoint.ForTest(1);
			private readonly NodeEndpoint anotherNodeInTheProcessOfJoining = NodeEndpoint.ForTest(2);

			public NewEndpointJoiningMasterWhenAnotherJoinIsInTheProcessOfJoining()
			{
				master.Join(endPoint);
				master.Join(anotherNodeInTheProcessOfJoining);
				master.Join(newEndpoint);
			}

			[Fact]
			public void SegmentAssignmentsWillNotChange()
			{
				Assert.True(master.Segments.All(x => x.AssignedEndpoint == endPoint));
			}

			[Fact]
			public void ThirdOfTheAvailableSegmentsWillBeReservedForTheNewNode()
			{
				Assert.Equal(1365, master.Segments.Count(x => x.InProcessOfMovingToEndpoint == newEndpoint));
			}

			[Fact]
			public void WillNotAffectJoiningOfExistingNode()
			{
				Assert.Equal(4096, master.Segments.Count(x => x.InProcessOfMovingToEndpoint == anotherNodeInTheProcessOfJoining));
			}
		}

		public class NewEndpointJoiningMasterWithTwoNodes
		{
			private readonly NodeEndpoint endPoint = NodeEndpoint.ForTest(0);
			private readonly DistributedHashTableMaster master = new DistributedHashTableMaster();
			private readonly NodeEndpoint newEndpoint = NodeEndpoint.ForTest(1);
			private readonly NodeEndpoint anotherNodeInTheMaster = NodeEndpoint.ForTest(2);

			public NewEndpointJoiningMasterWithTwoNodes()
			{
				master.Join(endPoint);
				var ranges = master.Join(anotherNodeInTheMaster);
				master.CaughtUp(anotherNodeInTheMaster, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
				master.Join(newEndpoint);
			}

			[Fact]
			public void WillNotChangeTotalNumberOfSegments()
			{
				Assert.Equal(8192, master.Segments.Count());
			}

			[Fact]
			public void SegmentAssignmentsWillNotChange()
			{
				Assert.False(master.Segments.Any(x => x.AssignedEndpoint == newEndpoint));
			}

			[Fact]
			public void ThirdOfTheAvailableSegmentsWillBeAssignedToNewNode()
			{
				Assert.Equal(2730, master.Segments.Count(x => x.InProcessOfMovingToEndpoint == newEndpoint));
			}
		}
	}
}