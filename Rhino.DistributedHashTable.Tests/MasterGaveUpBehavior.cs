using System.Linq;
using Rhino.DistributedHashTable.Internal;
using Xunit;

namespace Rhino.DistributedHashTable.Tests
{
	public class MasterGaveUpBehavior
	{
		public class OnGaveUp
		{
			private readonly DistributedHashTableMaster master;
			private readonly NodeEndpoint endPoint;

			public OnGaveUp()
			{
				master = new DistributedHashTableMaster();
				master.CaughtUp(NodeEndpoint.ForTest(9),
				                master.Join(NodeEndpoint.ForTest(9)).Select(x=>x.Index).ToArray());
				endPoint = NodeEndpoint.ForTest(5);
			}

			[Fact]
			public void WillRemoveThePendingMoveFromTheSegment()
			{
				var ranges = master.Join(endPoint);

				var range = ranges.First();
				Assert.NotNull(range.InProcessOfMovingToEndpoint);
				
				master.GaveUp(endPoint, range.Index);

				Assert.Null(range.InProcessOfMovingToEndpoint);
			}
		}
	}
}