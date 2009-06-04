using System.Linq;
using System.Net;
using Rhino.DistributedHashTable.Internal;
using Xunit;

namespace Rhino.DistributedHashTable.Tests
{
	public class MasterCaughtUpBehavior
	{
		public class OnCaughtUp
		{
			private readonly DistributedHashTableMaster master;
			private readonly NodeEndpoint endPoint;

			public OnCaughtUp()
			{
				master = new DistributedHashTableMaster();
				endPoint = NodeEndpoint.ForTest(9);
			}

			[Fact]
			public void WillRaiseTopologyChangedEvent()
			{
				var ranges = master.Join(endPoint);

				bool wasCalled = false;
				master.TopologyChanged += () =>  wasCalled = true;
				master.CaughtUp(endPoint, ranges.First().Index);
				Assert.True(wasCalled);
			}
		}
	}
}