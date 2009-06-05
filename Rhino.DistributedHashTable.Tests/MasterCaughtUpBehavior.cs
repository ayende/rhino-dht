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
				master.CaughtUp(endPoint, ReplicationType.Ownership, ranges.First().Index);
				Assert.True(wasCalled);
			}

			[Fact]
			public void WhenCatchingUpOnBackupsWillMoveFromPendingBackupsToBackups()
			{
				master.CaughtUp(endPoint, ReplicationType.Ownership,
				                master.Join(endPoint).Select(x => x.Index).ToArray());

				var anotherEndpoint = NodeEndpoint.ForTest(54);
				master.CaughtUp(anotherEndpoint, ReplicationType.Ownership,
				                master.Join(anotherEndpoint).Select(x => x.Index).ToArray());

				var segment = master.Topology.Segments.First(x => x.PendingBackups.Count > 0);

				master.CaughtUp(segment.PendingBackups.First(), ReplicationType.Backup,
				                segment.Index);

				segment = master.Topology.Segments[segment.Index];

				Assert.Empty(segment.PendingBackups);
				Assert.Equal(1, segment.Backups.Count);
			}
		}
	}
}