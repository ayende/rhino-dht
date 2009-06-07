using System;
using System.Linq;
using Rhino.DistributedHashTable.Internal;
using Xunit;

namespace Rhino.DistributedHashTable.Tests
{
	public class MasterGaveUpBehavior
	{
		public class OnGaveUp : MasterTestBase
		{
			private readonly DistributedHashTableMaster master;
			private readonly NodeEndpoint endPoint;

			public OnGaveUp()
			{
				master = new DistributedHashTableMaster();
				master.CaughtUp(NodeEndpoint.ForTest(9),
								ReplicationType.Ownership, 
								master.Join(NodeEndpoint.ForTest(9)).Select(x => x.Index).ToArray());
				endPoint = NodeEndpoint.ForTest(5);
			}

			[Fact]
			public void WillRemoveThePendingMoveFromTheSegment()
			{
				var ranges = master.Join(endPoint);

				var range = ranges.First();
				Assert.NotNull(range.InProcessOfMovingToEndpoint);

				master.GaveUp(endPoint, ReplicationType.Ownership, range.Index);

				Assert.Null(range.InProcessOfMovingToEndpoint);
			}

			[Fact]
			public void WhenGivingUpOnBackupsWillRemoveFromPendingBackups()
			{
				master.CaughtUp(endPoint, ReplicationType.Ownership,
								master.Join(endPoint).Select(x => x.Index).ToArray());

				var segment = master.Topology.Segments.First(x => x.PendingBackups.Count > 0);

				master.GaveUp(segment.PendingBackups.First(), ReplicationType.Backup,
								segment.Index);

				segment = master.Topology.Segments[segment.Index];

				Assert.Empty(segment.PendingBackups);
				Assert.Equal(0, segment.Backups.Count);
			}

			public override void Dispose()
			{
				master.Dispose();
			}
		}
	}
}