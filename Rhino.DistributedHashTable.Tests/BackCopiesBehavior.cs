using System.Net;
using Rhino.DistributedHashTable.Internal;
using Xunit;
using System.Linq;

namespace Rhino.DistributedHashTable.Tests
{
	public class BackCopiesBehavior
	{
		public class OnEmptyMaster
		{
			private readonly DistributedHashTableMaster master;
			private readonly NodeEndpoint endPoint;

			public OnEmptyMaster()
			{
				master = new DistributedHashTableMaster();
				endPoint = NodeEndpoint.ForTest(9);
			}

			[Fact]
			public void AddingNewNodeResultInAllSegmentsHavingNoBackupCopies()
			{
				master.Join(endPoint);

				Assert.True(master.Segments.All(x => x.PendingBackups.Count == 0));
			}
		}

		public class OnMasterWithOneExistingNode
		{
			private readonly DistributedHashTableMaster master;
			private readonly NodeEndpoint endPoint;

			public OnMasterWithOneExistingNode()
			{
				master = new DistributedHashTableMaster();
				endPoint = NodeEndpoint.ForTest(9);

				var existingEndpoint = NodeEndpoint.ForTest(3);
				var ranges = master.Join(existingEndpoint);
				master.CaughtUp(existingEndpoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
			}

			[Fact]
			public void AddingNewNodeResultInAllSegmentsHavingSingleBackupCopy()
			{
				var ranges = master.Join(endPoint);
				master.CaughtUp(endPoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
				Assert.True(master.Segments.All(x => x.PendingBackups.Count == 1));
			}

			[Fact]
			public void AddingNewNodeWillRaiseBackupChangedEvent()
			{
				bool wasChanged = false;
				master.BackupChanged += (state, point, range) => wasChanged = true;
				var ranges = master.Join(endPoint);
				master.CaughtUp(endPoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());

				Assert.True(wasChanged);
			}
		}

		public class OnMasterWithTwoNodes
		{
			private readonly DistributedHashTableMaster master;
			private readonly NodeEndpoint endPoint;

			public OnMasterWithTwoNodes()
			{
				master = new DistributedHashTableMaster();
				endPoint = NodeEndpoint.ForTest(9);

				var existingEndpoint = NodeEndpoint.ForTest(3);
				var ranges = master.Join(existingEndpoint);
				master.CaughtUp(existingEndpoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
				var anotherPoint = NodeEndpoint.ForTest(10);
				ranges = master.Join(anotherPoint);
				master.CaughtUp(anotherPoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
			}

			[Fact]
			public void AddingNewNodeResultInAllSegmentsHavingTwoBackupCopy()
			{
				var ranges = master.Join(endPoint);
				master.CaughtUp(endPoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
				Assert.True(master.Segments.All(x => x.PendingBackups.Count == 2));
			}
		}

		public class OnMasterWithThreeNodes
		{
			private readonly DistributedHashTableMaster master;
			private readonly NodeEndpoint endPoint;

			public OnMasterWithThreeNodes()
			{
				master = new DistributedHashTableMaster();
				endPoint = NodeEndpoint.ForTest(9);

				var existingEndpoint = NodeEndpoint.ForTest(3);
				var ranges = master.Join(existingEndpoint);
				master.CaughtUp(existingEndpoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
				var anotherPoint = NodeEndpoint.ForTest(10);
				ranges = master.Join(anotherPoint);
				master.CaughtUp(anotherPoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
				ranges = master.Join(endPoint);
				master.CaughtUp(endPoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
			}

			[Fact]
			public void AddingNewNodeResultInAllSegmentsHavingAtLeastTwoBackupCopy()
			{
				var yetAnotherEndPoint = NodeEndpoint.ForTest(7);
				var ranges = master.Join(yetAnotherEndPoint);
				master.CaughtUp(yetAnotherEndPoint, ReplicationType.Ownership, ranges.Select(x => x.Index).ToArray());
				Assert.True(master.Segments.All(x => x.PendingBackups.Count >= 2));
			}
		}
	}
}