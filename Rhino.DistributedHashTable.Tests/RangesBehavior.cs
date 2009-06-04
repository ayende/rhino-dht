using System.Net;
using Rhino.DistributedHashTable.Internal;
using Xunit;
using System.Linq;

namespace Rhino.DistributedHashTable.Tests
{
	public class SegmentsBehavior
	{
		public class WhenMasterCreatesSegment
		{
			private readonly DistributedHashTableMaster master = new DistributedHashTableMaster();

			[Fact]
			public void ThereShouldBe8192Segments()
			{
				Assert.Equal(8192, master.Segments.Count());
			}
		}
	}
}