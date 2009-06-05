using System;
using Rhino.DistributedHashTable.Hosting;
using Xunit;

namespace Rhino.DistributedHashTable.IntegrationTests
{
	public class WhenNodeIsStarted : FullIntegrationTest, IDisposable
	{
		private readonly DistributedHashTableMasterHost masterHost;
		private readonly Uri masterUri = new Uri("net.tcp://" + Environment.MachineName + ":2200/master");

		public WhenNodeIsStarted()
		{
			masterHost = new DistributedHashTableMasterHost();
			masterHost.Start();
		}


		[Fact(Skip = "not impl yet")]
		public void WillJoinMaster()
		{
			Assert.True(false);
		}

		public void Dispose()
		{
			masterHost.Dispose();
		}
	}
}