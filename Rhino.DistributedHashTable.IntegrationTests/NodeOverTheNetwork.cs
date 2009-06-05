using System;
using Rhino.DistributedHashTable.Client;
using Rhino.DistributedHashTable.Hosting;
using Xunit;
using System.Linq;

namespace Rhino.DistributedHashTable.IntegrationTests
{
	public class NodeOverTheNetwork : FullIntegrationTest, IDisposable
	{
		private readonly DistributedHashTableMasterHost masterHost;
		private readonly DistributedHashTableStorageHost storageHost;
		private readonly Uri masterUri = new Uri("rhino.dht://" + Environment.MachineName + ":2200/master");

		public NodeOverTheNetwork()
		{
			masterHost = new DistributedHashTableMasterHost();
			storageHost = new DistributedHashTableStorageHost(masterUri);
			masterHost.Start();
			storageHost.Start();
		}


		[Fact]
		public void WillJoinMaster()
		{
			var masterProxy = new DistributedHashTableMasterClient(masterUri);
			var topology = masterProxy.GetTopology();
			Assert.True(topology.Segments.All(x => x.AssignedEndpoint == storageHost.Endpoint));
		}

		public void Dispose()
		{
			storageHost.Dispose();
			masterHost.Dispose();
		}
	}
}