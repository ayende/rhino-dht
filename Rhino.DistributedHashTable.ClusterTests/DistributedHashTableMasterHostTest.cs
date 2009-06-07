using System;
using System.IO;
using System.Linq;
using System.Threading;
using Rhino.DistributedHashTable.Client;
using Rhino.DistributedHashTable.Hosting;
using Xunit;

namespace Rhino.DistributedHashTable.ClusterTests
{
	public class DistributedHashTableMasterHostTest
	{
		public class AfterRestart : FullIntegrationTest , IDisposable
		{
			readonly DistributedHashTableMasterHost host;
			private readonly Uri masterUri = new Uri("rhino.dht://" + Environment.MachineName + ":2200/master");

			public AfterRestart()
			{
				host = new DistributedHashTableMasterHost();

				host.Start();

				using (var storageHost = new DistributedHashTableStorageHost(masterUri))
				{
					storageHost.Start();

					var masterProxy = new DistributedHashTableMasterClient(masterUri);
					while(true)
					{
						var topology = masterProxy.GetTopology();
						if (topology.Segments.All(x => x.AssignedEndpoint != null))
							break;
						Thread.Sleep(100);
					}
				}

				//restart
				host.Dispose();
				host = new DistributedHashTableMasterHost();
				host.Start();
			}

			[Fact]
			public void ShouldRetainPreviousTopology()
			{
				var masterProxy = new DistributedHashTableMasterClient(masterUri);
				var topology = masterProxy.GetTopology();
				Assert.True(topology.Segments.All(x => x.AssignedEndpoint != null));
			}

			public void Dispose()
			{
				host.Dispose();
			}
		}
	}
}