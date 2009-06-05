using System;
using System.Linq;
using System.Threading;
using Rhino.DistributedHashTable.Client;
using Rhino.DistributedHashTable.Hosting;
using Rhino.DistributedHashTable.Parameters;
using Xunit;

namespace Rhino.DistributedHashTable.ClusterTests
{
	public class ClusterTests
	{
		public class JoiningToCluster : FullIntegrationTest, IDisposable
		{
			private readonly DistributedHashTableMasterHost masterHost;
			private readonly Uri masterUri = new Uri("rhino.dht://" + Environment.MachineName + ":2200/master");
			private readonly DistributedHashTableStorageHost storageHostA;
			private readonly DistributedHashTableStorageHost storageHostB;

			public JoiningToCluster()
			{
				masterHost = new DistributedHashTableMasterHost();
				storageHostA = new DistributedHashTableStorageHost(masterUri);
				storageHostB = new DistributedHashTableStorageHost(masterUri, "nodeB", 2203);
				masterHost.Start();
				storageHostA.Start();
			}

			public void Dispose()
			{
				storageHostB.Dispose();
				storageHostA.Dispose();
				masterHost.Dispose();
			}

			[Fact]
			public void TwoNodesCanJoinToTheCluster()
			{
				storageHostB.Start();

				var countOfSegmentsInA = 0;
				var countOfSegmentsInB = 0;
				var masterProxy = new DistributedHashTableMasterClient(masterUri);

				for (var i = 0; i < 50; i++)
				{
					var topology = masterProxy.GetTopology();
					var results = topology.Segments.GroupBy(x => x.AssignedEndpoint)
						.Select(x => new { x.Key, Count = x.Count() })
						.ToDictionary(x => x.Key, x => x.Count);

					results.TryGetValue(storageHostA.Endpoint, out countOfSegmentsInA);
					results.TryGetValue(storageHostB.Endpoint, out countOfSegmentsInB);
					if (countOfSegmentsInA == countOfSegmentsInB &&
					    countOfSegmentsInB == 4096)
						return;
					Thread.Sleep(500);
				}
				Assert.True(false,
				            "Should have found two nodes sharing responsability for the geometry: " + countOfSegmentsInA + " - " +
				            countOfSegmentsInB);
			}

			[Fact]
			public void WillReplicateValuesToSecondJoin()
			{
				var masterProxy = new DistributedHashTableMasterClient(masterUri);
				using (var nodeA = new DistributedHashTableStorageClient(storageHostA.Endpoint))
				{
					var topology = masterProxy.GetTopology();
					nodeA.Put(topology.Version, new ExtendedPutRequest
					{
						Bytes = new byte[] {2, 2, 0, 0},
						Key = "abc",
						Segment = 1
					});
				}

				storageHostB.Start(); //will replicate all odd segments here now

				for (var i = 0; i < 500; i++)
				{
					var topology = masterProxy.GetTopology();
					if(topology.Segments[1].AssignedEndpoint == 
					   storageHostB.Endpoint)
						break;
					Thread.Sleep(500);
				}
				using (var nodeB = new DistributedHashTableStorageClient(storageHostB.Endpoint))
				{
					var topology = masterProxy.GetTopology();
					var values = nodeB.Get(topology.Version, new ExtendedGetRequest
					{
						Key = "abc",
						Segment = 1
					});
					Assert.Equal(new byte[] {2, 2, 0, 0}, values[0][0].Data);
				}
			}
		}
	}
}