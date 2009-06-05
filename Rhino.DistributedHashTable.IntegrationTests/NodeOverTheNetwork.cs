using System;
using Rhino.DistributedHashTable.Client;
using Rhino.DistributedHashTable.Hosting;
using Rhino.DistributedHashTable.Parameters;
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
		public void NodeHaveJoinedMasterAutomatically()
		{
			var masterProxy = new DistributedHashTableMasterClient(masterUri);
			var topology = masterProxy.GetTopology();
			Assert.True(topology.Segments.All(x => x.AssignedEndpoint == storageHost.Endpoint));
		}

		[Fact]
		public void CanPutItem()
		{
			using (var storageProxy = new DistributedHashTableStorageClient(storageHost.Endpoint))
			{
				var masterProxy = new DistributedHashTableMasterClient(masterUri);
				var topology = masterProxy.GetTopology();
				var results = storageProxy.Put(topology.Version, new ExtendedPutRequest
				{
					Bytes = new byte[] {1, 2, 3, 4},
					Key = "test",
					Segment = 1,
				});
				Assert.False(results[0].ConflictExists);

				var values = storageProxy.Get(topology.Version, new ExtendedGetRequest
				{
					Key = "test",
                    Segment = 1
				});

				Assert.Equal(1, values[0].Length);
				Assert.Equal(new byte[] {1, 2, 3, 4}, values[0][0].Data);
			}
		}

		[Fact]
		public void CanRemoveItem()
		{
			using (var storageProxy = new DistributedHashTableStorageClient(storageHost.Endpoint))
			{
				var masterProxy = new DistributedHashTableMasterClient(masterUri);
				var topology = masterProxy.GetTopology();
				var results = storageProxy.Put(topology.Version, new ExtendedPutRequest
				{
					Bytes = new byte[] { 1, 2, 3, 4 },
					Key = "test",
					Segment = 1,
				});
				Assert.False(results[0].ConflictExists);

				var removed = storageProxy.Remove(topology.Version, new ExtendedRemoveRequest
				{
					Key = "test",
					SpecificVersion = results[0].Version,
					Segment = 1
				});
				Assert.True(removed[0]);

				var values = storageProxy.Get(topology.Version, new ExtendedGetRequest
				{
					Key = "test",
					Segment = 1
				});

				Assert.Equal(0, values[0].Length);
			}
		}

		public void Dispose()
		{
			storageHost.Dispose();
			masterHost.Dispose();
		}
	}
}