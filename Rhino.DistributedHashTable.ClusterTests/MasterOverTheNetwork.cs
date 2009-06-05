using System;
using Rhino.DistributedHashTable.Client;
using Rhino.DistributedHashTable.Hosting;
using Rhino.DistributedHashTable.Internal;
using Xunit;
using System.Linq;

namespace Rhino.DistributedHashTable.ClusterTests
{
	public class MasterOverTheNetwork
	{
		public class CanCommunicateWithMasterUsingClientProxy : FullIntegrationTest, IDisposable
		{
			private readonly DistributedHashTableMasterHost masterHost;
			private readonly DistributedHashTableMasterClient masterProxy;

			public CanCommunicateWithMasterUsingClientProxy()
			{
				masterHost = new DistributedHashTableMasterHost();

				masterHost.Start();

				masterProxy = new DistributedHashTableMasterClient(new Uri("rhino.dht://localhost:2200"));
			}

			[Fact]
			public void CanGetTopologyWhenThereAreNoNodes()
			{
				var topology = masterProxy.GetTopology();
				Assert.NotNull(topology);
				Assert.NotEqual(Guid.Empty, topology.Version);
				Assert.NotEqual(DateTime.MinValue, topology.Timestamp);
				Assert.Equal(8192, topology.Segments.Length);
				Assert.True(topology.Segments.All(x => x.AssignedEndpoint == null));
			}

			[Fact]
			public void CanJoinToMaster()
			{
				var endpoint = new NodeEndpoint
				{
					Async = new Uri("rhino.queues://localhost:2202/replication"),
					Sync = new Uri("rhino.dht://localhost:2201")
				};
				var segments = masterProxy.Join(endpoint);
				Assert.Equal(8192, segments.Length);
				Assert.True(segments.All(x => x.AssignedEndpoint.Equals(endpoint)));
			}

			[Fact]
			public void CanCatchUpOnSegment()
			{
				var endpoint = new NodeEndpoint
				{
					Async = new Uri("rhino.queues://localhost:2202/replication"),
					Sync = new Uri("rhino.dht://localhost:2201")
				};
				masterProxy.Join(new NodeEndpoint
				{
					Async = new Uri("rhino.queues://other:2202/replication"),
					Sync = new Uri("rhino.dht://other:2201")
				});
				var segments = masterProxy.Join(endpoint);

				masterProxy.CaughtUp(endpoint, ReplicationType.Ownership, segments[0].Index, segments[1].Index);
				
				var topology = masterProxy.GetTopology();
				Assert.Equal(endpoint, topology.Segments[segments[0].Index].AssignedEndpoint);
				Assert.Equal(endpoint, topology.Segments[segments[1].Index].AssignedEndpoint);

				Assert.NotEqual(endpoint, topology.Segments[segments[2].Index].AssignedEndpoint);
				Assert.Equal(endpoint, topology.Segments[segments[2].Index].InProcessOfMovingToEndpoint);
			}

			[Fact]
			public void CanGiveUpOnSegment()
			{
				var existingEndpoint = new NodeEndpoint
				{
					Async = new Uri("rhino.queues://other:2202/replication"),
					Sync = new Uri("rhino.dht://other:2201")
				};
				masterProxy.Join(existingEndpoint);
				
				var newEndpoint = new NodeEndpoint
				{
					Async = new Uri("rhino.queues://localhost:2202/replication"),
					Sync = new Uri("rhino.dht://localhost:2201")
				};
				
				var segments = masterProxy.Join(newEndpoint);

				masterProxy.GaveUp(newEndpoint, ReplicationType.Ownership, segments[0].Index, segments[1].Index);

				var topology = masterProxy.GetTopology();
				Assert.Equal(existingEndpoint,topology.Segments[segments[0].Index].AssignedEndpoint);
				Assert.Equal(existingEndpoint, topology.Segments[segments[1].Index].AssignedEndpoint);

				Assert.Null(topology.Segments[segments[0].Index].InProcessOfMovingToEndpoint);
				Assert.Null(topology.Segments[segments[1].Index].InProcessOfMovingToEndpoint);
			}

			public void Dispose()
			{
				masterHost.Dispose();
			}
		}

	}
}