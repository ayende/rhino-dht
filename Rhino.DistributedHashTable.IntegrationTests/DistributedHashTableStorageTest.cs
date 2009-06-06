using System;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.Mocks;
using Rhino.PersistentHashTable;
using Xunit;

namespace Rhino.DistributedHashTable.IntegrationTests
{
	public class DistributedHashTableStorageTest
	{
		public class ReadingValues : EsentTestBase, IDisposable
		{
			private readonly DistributedHashTableStorage distributedHashTableStorage;
			private readonly IDistributedHashTableNode node;
			private readonly int topologyVersion;

			public ReadingValues()
			{
				node = MockRepository.GenerateStub<IDistributedHashTableNode>();
				topologyVersion = 1;
				node.Stub(x => x.GetTopologyVersion()).Return(topologyVersion);
				distributedHashTableStorage = new DistributedHashTableStorage("test.esent",
				                                                              node);

				distributedHashTableStorage.Put(topologyVersion, new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[]{1,2,4},
					Segment = 0,
				});
			}

			[Fact]
			public void WillReturnNullForMissingValue()
			{
				var values = distributedHashTableStorage.Get(topologyVersion, new ExtendedGetRequest
				{
					Key = "missing-value",
					Segment = 0
				});
				Assert.Empty(values[0]);
			}

			[Fact]
			public void WillReturnValueForExistingValue()
			{
				var values = distributedHashTableStorage.Get(topologyVersion, new ExtendedGetRequest
				{
					Key = "test",
					Segment = 0
				});
				Assert.NotEmpty(values[0]);
			}

			public void Dispose()
			{
				distributedHashTableStorage.Dispose();
			}
		}

		public class WritingValues : EsentTestBase, IDisposable
		{
			private readonly DistributedHashTableStorage distributedHashTableStorage;
			private readonly IDistributedHashTableNode node;
			private readonly int topologyVersion;

			public WritingValues()
			{
				node = MockRepository.GenerateStub<IDistributedHashTableNode>();
				topologyVersion = 2;
				node.Stub(x => x.GetTopologyVersion()).Return(topologyVersion);
				distributedHashTableStorage = new DistributedHashTableStorage("test.esent",
				                                                              node);
			}

			[Fact]
			public void WillGetVersionNumberFromPut()
			{
				var results = distributedHashTableStorage.Put(topologyVersion, new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[] { 1, 2, 4 },
					Segment = 0,
				});
				Assert.NotNull(results[0].Version);
			}

			[Fact]
			public void WillReplicateToOtherSystems()
			{
				node.Stub(x => x.IsSegmentOwned(0)).Return(true);
				var request = new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[] { 1, 2, 4 },
					Segment = 0,
				};
				distributedHashTableStorage.Put(topologyVersion, request);
				node.AssertWasCalled(x=>x.SendToAllOtherBackups(0, request));
			}

			[Fact]
			public void WillReplicateToOwnerWhenFailedOverToAnotherNode()
			{
				node.Stub(x => x.IsSegmentOwned(0)).Return(false);
				var request = new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[] { 1, 2, 4 },
					Segment = 0,
				};
				distributedHashTableStorage.Put(topologyVersion, request);
				node.AssertWasCalled(x => x.SendToOwner(0, request));
			}

			[Fact]
			public void WillReplicateToOtherBackupsWhenFailedOverToAnotherNode()
			{
				node.Stub(x => x.IsSegmentOwned(0)).Return(false);
				var request = new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[] { 1, 2, 4 },
					Segment = 0,
				};
				distributedHashTableStorage.Put(topologyVersion, request);
				node.AssertWasCalled(x => x.SendToAllOtherBackups(0, request));
			}

			public void Dispose()
			{
				distributedHashTableStorage.Dispose();
			}
		}

		public class RemovingValues : EsentTestBase, IDisposable
		{
			private readonly DistributedHashTableStorage distributedHashTableStorage;
			private readonly IDistributedHashTableNode node;
			private readonly int topologyVersion;
			private ValueVersion version;

			public RemovingValues()
			{
				node = MockRepository.GenerateStub<IDistributedHashTableNode>();
				topologyVersion = 1;
				node.Stub(x => x.GetTopologyVersion()).Return(topologyVersion);
				distributedHashTableStorage = new DistributedHashTableStorage("test.esent",
				                                                              node);

				var results = distributedHashTableStorage.Put(topologyVersion, new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[] { 1, 2, 4 },
					Segment = 0,
				});
				version = results[0].Version;
			}

			[Fact]
			public void WillConfirmRemovalOfExistingValue()
			{
				var results = distributedHashTableStorage.Remove(topologyVersion, new ExtendedRemoveRequest
				{
					Key = "test",
					SpecificVersion = version, 
					Segment = 0,
				});
				Assert.True(results[0]);
			}

			[Fact]
			public void WillNotConfirmRemovalOfNonExistingValue()
			{
				var results = distributedHashTableStorage.Remove(topologyVersion, new ExtendedRemoveRequest
				{
					Key = "test2",
					SpecificVersion = version, 
					Segment = 0,
				});
				Assert.False(results[0]);
			}

			[Fact]
			public void WillReplicateToOtherSystems()
			{
				node.Stub(x => x.IsSegmentOwned(0)).Return(true);
				var request = new ExtendedRemoveRequest()
				{
					Key = "test",
					SpecificVersion = version, 
					Segment = 0,
				};
				distributedHashTableStorage.Remove(topologyVersion, request);
				node.AssertWasCalled(x => x.SendToAllOtherBackups(0, request));
			}

			[Fact]
			public void WillReplicateToOwnerWhenFailedOverToAnotherNode()
			{
				node.Stub(x => x.IsSegmentOwned(0)).Return(false);
				var request = new ExtendedRemoveRequest()
				{
					Key = "test",
					SpecificVersion = version, 
					Segment = 0,
				};
				distributedHashTableStorage.Remove(topologyVersion, request);
				node.AssertWasCalled(x => x.SendToOwner(0, request));
			}

			[Fact]
			public void WillReplicateToOtherBackupsWhenFailedOverToAnotherNode()
			{
				node.Stub(x => x.IsSegmentOwned(0)).Return(false);
				var request = new ExtendedRemoveRequest()
				{
					Key = "test",
					SpecificVersion = version, 
					Segment = 0,
				};
				distributedHashTableStorage.Remove(topologyVersion, request);
				node.AssertWasCalled(x => x.SendToAllOtherBackups(0, request));
			}

			public void Dispose()
			{
				distributedHashTableStorage.Dispose();
			}
		}
	}
}