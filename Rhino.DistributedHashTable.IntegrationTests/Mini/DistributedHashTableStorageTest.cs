using System;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.Mocks;
using Rhino.PersistentHashTable;
using Xunit;

namespace Rhino.DistributedHashTable.IntegrationTests.Mini
{
	public class DistributedHashTableStorageTest
	{
		public class ReadingValues : EsentTestBase, IDisposable
		{
			private readonly DistributedHashTableStorage distributedHashTableStorage;
			private readonly IDistributedHashTableNode node;
			private readonly Guid guid;

			public ReadingValues()
			{
				node = MockRepository.GenerateStub<IDistributedHashTableNode>();
				guid = Guid.NewGuid();
				node.Stub(x => x.GetTopologyVersion()).Return(guid);
				distributedHashTableStorage = new DistributedHashTableStorage("test.esent",
				                                                              node);

				distributedHashTableStorage.Put(guid, new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[]{1,2,4},
					TopologyVersion = guid,
					Segment = 0,
				});
			}

			[Fact]
			public void WillReturnNullForMissingValue()
			{
				var values = distributedHashTableStorage.Get(guid, new ExtendedGetRequest
				{
					Key = "missing-value",
					Segment = 0
				});
				Assert.Empty(values[0]);
			}

			[Fact]
			public void WillReturnValueForExistingValue()
			{
				var values = distributedHashTableStorage.Get(guid, new ExtendedGetRequest
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
			private readonly Guid guid;

			public WritingValues()
			{
				node = MockRepository.GenerateStub<IDistributedHashTableNode>();
				guid = Guid.NewGuid();
				node.Stub(x => x.GetTopologyVersion()).Return(guid);
				distributedHashTableStorage = new DistributedHashTableStorage("test.esent",
				                                                              node);
			}

			[Fact]
			public void WillGetVersionNumberFromPut()
			{
				var results = distributedHashTableStorage.Put(guid, new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[] { 1, 2, 4 },
					TopologyVersion = guid,
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
					TopologyVersion = guid,
					Segment = 0,
				};
				distributedHashTableStorage.Put(guid, request);
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
					TopologyVersion = guid,
					Segment = 0,
				};
				distributedHashTableStorage.Put(guid, request);
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
					TopologyVersion = guid,
					Segment = 0,
				};
				distributedHashTableStorage.Put(guid, request);
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
			private readonly Guid guid;
			private ValueVersion version;

			public RemovingValues()
			{
				node = MockRepository.GenerateStub<IDistributedHashTableNode>();
				guid = Guid.NewGuid();
				node.Stub(x => x.GetTopologyVersion()).Return(guid);
				distributedHashTableStorage = new DistributedHashTableStorage("test.esent",
				                                                              node);

				var results = distributedHashTableStorage.Put(guid, new ExtendedPutRequest
				{
					Key = "test",
					Bytes = new byte[] { 1, 2, 4 },
					TopologyVersion = guid,
					Segment = 0,
				});
				version = results[0].Version;
			}

			[Fact]
			public void WillConfirmRemovalOfExistingValue()
			{
				var results = distributedHashTableStorage.Remove(guid, new ExtendedRemoveRequest
				{
					Key = "test",
					SpecificVersion = version, 
					TopologyVersion = guid,
					Segment = 0,
				});
				Assert.True(results[0]);
			}

			[Fact]
			public void WillNotConfirmRemovalOfNonExistingValue()
			{
				var results = distributedHashTableStorage.Remove(guid, new ExtendedRemoveRequest
				{
					Key = "test2",
					SpecificVersion = version, 
					TopologyVersion = guid,
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
					TopologyVersion = guid,
					Segment = 0,
				};
				distributedHashTableStorage.Remove(guid, request);
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
					TopologyVersion = guid,
					Segment = 0,
				};
				distributedHashTableStorage.Remove(guid, request);
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
					TopologyVersion = guid,
					Segment = 0,
				};
				distributedHashTableStorage.Remove(guid, request);
				node.AssertWasCalled(x => x.SendToAllOtherBackups(0, request));
			}

			public void Dispose()
			{
				distributedHashTableStorage.Dispose();
			}
		}
	}
}