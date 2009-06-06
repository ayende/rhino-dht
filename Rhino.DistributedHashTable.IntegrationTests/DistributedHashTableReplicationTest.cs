using System;
using System.Linq;
using Rhino.DistributedHashTable.Exceptions;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Remote;
using Rhino.Mocks;
using Rhino.PersistentHashTable;
using Xunit;

namespace Rhino.DistributedHashTable.IntegrationTests
{
	public class DistributedHashTableReplicationTest
	{
		public class WhenThereAreNoKeysInTable: EsentTestBase, IDisposable
		{
			private readonly DistributedHashTableStorage distributedHashTableStorage;
			private readonly IDistributedHashTableNode node;
			private readonly int topologyVersion;
			private readonly IDistributedHashTableNodeReplication replication;

			public WhenThereAreNoKeysInTable()
			{
				node = MockRepository.GenerateStub<IDistributedHashTableNode>();
				topologyVersion = 7;
				node.Stub(x => x.GetTopologyVersion()).Return(topologyVersion);
				distributedHashTableStorage = new DistributedHashTableStorage("test.esent",
				                                                              node);
				replication = distributedHashTableStorage.Replication;
			}

			[Fact]
			public void WillAssignAllSegmentsWeAskForImmediately()
			{
				var ranges = Enumerable.Range(0, 500).ToArray();
				var assignedSegments = replication.AssignAllEmptySegments(NodeEndpoint.ForTest(1), ReplicationType.Ownership, ranges);
				Assert.Equal(ranges, assignedSegments);
			}

			
			[Fact]
			public void PuttingKeyInSegmentAsisgnedElsewhereShouldThrow()
			{
				var ranges = Enumerable.Range(0, 500).ToArray();
				replication.AssignAllEmptySegments(NodeEndpoint.ForTest(1), ReplicationType.Ownership, ranges);
				var exception = Assert.Throws<SeeOtherException>(() => node.Storage.Put(topologyVersion, new ExtendedPutRequest()
				{
					Key = "test",
					Segment = 0,
					Bytes = new byte[] {1},
				}));
				Assert.Equal(NodeEndpoint.ForTest(1), exception.Endpoint);
			}

			[Fact]
			public void GettingKeyInSegmentAsisgnedElsewhereShouldThrow()
			{
				var ranges = Enumerable.Range(0, 500).ToArray();
				replication.AssignAllEmptySegments(NodeEndpoint.ForTest(1), ReplicationType.Ownership, ranges);
				var exception = Assert.Throws<SeeOtherException>(() => node.Storage.Get(topologyVersion, new ExtendedGetRequest()
				{
					Key = "test",
					Segment = 0,
				}));
				Assert.Equal(NodeEndpoint.ForTest(1), exception.Endpoint);
			}


			[Fact]
			public void RemovingKeyInSegmentAsisgnedElsewhereShouldThrow()
			{
				var ranges = Enumerable.Range(0, 500).ToArray();
				replication.AssignAllEmptySegments(NodeEndpoint.ForTest(1), ReplicationType.Ownership, ranges);
				var exception = Assert.Throws<SeeOtherException>(() => node.Storage.Remove(topologyVersion, new ExtendedRemoveRequest
				{
					Key = "test",
					Segment = 0,
				}));
				Assert.Equal(NodeEndpoint.ForTest(1), exception.Endpoint);
			}

			public void Dispose()
			{
				distributedHashTableStorage.Dispose();	
			}
		}

		public class WhenThereAreKeysInTable : EsentTestBase, IDisposable
		{
			private readonly DistributedHashTableStorage distributedHashTableStorage;
			private readonly IDistributedHashTableNode node;
			private readonly int topologyVersion;
			private readonly IDistributedHashTableNodeReplication replication;
			private readonly PutResult putResult;

			public WhenThereAreKeysInTable()
			{
				node = MockRepository.GenerateStub<IDistributedHashTableNode>();
				topologyVersion = 9;
				node.Stub(x => x.GetTopologyVersion()).Return(topologyVersion);
				distributedHashTableStorage = new DistributedHashTableStorage("test.esent",
				                                                              node);
				replication = distributedHashTableStorage.Replication;
				putResult = distributedHashTableStorage.Put(topologyVersion, new ExtendedPutRequest
				{
					Tag = 0,
					Bytes = new byte[] {1},
					Key = "test",
					Segment = 0
				})[0];
			}

			[Fact]
			public void WillSkipSegmentsThatHasItemsInThem()
			{
				var ranges = Enumerable.Range(0, 500).ToArray();
				var assignedSegments = replication.AssignAllEmptySegments(NodeEndpoint.ForTest(1), ReplicationType.Ownership, ranges);
				Assert.Equal(ranges.Skip(1).ToArray(), assignedSegments);
			}
			
			[Fact]
			public void WillGiveExistingKeysFromSegment()
			{
				var result = replication.ReplicateNextPage(NodeEndpoint.ForTest(1), ReplicationType.Ownership, 0);
				Assert.Equal(1, result.PutRequests.Length);
				Assert.Equal(new byte[]{1}, result.PutRequests[0].Bytes);
			}

			[Fact]
			public void WillRememberKeysSentDuringPreviousReplication()
			{
				var result = replication.ReplicateNextPage(NodeEndpoint.ForTest(1), ReplicationType.Ownership, 0);
				Assert.Equal(1, result.PutRequests.Length);

				result = replication.ReplicateNextPage(NodeEndpoint.ForTest(1), ReplicationType.Ownership, 0);
				Assert.Equal(0, result.PutRequests.Length);
			}

			[Fact]
			public void WhenItemIsRemovedWillResultInRemovalRequestOnNextReplicationPage()
			{
				var result = replication.ReplicateNextPage(NodeEndpoint.ForTest(1), ReplicationType.Ownership, 0);
				Assert.Equal(1, result.PutRequests.Length);
				node.Storage.Remove(node.GetTopologyVersion(), new ExtendedRemoveRequest
				{
					Key = "test",
					SpecificVersion = putResult.Version,
					Segment = 0,
				});

				result = replication.ReplicateNextPage(NodeEndpoint.ForTest(1), ReplicationType.Ownership, 0);
				Assert.Equal(1, result.RemoveRequests.Length);
			}

			[Fact]
			public void WhenDoneGettingAllKeysWillAssignSegmentToEndpoint()
			{
				var result = replication.ReplicateNextPage(NodeEndpoint.ForTest(1), ReplicationType.Ownership, 0);
				Assert.Equal(1, result.PutRequests.Length);

				result = replication.ReplicateNextPage(NodeEndpoint.ForTest(1), ReplicationType.Ownership, 0);
				Assert.Equal(0, result.PutRequests.Length);

				var exception = Assert.Throws<SeeOtherException>(() => node.Storage.Remove(topologyVersion, new ExtendedRemoveRequest
				{
					Key = "test",
					Segment = 0,
				}));
				Assert.Equal(NodeEndpoint.ForTest(1), exception.Endpoint);
			}

			public void Dispose()
			{
				distributedHashTableStorage.Dispose();
			}
		}
	}
}