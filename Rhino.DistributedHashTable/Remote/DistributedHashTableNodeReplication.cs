using System;
using System.Collections.Generic;
using System.Text;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Remote
{
	public class DistributedHashTableNodeReplication : IDistributedHashTableNodeReplication
	{
		private readonly PersistentHashTable.PersistentHashTable hashTable;

		public DistributedHashTableNodeReplication(PersistentHashTable.PersistentHashTable hashTable)
		{
			this.hashTable = hashTable;
		}

		public ReplicationResult ReplicateNextPage(NodeEndpoint replicationEndpoint,
												   int range)
		{
			var putRequests = new List<ExtendedPutRequest>();
			var removalRequests = new List<ExtendedRemoveRequest>();
			bool done = false;
			hashTable.Batch(actions =>
			{
				foreach (var getRequest in actions.GetKeysForTag(range))
				{
					var alreadyReplicated = actions.HasReplicationInfo(getRequest.Key,
														  getRequest.SpecifiedVersion,
														  replicationEndpoint.GetHash());
					if (alreadyReplicated)
						continue;

					var values = actions.Get(getRequest);
					if (values.Length != 1)
						continue;
					var value = values[0];

					putRequests.Add(new ExtendedPutRequest
					{
						Bytes = value.Data,
						ExpiresAt = value.ExpiresAt,
						IsReadOnly = value.ReadOnly,
						IsReplicationRequest = true,
						Key = value.Key,
						ParentVersions = value.ParentVersions,
						ReplicationTimeStamp = value.Timestamp,
						ReplicationVersion = value.Version,
						Tag = value.Tag,
						Segment = value.Tag.Value,
					});

					actions.AddReplicationInfo(getRequest.Key,
											   getRequest.SpecifiedVersion,
											   replicationEndpoint.GetHash());

					if (putRequests.Count >= 100)
						break;
				}

				foreach (var request in actions.ConsumeRemovalReplicationInfo(replicationEndpoint.GetHash()))
				{
					removalRequests.Add(new ExtendedRemoveRequest
					{
						Key = request.Key,
                        SpecificVersion = request.SpecificVersion
					});
					if (removalRequests.Count >= 100)
						break;
				}

				done = putRequests.Count == 0 && removalRequests.Count == 0;
				if (done)
				{
					MarkSegmentAsAssignedToEndpoint(actions, replicationEndpoint, range);
				}

				actions.Commit();
			});

			return new ReplicationResult
			{
				PutRequests = putRequests.ToArray(),
				RemoveRequests = removalRequests.ToArray(),
				Done = done
			};
		}

		public int[] AssignAllEmptySegments(NodeEndpoint replicationEndpoint,
										   int[] ranges)
		{
			var reservedSegments = new List<int>();

			hashTable.Batch(actions =>
			{
				foreach (var range in ranges)
				{
					if (actions.HasTag((int)range))
						continue;
					if (MarkSegmentAsAssignedToEndpoint(actions, replicationEndpoint, range) == false)
						continue;
					reservedSegments.Add(range);
				}
				actions.Commit();
			});

			return reservedSegments.ToArray();
		}

		private static bool MarkSegmentAsAssignedToEndpoint(PersistentHashTableActions actions,
												   NodeEndpoint endpoint,
												   int range)
		{
			var result = actions.Put(new PutRequest
			{
				Key = Constants.MovedSegment + range,
				OptimisticConcurrency = true,
				Bytes = endpoint.ToBytes(),
			});
			return result.ConflictExists == false;
		}
	}
}