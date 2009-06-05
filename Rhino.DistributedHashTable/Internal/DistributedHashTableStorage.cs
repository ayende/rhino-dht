using System;
using System.Collections.Generic;
using System.Transactions;
using log4net;
using Rhino.DistributedHashTable.Exceptions;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Remote;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Internal
{
	public class DistributedHashTableStorage : IDistributedHashTableStorage
	{
		private ILog log = LogManager.GetLogger(typeof(DistributedHashTableStorage));

		public Guid TopologyVersion
		{
			get
			{
				return distributedHashTableNode.GetTopologyVersion();
			}
		}

		private readonly PersistentHashTable.PersistentHashTable hashTable;
		private readonly IDistributedHashTableNode distributedHashTableNode;

		public DistributedHashTableStorage(
			string database,
			IDistributedHashTableNode distributedHashTableNode)
		{
			try
			{
				hashTable = new PersistentHashTable.PersistentHashTable(database);
				hashTable.Initialize();
			}
			catch (Exception)
			{
				try
				{
					hashTable.Dispose();
				}
				catch
				{
					// not much to do if we fail here
				}
				throw;
			}
			this.distributedHashTableNode = distributedHashTableNode;
			distributedHashTableNode.Storage = this;
			Replication = new DistributedHashTableNodeReplication(hashTable);
		}

		public IDistributedHashTableNodeReplication Replication { get; private set; }

		public void Dispose()
		{
			GC.SuppressFinalize(this);
			hashTable.Dispose();
		}

		~DistributedHashTableStorage()
		{
			try
			{
				hashTable.Dispose();
			}
			catch (Exception)
			{
				//not much I can do
			}
		}

		public PutResult[] Put(Guid topologyVersion, params ExtendedPutRequest[] valuesToAdd)
		{
			AssertMatchingTopologyVersion(topologyVersion);
			var results = new List<PutResult>();
			using (var tx = new TransactionScope())
			{
				hashTable.Batch(actions =>
				{
					foreach (var request in valuesToAdd)
					{
						AssertSegmentNotMoved(actions, request.Segment);

						request.Tag = request.Segment;

						if (request.ParentVersions == null)
							throw new ArgumentException("Could not accept request with no ParentVersions");
						if (request.Key.StartsWith(Constants.RhinoDhtStartToken))
							throw new ArgumentException(Constants.RhinoDhtStartToken + " is a reserved key prefix");
						var put = actions.Put(request);
						//prepare the value for replication
						request.ReplicationVersion = put.Version;
						
						results.Add(put);
					}

					HandleReplication(valuesToAdd);

					actions.Commit();
				});

				tx.Complete();
			}
			return results.ToArray();
		}

		private static void AssertSegmentNotMoved(PersistentHashTableActions actions,
		                                 int? segment)
		{
			if(segment < 0)
				throw new ArgumentNullException("segment", "Segment cannot be negative");

			var values = actions.Get(new GetRequest
			{
				Key = Constants.MovedSegment + segment
			});
			if(values.Length>0)
			{
				throw new SeeOtherException("This key belongs to a segment assigned to another node")
				{
					Endpoint = NodeEndpoint.FromBytes(values[0].Data)
				};
			}
		}

		private void AssertMatchingTopologyVersion(Guid topologyVersion)
		{
			if(TopologyVersion != topologyVersion)
			{
				log.InfoFormat("Got request for topology {0} but current local version is {1}",
				               TopologyVersion,
				               topologyVersion);
				throw new TopologyVersionDoesNotMatchException("Topology Version doesn't match, you need to refresh the topology from the master");
			}
		}

		public bool[] Remove(Guid topologyVersion, params ExtendedRemoveRequest[] valuesToRemove)
		{
			AssertMatchingTopologyVersion(topologyVersion);
			var results = new List<bool>();
			using (var tx = new TransactionScope())
			{
				hashTable.Batch(actions =>
				{
					foreach (var request in valuesToRemove)
					{
						AssertSegmentNotMoved(actions, request.Segment);

						if (request.SpecificVersion == null)
							throw new ArgumentException("Could not accept request with no SpecificVersion");

						if (request.Key.StartsWith(Constants.RhinoDhtStartToken))
							throw new ArgumentException(Constants.RhinoDhtStartToken + " is a reserved key prefix");

						foreach (var hash in actions.GetReplicationHashes(request.Key, request.SpecificVersion))
						{
							actions.AddReplicationRemovalInfo(
								request.Key,
								request.SpecificVersion,
								hash
								);
						}

						var remove = actions.Remove(request);
						
						results.Add(remove);
					}

					HandleReplication(valuesToRemove);
					actions.Commit();
				});

				tx.Complete();
			}
			return results.ToArray();
		}

		private void HandleReplication(
			IExtendedRequest[] valuesToSend)
		{
			// if this is the replication request, this is a replicated value,
			// and we don't need to do anything with replication, we only need
			// to check the first value, because all requests have the same purpose
			if (valuesToSend[0].IsReplicationRequest) 
				return;

			if (distributedHashTableNode.IsSegmentOwned(valuesToSend[0].Segment) == false)
			{
				// if this got to us because of fail over, and we need to replicate to the real owner
				// and to any other backups
				distributedHashTableNode.SendToOwner(valuesToSend[0].Segment, valuesToSend);
			}
			distributedHashTableNode.SendToAllOtherBackups(valuesToSend[0].Segment, valuesToSend);
		}

		public Value[][] Get(Guid topologyVersion,params ExtendedGetRequest[] valuesToGet)
		{
			AssertMatchingTopologyVersion(topologyVersion);

			var results = new List<Value[]>();
			hashTable.Batch(actions =>
			{
				foreach (var request in valuesToGet)
				{
					AssertSegmentNotMoved(actions, request.Segment);
					var values = actions.Get(request);
					results.Add(values);
				}

				actions.Commit();
			});
			return results.ToArray();
		}
	}
}