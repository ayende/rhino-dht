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
		private readonly ILog log = LogManager.GetLogger(typeof(DistributedHashTableStorage));

		public int TopologyVersion
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

		public PutResult[] Put(int topologyVersion, params ExtendedPutRequest[] valuesToAdd)
		{
			AssertMatchingTopologyVersion(topologyVersion);
			var results = new List<PutResult>();
			using (var tx = new TransactionScope())
			{
				hashTable.Batch(actions =>
				{
					foreach (var request in valuesToAdd)
					{
						if (request.IsReplicationRequest == false && request.IsLocal == false)
							AssertSegmentNotMoved(actions, request.Segment);

						request.Tag = request.Segment;

						if (request.ParentVersions == null)
							throw new ArgumentException("Could not accept request with no ParentVersions");
						if (request.Key.StartsWith(Constants.RhinoDhtStartToken) && request.IsLocal == false)
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

		private void AssertMatchingTopologyVersion(int topologyVersionFromClient)
		{
			if (TopologyVersion == topologyVersionFromClient)
				return;
			
			//client thinks that the version is newer
			if(topologyVersionFromClient > TopologyVersion)
			{
				log.InfoFormat("Got request for topology {0} but current local version is {1}, forcing topology update, request will still fail",
							   topologyVersionFromClient, 
							   TopologyVersion);
				distributedHashTableNode.UpdateTopology();
			}
			else
			{
				log.InfoFormat("Got request for topology {0} but current local version is {1}",
							   topologyVersionFromClient,
							   TopologyVersion);
			}
			throw new TopologyVersionDoesNotMatchException(
				"Topology Version doesn't match, you need to refresh the topology from the master");
		}

		public bool[] Remove(int topologyVersion, params ExtendedRemoveRequest[] valuesToRemove)
		{
			AssertMatchingTopologyVersion(topologyVersion);
			var results = new List<bool>();
			using (var tx = new TransactionScope())
			{
				hashTable.Batch(actions =>
				{
					foreach (var request in valuesToRemove)
					{
						if (request.IsReplicationRequest == false && request.IsLocal == false) 
							AssertSegmentNotMoved(actions, request.Segment);

						if (request.Key.StartsWith(Constants.RhinoDhtStartToken) && request.IsLocal == false)
							throw new ArgumentException(Constants.RhinoDhtStartToken + " is a reserved key prefix");

						if(request.SpecificVersion!=null)
						{
							RegisterRemovalForReplication(request, actions, request.SpecificVersion);
						}
						else// all versions
						{
							var values = actions.Get(new GetRequest
							{
								Key = request.Key
							});
							foreach (var value in values)
							{
								RegisterRemovalForReplication(request, actions, value.Version);
							}
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

		private static void RegisterRemovalForReplication(RemoveRequest request,
		                                           PersistentHashTableActions actions,
		                                           ValueVersion version)
		{
			foreach (var hash in actions.GetReplicationHashes(request.Key, version))
			{
				actions.AddReplicationRemovalInfo(
					request.Key,
					version,
					hash
					);
			}
		}

		private void HandleReplication(
			IExtendedRequest[] valuesToSend)
		{
			// if this is the replication request, this is a replicated value,
			// and we don't need to do anything with replication, we only need
			// to check the first value, because all requests have the same purpose
			if (valuesToSend[0].IsReplicationRequest || valuesToSend[0].IsLocal) 
				return;

			if (distributedHashTableNode.IsSegmentOwned(valuesToSend[0].Segment) == false)
			{
				// if this got to us because of fail over, and we need to replicate to the real owner
				// and to any other backups
				distributedHashTableNode.SendToOwner(valuesToSend[0].Segment, valuesToSend);
			}
			distributedHashTableNode.SendToAllOtherBackups(valuesToSend[0].Segment, valuesToSend);
		}

		public Value[][] Get(int topologyVersion, params ExtendedGetRequest[] valuesToGet)
		{
			AssertMatchingTopologyVersion(topologyVersion);

			var results = new List<Value[]>();
			hashTable.Batch(actions =>
			{
				foreach (var request in valuesToGet)
				{
					if (request.IsReplicationRequest == false && request.IsLocal == false) 
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