using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.DistributedHashTable.Client.Exceptions;
using Rhino.DistributedHashTable.Client.Pooling;
using Rhino.DistributedHashTable.Client.Util;
using Rhino.DistributedHashTable.Exceptions;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Parameters;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Client
{
	public class DistributedHashTable : IDistributedHashTable
	{
		private readonly IDistributedHashTableMaster master;
		private readonly IConnectionPool pool;
		private Topology topology;

		public DistributedHashTable(IDistributedHashTableMaster master, IConnectionPool pool)
		{
			this.master = master;
			this.pool = pool;
			topology = master.GetTopology();
		}

		public PutResult[] Put(params PutRequest[] valuesToAdd)
		{
			return PutInternal(valuesToAdd, 0);
		}

		private PutResult[] PutInternal(PutRequest[] valuesToAdd, int backupIndex)
		{
			var results = new PutResult[valuesToAdd.Length];

			var groupedByEndpoint = from req in valuesToAdd
									let er = new
									{
										OriginalIndex = Array.IndexOf(valuesToAdd, req),
										Put = new ExtendedPutRequest
										{
											Bytes = req.Bytes,
											ExpiresAt = req.ExpiresAt,
											IsReadOnly = req.IsReadOnly,
											Key = req.Key,
											OptimisticConcurrency = req.OptimisticConcurrency,
											ParentVersions = req.ParentVersions,
											Segment = GetSegmentFromKey(req.Key),
										}
									}
									group er by GetEndpointByBackupIndex(topology.Segments[er.Put.Segment], backupIndex) into g
									select g;

			foreach (var endpoint in groupedByEndpoint)
			{
				if (endpoint.Key == null)
					throw new NoMoreBackupsException();

				var requests = endpoint.ToArray();
				var putRequests = requests.Select(x => x.Put).ToArray();

				var putsResults = GetPutsResults(endpoint.Key, putRequests, backupIndex);
				for (var i = 0; i < putsResults.Length; i++)
				{
					results[requests[i].OriginalIndex] = putsResults[i];
				}
			}
			return results;
		}

		private static NodeEndpoint GetEndpointByBackupIndex(Segment segment, int backupIndex)
		{
			if (backupIndex == 0)
				return segment.AssignedEndpoint;
			return segment.Backups.ElementAtOrDefault(backupIndex - 1);
		}

		private PutResult[] GetPutsResults(NodeEndpoint endpoint,
												ExtendedPutRequest[] putRequests,
												int backupIndex)
		{
			try
			{
				using (var client = pool.Create(endpoint))
				{
					return client.Put(topology.Version, putRequests);
				}
			}
			catch (SeeOtherException soe)
			{
				return GetPutsResults(soe.Endpoint, putRequests, backupIndex);
			}
			catch (TopologyVersionDoesNotMatchException)
			{
				RefreshTopology();
				return PutInternal(putRequests, backupIndex);
			}
			catch (Exception)
			{
				try
				{
					return PutInternal(putRequests, backupIndex + 1);
				}
				catch (NoMoreBackupsException)
				{
				}
				throw;
			}
		}

		private void RefreshTopology()
		{
			topology = master.GetTopology();
		}

		private static int GetSegmentFromKey(string key)
		{
			// we use @ as a locality separator, that is:
			// foo@5 and bar@5 are both going to reside in the same
			// segment, ensuring locality & one shot calls
			var partialKey = key.Split('@').Last();
			var crc32 = (int)Crc32.Compute(Encoding.Unicode.GetBytes(partialKey));
			return Math.Abs(crc32 % Constants.NumberOfSegments);
		}

		public Value[][] Get(params GetRequest[] valuesToGet)
		{
			return GetInternal(valuesToGet, 0);

		}

		private Value[][] GetInternal(GetRequest[] valuesToGet,
									  int backupIndex)
		{
			var results = new Value[valuesToGet.Length][];

			var groupedByEndpoint = from req in valuesToGet
									let er = new
									{
										OriginalIndex = Array.IndexOf(valuesToGet, req),
										Get = new ExtendedGetRequest
										{
											Key = req.Key,
											SpecifiedVersion = req.SpecifiedVersion,
											Segment = GetSegmentFromKey(req.Key),
										}
									}
									group er by GetEndpointByBackupIndex(topology.Segments[er.Get.Segment], backupIndex) into g
									select g;
			foreach (var endpoint in groupedByEndpoint)
			{
				if (endpoint.Key == null)
					throw new NoMoreBackupsException();

				var requests = endpoint.ToArray();
				var getRequests = requests.Select(x => x.Get).ToArray();

				var putsResults = GetGetsResults(endpoint.Key, getRequests, backupIndex);
				for (var i = 0; i < putsResults.Length; i++)
				{
					results[requests[i].OriginalIndex] = putsResults[i];
				}

			}

			return results;
		}

		private Value[][] GetGetsResults(NodeEndpoint endpoint,
									ExtendedGetRequest[] getRequests,
									int backupIndex)
		{
			try
			{
				using (var client = pool.Create(endpoint))
				{
					return client.Get(topology.Version, getRequests);
				}
			}
			catch (SeeOtherException soe)
			{
				return GetGetsResults(soe.Endpoint, getRequests, backupIndex);
			}
			catch (TopologyVersionDoesNotMatchException)
			{
				RefreshTopology();
				return GetInternal(getRequests, backupIndex);
			}
			catch (Exception)
			{
				try
				{
					return GetInternal(getRequests, backupIndex + 1);
				}
				catch (NoMoreBackupsException)
				{
				}
				throw;
			}
		}

		public bool[] Remove(params RemoveRequest[] valuesToRemove)
		{
			return RemoveInternal(valuesToRemove, 0);
		}

		private bool[] RemoveInternal(RemoveRequest[] valuesToRemove,
									  int backupIndex)
		{
			var results = new bool[valuesToRemove.Length];

			var groupedByEndpoint = from req in valuesToRemove
									let er = new
									{
										OriginalIndex = Array.IndexOf(valuesToRemove, req),
										Remove = new ExtendedRemoveRequest
										{
											Key = req.Key,
											SpecificVersion = req.SpecificVersion,
											Segment = GetSegmentFromKey(req.Key),
										}
									}
									group er by GetEndpointByBackupIndex(topology.Segments[er.Remove.Segment], backupIndex) into g
									select g;

			foreach (var endpoint in groupedByEndpoint)
			{
				if (endpoint.Key == null)
					throw new NoMoreBackupsException();

				var requests = endpoint.ToArray();
				var removeRequests = requests.Select(x => x.Remove).ToArray();

				var removesResults = GetRemovesResults(endpoint.Key, removeRequests, backupIndex);
				for (var i = 0; i < removesResults.Length; i++)
				{
					results[requests[i].OriginalIndex] = removesResults[i];
				}
			}
			return results;
		}

		private bool[] GetRemovesResults(NodeEndpoint endpoint,
									  ExtendedRemoveRequest[] removeRequests,
									  int backupIndex)
		{
			try
			{
				using (var client = pool.Create(endpoint))
				{
					return client.Remove(topology.Version, removeRequests);
				}
			}
			catch (SeeOtherException soe)
			{
				return GetRemovesResults(soe.Endpoint, removeRequests, backupIndex);
			}
			catch (TopologyVersionDoesNotMatchException)
			{
				RefreshTopology();
				return RemoveInternal(removeRequests, backupIndex);
			}
			catch (Exception)
			{
				try
				{
					return RemoveInternal(removeRequests, backupIndex + 1);
				}
				catch (NoMoreBackupsException)
				{
				}
				throw;
			}
		}

		public int[] AddItems(params AddItemRequest[] itemsToAdd)
		{
			throw new NotImplementedException();
		}

		public void RemoteItems(params RemoveItemRequest[] itemsToRemove)
		{
			throw new NotImplementedException();
		}

		public KeyValuePair<int, byte[]>[] GetItems(GetItemsRequest request)
		{
			throw new NotImplementedException();
		}
	}
}