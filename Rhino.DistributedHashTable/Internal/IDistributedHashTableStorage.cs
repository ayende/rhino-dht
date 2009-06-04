using System;
using Rhino.DistributedHashTable.Parameters;
using Rhino.DistributedHashTable.Remote;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Internal
{
	public interface IDistributedHashTableStorage : IDisposable
	{
		PutResult[] Put(Guid topologyVersion, params ExtendedPutRequest[] valuesToAdd);
		
		bool[] Remove(Guid topologyVersion, params ExtendedRemoveRequest[] valuesToRemove);
		
		Value[][] Get(Guid topologyVersion, params ExtendedGetRequest[] valuesToGet);

		IDistributedHashTableNodeReplication Replication { get; }
	}
}