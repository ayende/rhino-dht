using System;
using Rhino.DistributedHashTable.Parameters;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Internal
{
	public interface IDistributedHashTableStorage : IDisposable
	{
		PutResult[] Put(int topologyVersion, params ExtendedPutRequest[] valuesToAdd);

		bool[] Remove(int topologyVersion, params ExtendedRemoveRequest[] valuesToRemove);

		Value[][] Get(int topologyVersion, params ExtendedGetRequest[] valuesToGet);
	}
}