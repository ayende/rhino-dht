using System.Collections.Generic;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Client
{
	public interface IDistributedHashTable
	{
		PutResult[] Put(params PutRequest[] valuesToAdd);
		Value[][] Get(params GetRequest[] valuesToGet);
		bool[] Remove(params RemoveRequest[] valuesToRemove);

		int[] AddItems(params AddItemRequest[] itemsToAdd);
		void RemoteItems(params RemoveItemRequest[] itemsToRemove);
		KeyValuePair<int, byte[]>[] GetItems(GetItemsRequest request);
	}
}