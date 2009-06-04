using System.Collections.Generic;

namespace Rhino.DistributedHashTable
{
	using PersistentHashTable;

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