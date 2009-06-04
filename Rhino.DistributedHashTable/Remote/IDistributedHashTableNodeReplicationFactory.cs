using System;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Remote
{
	public interface IDistributedHashTableNodeReplicationFactory
	{
		IDistributedHashTableNodeReplication Create(NodeEndpoint endpoint);
	}
}