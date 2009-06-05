using System;
using Rhino.DistributedHashTable.Client;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Remote
{
	public class NonPooledDistributedHashTableNodeFactory 
		:	IDistributedHashTableNodeReplicationFactory,
			IDistributedHashTableRemoteNodeFactory
	{
		IDistributedHashTableNodeReplication  IDistributedHashTableNodeReplicationFactory.Create(NodeEndpoint endpoint)
		{
			return new DistributedHashTableStorageClient(endpoint).Replication;
		}

		IDistributedHashTableRemoteNode IDistributedHashTableRemoteNodeFactory.Create(NodeEndpoint endpoint)
		{
			return new DistributedHashTableStorageClient(endpoint);
		}
	}
}