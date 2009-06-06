using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Client.Pooling
{
	public interface IConnectionPool
	{
		IDistributedHashTableStorage Create(NodeEndpoint endpoint);
	}
}