using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Remote
{
	public interface IDistributedHashTableRemoteNodeFactory
	{
		IDistributedHashTableRemoteNode Create(NodeEndpoint endpoint);
	}
}