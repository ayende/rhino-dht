using System;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Remote
{
	public interface IDistributedHashTableRemoteNode : IDisposable
	{
		void UpdateTopology();
	}
}