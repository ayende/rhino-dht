using System;
using Rhino.DistributedHashTable.Parameters;

namespace Rhino.DistributedHashTable.Internal
{
	public interface IMessageSerializer
	{
		byte[] Serialize(IExtendedRequest[] requests);
		IExtendedRequest[] Deserialize(byte[] data);
	}
}