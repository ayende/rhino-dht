using System;
using System.IO;
using System.Runtime.Serialization;
using Rhino.DistributedHashTable.Parameters;

namespace Rhino.DistributedHashTable.Internal
{
	public class BinaryMessageSerializer : IMessageSerializer
	{
		public byte[] Serialize(IExtendedRequest[] requests)
		{
			using (var stream = new MemoryStream())
			{
				new NetDataContractSerializer().Serialize(stream,requests);
				return stream.ToArray();
			}
		}

		public IExtendedRequest[] Deserialize(byte[] data)
		{
			using (var stream = new MemoryStream(data))
			{
				return (IExtendedRequest[])new NetDataContractSerializer().Deserialize(stream);
			}
		}
	}
}