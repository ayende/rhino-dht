using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace Rhino.DistributedHashTable.Internal
{
	public class NodeEndpoint
	{
		private byte[] serializedEndpoint;
		private byte[] hash;
		public Uri Sync { get; set; }
		public Uri Async { get; set; }

		public static NodeEndpoint ForTest(int port)
		{
			return new NodeEndpoint
			{
				Async = new Uri("rhino.queues://test:" + port),
				Sync = new Uri("tcp://test:" + port)
			};
		}

		public bool Equals(NodeEndpoint other)
		{
			if (ReferenceEquals(null, other))
				return false;
			if (ReferenceEquals(this, other))
				return true;
			return Equals(other.Sync, Sync) && Equals(other.Async, Async);
		}

		public override string ToString()
		{
			return string.Format("Sync: {0}, Async: {1}", Sync, Async);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj))
				return false;
			if (ReferenceEquals(this, obj))
				return true;
			return Equals((NodeEndpoint)obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return ((Sync != null ? Sync.GetHashCode() : 0) * 397) ^ (Async != null ? Async.GetHashCode() : 0);
			}
		}

		public byte[] ToBytes()
		{
			if (serializedEndpoint == null)
			{
				serializedEndpoint = Encoding.Unicode.GetBytes(Async +
														   Environment.NewLine +
														   Sync);
			}
			return serializedEndpoint;
		}

		public static NodeEndpoint FromBytes(byte[] bytes)
		{
			var text = Encoding.Unicode.GetString(bytes);
			var parts = text.Split(new[] {Environment.NewLine}, StringSplitOptions.RemoveEmptyEntries);
			return new NodeEndpoint
			{
				Async = new Uri(parts[0]),
                Sync = new Uri(parts[1])
			};

		}

		public byte[] GetHash()
		{
			if(hash==null)
			{
				hash = SHA256.Create().ComputeHash(ToBytes());
			}
			return hash;
		}
	}
}