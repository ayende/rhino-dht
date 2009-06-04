using System;
using Rhino.DistributedHashTable.Parameters;

namespace Rhino.DistributedHashTable.Remote
{
	public class ReplicationResult
	{
		public ExtendedPutRequest[] PutRequests { get; set; }
		public ExtendedRemoveRequest[] RemoveRequests { get; set; }

		public bool Done { get; set; }
	}
}