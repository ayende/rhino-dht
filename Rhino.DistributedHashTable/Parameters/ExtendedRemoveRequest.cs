using System;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Parameters
{
	public class ExtendedRemoveRequest : RemoveRequest, IExtendedRequest
	{
		public ExtendedRemoveRequest()
		{
			Segment = -1;
		}
		public int Segment { get; set; }
		public bool IsReplicationRequest { get; set; }
		public bool IsLocal { get; set; }
	}
}