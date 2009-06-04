using System;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Parameters
{
	public class ExtendedPutRequest : PutRequest, IExtendedRequest
	{
		public Guid TopologyVersion { get; set; }
		public int? Segment { get; set; }
		public bool IsReplicationRequest { get; set; }
	}
}