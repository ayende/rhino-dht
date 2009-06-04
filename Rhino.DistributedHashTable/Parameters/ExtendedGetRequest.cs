using System;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Parameters
{
	public class ExtendedGetRequest : GetRequest, IExtendedRequest
	{
		public Guid TopologyVersion { get; set; }
		public int? Segment { get; set; }
		public bool IsReplicationRequest { get; set; }
	}
}