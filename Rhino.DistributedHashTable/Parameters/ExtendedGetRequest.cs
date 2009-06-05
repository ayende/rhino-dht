using System;
using Rhino.PersistentHashTable;

namespace Rhino.DistributedHashTable.Parameters
{
	public class ExtendedGetRequest : GetRequest, IExtendedRequest
	{
		public ExtendedGetRequest()
		{
			Segment = -1;
		}
		public Guid TopologyVersion { get; set; }
		public int Segment { get; set; }
		public bool IsReplicationRequest { get; set; }
	}
}