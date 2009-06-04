using System;

namespace Rhino.DistributedHashTable.Parameters
{
	public interface IExtendedRequest
	{
		Guid TopologyVersion { get; set; }
		int? Segment { get; set; }
		bool IsReplicationRequest { get; set; }
	}
}