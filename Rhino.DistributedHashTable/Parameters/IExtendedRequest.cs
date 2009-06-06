using System;

namespace Rhino.DistributedHashTable.Parameters
{
	public interface IExtendedRequest
	{
		int Segment { get; set; }
		bool IsReplicationRequest { get; set; }
		bool IsLocal { get; set; }
	}
}