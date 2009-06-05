using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Rhino.DistributedHashTable.Internal
{
	public class Topology
	{
		public Guid Version { get; set; }
		public DateTime Timestamp { get; set; }

		public Segment[] Segments { get; set; }

		public Topology()
		{
		}

		public Topology(Segment[] ranges, Guid version)
		{
			Segments = ranges;
			Version = version;
			Timestamp = DateTime.Now;
		}

		public Topology(Segment[] ranges)
			: this(ranges, Guid.NewGuid())
		{
		}

		public bool IsOwnedBy(NodeEndpoint endpoint,
		                      int range)
		{
			return GetSegment(range).AssignedEndpoint == endpoint;
		}

		public Segment GetSegment(int range)
		{
			return Segments[range];
		}
	}
}