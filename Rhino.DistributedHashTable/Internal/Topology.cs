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

		public Topology(Segment[] segments, Guid version)
		{
			Segments = segments;
			Version = version;
			Timestamp = DateTime.Now;
		}

		public Topology(Segment[] segments)
			: this(segments, Guid.NewGuid())
		{
		}

		public bool IsOwnedBy(NodeEndpoint endpoint,
		                      int segment)
		{
			return GetSegment(segment).AssignedEndpoint == endpoint;
		}

		public Segment GetSegment(int segment)
		{
			return Segments[segment];
		}
	}
}