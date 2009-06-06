using System;

namespace Rhino.DistributedHashTable.Internal
{
	public class Topology
	{
		public int Version { get; set; }
		public DateTime Timestamp { get; set; }

		public Segment[] Segments { get; set; }

		public Topology()
		{
		}

		public Topology(Segment[] segments, int version)
		{
			Segments = segments;
			Version = version;
			Timestamp = DateTime.Now;
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