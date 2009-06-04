using System;
using System.Collections.Generic;
using System.Net;
using System.Linq;

namespace Rhino.DistributedHashTable.Internal
{
	public class Segment
	{
		/// <summary>
		/// This is used to ensure that we can merge appropriately
		/// between the masters
		/// </summary>
		public Guid Version { get; set; }

		public int Index { get; set; }
		public NodeEndpoint AssignedEndpoint { get; set; }
		public NodeEndpoint InProcessOfMovingToEndpoint { get; set; }
		public ICollection<NodeEndpoint> Backups { get; set; }

		public Segment()
		{
			Backups = new HashSet<NodeEndpoint>();
			Version = Guid.NewGuid();
		}

		public override string ToString()
		{
			return string.Format("Index: {0,10}, AssignedEndpoint: {1}, InProcessOfMovingToEndpoint: {2}, Backups: [{3}]", Index, AssignedEndpoint, InProcessOfMovingToEndpoint,
				string.Join(", ", Backups.Select(x=>x.ToString()).ToArray()));
		}

		public bool BelongsTo(NodeEndpoint endpoint)
		{
			return endpoint.Equals(AssignedEndpoint) ||
			       endpoint.Equals(InProcessOfMovingToEndpoint);
		}

		public bool Match(Segment other)
		{
			return other.Version == Version;
		}
	}
}