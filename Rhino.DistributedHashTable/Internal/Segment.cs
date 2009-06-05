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
		public HashSet<NodeEndpoint> PendingBackups { get; set; }
		public HashSet<NodeEndpoint> Backups { get; set; }

		public int BackupsCount
		{
			get
			{
				return PendingBackups.Count + Backups.Count;
			}
		}

		public Segment()
		{
			PendingBackups = new HashSet<NodeEndpoint>();
			Backups = new HashSet<NodeEndpoint>();
			Version = Guid.NewGuid();
		}

		public override string ToString()
		{
			return string.Format("Index: {0,10}, AssignedEndpoint: {1}, InProcessOfMovingToEndpoint: {2}", 
				Index, AssignedEndpoint, InProcessOfMovingToEndpoint);
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