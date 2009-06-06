using System;
using System.Collections.Generic;
using System.Net;
using System.Linq;

namespace Rhino.DistributedHashTable.Internal
{
	public class Segment
	{
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
		}

		public override string ToString()
		{
			Uri assigned = null;
			Uri inProcess = null;
			if(AssignedEndpoint!=null)
				assigned = AssignedEndpoint.Sync;
			if (InProcessOfMovingToEndpoint != null)
				inProcess = InProcessOfMovingToEndpoint.Sync;
			return string.Format("Index: {0,4}, Assigned: {1}, Tentative: {2}, Backups: {3}, Tentative Backups: {4}",
			                     Index, assigned, inProcess, Backups.Count, PendingBackups.Count);
		}

		public bool BelongsTo(NodeEndpoint endpoint)
		{
			return endpoint.Equals(AssignedEndpoint) ||
				   endpoint.Equals(InProcessOfMovingToEndpoint);
		}

		public bool Match(Segment other)
		{
			return other.Index == Index;
		}
	}
}