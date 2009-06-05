using System.Collections.Generic;
using System.Linq;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Commands
{
	public class RearsegmentBackups
	{
		private readonly HashSet<NodeEndpoint> endpoints;
		private readonly int fairDistribution;
		private readonly int numberOfBackCopiesToKeep;
		private readonly IEnumerable<Segment> segments;
		private readonly List<BackupCount> currentDistribution;

		public ICollection<BackUpAdded> Changed = new List<BackUpAdded>();

		public RearsegmentBackups(IEnumerable<Segment> segments,
		                        HashSet<NodeEndpoint> endpoints,
		                        int numberOfBackCopiesToKeep)
		{
			this.segments = segments;
			this.endpoints = endpoints;
			this.numberOfBackCopiesToKeep = numberOfBackCopiesToKeep;
			fairDistribution = (segments.Count() * numberOfBackCopiesToKeep) / endpoints.Count() + 1;
			currentDistribution = PrepareDistributions();
		}

		public bool Rearranging()
		{
			foreach (var segmentLoop in segments.Where(x => x.BackupsCount < numberOfBackCopiesToKeep))
			{
				var segment = segmentLoop;
				var endpointsToAddToBackups = currentDistribution
					.Where(
					x => segment.AssignedEndpoint != x.Endpoint &&
					     segment.InProcessOfMovingToEndpoint != x.Endpoint &&
					     x.Count < fairDistribution &&
					     segment.PendingBackups.Contains(x.Endpoint) == false && 
						 segment.Backups.Contains(x.Endpoint) == false
					)
					.Take(numberOfBackCopiesToKeep - segment.PendingBackups.Count);

				foreach (var endpointToAddToBackups in endpointsToAddToBackups)
				{
					Changed.Add(new BackUpAdded
					{
						Endpoint = endpointToAddToBackups.Endpoint,
						Segment = segment
					});
					segment.PendingBackups.Add(endpointToAddToBackups.Endpoint);
					endpointToAddToBackups.Count += 1;
				}
			}
			return Changed.Count > 0;
		}

		private List<BackupCount> PrepareDistributions()
		{
			var distribution = (
			                          	from backup in segments.SelectMany(x => x.PendingBackups)
			                          	group backup by backup
			                          	into g
			                          		select new BackupCount { Endpoint = g.Key, Count = g.Count() }
			                          ).ToList();

			foreach (var endpointThatHasNoBackups in endpoints.Except(distribution.Select(x => x.Endpoint)))
			{
				distribution.Add(new BackupCount
				{
					Count = 0,
					Endpoint = endpointThatHasNoBackups
				});
			}
			return distribution;
		}

		private class BackupCount
		{
			public int Count;
			public NodeEndpoint Endpoint;
		}

		public class BackUpAdded
		{
			public Segment Segment;
			public NodeEndpoint Endpoint;
		}
	}
}