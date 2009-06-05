using System.Collections.Generic;
using System.Linq;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Commands
{
	public class RearrangeBackups
	{
		private readonly HashSet<NodeEndpoint> endpoints;
		private readonly int fairDistribution;
		private readonly int numberOfBackCopiesToKeep;
		private readonly IEnumerable<Segment> ranges;
		private readonly List<BackupCount> currentDistribution;

		public ICollection<BackUpAdded> Changed = new List<BackUpAdded>();

		public RearrangeBackups(IEnumerable<Segment> ranges,
		                        HashSet<NodeEndpoint> endpoints,
		                        int numberOfBackCopiesToKeep)
		{
			this.ranges = ranges;
			this.endpoints = endpoints;
			this.numberOfBackCopiesToKeep = numberOfBackCopiesToKeep;
			fairDistribution = (ranges.Count() * numberOfBackCopiesToKeep) / endpoints.Count() + 1;
			currentDistribution = PrepareDistributions();
		}

		public bool Rearranging()
		{
			foreach (var range in ranges.Where(x => x.Backups.Count < numberOfBackCopiesToKeep))
			{
				var endpointsToAddToBackups = currentDistribution
					.Where(
					x => range.AssignedEndpoint != x.Endpoint &&
					     range.InProcessOfMovingToEndpoint != x.Endpoint &&
					     x.Count < fairDistribution &&
					     range.Backups.Contains(x.Endpoint) == false
					)
					.Take(numberOfBackCopiesToKeep - range.Backups.Count);

				foreach (var endpointToAddToBackups in endpointsToAddToBackups)
				{
					Changed.Add(new BackUpAdded
					{
						Endpoint = endpointToAddToBackups.Endpoint,
						Segment = range
					});
					range.Backups.Add(endpointToAddToBackups.Endpoint);
					endpointToAddToBackups.Count += 1;
				}
			}
			return Changed.Count > 0;
		}

		private List<BackupCount> PrepareDistributions()
		{
			var currentDistribution = (
			                          	from backup in ranges.SelectMany(x => x.Backups)
			                          	group backup by backup
			                          	into g
			                          		select new BackupCount { Endpoint = g.Key, Count = g.Count() }
			                          ).ToList();

			foreach (var endpointThatHasNoBackups in endpoints.Except(currentDistribution.Select(x => x.Endpoint)))
			{
				currentDistribution.Add(new BackupCount
				{
					Count = 0,
					Endpoint = endpointThatHasNoBackups
				});
			}
			return currentDistribution;
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