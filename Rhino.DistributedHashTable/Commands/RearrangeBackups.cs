using System.Collections.Generic;
using System.Linq;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Commands
{
	public class RearrangeBackups
	{
		private readonly IEnumerable<NodeEndpoint> endPoints;
		private readonly int fairDistribution;
		private readonly int numberOfBackCopiesToKeep;
		private readonly IEnumerable<Segment> ranges;
		private readonly List<BackupCount> currentDistribution;

		public ICollection<BackUpAdded> Changed = new List<BackUpAdded>();

		public RearrangeBackups(IEnumerable<Internal.Segment> ranges,
		                        IEnumerable<NodeEndpoint> endPoints,
		                        int numberOfBackCopiesToKeep)
		{
			this.ranges = ranges;
			this.endPoints = endPoints;
			this.numberOfBackCopiesToKeep = numberOfBackCopiesToKeep;
			fairDistribution = (ranges.Count() * numberOfBackCopiesToKeep) / endPoints.Count() + 1;
			currentDistribution = PrepareDistributions();
		}

		public bool Rearranging()
		{
			foreach (var range in ranges.Where(x => x.Backups.Count < numberOfBackCopiesToKeep))
			{
				var endPointsToAddToBackups = currentDistribution
					.Where(
					x => range.AssignedEndpoint != x.Endpoint &&
					     range.InProcessOfMovingToEndpoint != x.Endpoint &&
					     x.Count < fairDistribution &&
					     range.Backups.Contains(x.Endpoint) == false
					)
					.Take(numberOfBackCopiesToKeep - range.Backups.Count);

				foreach (var endPointToAddToBackups in endPointsToAddToBackups)
				{
					Changed.Add(new BackUpAdded
					{
						Endpoint = endPointToAddToBackups.Endpoint,
						Segment = range
					});
					range.Backups.Add(endPointToAddToBackups.Endpoint);
					endPointToAddToBackups.Count += 1;
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

			foreach (var endPointThatHasNoBackups in endPoints.Except(currentDistribution.Select(x => x.Endpoint)))
			{
				currentDistribution.Add(new BackupCount
				{
					Count = 0,
					Endpoint = endPointThatHasNoBackups
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