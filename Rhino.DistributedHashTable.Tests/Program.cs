namespace Rhino.DistributedHashTable.Tests
{
	public class Program
	{
		private static void Main(string[] args)
		{
			new MasterJoinBehavior.NewEndpointJoiningMasterWithTwoNodes().SegmentAssignmentsWillNotChange();
		}
	}
}