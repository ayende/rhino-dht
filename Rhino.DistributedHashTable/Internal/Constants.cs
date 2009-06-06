namespace Rhino.DistributedHashTable.Internal
{
	public static class Constants
	{
		public const int NumberOfSegments = 8192;
		public const string RhinoDhtStartToken = "@rdht://";
		public const string MovedSegment = RhinoDhtStartToken + "Segment/Moved/";
	}
}