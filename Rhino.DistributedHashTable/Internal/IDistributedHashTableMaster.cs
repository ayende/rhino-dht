namespace Rhino.DistributedHashTable.Internal
{
	public interface IDistributedHashTableMaster
	{
		/// <summary>
		/// This method is called when a new node wants to join the cluster.
		/// The result is the segments that this node is responsible for, if it is an
		/// existing one, or the list of segments that it needs to pull from the currently 
		/// assigned node.
		/// Note:
		/// that if it needs to pull date from the currently assigned node, it will
		/// also need to call the <see cref="DistributedHashTableMaster.CaughtUp"/> method to let the master know 
		/// that it is done and that the topology changed.
		/// </summary>
		Segment[] Join(NodeEndpoint endpoint);

		/// <summary>
		/// Notify the master that the endpoint has caught up on all the specified segments
		/// </summary>
		void CaughtUp(NodeEndpoint endpoint, ReplicationType type, params int[] caughtUpSegments);

		Topology GetTopology();

		/// <summary>
		/// Notify the master that the endpoint will not replicate this segment
		/// </summary>
		void GaveUp(NodeEndpoint endpoint,
		            ReplicationType type,
		            params int[] segmentsGivingUpOn);


	}
}