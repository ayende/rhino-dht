using Rhino.DistributedHashTable.Exceptions;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Remote
{
	public interface IDistributedHashTableNodeReplication
	{
		/// <summary>
		/// retreive a page of put requests for online replication
		/// will mark each returned result with the replication endpoint so it will know
		/// whatever values has been changed (changing the value remove the replication version)
		/// If there are no more results, the segment is assigned to the endpoint
		/// </summary>
		ReplicationResult ReplicateNextPage(NodeEndpoint replicationEndpoint,
		                                    ReplicationType type,
		                                    int segment);


		/// <summary>
		/// This method assign all the empty segments in the specified list to the 
		/// specified endpoint, returning all the segments that
		/// had no elements in them.
		/// The idea it to speed up segment assignment by first getting rid of 
		/// all the empty segments.
		/// After this methods return, any call to a node with a segment that was 
		/// returned from this method will raise a <seealso cref="SeeOtherException"/>
		/// </summary>
		int[] AssignAllEmptySegments(NodeEndpoint replicationEndpoint,
		                             ReplicationType type,
		                             int[] segments);
	}
}