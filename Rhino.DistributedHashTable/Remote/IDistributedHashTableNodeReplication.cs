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
		/// If there are no more results, the range is assigned to the endpoint
		/// </summary>
		ReplicationResult ReplicateNextPage(NodeEndpoint replicationEndpoint, int range);


		/// <summary>
		/// This method assign all the empty ranges in the specified list to the 
		/// specified endpoint, returning all the ranges that
		/// had no elements in them.
		/// The idea it to speed up range assignment by first getting rid of 
		/// all the empty ranges.
		/// After this methods return, any call to a node with a range that was 
		/// returned from this method will raise a <seealso cref="SeeOtherException"/>
		/// </summary>
		int[] AssignAllEmptySegments(NodeEndpoint replicationEndpoint, int[] ranges);
	}
}