using System;
using Rhino.DistributedHashTable.Parameters;

namespace Rhino.DistributedHashTable.Internal
{
	public interface IDistributedHashTableNode : IDisposable
	{
		void UpdateTopology();

		int GetTopologyVersion();

		bool IsSegmentOwned(int segment);

		void SendToOwner(int segment,
						 params IExtendedRequest[] requests);

		void SendToAllOtherBackups(int segment,
								   params IExtendedRequest[] requests);

		void DoneReplicatingSegments(ReplicationType type, params int[] segments);


		IDistributedHashTableStorage Storage { get; set; }
		NodeEndpoint Endpoint { get; }
		void GivingUpOn(ReplicationType type, params int[] segmentsGivingUpOn);
		void Start();
	}
}