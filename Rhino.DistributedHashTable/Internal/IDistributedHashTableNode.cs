using System;
using Rhino.DistributedHashTable.Parameters;

namespace Rhino.DistributedHashTable.Internal
{
	public interface IDistributedHashTableNode : IDisposable
	{
		void UpdateTopology();

		Guid GetTopologyVersion();

		bool IsSegmentOwned(int range);

		void SendToOwner(int range,
						 params IExtendedRequest[] requests);

		void SendToAllOtherBackups(int range,
								   params IExtendedRequest[] requests);

		void DoneReplicatingSegments(params int[] ranges);


		IDistributedHashTableStorage Storage { get; set; }
		NodeEndpoint Endpoint { get; }
		void GivingUpOn(params int[] rangesGivingUpOn);
		void Start();
	}
}