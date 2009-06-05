using System;
using log4net;
using Rhino.DistributedHashTable.Internal;
using Rhino.DistributedHashTable.Remote;

namespace Rhino.DistributedHashTable.Commands
{
	public class NotifyEndpointsAboutTopologyChange : ICommand
	{
		private readonly IDistributedHashTableRemoteNodeFactory distributedHashTableRemoteNodeFactory;
		private readonly ILog log = LogManager.GetLogger(typeof(NotifyEndpointsAboutTopologyChange));
		private bool continueWorking = true;
		private readonly NodeEndpoint[] endpoints;
		private IDistributedHashTableRemoteNode currentNode;

		public NotifyEndpointsAboutTopologyChange(NodeEndpoint[] endpoints,
											 IDistributedHashTableRemoteNodeFactory distributedHashTableRemoteNodeFactory)
		{
			this.endpoints = endpoints;
			this.distributedHashTableRemoteNodeFactory = distributedHashTableRemoteNodeFactory;
		}

		public void AbortExecution()
		{
			continueWorking = false;
			if (currentNode != null)
				currentNode.Dispose();
		}

		public bool Execute()
		{
			foreach (var endpoint in endpoints)
			{
				if (continueWorking == false)
					return false;

				try
				{
					log.DebugFormat("Notifying {0} about topology", endpoint.Sync);
					using (var node = distributedHashTableRemoteNodeFactory.Create(endpoint))
					{
						currentNode = node;
						node.UpdateTopology();
					}
					log.DebugFormat("Notified {0} about topology", endpoint.Sync);
				}
				catch (Exception e)
				{
					log.Warn(string.Format("Failed to send topology change to {0}", endpoint.Sync), e);
				}
			}
			return true;
		}
	}
}