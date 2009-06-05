using System;
using log4net;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Commands
{
	public class UpdateTopologyCommand : ICommand
	{
		private bool continueWorking = true;
		private ILog log = LogManager.GetLogger(typeof (UpdateTopologyCommand));

		private readonly IDistributedHashTableMaster master;
		private readonly DistributedHashTableNode node;
		public UpdateTopologyCommand(IDistributedHashTableMaster master,
		                             DistributedHashTableNode node)
		{
			this.master = master;
			this.node = node;
		}

		public void AbortExecution()
		{
			continueWorking = false;
		}

		public bool Execute()
		{
			if (continueWorking == false)
				return false;
			try
			{
				node.Topology = master.GetTopology();
				log.DebugFormat("Updated toplogy to version {0}", node.Topology.Version);
				return true;
			}
			catch (Exception e)
			{
				log.Warn("Unable to update topology, we are probably running on incosistent topology", e);
				return false;
			}
		}
	}
}