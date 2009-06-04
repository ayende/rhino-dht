using System;
using System.Threading;
using Rhino.DistributedHashTable.Commands;

namespace Rhino.DistributedHashTable.Internal
{
	public class ThreadPoolExecuter : IExecuter
	{
		public void RegisterForExecution(ICommand command)
		{
			ThreadPool.QueueUserWorkItem(_ => command.Execute());
		}
	}
}