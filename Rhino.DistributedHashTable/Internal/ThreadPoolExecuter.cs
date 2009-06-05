using System;
using System.Collections.Generic;
using System.Threading;
using Rhino.DistributedHashTable.Commands;

namespace Rhino.DistributedHashTable.Internal
{
	public class ThreadPoolExecuter : IExecuter
	{
		private volatile bool disposed;
		readonly List<ICommand> executingCommands = new List<ICommand>();

		public void RegisterForExecution(ICommand command)
		{
			if (disposed)
				throw new ObjectDisposedException("ThreadPoolExecuter");
			ThreadPool.QueueUserWorkItem(_ =>
			{
				if (disposed)
					return;
				lock(executingCommands)
					executingCommands.Add(command);
				try
				{
					command.Execute();
				}
				finally 
				{
					lock (executingCommands)
					{
						executingCommands.Remove(command);
					}
				}
			});
		}

		public void Dispose()
		{
			disposed = true;
			lock(executingCommands)
			{
				foreach (var cmd in executingCommands)
				{
					cmd.AbortExecution();
				}
			}
			while(true)
			{
				lock (executingCommands)
				{
					if(executingCommands.Count==0)
						return;
				}
				Thread.Sleep(100);
			}
		}
	}
}