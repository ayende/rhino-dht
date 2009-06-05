using System;
using Rhino.DistributedHashTable.Commands;

namespace Rhino.DistributedHashTable.Internal
{
	public interface IExecuter : IDisposable
	{
		void RegisterForExecution(ICommand command);
	}
}