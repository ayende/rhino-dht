using Rhino.DistributedHashTable.Commands;

namespace Rhino.DistributedHashTable.Internal
{
	public interface IExecuter
	{
		void RegisterForExecution(ICommand command);
	}
}