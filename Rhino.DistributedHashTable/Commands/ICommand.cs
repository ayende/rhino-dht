namespace Rhino.DistributedHashTable.Commands
{
	public interface ICommand
	{
		void AbortExecution();
		bool Execute();
	}
}