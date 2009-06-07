using log4net.Appender;
using log4net.Config;
using log4net.Layout;

namespace Rhino.DistributedHashTable.ClusterTests
{
	public class WithDebug
	{
		static WithDebug()
		{
			BasicConfigurator.Configure(new DebugAppender
			{
				Layout = new SimpleLayout()
			});	
		}

	}
}