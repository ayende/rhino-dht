using System.IO;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;

namespace Rhino.DistributedHashTable.IntegrationTests
{
	public class FullIntegrationTest
	{
		static FullIntegrationTest()
		{
			BasicConfigurator.Configure(new DebugAppender
			{
				Layout = new SimpleLayout()
			});	
		}

		public FullIntegrationTest()
		{
			if(Directory.Exists("node.queue.esent"))
				Directory.Delete("node.queue.esent", true);

			if (Directory.Exists("node.data.esent"))
				Directory.Delete("node.data.esent", true);

			if (Directory.Exists("nodeB.queue.esent"))
				Directory.Delete("nodeB.queue.esent", true);

			if (Directory.Exists("nodeB.data.esent"))
				Directory.Delete("nodeB.data.esent", true);
		}
	}
}