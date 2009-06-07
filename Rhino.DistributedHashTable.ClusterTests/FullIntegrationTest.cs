using System.IO;

namespace Rhino.DistributedHashTable.ClusterTests
{
	public class FullIntegrationTest : WithDebug
	{
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

			if (Directory.Exists("master.esent"))
				Directory.Delete("master.esent", true);
		}
	}
}