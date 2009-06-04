using System.IO;

namespace Rhino.DistributedHashTable.IntegrationTests
{
	public class FullIntegrationTest
	{
		public FullIntegrationTest()
		{
			if(Directory.Exists("node.queue.esent"))
				Directory.Delete("node.queue.esent", true);

			if (Directory.Exists("node.data.esent"))
				Directory.Delete("node.data.esent", true);

		}
	}
}