using System.IO;

namespace Rhino.DistributedHashTable.IntegrationTests
{
	public class EsentTestBase
	{
		public EsentTestBase()
		{
			if (Directory.Exists("test.esent"))
				Directory.Delete("test.esent", true);
			
			Directory.CreateDirectory("test.esent");
		}
	}
}