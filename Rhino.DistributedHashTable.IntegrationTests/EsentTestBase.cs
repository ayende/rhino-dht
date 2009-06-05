using System.IO;

namespace Rhino.DistributedHashTable.IntegrationTests.Mini
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