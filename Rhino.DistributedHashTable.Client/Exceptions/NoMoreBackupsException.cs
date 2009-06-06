using System;
using System.Runtime.Serialization;

namespace Rhino.DistributedHashTable.Client.Exceptions
{
	[Serializable]
	public class NoMoreBackupsException : Exception
	{
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public NoMoreBackupsException()
		{
		}

		public NoMoreBackupsException(string message) : base(message)
		{
		}

		public NoMoreBackupsException(string message,
		                  Exception inner) : base(message, inner)
		{
		}

		protected NoMoreBackupsException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}