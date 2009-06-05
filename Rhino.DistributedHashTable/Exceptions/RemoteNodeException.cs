using System;
using System.Runtime.Serialization;

namespace Rhino.DistributedHashTable.Exceptions
{
	[Serializable]
	public class RemoteNodeException : Exception
	{
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public RemoteNodeException()
		{
		}

		public RemoteNodeException(string message) : base(message)
		{
		}

		public RemoteNodeException(string message,
		                  Exception inner) : base(message, inner)
		{
		}

		protected RemoteNodeException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}