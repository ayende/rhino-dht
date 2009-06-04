using System;
using System.Runtime.Serialization;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Exceptions
{
	[Serializable]
	public class SeeOtherException : Exception
	{
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public Internal.NodeEndpoint Endpoint { get; set; }

		public SeeOtherException()
		{
		}

		public SeeOtherException(string message) : base(message)
		{
		}

		public SeeOtherException(string message,
		                  Exception inner) : base(message, inner)
		{
		}

		protected SeeOtherException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}