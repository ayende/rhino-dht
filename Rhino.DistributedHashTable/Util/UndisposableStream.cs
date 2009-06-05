using System;
using System.IO;

namespace Rhino.DistributedHashTable.Util
{
	public class UndisposableStream : Stream
	{
		private readonly Stream stream;

		public UndisposableStream(Stream stream)
		{
			this.stream = stream;
		}

		public override void Close()
		{
			//ignore close requests
			//stream.Close();
		}

		public override void Flush()
		{
			stream.Flush();
		}

		public override IAsyncResult BeginRead(byte[] buffer,
		                              int offset,
		                              int count,
		                              AsyncCallback callback,
		                              object state)
		{
			return stream.BeginRead(buffer, offset, count, callback, state);
		}

		public override int EndRead(IAsyncResult asyncResult)
		{
			return stream.EndRead(asyncResult);
		}

		public override IAsyncResult BeginWrite(byte[] buffer,
		                               int offset,
		                               int count,
		                               AsyncCallback callback,
		                               object state)
		{
			return stream.BeginWrite(buffer, offset, count, callback, state);
		}

		public override void EndWrite(IAsyncResult asyncResult)
		{
			stream.EndWrite(asyncResult);
		}

		public override long Seek(long offset,
		                 SeekOrigin origin)
		{
			return stream.Seek(offset, origin);
		}

		public override void SetLength(long value)
		{
			stream.SetLength(value);
		}

		public override int Read(byte[] buffer,
		                int offset,
		                int count)
		{
			return stream.Read(buffer, offset, count);
		}

		public override int ReadByte()
		{
			return stream.ReadByte();
		}

		public override void Write(byte[] buffer,
		                  int offset,
		                  int count)
		{
			stream.Write(buffer, offset, count);
		}

		public override void WriteByte(byte value)
		{
			stream.WriteByte(value);
		}

		public override bool CanRead
		{
			get { return stream.CanRead; }
		}

		public override bool CanSeek
		{
			get { return stream.CanSeek; }
		}

		public override bool CanTimeout
		{
			get { return stream.CanTimeout; }
		}

		public override bool CanWrite
		{
			get { return stream.CanWrite; }
		}

		public override long Length
		{
			get { return stream.Length; }
		}

		public override long Position
		{
			get { return stream.Position; }
			set { stream.Position = value; }
		}

		public override int ReadTimeout
		{
			get { return stream.ReadTimeout; }
			set { stream.ReadTimeout = value; }
		}

		public override int WriteTimeout
		{
			get { return stream.WriteTimeout; }
			set { stream.WriteTimeout = value; }
		}
	}
}