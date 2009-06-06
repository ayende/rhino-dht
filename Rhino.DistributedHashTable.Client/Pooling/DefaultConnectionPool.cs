using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using log4net;
using Rhino.DistributedHashTable.Internal;

namespace Rhino.DistributedHashTable.Client.Pooling
{
	public class DefaultConnectionPool : IConnectionPool
	{
		private static readonly ILog log = LogManager.GetLogger(typeof (DefaultConnectionPool));
		readonly object locker = new object();

		private readonly Dictionary<NodeEndpoint, LinkedList<PooledDistributedHashTableStorageClientConnection>> pooledConnections =
			new Dictionary<NodeEndpoint, LinkedList<PooledDistributedHashTableStorageClientConnection>>();

		public IDistributedHashTableStorage Create(NodeEndpoint endpoint)
		{
			PooledDistributedHashTableStorageClientConnection storage = null;
			lock (locker)
			{
				LinkedList<PooledDistributedHashTableStorageClientConnection> value;
				if (pooledConnections.TryGetValue(endpoint, out value) && value.Count > 0)
				{
					storage = value.First.Value;
					value.RemoveFirst();
				}
			}
			if (storage != null)
			{
				if (storage.Connected == false)
				{
					log.DebugFormat("Found unconnected connection in the pool for {0}", endpoint.Sync);
					try
					{
						storage.Dispose();
					}
					catch (Exception e)
					{
						log.Debug("Error when disposing unconnected connection in the pool", e);
					}
				}
				else
				{
					return storage;
				}
			}
			log.DebugFormat("Creating new connection in the pool to {0}", endpoint.Sync);
			return new PooledDistributedHashTableStorageClientConnection(this, endpoint);
		}

		private void PutMeBack(PooledDistributedHashTableStorageClientConnection connection)
		{
			lock (locker)
			{
				LinkedList<PooledDistributedHashTableStorageClientConnection> value;
				if (pooledConnections.TryGetValue(connection.Endpoint, out value) == false)
				{
					pooledConnections[connection.Endpoint] = value = new LinkedList<PooledDistributedHashTableStorageClientConnection>();
				}
				value.AddLast(connection);
			}
			log.DebugFormat("Put connection for {0} back in the pool", connection.Endpoint.Sync);
		}

		class PooledDistributedHashTableStorageClientConnection : DistributedHashTableStorageClient
		{
			private readonly DefaultConnectionPool pool;

			public PooledDistributedHashTableStorageClientConnection(
				DefaultConnectionPool pool,
				NodeEndpoint endpoint) : base(endpoint)
			{
				this.pool = pool;
			}

			public bool Connected
			{
				get { return client.Connected; }
			}

			public override void Dispose()
			{
				if(Marshal.GetExceptionCode() != 0)//we are here because of some sort of error
				{
					log.Debug("There was an error during the usage of pooled client connection, will not return it to the pool (may be poisioned)");
					base.Dispose();
				}
				else if(Connected == false)
				{
					log.Debug("The connection was disconnected, will not return connection to the pool");
					base.Dispose();
				}
				else
				{
					pool.PutMeBack(this);
				}
			}
		}
	}
}