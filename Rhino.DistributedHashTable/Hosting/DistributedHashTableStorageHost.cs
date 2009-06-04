//using System;
//using System.Net;
//using System.ServiceModel;
//using Rhino.DistributedHashTable.Internal;
//using Rhino.DistributedHashTable.Remote;
//using Rhino.Queues;

//namespace Rhino.DistributedHashTable.Hosting
//{
//    public class DistributedHashTableStorageHost : IDisposable
//    {
//        private readonly IDistributedHashTableNode node;
//        private readonly QueueManager queueManager;
//        private readonly IDistributedHashTableMaster masterChannel;
//        private readonly IDistributedHashTableStorage storage;
//        private readonly ServiceHost serviceHost;

//        public DistributedHashTableStorageHost(Uri master)
//            : this(master, "node", 2201)
//        {
//        }

//        public DistributedHashTableStorageHost(
//            Uri master,
//            string name,
//            int port)
//        {
//            Endpoint = new NodeEndpoint
//            {
//                Sync = new UriBuilder("net.tcp://" + Environment.MachineName + "/" + name)
//                {
//                    Port = port
//                }.Uri,
//                Async = new UriBuilder("rhino.queues://" + Environment.MachineName + "/" + name)
//                {
//                    Port = port + 1
//                }.Uri,
//            };
//            queueManager = new QueueManager(new IPEndPoint(IPAddress.Any, port + 1), name + ".queue.esent");

//            masterChannel = new ChannelFactory<IDistributedHashTableMaster>(Binding.DhtDefault, master.ToString())
//                .CreateChannel();

//            node = new DistributedHashTableNode(
//                masterChannel,
//                new ThreadPoolExecuter(),
//                new NetDataMessageSerializer(),
//                Endpoint,
//                queueManager,
//                new DistributedHashTableNodeReplicationFactory()
//                );
//            storage = new DistributedHashTableStorage(name + ".data.esent", node);

//            serviceHost = new ServiceHost(storage);
//            serviceHost.AddServiceEndpoint(typeof (IDistributedHashTableStorage),
//                                           Binding.DhtDefault, node.Endpoint.Sync);
//        }

//        public NodeEndpoint Endpoint { get; private set; }

//        public void Start()
//        {
//            node.Start();
//            serviceHost.Open();
//        }

//        public void Dispose()
//        {
//            serviceHost.Close(TimeSpan.Zero);
//            storage.Dispose();
//            queueManager.Dispose();
//            ((IClientChannel)masterChannel).Close(TimeSpan.Zero);
//        }
//    }
//}