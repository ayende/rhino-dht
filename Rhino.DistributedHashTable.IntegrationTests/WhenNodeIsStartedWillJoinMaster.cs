//using System;
//using System.Linq;
//using System.ServiceModel;
//using Rhino.DistributedHashTable.Hosting;
//using Rhino.DistributedHashTable.Internal;
//using Xunit;

//namespace Rhino.DistributedHashTable.IntegrationTests
//{
//    public class WhenNodeIsStarted : FullIntegrationTest, IDisposable
//    {
//        private readonly DistributedHashTableMasterHost masterHost;
//        private readonly DistributedHashTableStorageHost storageHost;
//        private readonly Uri masterUri = new Uri("net.tcp://" + Environment.MachineName + ":2200/master");

//        public WhenNodeIsStarted()
//        {
//            masterHost = new DistributedHashTableMasterHost();
//            storageHost = new DistributedHashTableStorageHost(
//                masterUri);

//            masterHost.Start();
//            storageHost.Start();
//        }


//        [Fact]
//        public void WillJoinMaster()
//        {
//            var channel = ChannelFactory<IDistributedHashTableMaster>.CreateChannel(
//                Binding.DhtDefault,
//                new EndpointAddress(masterUri));
//            try
//            {
//                var topology = channel.GetTopology();
//                Assert.True(topology.Segments.All(x => x.AssignedEndpoint == storageHost.Endpoint));
//            }
//            finally
//            {
//                ((IClientChannel)channel).Dispose();
//            }
//        }

//        public void Dispose()
//        {
//            masterHost.Dispose();
//        }
//    }
//}