using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;

using NUnit.Framework;
//using Microsoft.VisualStudio.TestTools.UnitTesting;

using Avro.ipc;
using Avro.ipc.Generic;
using Avro.Generic;

namespace Avro.Test.Ipc
{
    [TestFixture]
    //[TestClass]
    public class HttpClientServerTest
    {
        private HttpListenerServer server;
        private MailResponder mailResponder;
        private HttpTransceiver transceiver;
        private GenericRequestor proxy;

        const string URL = @"http://localhost:18080/avro/test/ipc/mailResponder/";

        [TestFixtureSetUp]
        //[TestInitialize]
        public void Init()
        {
            mailResponder = new MailResponder();

            server = new HttpListenerServer(new string[] { URL }, mailResponder);
            server.Start();

            HttpWebRequest requestTemplate = (HttpWebRequest)HttpWebRequest.Create(URL);
            requestTemplate.Timeout = 6000;
            requestTemplate.Proxy = null;
            transceiver = new HttpTransceiver(requestTemplate);
            proxy = new GenericRequestor(transceiver, MailResponder.Protocol);
        }

        [TestFixtureTearDown]
        //[TestCleanup]
        public void Cleanup()
        {
            server.Stop();
        }

        private string Send(GenericRecord message)
        {
            var request = new GenericRecord(MailResponder.Protocol.Messages["send"].Request);
            request.Add("message", message);

            var result = (string)proxy.Request("send", request);
            return result;
        }

        [Test]
        //[TestMethod]
        public void TestRequestResponse()
        {
            for (int x = 0; x < 5; x++)
            {
                var message = SocketServerTest.CreateMessage();

                var result = Send(message);
                SocketServerTest.VerifyResponse(result);
            }
        }
    }
}
