using System.Linq;
using NUnit.Framework;

namespace Avro.Codec.XZ.Test
{
    public class Tests
    {
        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            XZCodec.Initialize();
        }

        [OneTimeTearDown]
        public void OneTimeTearDOwn()
        {
            XZCodec.Uninitialize();
        }

        [TestCase(0)]
        [TestCase(1000)]
        [TestCase(64 * 1024)]
        [TestCase(1 * 1024 * 1024)]
        public void CompressDecompress(int length)
        {
            byte[] data = Enumerable.Range(0, length).Select(x => (byte)x).ToArray();

            XZCodec codec = new XZCodec();

            byte[] compressed = codec.Compress(data);
            byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

            CollectionAssert.AreEqual(data, uncompressed);
        }

        [Test]
        [TestCase(XZLevel.Level1, ExpectedResult = "xz-1")]
        [TestCase(XZLevel.Level2, ExpectedResult = "xz-2")]
        [TestCase(XZLevel.Level3, ExpectedResult = "xz-3")]
        public string ToStringAndName(XZLevel level)
        {
            XZCodec codec = new XZCodec(level);

            Assert.AreEqual("xz", codec.GetName());

            return codec.ToString();
        }
    }
}
