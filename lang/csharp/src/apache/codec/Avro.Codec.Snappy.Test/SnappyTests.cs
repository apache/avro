using System.Linq;
using NUnit.Framework;

namespace Avro.Codec.Snappy.Test
{
    public class Tests
    {
        [TestCase(0)]
        [TestCase(1000)]
        [TestCase(64*1024)]
        [TestCase(1*1024*1024)]
        public void CompressDecompress(int length)
        {
            byte[] data = Enumerable.Range(0, length).Select(x => (byte)x).ToArray();

            SnappyCodec codec = new SnappyCodec();

            byte[] compressed = codec.Compress(data);
            byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

            CollectionAssert.AreEqual(data, uncompressed);
        }

        [Test]
        public void ToStringAndName()
        {
            SnappyCodec codec = new SnappyCodec();

            Assert.AreEqual("snappy", codec.GetName());
            Assert.AreEqual("snappy", codec.ToString());
        }
    }
}
