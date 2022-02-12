using System.Linq;
using NUnit.Framework;

namespace Avro.Codec.BZip2.Test
{
    public class Tests
    {
        [TestCase(0)]
        [TestCase(1000)]
        [TestCase(64 * 1024)]
        [TestCase(1 * 1024 * 1024)]
        public void CompressDecompress(int length)
        {
            byte[] data = Enumerable.Range(0, length).Select(x => (byte)x).ToArray();

            BZip2Codec codec = new BZip2Codec();

            byte[] compressed = codec.Compress(data);
            byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

            CollectionAssert.AreEqual(data, uncompressed);
        }

        [Test]
        [TestCase(BZip2Level.Level1, ExpectedResult = "bzip2-1")]
        [TestCase(BZip2Level.Level2, ExpectedResult = "bzip2-2")]
        [TestCase(BZip2Level.Level3, ExpectedResult = "bzip2-3")]
        public string ToStringAndName(BZip2Level level)
        {
            BZip2Codec codec = new BZip2Codec(level);

            Assert.AreEqual("bzip2", codec.GetName());

            return codec.ToString();
        }
    }
}
