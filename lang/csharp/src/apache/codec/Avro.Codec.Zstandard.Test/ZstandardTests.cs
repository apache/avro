using System.Linq;
using NUnit.Framework;

namespace Avro.Codec.Zstandard.Test
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

            ZstandardCodec codec = new ZstandardCodec();

            byte[] compressed = codec.Compress(data);
            byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

            CollectionAssert.AreEqual(data, uncompressed);
        }

        [Test]
        [TestCase(ZstandardLevel.Level1, ExpectedResult = "zstandard[1]")]
        [TestCase(ZstandardLevel.Level2, ExpectedResult = "zstandard[2]")]
        [TestCase(ZstandardLevel.Level3, ExpectedResult = "zstandard[3]")]
        public string ToStringAndName(ZstandardLevel level)
        {
            ZstandardCodec codec = new ZstandardCodec(level);

            Assert.AreEqual("zstandard", codec.GetName());

            return codec.ToString();
        }
    }
}
