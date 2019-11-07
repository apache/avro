using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Avro.test
{
    [TestFixture]
    class AvroDecimalTests
    {
        [TestCase]
        public void TestToByteArray()
        {
            foreach (var decimalVal in new decimal[] { decimal.MinValue + 1, decimal.MinValue, decimal.MaxValue, decimal.MaxValue - 1 })
            {
                Assert.AreEqual(decimalVal, (decimal)new AvroDecimal(decimalVal));

                CollectionAssert.AreEqual(GetDecimalBytes(decimalVal), new AvroDecimal(decimalVal).ToByteArray());
            }
        }

        public static byte[] GetDecimalBytes(decimal d)
        {
            byte[] bytes = new byte[16];

            int[] bits = decimal.GetBits(d);
            int lo = bits[0];
            int mid = bits[1];
            int hi = bits[2];
            int flags = bits[3];

            bytes[0] = (byte)lo;
            bytes[1] = (byte)(lo >> 8);
            bytes[2] = (byte)(lo >> 0x10);
            bytes[3] = (byte)(lo >> 0x18);
            bytes[4] = (byte)mid;
            bytes[5] = (byte)(mid >> 8);
            bytes[6] = (byte)(mid >> 0x10);
            bytes[7] = (byte)(mid >> 0x18);
            bytes[8] = (byte)hi;
            bytes[9] = (byte)(hi >> 8);
            bytes[10] = (byte)(hi >> 0x10);
            bytes[11] = (byte)(hi >> 0x18);
            bytes[12] = (byte)flags;
            bytes[13] = (byte)(flags >> 8);
            bytes[14] = (byte)(flags >> 0x10);
            bytes[15] = (byte)(flags >> 0x18);

            return bytes;
        }
    }
}
