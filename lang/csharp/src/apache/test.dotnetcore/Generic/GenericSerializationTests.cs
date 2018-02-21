using System.IO;
using Avro.IO;
using Avro.Specific;
using Avro.Test.Generic.Autogen;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Avro.Test.Generic
{
    [TestFixture]
    public class GenericSerializationTests
    {
        [Test]
        public void RoundTripObject()
        {
            // original object
            var origObj = new HeaderDTO()
            {
                Info = new InfoDto()
                {
                    Count = 1,
                }
            };

            // Serialize
            var origObjBytes = Serialize(origObj);

            // Deserialize
            var regenObj = Deserialize<HeaderDTO>(origObjBytes);

            // Verify everything survived the roundtrip, simplest to compare their JSON
            Assert.IsTrue(string.Equals(JsonConvert.SerializeObject(origObj), JsonConvert.SerializeObject(regenObj)));
        }

        public static byte[] Serialize<T>(T thisObj) where T : ISpecificRecord
        {
            using (var ms = new MemoryStream())
            {
                var enc = new BinaryEncoder(ms);
                var writer = new SpecificDefaultWriter(thisObj.Schema); // Schema comes from pre-compiled, code-gen phase
                writer.Write(thisObj, enc);
                return ms.ToArray();
            }
        }

        public static T Deserialize<T>(byte[] bytes) where T : ISpecificRecord, new()
        {
            using (var ms = new MemoryStream(bytes))
            {
                var dec = new BinaryDecoder(ms);
                var regenObj = new T();

                var reader = new SpecificDefaultReader(regenObj.Schema, regenObj.Schema);
                reader.Read(regenObj, dec);
                return regenObj;
            }
        }
    }
}
