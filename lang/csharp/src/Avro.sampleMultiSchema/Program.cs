using System;

namespace Avro.sampleMultiSchema
{
    public static class Program
    {
        public static int Main(string[] args)
        {
            return Avro.AvroGen.Main(new string[] { "-ms", "avroFiles", "generated" });
        }
    }
}
