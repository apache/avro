using System;

namespace Avro.sampleMultiSchema
{
    public static class Program
    {
        public static int Main(string[] args)
        {
            return Avro.AvroGenTool.Main(new string[] { "-ms", "avroFiles/models", "generated" });
            return 0;
        }
    }
}
