using System;
using System.Collections.Generic;
using Avro.POCO;

namespace Avro.Test
{
    [Avro(true)]
    public class LogMessage
    {
        private Dictionary<string, string> _tags = new Dictionary<string, string>();

        [AvroField(0)]
        public string IP { get; set; }

        [AvroField(1)]
        public string Message { get; set; }

        [AvroField(2, typeof(DateTimeOffsetConverter))]
        public DateTimeOffset TimeStamp { get; set; }

        [AvroField(3)]
        public Dictionary<string, string> Tags { get => _tags; set => _tags = value; }

        [AvroField(4)]
        public MessageTypes Severity { get; set; }
    }
}
