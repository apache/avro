using System;
using System.Collections.Generic;

namespace Avro.Test
{
    public class LogMessage2
    {
        public string IP { get; set; }

        public string Message { get; set; }

        public DateTimeOffset TimeStamp { get; set; }

        private Dictionary<string, string> _tags = new Dictionary<string, string>();

        public Dictionary<string, string> Tags { get => _tags; set => _tags = value; }

        public MessageTypes Severity { get; set; }
    }
}
