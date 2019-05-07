using System;

namespace Avro.POCO
{
    public class AvroAttribute : Attribute
    {
        public bool ByPosition { get; set; }

        public AvroAttribute(bool byPosition)
        {
            ByPosition = byPosition;
        }
    }
}
