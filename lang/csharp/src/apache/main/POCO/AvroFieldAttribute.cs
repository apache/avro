using System;
using System.Reflection;

namespace Avro.POCO
{
    public class AvroFieldAttribute : Attribute
    {
        /// <summary>
        /// Sequence number of the field in the Avro Schema
        /// </summary>
        /// <value></value>
        public int FieldPos { get; set; }

        /// <summary>
        /// Convert the property into a standard Avro type - e.g. DateTimeOffset to long
        /// </summary>
        /// <value></value>
        public IAvroFieldConverter Converter { get; set; }

        /// <summary>
        /// Attribute to hold field position and optionally a converter
        /// </summary>
        /// <param name="fieldPos"></param>
        /// <param name="converter"></param>
        public AvroFieldAttribute(int fieldPos, Type converter = null)
        {
            FieldPos = fieldPos;
            if (converter != null)
            {
                Converter = (IAvroFieldConverter)Activator.CreateInstance(converter);
            }
        }
        public AvroFieldAttribute(Type converter)
        {
            FieldPos = -1;
            if (converter != null)
            {
                Converter = (IAvroFieldConverter)Activator.CreateInstance(converter);
            }
        }
    }
}
