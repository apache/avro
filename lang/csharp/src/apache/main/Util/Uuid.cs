using System;

namespace Avro.Util
{
    /// <summary>
    /// UUid logical type
    /// </summary>
    public class Uuid : LogicalType
    {
        /// <summary>
        /// Logical type name
        /// </summary>
        public static readonly string LogicalTypeName = "uuid";

        /// <summary>
        /// Constructs Uuid object
        /// </summary>
        public Uuid() : base(LogicalTypeName) { }


        /// <inheritdoc />
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            return logicalValue.ToString();
        }

        /// <inheritdoc />
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            return new Guid((string) baseValue);
        }

        /// <inheritdoc />
        public override Type GetCSharpType(bool nullible)
        {
            return nullible ? typeof(Guid?) : typeof(Guid);
        }

        /// <inheritdoc />
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is Guid;
        }

        /// <inheritdoc />
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.String != schema.BaseSchema.Tag)
                throw new AvroTypeException("'uuid' can only be used with an underlying string type");
        }
    }
}
