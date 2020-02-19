using System;

namespace Avro.Util
{
    public class Uuid : LogicalType
    {

        public static readonly string LogicalTypeName = "uuid";

        public Uuid() : base(LogicalTypeName) { }

        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            return logicalValue.ToString();
        }

        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            return new Guid((string) baseValue);
        }

        public override Type GetCSharpType(bool nullible)
        {
            return nullible ? typeof(Guid?) : typeof(Guid);
        }

        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is Guid;
        }

        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.String != schema.BaseSchema.Tag)
                throw new AvroTypeException("'uuid' can only be used with an underlying string type");
        }
    }
}
