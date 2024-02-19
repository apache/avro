using System;
using System.Collections.Generic;
using System.Text;

namespace Avro.Util
{
    public class UnknownLogicalType : LogicalType
    {
        public LogicalSchema Schema { get; }

        public UnknownLogicalType(LogicalSchema schema) : base(schema.LogicalTypeName)
        {
            this.Schema = schema;
        }

        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        { 
            throw new NotImplementedException();
        }

        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            throw new NotImplementedException();
        }

        public override Type GetCSharpType(bool nullible)
        {
            // handle all Primitive Types
            switch (this.Schema.BaseSchema.Name)
            {
                case @"string":
                    return typeof(System.String);
                case @"boolean":
                    return typeof(System.Boolean);
                case @"int":
                    return typeof(System.Int32);
                case @"long":
                    return typeof(System.Int64);
                case @"float":
                    return typeof(System.Single);
                case @"double":
                    return typeof(System.Double);
                case @"bytes":
                    return typeof(System.Byte[]);
                default:
                    return typeof(System.Object);
            }
        }

        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            //         => throw new NotImplementedException();
            return true;
        }

    }
}
