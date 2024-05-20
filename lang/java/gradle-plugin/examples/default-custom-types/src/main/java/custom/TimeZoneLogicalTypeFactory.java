package custom;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class TimeZoneLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
    @Override
    public LogicalType fromSchema(Schema schema) {
        return TimeZoneLogicalType.INSTANCE;
    }
}
