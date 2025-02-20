package custom;

import java.util.TimeZone;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

@SuppressWarnings("unused")
public class TimeZoneConversion extends Conversion<TimeZone> {
    public static final String LOGICAL_TYPE_NAME = "timezone";

    @Override
    public Class<TimeZone> getConvertedType() {
        return TimeZone.class;
    }

    @Override
    public String getLogicalTypeName() {
        return LOGICAL_TYPE_NAME;
    }

    @Override
    public TimeZone fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return TimeZone.getTimeZone(value.toString());
    }

    @Override
    public CharSequence toCharSequence(TimeZone value, Schema schema, LogicalType type) {
        return value.getID();
    }

    @Override
    public Schema getRecommendedSchema() {
        return TimeZoneLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.STRING));
    }
}
