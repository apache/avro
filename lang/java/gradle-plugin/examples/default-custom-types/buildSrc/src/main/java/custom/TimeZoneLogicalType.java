package custom;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class TimeZoneLogicalType extends LogicalType {
    static final TimeZoneLogicalType INSTANCE = new TimeZoneLogicalType();

    private TimeZoneLogicalType() {
        super(TimeZoneConversion.LOGICAL_TYPE_NAME);
    }

    @Override
    public void validate(Schema schema) {
        super.validate(schema);
        if (schema.getType() != Schema.Type.STRING) {
            throw new IllegalArgumentException("Timezone can only be used with an underlying string type");
        }
    }
}
