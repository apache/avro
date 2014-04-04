package org.apache.avro.generic;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

/**
 * <p>This {@link RecordMapping} writes a {@code BigDecimal}
 * object as an Avro record with the following schema:</p>
 * <pre><code>
 * {
 *   "type": "record",
 *   "name": "org.apache.avro.Decimal",
 *   "fields": [
 *     {"name": "scale", "type": "int"},
 *     {"name": "value", "type": "bytes"}
 *   ]
 * }
 * </code></pre>
 */
public class DecimalRecordMapping extends RecordMapping<BigDecimal> {
  public DecimalRecordMapping() {
    this(null, null);
  }

  public DecimalRecordMapping(Integer maxPrecision, Integer maxScale) {
    super(schema(maxPrecision, maxScale), BigDecimal.class);
  }

  private static Schema schema(Integer maxPrecision, Integer maxScale) {
    RecordBuilder<Schema> builder = SchemaBuilder.record("org.apache.avro.Decimal");
    if (maxPrecision != null && maxScale != null) {
      builder.prop("maxPrecision", Integer.toString(maxPrecision))
             .prop("maxScale", Integer.toString(maxScale));
    }
    return builder.fields()
        .requiredInt("scale")
        .requiredBytes("value")
        .endRecord();
  }

  @Override
  public void write(Object datum, Encoder out) throws IOException {
    BigDecimal decimal = (BigDecimal) datum;
    out.writeInt(decimal.scale());
    out.writeBytes(decimal.unscaledValue().toByteArray());
  }

  @Override
  public BigDecimal read(Object reuse, Decoder in) throws IOException {
    // BigDecimal instances are immutable so can't reuse
    int scale = in.readInt();
    ByteBuffer byteBuffer = in.readBytes(null);
    byte[] value = new byte[byteBuffer.remaining()];
    byteBuffer.get(value);
    return new BigDecimal(new BigInteger(value), scale);
  }

}
