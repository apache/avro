package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.avro.AvroTypeException;

/**
 * A derived encoder that does the skipping of fields that match the index. It also encodes unions of null and a single
 * type as a more normal key=value rather than key={type=value}.
 * @author zfarkas
 */
public final class ExtendedJsonEncoder extends JsonEncoder implements DecimalEncoder {


  public ExtendedJsonEncoder(final Schema sc, final OutputStream out) throws IOException {
      super(sc, out);
  }

  public ExtendedJsonEncoder(final Schema sc, final OutputStream out, final boolean pretty) throws IOException {
    super(sc, out, pretty);
  }

  public ExtendedJsonEncoder(final Schema sc, final JsonGenerator out) throws IOException {
    super(sc, out);
  }

  public Parser getParser() {
    return parser;
  }

  public static boolean isNullableSingle(final Symbol.Alternative top) {
    return top.size() == 2 && ("null".equals(top.getLabel(0)) || "null".equals(top.getLabel(1)));
  }

  public static String getNullableSingle(final Symbol.Alternative top) {
    final String label = top.getLabel(0);
    return "null".equals(label) ? top.getLabel(1) : label;
  }

  /**
   * Overwrite this function to optime json decoding of union {null, type}.
   * @param unionIndex
   * @throws IOException
   */

  @Override
  public void writeIndex(final int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    Symbol symbol = top.getSymbol(unionIndex);
    if (symbol != Symbol.NULL && !isNullableSingle(top)) {
      out.writeStartObject();
      out.writeFieldName(top.getLabel(unionIndex));
      parser.pushSymbol(Symbol.UNION_END);
    }
    parser.pushSymbol(symbol);
  }

  @Override
  public void writeDecimal(final BigDecimal decimal, final Schema schema) throws IOException {
    switch (schema.getType()) {
      case STRING:
        parser.advance(Symbol.STRING);
        break;
      case BYTES:
        parser.advance(Symbol.BYTES);
        break;
      default:
        throw new AvroTypeException("Invalid schema for decimal " + schema);
    }
    out.writeNumber(decimal);
  }

  @Override
  public void writeBigInteger(BigInteger decimal, Schema schema) throws IOException {
    switch (schema.getType()) {
      case STRING:
        parser.advance(Symbol.STRING);
        break;
      case BYTES:
        parser.advance(Symbol.BYTES);
        break;
      default:
        throw new AvroTypeException("Invalid schema for decimal " + schema);
    }
    out.writeNumber(decimal);
  }

}
