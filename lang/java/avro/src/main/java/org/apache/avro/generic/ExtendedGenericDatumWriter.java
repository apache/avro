package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Extension of the AVRO GenericDatumWriter that does the following extras over the base class
 * If the value being written equals the schema default we dont write the field or value at all!
 * We also encode unions of null and a singletype as a more normal key=value rather than key={type=value}.
 *
 * @author zfarkas
 */
public class ExtendedGenericDatumWriter<D> extends GenericDatumWriter<D> {

    public ExtendedGenericDatumWriter(final GenericData data) {
        super(data);
    }

    public ExtendedGenericDatumWriter(final Schema root) {
        super(root);
    }

    public ExtendedGenericDatumWriter(final Schema root, final GenericData data) {
        super(root, data);
    }

   public static boolean equals(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }

    /**
     * Overwritten to skip serializing fields that have default values.
     *
     * @param datum
     * @param f
     * @param out
     * @param state
     * @throws IOException
     */
    //CHECKSTYLE IGNORE DesignForExtension FOR NEXT 30 LINES
    @Override
    protected void writeField(final Object datum, final Schema.Field f, final Encoder out, final Object state)
            throws IOException {
        GenericData data = getData();
        if (f.defaultVal() != null) {
            Object defaultValue = data.getDefaultValue(f);
            Object value = data.getField(datum, f.name(), f.pos());

            if (equals(value, defaultValue) && out instanceof ExtendedJsonEncoder) {
                    Parser parser = ((ExtendedJsonEncoder) out).getParser();
                    Symbol topSymbol = parser.topSymbol();
                    switch (topSymbol.kind) {
                        case TERMINAL:
                        case IMPLICIT_ACTION:
                            break;
                        default:
                            // expand production
                            final Symbol nextSymbol = topSymbol.production[topSymbol.production.length - 1];
                            if (nextSymbol instanceof Symbol.ImplicitAction) {
                                if (((Symbol.ImplicitAction) nextSymbol).isTrailing) {
                                    throw new IllegalStateException("Cannot start with a trailing implicit"
                                            + topSymbol);
                                } else {
                                    parser.advance(nextSymbol);
                                }
                            } else {
                                throw new IllegalStateException("Invalid state " + topSymbol);
                            }
                            parser.pushSymbol(nextSymbol);
                    }
                    List<Symbol> holdings = new ArrayList<Symbol>(4);
                    Symbol advanceTo = null;
                    boolean done = false;
                    while (!done) {
                        if (parser.depth() > 0) {
                          advanceTo = parser.popSymbol();
                          if (advanceTo instanceof Symbol.FieldAdjustAction
                                  && ((Symbol.FieldAdjustAction) advanceTo).fname.equals(f.name())
                                  && ((Symbol.FieldAdjustAction) advanceTo).rindex == f.pos()) {
                              done = true;
                          }
                          holdings.add(advanceTo);
                        } else {
                          done = true;
                        }
                    }
                    for (int i = holdings.size() - 1; i >= 0; i--) {
                        parser.pushSymbol(holdings.get(i));
                    }
                    parser.advance(advanceTo);
                    int count = 1;
                    while (count > 0) {
                        Symbol currentSymbol = parser.popSymbol();
                        if (currentSymbol.getClass() == Symbol.ImplicitAction.class) {
                            if (((Symbol.ImplicitAction) currentSymbol).isTrailing) {
                                count--;
                            } else {
                                count++;
                            }
                        }
                    }
            } else {
                super.writeField(datum, f, out, state);
            }
        } else {
            super.writeField(datum, f, out, state);
        }

    }

}

