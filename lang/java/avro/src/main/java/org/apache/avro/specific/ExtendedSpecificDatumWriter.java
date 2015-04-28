package org.apache.avro.specific;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.ExtendedGenericDatumWriter;
import org.apache.avro.io.Encoder;


/**
 * @param <T>
 * @author zfarkas
 */
//CHECKSTYLE IGNORE DesignForExtension FOR NEXT 2000 LINES
public class ExtendedSpecificDatumWriter<T> extends ExtendedGenericDatumWriter<T> {

    public ExtendedSpecificDatumWriter(final Class<T> c) {
        super(SpecificData.get().getSchema(c), SpecificData.get());
    }

    public ExtendedSpecificDatumWriter(final GenericData data) {
        super(data);
    }

    public ExtendedSpecificDatumWriter(final Schema root) {
        super(root);
    }

    public ExtendedSpecificDatumWriter(final Schema root, final GenericData data) {
        super(root, data);
    }


    /**
     * Returns the {@link SpecificData} implementation used by this writer.
     * copied from SpecificDatumWriter. (due to lack of mixins in java.
     */
    public SpecificData getSpecificData() {
        return (SpecificData) getData();
    }

    /**
     * copied from SpecificDatumWriter. (due to lack of mixins in java.
     *
     * @param schema
     * @param datum
     * @param out
     * @throws IOException
     */
    @Override
    protected void writeEnum(final Schema schema, final Object datum, final Encoder out)
            throws IOException {
        if (!(datum instanceof Enum)) {
            super.writeEnum(schema, datum, out);        // punt to generic
        } else {
            out.writeEnum(((Enum) datum).ordinal());
        }
    }

    /**
     * copied from SpecificDatumWriter. (due to lack of mixins in java.
     *
     * @param schema
     * @param pDatum
     * @param out
     * @throws IOException
     */

    @Override
    protected void writeString(final Schema schema, final Object pDatum, final Encoder out)
            throws IOException {
        Object datum = pDatum;
        if (!(datum instanceof CharSequence)
                && getSpecificData().isStringable(datum.getClass())) {
            datum = datum.toString();                   // convert to string
        }
        writeString(datum, out);
    }

}
