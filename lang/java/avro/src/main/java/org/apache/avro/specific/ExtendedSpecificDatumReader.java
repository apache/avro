package org.apache.avro.specific;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.ExtendedGenericDatumReader;
import org.apache.avro.util.ClassUtils;

/**
 * @author zfarkas
 */
//CHECKSTYLE IGNORE DesignForExtension FOR NEXT 900 LINES
public class ExtendedSpecificDatumReader<T> extends ExtendedGenericDatumReader<T> {

    public ExtendedSpecificDatumReader() {
        this(null, null, SpecificData.get());
    }

    /**
     * Construct for reading instances of a class.
     */
    public ExtendedSpecificDatumReader(final Class<T> c) {
        this(new SpecificData(c.getClassLoader()));
        setSchema(getSpecificData().getSchema(c));
    }

    /**
     * Construct where the writer's and reader's schemas are the same.
     */
    public ExtendedSpecificDatumReader(final Schema schema) {
        this(schema, schema, SpecificData.get());
    }

    /**
     * Construct given writer's and reader's schema.
     */
    public ExtendedSpecificDatumReader(final Schema writer, final Schema reader) {
        this(writer, reader, SpecificData.get());
    }

    /**
     * Construct given writer's schema, reader's schema, and a {@link
     * SpecificData}.
     */
    public ExtendedSpecificDatumReader(final Schema writer, final Schema reader,
                                    final SpecificData data) {
        super(writer, reader, data);
    }

    /**
     * Construct given a {@link SpecificData}.
     */
    public ExtendedSpecificDatumReader(final SpecificData data) {
        super(data);
    }


    /**
     * Return the contained {@link SpecificData}.
     * copied from SpecificDatumReader. (due to lack of mixins in java.
     */
    public SpecificData getSpecificData() {
        return (SpecificData) getData();
    }


    /**
     * copied from SpecificDatumReader. (due to lack of mixins in java.
     *
     * @param actual
     */
    @Override
    public final void setSchema(final Schema actual) {
        // if expected is unset and actual is a specific record,
        // then default expected to schema of currently loaded class
        if (getExpected() == null && actual != null
                && actual.getType() == Schema.Type.RECORD) {
            SpecificData data = getSpecificData();
            Class c = data.getClass(actual);
            if (c != null && SpecificRecord.class.isAssignableFrom(c)) {
                setExpected(data.getSchema(c));
            }
        }
        super.setSchema(actual);
    }

    /**
     * copied from SpecificDatumReader. (due to lack of mixins in java.
     *
     * @param schema
     * @return
     */
    @Override
    protected Class findStringClass(final Schema schema) {
        try {
            Class stringClass = null;
            switch (schema.getType()) {
                case STRING:
                    stringClass = getPropAsClass(schema, SpecificData.CLASS_PROP);
                    break;
                case MAP:
                    stringClass = getPropAsClass(schema, SpecificData.KEY_CLASS_PROP);
                    break;
                default:
                    break;
            }
            if (stringClass != null) {
                return stringClass;
            }
            return super.findStringClass(schema);
        } catch (ClassNotFoundException ex) {
            throw new AvroRuntimeException(ex);
        }
    }

    /**
     * copied from SpecificDatumReader. (due to lack of mixins in java.
     *
     * @param schema
     * @param prop
     * @return
     */
    private Class getPropAsClass(final Schema schema, final String prop) throws ClassNotFoundException {
        String name = schema.getProp(prop);
        if (name == null) {
            return null;
        }
        return ClassUtils.forName(getData().getClassLoader(), name);
    }

}
