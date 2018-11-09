/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io.fastreader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.fastreader.readers.ArrayReader;
import org.apache.avro.io.fastreader.readers.BooleanReader;
import org.apache.avro.io.fastreader.readers.BytesReader;
import org.apache.avro.io.fastreader.readers.DoubleReader;
import org.apache.avro.io.fastreader.readers.EnumReader;
import org.apache.avro.io.fastreader.readers.FailureReader;
import org.apache.avro.io.fastreader.readers.FieldReader;
import org.apache.avro.io.fastreader.readers.FixedReader;
import org.apache.avro.io.fastreader.readers.FloatReader;
import org.apache.avro.io.fastreader.readers.IntegerReader;
import org.apache.avro.io.fastreader.readers.LongReader;
import org.apache.avro.io.fastreader.readers.MapReader;
import org.apache.avro.io.fastreader.readers.NullReader;
import org.apache.avro.io.fastreader.readers.ReconfigurableReader;
import org.apache.avro.io.fastreader.readers.RecordReader;
import org.apache.avro.io.fastreader.readers.RecordReader.Stage;
import org.apache.avro.io.fastreader.readers.StringReader;
import org.apache.avro.io.fastreader.readers.UnionReader;
import org.apache.avro.io.fastreader.readers.Utf8Reader;
import org.apache.avro.io.fastreader.readers.conversion.BytesConversionReader;
import org.apache.avro.io.fastreader.readers.conversion.FixedConversionReader;
import org.apache.avro.io.fastreader.readers.conversion.GenericConversionReader;
import org.apache.avro.io.fastreader.readers.conversion.IntegerConversionReader;
import org.apache.avro.io.fastreader.readers.conversion.LongConversionReader;
import org.apache.avro.io.fastreader.readers.conversion.StringConversionReader;
import org.apache.avro.io.fastreader.readers.promotion.DoublePromotionReader;
import org.apache.avro.io.fastreader.readers.promotion.FloatPromotionReader;
import org.apache.avro.io.fastreader.readers.promotion.LongPromotionReader;
import org.apache.avro.io.fastreader.steps.ExecutionStep;
import org.apache.avro.io.fastreader.steps.FieldDefaulter;
import org.apache.avro.io.fastreader.steps.FieldSetter;
import org.apache.avro.io.fastreader.steps.FieldSkipper;
import org.apache.avro.io.fastreader.utils.ReflectionUtils;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.WeakIdentityHashMap;

public class FastReader {

  private static final ExecutionStep[] EMPTY_EXECUTION_STEPS = new ExecutionStep[0];

  /**
   * Generic/SpecificData instance that contains basic functionalities like instantiation of objects
   */
  private final GenericData data;

  /** first schema is reader schema, second is writer schema */
  private final Map<Schema, Map<Schema, RecordReader<?>>> readerCache = new WeakIdentityHashMap<>();


  /**
   * if set to <b>true</b>, fail in reader/writer creation when an illegal conversion state exists,
   * if set to <b>false</b>, fail when such a state is encountered during serialization or
   * deserialization (in many cases, it might not even occur, e.g. in unions or enums)
   */
  private boolean failFast = false;

  private boolean keyClassEnabled = true;

  private boolean classPropEnabled = true;

  public static FastReader get() {
    return new FastReader(GenericData.get());
  }

  public static FastReader getSpecific() {
    return new FastReader(SpecificData.get());
  }

  public FastReader(GenericData parentData) {
    this.data = parentData;
  }

  public FastReader withFailFast(boolean failFast) {
    this.failFast = failFast;
    return this;
  }

  public boolean isFailingFast() {
    return this.failFast;
  }

  public FastReader withKeyClassEnabled(boolean enabled) {
    this.keyClassEnabled = enabled;
    return this;
  }

  public boolean isKeyClassEnabled() {
    return this.keyClassEnabled;
  }

  public FastReader withClassPropEnabled(boolean enabled) {
    this.classPropEnabled = enabled;
    return this;
  }

  public boolean isClassPropEnabled() {
    return this.classPropEnabled;
  }

  public <D> DatumReader<D> createReconfigurableDatumReader(Schema writerSchema,
      Schema readerSchema) {
    return new ReconfigurableReader<>(this, readerSchema, writerSchema);
  }

  @SuppressWarnings("unchecked")
  public <D> DatumReader<D> createDatumReader(Schema writerSchema, Schema readerSchema) {
    return (DatumReader<D>) getReaderFor(readerSchema, writerSchema);
  }

  public <D extends IndexedRecord> RecordReader<D> createRecordReader(Schema readerSchema,
      Schema writerSchema) {
    // record readers are created in a two-step process, first registering it, then initializing it,
    // to prevent endless loops on recursive types
    RecordReader<D> recordReader = getRecordReaderFromCache(readerSchema, writerSchema);

    // only need to initialize once
    if (recordReader.getInitializationStage() == Stage.NEW) {
      initializeRecordReader(recordReader, readerSchema, writerSchema);
    }

    return recordReader;
  }

  private <D extends IndexedRecord> RecordReader<D> initializeRecordReader(
      RecordReader<D> recordReader, Schema readerSchema, Schema writerSchema) {
    try {
      recordReader.startInitialization();

      // generate supplier for the new object instances
      Supplier<? extends IndexedRecord> supplier = getObjectSupplier(readerSchema);
      IntFunction<Conversion<?>> conversionSupplier = getConversionSupplier(supplier.get());

      ExecutionStep[] readSteps = new ExecutionStep[writerSchema.getFields().size()];
      List<ExecutionStep> defaultingSteps = new ArrayList<>();

      // populate read steps with steps that read data that occurs in readerSchema
      for (Schema.Field readerField : readerSchema.getFields()) {
        Schema.Field writerField = findSourceField(readerField, writerSchema);
        if (writerField != null) {
          // fields of reader schema that are found in writer schema will be read via FieldReader
          // instances
          Conversion<?> conversion = conversionSupplier.apply(readerField.pos());
          FieldReader<?> fieldReader =
              getReaderFor(readerField.schema(), writerField.schema(), conversion);
          int writerPos = writerField.pos();
          if (readSteps[writerPos] != null) {
            throw new IllegalStateException(
                "Schemas not compatible. Two uses of writer field " + writerField.name());
          }
          readSteps[writerPos] = new FieldSetter<>(readerField.pos(), fieldReader);
        } else {
          defaultingSteps.add(getDefaultingStep(readerSchema, writerSchema, readerField));
        }
      }

      // fields of writer schema that are not being red from will be skipped
      for (int i = 0; i < readSteps.length; i++) {
        if (readSteps[i] == null) {
          Schema fieldSchema = writerSchema.getFields().get(i).schema();
          readSteps[i] = new FieldSkipper(getReaderFor(fieldSchema, fieldSchema));
        }
      }

      ExecutionStep[] defaultingStepsArray = defaultingSteps.toArray(EMPTY_EXECUTION_STEPS);

      // store execution plan in RecordReader
      recordReader.finishInitialization(readSteps, defaultingStepsArray, supplier);

      return recordReader;
    } catch (final Exception e) {
      // if an exception is set, don't leave record reader in 'initializing' state.
      recordReader.reset();
      throw e;
    }
  }


  private ExecutionStep getDefaultingStep(Schema readerSchema, Schema writerSchema, Schema.Field field) {
    try {
      Object defaultValue = data.getDefaultValue(field);
      return new FieldDefaulter(field.pos(), defaultValue);
    } catch (AvroMissingFieldException e) {
      return getFailureReaderOrFail("Found " + writerSchema.getFullName() + ", expecting " + readerSchema.getFullName() + ", missing required field " + field.name() );
    }
  }


  private IntFunction<Conversion<?>> getConversionSupplier(IndexedRecord record) {
    if (record instanceof SpecificRecordBase) {
      return ((SpecificRecordBase) record)::getConversion;
    } else {
      return index -> null;
    }
  }

  @SuppressWarnings("unchecked")
  private <D extends IndexedRecord> RecordReader<D> getRecordReaderFromCache(Schema readerSchema,
      Schema writerSchema) {
    synchronized ( readerCache ) {
      Map<Schema, RecordReader<?>> writerMap =
        readerCache.computeIfAbsent(readerSchema, k -> new WeakIdentityHashMap<>());
      return (RecordReader<D>) writerMap.computeIfAbsent(writerSchema, k -> new RecordReader<>());
    }
  }

  @SuppressWarnings("unchecked")
  private Supplier<? extends IndexedRecord> getObjectSupplier(Schema readerSchema) {
    final Object object = data.newRecord(null, readerSchema);
    final Class<?> clazz = object.getClass();

    if (IndexedRecord.class.isAssignableFrom(clazz)) {
      Supplier<IndexedRecord> supplier;

      if (SchemaConstructable.class.isAssignableFrom(clazz)) {
        supplier = ReflectionUtils.getOneArgConstructorAsSupplier((Class<IndexedRecord>) clazz,
            Schema.class, readerSchema);
      } else {
        supplier = ReflectionUtils.getConstructorAsSupplier((Class<IndexedRecord>) clazz);
      }

      if (supplier != null) {
        // test supplier and check for schema match with created class
        // (otherwise, if the SpecificRecord has a different schema, BAD things will happen)
        IndexedRecord sampleRecord = supplier.get();
        if (sampleRecord.getSchema().equals(readerSchema)) {
          return supplier;
        }
      }
    }

    return () -> new GenericData.Record(readerSchema);
  }


  private FieldReader<?> getReaderFor(Schema readerSchema, Schema writerSchema) {
    return getReaderFor(readerSchema, writerSchema, null);
  }

  private FieldReader<?> getReaderFor(Schema readerSchema, Schema writerSchema,
      Conversion<?> explicitConversion) {
    final FieldReader<?> baseReader = getReaderForBaseType(readerSchema, writerSchema);
    return applyConversions(readerSchema, baseReader, explicitConversion);
  }

  private FieldReader<?> applyConversions(Schema readerSchema, FieldReader<?> reader,
      Conversion<?> explicitConversion) {
    Conversion<?> conversion = explicitConversion;

    if (conversion == null) {
      if (readerSchema.getLogicalType() == null) {
        return reader;
      }

      conversion = data.getConversionFor(readerSchema.getLogicalType());
    }

    if (conversion == null) {
      return reader;
    }

    switch (readerSchema.getType()) {
      case LONG:
        return new LongConversionReader<>(reader, conversion, readerSchema);
      case INT:
        return new IntegerConversionReader<>(reader, conversion, readerSchema);
      case BYTES:
        return new BytesConversionReader<>(reader, conversion, readerSchema);
      case FIXED:
        return new FixedConversionReader<>(reader, conversion, readerSchema);
      default:
        // use less optimized converter for all more exotic types
        return new GenericConversionReader<>(reader, conversion, readerSchema);
    }
  }

  private FieldReader<?> getReaderForBaseType(Schema readerSchema, Schema writerSchema) {
    if ( readerSchema.getType() != writerSchema.getType() ) {
      return resolveMismatchedReader( readerSchema, writerSchema );
    }

    switch (readerSchema.getType()) {
      case STRING:
        return createStringReader(readerSchema, writerSchema);
      case INT:
        return IntegerReader.get();
      case NULL:
        return NullReader.get();
      case RECORD:
        return createRecordReader(readerSchema, writerSchema);
      case DOUBLE:
        return DoubleReader.get();
      case UNION:
        return createUnionReader(readerSchema, writerSchema);
      case MAP:
        return createMapReader(readerSchema, writerSchema);
      case LONG:
        return LongReader.get();
      case ARRAY:
        return createArrayReader(readerSchema, writerSchema);
      case BOOLEAN:
        return BooleanReader.get();
      case BYTES:
        return BytesReader.get();
      case ENUM:
        return createEnumReader(readerSchema, writerSchema);
      case FIXED:
        return createFixedReader(readerSchema, writerSchema);
      case FLOAT:
        return FloatReader.get();
      default:
        return getFailureReaderOrFail("Type " + readerSchema.getType() + " not yet supported.");
    }
  }

  private FieldReader<?> resolveMismatchedReader( Schema readerSchema, Schema writerSchema ) {
    if ( writerSchema.getType() == Type.UNION ) { // and reader isnt union type
      Schema pseudoUnion = Schema.createUnion( readerSchema );
      return getReaderFor( pseudoUnion, writerSchema );
    }

    switch ( readerSchema.getType() ) {
      case UNION:
        Schema compatibleReaderSchema = findUnionType( writerSchema, readerSchema.getTypes() );
        if ( compatibleReaderSchema != null ) {
          return getReaderFor( compatibleReaderSchema, writerSchema );
        }
        break;

      case LONG:
        if ( writerSchema.getType() == Type.INT ) {
          return new LongPromotionReader( IntegerReader.get() );
        }
        break;

      case DOUBLE:
        switch ( writerSchema.getType() ) {
          case FLOAT: return new DoublePromotionReader(FloatReader.get());
          case LONG: return new DoublePromotionReader(LongReader.get());
          case INT:  return new DoublePromotionReader(IntegerReader.get());
          default:
        }
        break;

      case FLOAT:
        switch ( writerSchema.getType() ) {
          case LONG: return new FloatPromotionReader(LongReader.get());
          case INT: return new FloatPromotionReader(IntegerReader.get());
          default:
        }
        break;

      case STRING:
        if ( writerSchema.getType() == Type.BYTES ) {
          return createStringReader( readerSchema, readerSchema );
        }
        break;

      case BYTES:
        if ( writerSchema.getType() == Type.STRING ) {
          return BytesReader.get();
        }
        break;

      default:
    }

    return getSchemaMismatchError(readerSchema, writerSchema);
}

  private FieldReader<?> createStringReader(Schema readerSchema, Schema writerSchema) {
    FieldReader<?> stringReader = createSimpleStringReader(readerSchema);
    if (isClassPropEnabled()) {
      return getTransformingStringReader(readerSchema.getProp(SpecificData.CLASS_PROP),
          stringReader);
    } else {
      return stringReader;
    }
  }

  private FieldReader<?> createSimpleStringReader(Schema readerSchema) {
    String stringProperty = readerSchema.getProp(GenericData.STRING_PROP);
    if (GenericData.StringType.String.name().equals(stringProperty)) {
      return StringReader.get();
    } else {
      return Utf8Reader.get();
    }
  }

  private FieldReader<?> createUnionReader(Schema readerSchema, Schema writerSchema) {
    List<Schema> readerTypes = readerSchema.getTypes();
    List<Schema> writerTypes = writerSchema.getTypes();

    FieldReader<?>[] unionReaders = new FieldReader[writerTypes.size()];

    int i = 0;
    for (Schema thisWriterSchema : writerTypes) {
      Schema thisReaderSchema = findUnionType(thisWriterSchema, readerTypes);
      if (thisReaderSchema == null) {
        unionReaders[i++] = getFailureReaderOrFail("Found " + thisWriterSchema.getType().getName().toLowerCase() + ", expecting " + getUnionDescriptor(readerSchema));
      } else {
        unionReaders[i++] = getReaderFor(thisReaderSchema, thisWriterSchema);
      }
    }

    return new UnionReader(unionReaders);
  }


  private String getUnionDescriptor(Schema schema) {
    List<Schema> types = schema.getTypes();
    if ( types.size() == 1 ) {
      return schema.getTypes().get(0).getName().toLowerCase();
    }

    return "[" + schema.getTypes().stream().map( Schema::getName ).collect( Collectors.joining(",") ) + "]";
  }

  private Schema findUnionType(Schema schema, List<Schema> readerTypes) {
    // try for perfect match first
    for ( Schema thisReaderSchema : readerTypes ) {
      if ( thisReaderSchema.equals( schema ) ) {
        return thisReaderSchema;
      }
    }

    // then try for compatible
    for (Schema thisReaderSchema : readerTypes) {
      if ( areCompatible( thisReaderSchema, schema ) ) {
        return thisReaderSchema;
      }
    }
    return null;
  }


  private boolean areCompatible( Schema readerSchema, Schema writerSchema ) {
    try {
      FieldReader<?> reader = getReaderFor(readerSchema, writerSchema);
      return !( reader instanceof FailureReader );
    }
    catch ( Exception e ) {
      return false;
    }
  }

  private Schema.Field findSourceField(Schema.Field field, Schema writerSchema) {
    Schema.Field sourceField = findSourceFieldByName(field.name(), writerSchema);
    if (sourceField == null) {
      for (String thisAlias : emptyIfNull(field.aliases())) {
        sourceField = findSourceFieldByName(thisAlias, writerSchema);
        if (sourceField != null) {
          break;
        }
      }
    }
    return sourceField;
  }

  private Schema.Field findSourceFieldByName(String fieldName, Schema writerSchema) {
    for (Schema.Field thisWriterField : writerSchema.getFields()) {
      if (thisWriterField.name().equals(fieldName)) {
        return thisWriterField;
      }
      for (String thisAlias : emptyIfNull(thisWriterField.aliases())) {
        if (thisAlias.equals(fieldName)) {
          return thisWriterField;
        }
      }
    }
    return null;
  }

  private FieldReader<?> createMapReader(Schema readerSchema, Schema writerSchema) {
    FieldReader<?> keyReader = createMapKeyReader(readerSchema);
    FieldReader<?> valueReader =
        getReaderFor(readerSchema.getValueType(), writerSchema.getValueType());
    return new MapReader<>(keyReader, valueReader);
  }


  private FieldReader<?> createMapKeyReader(Schema readerSchema) {
    FieldReader<?> stringReader = createSimpleStringReader(readerSchema);
    if (isKeyClassEnabled()) {
      return getTransformingStringReader(readerSchema.getProp(SpecificData.KEY_CLASS_PROP),
          createSimpleStringReader(readerSchema));
    } else {
      return stringReader;
    }
  }


  private FieldReader<?> getTransformingStringReader(String valueClass,
      FieldReader<?> stringReader) {
    if (valueClass == null) {
      return stringReader;
    } else {
      Function<String, ?> transformer = findClass(valueClass)
          .map(clazz -> ReflectionUtils.getConstructorAsFunction(String.class, clazz)).orElse(null);

      if (transformer != null) {
        return new StringConversionReader(transformer, StringReader.get());
      }
    }

    return stringReader;
  }


  private Optional<Class<?>> findClass(String clazz) {
    try {
      return Optional.of(data.getClassLoader().loadClass(clazz));
    } catch (ReflectiveOperationException e) {
      return Optional.empty();
    }
  }

  private FieldReader<?> createArrayReader(Schema readerSchema, Schema writerSchema) {
    FieldReader<?> fieldReader =
        getReaderFor(readerSchema.getElementType(), writerSchema.getElementType());
    return ArrayReader.of(fieldReader, readerSchema);
  }

  private FieldReader<?> createEnumReader(Schema readerSchema, Schema writerSchema) {
    List<String> writerSymbols = writerSchema.getEnumSymbols();
    List<String> readerSymbols = readerSchema.getEnumSymbols();
    Object[] enumObjects = new Object[writerSymbols.size()];

    // pre-get all possible instances of the enum and cache them
    int i = 0;
    for (String thisWriterSymbol : writerSymbols) {
      if (readerSymbols.contains(thisWriterSymbol)) {
        enumObjects[i] = data.createEnum(thisWriterSymbol, readerSchema);
      } else if (isFailingFast()) {
        fail("Enum reader does not contain writer's symbol " + thisWriterSymbol);
      }
      i++;
    }

    return new EnumReader(enumObjects, writerSchema);
  }

  private FieldReader<?> createFixedReader(Schema readerSchema, Schema writerSchema) {
    if (readerSchema.getFixedSize() != writerSchema.getFixedSize()) {
      return getFailureReaderOrFail("Reader and writer schemas do not match. Fixed "
          + readerSchema.getName() + " expects " + readerSchema.getFixedSize()
          + " bytes, but writer is writing " + writerSchema.getFixedSize());
    }

    return new FixedReader(data, readerSchema);
  }

  private FailureReader getSchemaMismatchError(Schema readerSchema, Schema writerSchema) {
    return getFailureReaderOrFail( "Found " + writerSchema.getType() + ", expecting " + readerSchema.getType() );
  }

  private FailureReader getFailureReaderOrFail(String message) {
    if (failFast) {
      throw new InvalidDataException(message);
    } else {
      return new FailureReader(message);
    }
  }

  private void fail(String message) {
    throw new InvalidDataException(message);
  }

  private static <T> Collection<T> emptyIfNull(Collection<T> collection) {
    return (collection == null) ? Collections.emptyList() : collection;
  }
}
