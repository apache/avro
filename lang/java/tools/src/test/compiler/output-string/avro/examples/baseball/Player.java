/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package avro.examples.baseball;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

/** 選手 is Japanese for player. */
@org.apache.avro.specific.AvroGenerated
public class Player extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 3865593031278745715L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Player\",\"namespace\":\"avro.examples.baseball\",\"doc\":\"選手 is Japanese for player.\",\"fields\":[{\"name\":\"number\",\"type\":\"int\",\"doc\":\"The number of the player\"},{\"name\":\"first_name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"last_name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"position\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"enum\",\"name\":\"Position\",\"symbols\":[\"P\",\"C\",\"B1\",\"B2\",\"B3\",\"SS\",\"LF\",\"CF\",\"RF\",\"DH\"]}}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Player> ENCODER =
      new BinaryMessageEncoder<Player>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Player> DECODER =
      new BinaryMessageDecoder<Player>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<Player> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<Player> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<Player> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Player>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this Player to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a Player from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a Player instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static Player fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  /** The number of the player */
  private int number;
  private java.lang.String first_name;
  private java.lang.String last_name;
  private java.util.List<avro.examples.baseball.Position> position;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Player() {}

  /**
   * All-args constructor.
   * @param number The number of the player
   * @param first_name The new value for first_name
   * @param last_name The new value for last_name
   * @param position The new value for position
   */
  public Player(java.lang.Integer number, java.lang.String first_name, java.lang.String last_name, java.util.List<avro.examples.baseball.Position> position) {
    this.number = number;
    this.first_name = first_name;
    this.last_name = last_name;
    this.position = position;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return number;
    case 1: return first_name;
    case 2: return last_name;
    case 3: return position;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
      case 0:
        number = (java.lang.Integer)value$;
        break;
      case 1:
        first_name = value$ != null ? value$.toString() : null;
        break;
      case 2:
        last_name = value$ != null ? value$.toString() : null;
        break;
      case 3:
        position = (java.util.List<avro.examples.baseball.Position>)value$;
        break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'number' field.
   * @return The number of the player
   */
  public int getNumber() {
    return number;
  }


  /**
   * Sets the value of the 'number' field.
   * The number of the player
   * @param value the value to set.
   */
  public void setNumber(int value) {
    this.number = value;
  }

  /**
   * Gets the value of the 'first_name' field.
   * @return The value of the 'first_name' field.
   */
  public java.lang.String getFirstName() {
    return first_name;
  }


  /**
   * Sets the value of the 'first_name' field.
   * @param value the value to set.
   */
  public void setFirstName(java.lang.String value) {
    this.first_name = value;
  }

  /**
   * Gets the value of the 'last_name' field.
   * @return The value of the 'last_name' field.
   */
  public java.lang.String getLastName() {
    return last_name;
  }


  /**
   * Sets the value of the 'last_name' field.
   * @param value the value to set.
   */
  public void setLastName(java.lang.String value) {
    this.last_name = value;
  }

  /**
   * Gets the value of the 'position' field.
   * @return The value of the 'position' field.
   */
  public java.util.List<avro.examples.baseball.Position> getPosition() {
    return position;
  }


  /**
   * Sets the value of the 'position' field.
   * @param value the value to set.
   */
  public void setPosition(java.util.List<avro.examples.baseball.Position> value) {
    this.position = value;
  }

  /**
   * Creates a new Player RecordBuilder.
   * @return A new Player RecordBuilder
   */
  public static avro.examples.baseball.Player.Builder newBuilder() {
    return new avro.examples.baseball.Player.Builder();
  }

  /**
   * Creates a new Player RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Player RecordBuilder
   */
  public static avro.examples.baseball.Player.Builder newBuilder(avro.examples.baseball.Player.Builder other) {
    if (other == null) {
      return new avro.examples.baseball.Player.Builder();
    } else {
      return new avro.examples.baseball.Player.Builder(other);
    }
  }

  /**
   * Creates a new Player RecordBuilder by copying an existing Player instance.
   * @param other The existing instance to copy.
   * @return A new Player RecordBuilder
   */
  public static avro.examples.baseball.Player.Builder newBuilder(avro.examples.baseball.Player other) {
    if (other == null) {
      return new avro.examples.baseball.Player.Builder();
    } else {
      return new avro.examples.baseball.Player.Builder(other);
    }
  }

  /**
   * RecordBuilder for Player instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Player>
    implements org.apache.avro.data.RecordBuilder<Player> {

    /** The number of the player */
    private int number;
    private java.lang.String first_name;
    private java.lang.String last_name;
    private java.util.List<avro.examples.baseball.Position> position;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(avro.examples.baseball.Player.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.number)) {
        this.number = data().deepCopy(fields()[0].schema(), other.number);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.first_name)) {
        this.first_name = data().deepCopy(fields()[1].schema(), other.first_name);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.last_name)) {
        this.last_name = data().deepCopy(fields()[2].schema(), other.last_name);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.position)) {
        this.position = data().deepCopy(fields()[3].schema(), other.position);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing Player instance
     * @param other The existing instance to copy.
     */
    private Builder(avro.examples.baseball.Player other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.number)) {
        this.number = data().deepCopy(fields()[0].schema(), other.number);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.first_name)) {
        this.first_name = data().deepCopy(fields()[1].schema(), other.first_name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.last_name)) {
        this.last_name = data().deepCopy(fields()[2].schema(), other.last_name);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.position)) {
        this.position = data().deepCopy(fields()[3].schema(), other.position);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'number' field.
      * The number of the player
      * @return The value.
      */
    public int getNumber() {
      return number;
    }


    /**
      * Sets the value of the 'number' field.
      * The number of the player
      * @param value The value of 'number'.
      * @return This builder.
      */
    public avro.examples.baseball.Player.Builder setNumber(int value) {
      validate(fields()[0], value);
      this.number = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'number' field has been set.
      * The number of the player
      * @return True if the 'number' field has been set, false otherwise.
      */
    public boolean hasNumber() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'number' field.
      * The number of the player
      * @return This builder.
      */
    public avro.examples.baseball.Player.Builder clearNumber() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'first_name' field.
      * @return The value.
      */
    public java.lang.String getFirstName() {
      return first_name;
    }


    /**
      * Sets the value of the 'first_name' field.
      * @param value The value of 'first_name'.
      * @return This builder.
      */
    public avro.examples.baseball.Player.Builder setFirstName(java.lang.String value) {
      validate(fields()[1], value);
      this.first_name = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'first_name' field has been set.
      * @return True if the 'first_name' field has been set, false otherwise.
      */
    public boolean hasFirstName() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'first_name' field.
      * @return This builder.
      */
    public avro.examples.baseball.Player.Builder clearFirstName() {
      first_name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'last_name' field.
      * @return The value.
      */
    public java.lang.String getLastName() {
      return last_name;
    }


    /**
      * Sets the value of the 'last_name' field.
      * @param value The value of 'last_name'.
      * @return This builder.
      */
    public avro.examples.baseball.Player.Builder setLastName(java.lang.String value) {
      validate(fields()[2], value);
      this.last_name = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'last_name' field has been set.
      * @return True if the 'last_name' field has been set, false otherwise.
      */
    public boolean hasLastName() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'last_name' field.
      * @return This builder.
      */
    public avro.examples.baseball.Player.Builder clearLastName() {
      last_name = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'position' field.
      * @return The value.
      */
    public java.util.List<avro.examples.baseball.Position> getPosition() {
      return position;
    }


    /**
      * Sets the value of the 'position' field.
      * @param value The value of 'position'.
      * @return This builder.
      */
    public avro.examples.baseball.Player.Builder setPosition(java.util.List<avro.examples.baseball.Position> value) {
      validate(fields()[3], value);
      this.position = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'position' field has been set.
      * @return True if the 'position' field has been set, false otherwise.
      */
    public boolean hasPosition() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'position' field.
      * @return This builder.
      */
    public avro.examples.baseball.Player.Builder clearPosition() {
      position = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Player build() {
      try {
        Player record = new Player();
        record.number = fieldSetFlags()[0] ? this.number : (java.lang.Integer) defaultValue(fields()[0]);
        record.first_name = fieldSetFlags()[1] ? this.first_name : (java.lang.String) defaultValue(fields()[1]);
        record.last_name = fieldSetFlags()[2] ? this.last_name : (java.lang.String) defaultValue(fields()[2]);
        record.position = fieldSetFlags()[3] ? this.position : (java.util.List<avro.examples.baseball.Position>) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Player>
    WRITER$ = (org.apache.avro.io.DatumWriter<Player>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Player>
    READER$ = (org.apache.avro.io.DatumReader<Player>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeInt(this.number);

    out.writeString(this.first_name);

    out.writeString(this.last_name);

    long size0 = this.position.size();
    out.writeArrayStart();
    out.setItemCount(size0);
    long actualSize0 = 0;
    for (avro.examples.baseball.Position e0: this.position) {
      actualSize0++;
      out.startItem();
      out.writeEnum(e0.ordinal());
    }
    out.writeArrayEnd();
    if (actualSize0 != size0)
      throw new java.util.ConcurrentModificationException("Array-size written was " + size0 + ", but element count was " + actualSize0 + ".");

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.number = in.readInt();

      this.first_name = in.readString();

      this.last_name = in.readString();

      long size0 = in.readArrayStart();
      java.util.List<avro.examples.baseball.Position> a0 = this.position;
      if (a0 == null) {
        a0 = new SpecificData.Array<avro.examples.baseball.Position>((int)size0, SCHEMA$.getField("position").schema());
        this.position = a0;
      } else a0.clear();
      SpecificData.Array<avro.examples.baseball.Position> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<avro.examples.baseball.Position>)a0 : null);
      for ( ; 0 < size0; size0 = in.arrayNext()) {
        for ( ; size0 != 0; size0--) {
          avro.examples.baseball.Position e0 = (ga0 != null ? ga0.peek() : null);
          e0 = avro.examples.baseball.Position.values()[in.readEnum()];
          a0.add(e0);
        }
      }

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.number = in.readInt();
          break;

        case 1:
          this.first_name = in.readString();
          break;

        case 2:
          this.last_name = in.readString();
          break;

        case 3:
          long size0 = in.readArrayStart();
          java.util.List<avro.examples.baseball.Position> a0 = this.position;
          if (a0 == null) {
            a0 = new SpecificData.Array<avro.examples.baseball.Position>((int)size0, SCHEMA$.getField("position").schema());
            this.position = a0;
          } else a0.clear();
          SpecificData.Array<avro.examples.baseball.Position> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<avro.examples.baseball.Position>)a0 : null);
          for ( ; 0 < size0; size0 = in.arrayNext()) {
            for ( ; size0 != 0; size0--) {
              avro.examples.baseball.Position e0 = (ga0 != null ? ga0.peek() : null);
              e0 = avro.examples.baseball.Position.values()[in.readEnum()];
              a0.add(e0);
            }
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}











