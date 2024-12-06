// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/test/protobuf/test_multiple_files.proto

// Protobuf Java Version: 4.26.1
package org.apache.avro.protobuf.multiplefiles;

/**
 * <pre>
 * a nested enum
 * </pre>
 *
 * Protobuf type {@code org.apache.avro.protobuf.multiplefiles.M}
 */
public final class M extends com.google.protobuf.GeneratedMessage implements
    // @@protoc_insertion_point(message_implements:org.apache.avro.protobuf.multiplefiles.M)
    MOrBuilder {
  private static final long serialVersionUID = 0L;
  static {
    com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
        com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC, /* major= */ 4, /* minor= */ 26, /* patch= */ 1,
        /* suffix= */ "", M.class.getName());
  }

  // Use M.newBuilder() to construct.
  private M(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
    super(builder);
  }

  private M() {
  }

  public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
    return org.apache.avro.protobuf.multiplefiles.TestMultipleFiles.internal_static_org_apache_avro_protobuf_multiplefiles_M_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
    return org.apache.avro.protobuf.multiplefiles.TestMultipleFiles.internal_static_org_apache_avro_protobuf_multiplefiles_M_fieldAccessorTable
        .ensureFieldAccessorsInitialized(org.apache.avro.protobuf.multiplefiles.M.class,
            org.apache.avro.protobuf.multiplefiles.M.Builder.class);
  }

  /**
   * Protobuf enum {@code org.apache.avro.protobuf.multiplefiles.M.N}
   */
  public enum N implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>A = 1;</code>
     */
    A(1),;

    static {
      com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
          com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC, /* major= */ 4, /* minor= */ 26, /* patch= */ 1,
          /* suffix= */ "", N.class.getName());
    }
    /**
     * <code>A = 1;</code>
     */
    public static final int A_VALUE = 1;

    public final int getNumber() {
      return value;
    }

    /**
     * @param value The numeric wire value of the corresponding enum entry.
     * @return The enum associated with the given numeric wire value.
     * @deprecated Use {@link #forNumber(int)} instead.
     */
    @java.lang.Deprecated
    public static N valueOf(int value) {
      return forNumber(value);
    }

    /**
     * @param value The numeric wire value of the corresponding enum entry.
     * @return The enum associated with the given numeric wire value.
     */
    public static N forNumber(int value) {
      switch (value) {
      case 1:
        return A;
      default:
        return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<N> internalGetValueMap() {
      return internalValueMap;
    }

    private static final com.google.protobuf.Internal.EnumLiteMap<N> internalValueMap = new com.google.protobuf.Internal.EnumLiteMap<N>() {
      public N findValueByNumber(int number) {
        return N.forNumber(number);
      }
    };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor getValueDescriptor() {
      return getDescriptor().getValues().get(ordinal());
    }

    public final com.google.protobuf.Descriptors.EnumDescriptor getDescriptorForType() {
      return getDescriptor();
    }

    public static final com.google.protobuf.Descriptors.EnumDescriptor getDescriptor() {
      return org.apache.avro.protobuf.multiplefiles.M.getDescriptor().getEnumTypes().get(0);
    }

    private static final N[] VALUES = values();

    public static N valueOf(com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException("EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }

    private final int value;

    private N(int value) {
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:org.apache.avro.protobuf.multiplefiles.M.N)
  }

  private byte memoizedIsInitialized = -1;

  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1)
      return true;
    if (isInitialized == 0)
      return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output) throws java.io.IOException {
    getUnknownFields().writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1)
      return size;

    size = 0;
    size += getUnknownFields().getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof org.apache.avro.protobuf.multiplefiles.M)) {
      return super.equals(obj);
    }
    org.apache.avro.protobuf.multiplefiles.M other = (org.apache.avro.protobuf.multiplefiles.M) obj;

    if (!getUnknownFields().equals(other.getUnknownFields()))
      return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (29 * hash) + getUnknownFields().hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage.parseWithIOException(PARSER, input);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry) throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseDelimitedFrom(java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry) throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage.parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage.parseWithIOException(PARSER, input);
  }

  public static org.apache.avro.protobuf.multiplefiles.M parseFrom(com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry) throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() {
    return newBuilder();
  }

  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }

  public static Builder newBuilder(org.apache.avro.protobuf.multiplefiles.M prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }

  @java.lang.Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(com.google.protobuf.GeneratedMessage.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }

  /**
   * <pre>
   * a nested enum
   * </pre>
   *
   * Protobuf type {@code org.apache.avro.protobuf.multiplefiles.M}
   */
  public static final class Builder extends com.google.protobuf.GeneratedMessage.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:org.apache.avro.protobuf.multiplefiles.M)
      org.apache.avro.protobuf.multiplefiles.MOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.avro.protobuf.multiplefiles.TestMultipleFiles.internal_static_org_apache_avro_protobuf_multiplefiles_M_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
      return org.apache.avro.protobuf.multiplefiles.TestMultipleFiles.internal_static_org_apache_avro_protobuf_multiplefiles_M_fieldAccessorTable
          .ensureFieldAccessorsInitialized(org.apache.avro.protobuf.multiplefiles.M.class,
              org.apache.avro.protobuf.multiplefiles.M.Builder.class);
    }

    // Construct using org.apache.avro.protobuf.multiplefiles.M.newBuilder()
    private Builder() {

    }

    private Builder(com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      super(parent);

    }

    @java.lang.Override
    public Builder clear() {
      super.clear();
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
      return org.apache.avro.protobuf.multiplefiles.TestMultipleFiles.internal_static_org_apache_avro_protobuf_multiplefiles_M_descriptor;
    }

    @java.lang.Override
    public org.apache.avro.protobuf.multiplefiles.M getDefaultInstanceForType() {
      return org.apache.avro.protobuf.multiplefiles.M.getDefaultInstance();
    }

    @java.lang.Override
    public org.apache.avro.protobuf.multiplefiles.M build() {
      org.apache.avro.protobuf.multiplefiles.M result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public org.apache.avro.protobuf.multiplefiles.M buildPartial() {
      org.apache.avro.protobuf.multiplefiles.M result = new org.apache.avro.protobuf.multiplefiles.M(this);
      onBuilt();
      return result;
    }

    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof org.apache.avro.protobuf.multiplefiles.M) {
        return mergeFrom((org.apache.avro.protobuf.multiplefiles.M) other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(org.apache.avro.protobuf.multiplefiles.M other) {
      if (other == org.apache.avro.protobuf.multiplefiles.M.getDefaultInstance())
        return this;
      this.mergeUnknownFields(other.getUnknownFields());
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry) throws java.io.IOException {
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!super.parseUnknownField(input, extensionRegistry, tag)) {
              done = true; // was an endgroup tag
            }
            break;
          } // default:
          } // switch (tag)
        } // while (!done)
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      } finally {
        onChanged();
      } // finally
      return this;
    }

    // @@protoc_insertion_point(builder_scope:org.apache.avro.protobuf.multiplefiles.M)
  }

  // @@protoc_insertion_point(class_scope:org.apache.avro.protobuf.multiplefiles.M)
  private static final org.apache.avro.protobuf.multiplefiles.M DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new org.apache.avro.protobuf.multiplefiles.M();
  }

  public static org.apache.avro.protobuf.multiplefiles.M getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<M> PARSER = new com.google.protobuf.AbstractParser<M>() {
    @java.lang.Override
    public M parsePartialFrom(com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      Builder builder = newBuilder();
      try {
        builder.mergeFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(builder.buildPartial());
      } catch (com.google.protobuf.UninitializedMessageException e) {
        throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(e).setUnfinishedMessage(builder.buildPartial());
      }
      return builder.buildPartial();
    }
  };

  public static com.google.protobuf.Parser<M> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<M> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public org.apache.avro.protobuf.multiplefiles.M getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
