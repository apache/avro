/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro;

public class CustomTypeConverter extends Conversion<CustomType> {
  private static final CustomTypeLogicalTypeFactory logicalTypeFactory = new CustomTypeLogicalTypeFactory();

  @Override
  public Class<CustomType> getConvertedType() {
    return CustomType.class;
  }

  @Override
  public String getLogicalTypeName() {
    return logicalTypeFactory.getTypeName();
  }

  @Override
  public Schema getRecommendedSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  @Override
  public CustomType fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    return new CustomType(value);
  }

  @Override
  public CharSequence toCharSequence(CustomType value, Schema schema, LogicalType type) {
    return value.getName();
  }
}
