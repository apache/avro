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

public class JsonSchemaFormatterFactory implements SchemaFormatterFactory {

  public static final String VARIANT_NAME_PRETTY = "pretty";
  public static final String VARIANT_NAME_INLINE = "inline";

  @Override
  public SchemaFormatter getDefaultFormatter() {
    return getFormatterForVariant(VARIANT_NAME_PRETTY);
  }

  @Override
  public SchemaFormatter getFormatterForVariant(String variantName) {
    if (VARIANT_NAME_PRETTY.equals(variantName)) {
      return new JsonSchemaFormatter(true);
    } else if (VARIANT_NAME_INLINE.equals(variantName)) {
      return new JsonSchemaFormatter(false);
    } else {
      throw new AvroRuntimeException("Unknown JSON variant: " + variantName);
    }
  }
}
