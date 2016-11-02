/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.avro.Schema;

public interface DecimalEncoder {

  static boolean OPTIMIZED_JSON_DECIMAL_WRITE =
          Boolean.parseBoolean(System.getProperty("avro.optimized_decimal_write", "true"));

  void writeDecimal(final BigDecimal decimal, Schema schema) throws IOException;

  void writeBigInteger(final BigInteger decimal, Schema schema) throws IOException;

}
