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
package org.apache.avro.io.fastreader.readers.promotion;

import java.io.IOException;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.fastreader.readers.FieldReader;

public class DoublePromotionReader implements FieldReader<Double> {

  private final FieldReader<? extends Number> numberReader;

  public DoublePromotionReader( FieldReader<? extends Number> numberReader ) {
    this.numberReader = numberReader;
  }

  @Override
  public Double read(Double reuse, Decoder decoder) throws IOException {
    return numberReader.read( null, decoder ).doubleValue();
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    numberReader.skip( decoder );
  }

}
