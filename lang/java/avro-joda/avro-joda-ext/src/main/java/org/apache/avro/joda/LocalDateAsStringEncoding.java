/*
 * Copyright 2014 The Apache Software Foundation.
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
package org.apache.avro.joda;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;
import org.joda.time.LocalDate;


/**
 *
 * @author zfarkas
 */
public class LocalDateAsStringEncoding extends CustomEncoding<LocalDate>{

  {
    schema = Schema.create(Schema.Type.STRING);
    schema.addProp("CustomEncoding", "LocalDateAsStringEncoding");
  }
  
  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    out.writeString(IsoDate.FMT.print((LocalDate) datum));
  }

  @Override
  protected LocalDate read(Object reuse, Decoder in) throws IOException {
    return IsoDate.FMT.parseLocalDate(in.readString());
  }
  
}
