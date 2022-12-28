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
package org.apache.avro.reflect;

import java.io.IOException;
import java.util.Date;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

/**
 * This is a null-safe version of {@link DateAsLongEncoding}.
 *
 * Java {@link Date} fields will be translated to an Avro union, `type:
 * ["null","long"]`.
 */
public class DateAsNullableLongEncoding extends CustomEncoding<Date> {

  private static final int UNION_INDEX_NULL = 0;
  private static final int UNION_INDEX_LONG = 1;

  public DateAsNullableLongEncoding() {
    schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
  }

  @Override
  protected final void write(Object datum, Encoder out) throws IOException {
    if (datum == null) {
      out.writeIndex(UNION_INDEX_NULL);
      out.writeNull();
    } else {
      out.writeIndex(UNION_INDEX_LONG);
      out.writeLong(((Date) datum).getTime());
    }
  }

  @Override
  protected final Date read(Object reuse, Decoder in) throws IOException {
    if (in.readIndex() == UNION_INDEX_NULL) {
      in.readNull();
      return null;
    } else {
      if (reuse instanceof Date) {
        ((Date) reuse).setTime(in.readLong());
        return (Date) reuse;
      } else {
        return new Date(in.readLong());
      }
    }
  }

}
