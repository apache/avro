/*
 * Copyright 2015 The Apache Software Foundation.
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
package org.apache.avro.compiler.idl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SchemaResolver;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.ExtendedGenericDatumReader;
import org.apache.avro.generic.ExtendedGenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

/**
 *
 * @author zoly
 */
public class TestProtocol {

    @Test
    public void test() throws ParseException, IOException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Idl idl = new Idl(cl.getResourceAsStream("test/simple.avdl"),
                "UTF-8");
        Protocol protocol = idl.CompilationUnit();
        String json = protocol.toString(true);
        System.out.println(json);
    }
}
