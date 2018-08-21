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

package org.apache.avro.codegentest;

import org.apache.avro.codegentest.testdata.LogicalTypesWithCustomConversion;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;

public class TestCustomConversion {

    @Test
    public void testNullValues() throws IOException {
        LogicalTypesWithCustomConversion instanceOfGeneratedClass = LogicalTypesWithCustomConversion.newBuilder()
                .setNonNullCustomField(new CustomDecimal(BigInteger.valueOf(100), 2))
                .build();
        final byte[] serialized = serialize(instanceOfGeneratedClass);
        final LogicalTypesWithCustomConversion copy = deserialize(serialized);
        Assert.assertEquals(instanceOfGeneratedClass.getNonNullCustomField(), copy.getNonNullCustomField());
        Assert.assertEquals(instanceOfGeneratedClass.getNullableCustomField(), copy.getNullableCustomField());
    }

    @Test
    public void testNonNullValues() throws IOException {
        LogicalTypesWithCustomConversion instanceOfGeneratedClass = LogicalTypesWithCustomConversion.newBuilder()
                .setNonNullCustomField(new CustomDecimal(BigInteger.valueOf(100), 2))
                .setNullableCustomField(new CustomDecimal(BigInteger.valueOf(3000), 2))
                .build();
        final byte[] serialized = serialize(instanceOfGeneratedClass);
        final LogicalTypesWithCustomConversion copy = deserialize(serialized);
        Assert.assertEquals(instanceOfGeneratedClass.getNullableCustomField(), copy.getNullableCustomField());
        Assert.assertEquals(instanceOfGeneratedClass.getNonNullCustomField(), copy.getNonNullCustomField());
    }

    private byte[] serialize(LogicalTypesWithCustomConversion object) {
        SpecificDatumWriter<LogicalTypesWithCustomConversion> datumWriter = new SpecificDatumWriter<>(LogicalTypesWithCustomConversion.getClassSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            datumWriter.write(object, EncoderFactory.get().directBinaryEncoder(outputStream, null));
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private LogicalTypesWithCustomConversion deserialize(byte[] bytes) {
        SpecificDatumReader<LogicalTypesWithCustomConversion> datumReader = new SpecificDatumReader<>(LogicalTypesWithCustomConversion.getClassSchema(), LogicalTypesWithCustomConversion.getClassSchema());
        try {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            return datumReader.read(null, DecoderFactory.get().directBinaryDecoder(byteArrayInputStream, null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
