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

import org.apache.avro.codegentest.testdata.LogicalTypesWithDefaults;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.joda.time.LocalDate;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class TestLogicalTypesWithDefaults {

    LocalDate DEFAULT_VALUE = LocalDate.parse("1973-05-19");

    @Test
    public void testDefaultValueOfNullableField() throws IOException {
        LogicalTypesWithDefaults instanceOfGeneratedClass = LogicalTypesWithDefaults.newBuilder()
                .setNonNullDate(LocalDate.now())
                .build();
        final byte[] serialized = serialize(instanceOfGeneratedClass);
        final LogicalTypesWithDefaults copy = deserialize(serialized);
        Assert.assertEquals(DEFAULT_VALUE, instanceOfGeneratedClass.getNullableDate());
        Assert.assertEquals(instanceOfGeneratedClass.getNullableDate(), copy.getNullableDate());
        Assert.assertEquals(instanceOfGeneratedClass.getNonNullDate(), copy.getNonNullDate());
    }

    @Test
    public void testDefaultValueOfNonNullField() throws IOException {
        LogicalTypesWithDefaults instanceOfGeneratedClass = LogicalTypesWithDefaults.newBuilder()
                .setNullableDate(LocalDate.now())
                .build();
        final byte[] serialized = serialize(instanceOfGeneratedClass);
        final LogicalTypesWithDefaults copy = deserialize(serialized);
        Assert.assertEquals(DEFAULT_VALUE, instanceOfGeneratedClass.getNonNullDate());
        Assert.assertEquals(instanceOfGeneratedClass.getNullableDate(), copy.getNullableDate());
        Assert.assertEquals(instanceOfGeneratedClass.getNonNullDate(), copy.getNonNullDate());
    }

    @Test
    public void testWithValues() throws IOException {
        LogicalTypesWithDefaults instanceOfGeneratedClass = LogicalTypesWithDefaults.newBuilder()
                .setNullableDate(LocalDate.now())
                .setNonNullDate(LocalDate.now())
                .build();
        final byte[] serialized = serialize(instanceOfGeneratedClass);
        final LogicalTypesWithDefaults copy = deserialize(serialized);
        Assert.assertEquals(instanceOfGeneratedClass.getNullableDate(), copy.getNullableDate());
        Assert.assertEquals(instanceOfGeneratedClass.getNonNullDate(), copy.getNonNullDate());
    }

    private byte[] serialize(LogicalTypesWithDefaults object) {
        SpecificDatumWriter<LogicalTypesWithDefaults> datumWriter = new SpecificDatumWriter<>(LogicalTypesWithDefaults.getClassSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            datumWriter.write(object, EncoderFactory.get().directBinaryEncoder(outputStream, null));
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private LogicalTypesWithDefaults deserialize(byte[] bytes) {
        SpecificDatumReader<LogicalTypesWithDefaults> datumReader = new SpecificDatumReader<>(LogicalTypesWithDefaults.getClassSchema(), LogicalTypesWithDefaults.getClassSchema());
        try {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            return datumReader.read(null, DecoderFactory.get().directBinaryDecoder(byteArrayInputStream, null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
