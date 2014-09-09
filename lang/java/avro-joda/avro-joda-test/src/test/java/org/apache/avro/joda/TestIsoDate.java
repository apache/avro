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
import java.math.BigDecimal;
import java.nio.charset.Charset;
import junit.framework.Assert;
import org.apache.avro.AvroUtils;
import org.apache.avro.LogicalType;
import org.apache.avro.test.TestRecord;
import org.apache.avro.test.TestRecord2;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Test;

/**
 *
 * @author zfarkas
 */
public class TestIsoDate {
    
    @Test
    public void testIsoDate() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("logicalType", TextNode.valueOf("isodate"));
        LogicalType type = LogicalType.fromJsonNode(node);
        Assert.assertTrue("isodate logical type must be defined", type != null);
        System.out.println("Type is " + type);
    }
    
    @Test
    public void testSerialization() throws IOException {
        TestRecord record = TestRecord.newBuilder()
                .setDecimalVal(new BigDecimal("3.14"))
                .setDecimalVal2(new BigDecimal("3.14"))
                .setDecimalVal3(new BigDecimal("3.14"))
                .setIntVal(0)
                .setDoubleVal(3.5).setDateVal(new LocalDate())
                .setDateVal2(new LocalDate())
                .setDateVal3(new LocalDate())
                .setDateTimeVal(new DateTime())
                .build();
        byte [] result = AvroUtils.writeAvroBin(record);
        TestRecord record2 = AvroUtils.readAvroBin(result, TestRecord.class);
        Assert.assertEquals(record, record2);
        result = AvroUtils.writeAvroJson(record);
        System.out.println(new String(result, Charset.forName("UTF-8")));
        record2 = AvroUtils.readAvroJson(result, TestRecord.class);
        Assert.assertEquals(record, record2);
    }

    
    @Test
    public void testSerialization2() throws IOException {
        TestRecord record = TestRecord.newBuilder()
                .setDecimalVal(new BigDecimal("3.14"))
                .setDecimalVal2(new BigDecimal("3.14"))
                .setDecimalVal3(null)
                .setDecimalVal4(2)
                .setIntVal(0)
                .setDoubleVal(3.5).setDateVal(new LocalDate())
                .setDateVal2(new LocalDate())
                .setDateVal3(new LocalDate())
                .setDateTimeVal(new DateTime())
                .build();
        byte [] result = AvroUtils.writeAvroBin(record);
        TestRecord record2 = AvroUtils.readAvroBin(result, TestRecord.class);
        Assert.assertEquals(record, record2);
        result = AvroUtils.writeAvroJson(record);
        System.out.println(new String(result, Charset.forName("UTF-8")));
        record2 = AvroUtils.readAvroJson(result, TestRecord.class);
        Assert.assertEquals(record, record2);
    }
    
        @Test
    public void testSerializationFromIdl() throws IOException {
        TestRecord2 record = TestRecord2.newBuilder()
                .setDecimalVal(new BigDecimal("3.14"))
                .setDecimalVal2(new BigDecimal("3.14"))
                .setDecimalVal3(null)
                .setDecimalVal4(2)
                .setIntVal(0)
                .setDoubleVal(3.5).setDateVal(new LocalDate())
                .setDateVal2(new LocalDate())
                .setDateVal3(new LocalDate())
                .setDateTimeVal(new DateTime())
                .build();
        byte [] result = AvroUtils.writeAvroBin(record);
        TestRecord2 record2 = AvroUtils.readAvroBin(result, TestRecord2.class);
        Assert.assertEquals(record, record2);
        result = AvroUtils.writeAvroJson(record);
        System.out.println(new String(result, Charset.forName("UTF-8")));
        record2 = AvroUtils.readAvroJson(result, TestRecord2.class);
        Assert.assertEquals(record, record2);
    }

    
    

    
    
    
    
}
