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
package org.apache.avro.io.parsing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;


public class SymbolTest {


    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"SampleNode\","
            + "\"namespace\":\"org.spf4j.ssdump2.avro\",\n" +
" \"fields\":[\n" +
"    {\"name\":\"count\",\"type\":\"int\",\"default\":0},\n" +
"    {\"name\":\"otherNode\",\"type\":[\"null\",\"SampleNode\"], \"default\" : null},\n" +
"    {\"name\":\"subNodes\",\"type\":\n" +
"       {\"type\":\"array\",\"items\":{\n" +
"           \"type\":\"record\",\"name\":\"SamplePair\",\n" +
"           \"fields\":[\n" +
"              {\"name\":\"method\",\"type\":\n" +
"                  {\"type\":\"record\",\"name\":\"Method\",\n" +
"                  \"fields\":[\n" +
"                     {\"name\":\"declaringClass\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},\n" +
"                     {\"name\":\"methodName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}\n" +
"                  ]}},\n" +
"              {\"name\":\"node\",\"type\":\"SampleNode\"},"
            + "{\"name\":\"otherNode\",\"type\":\"SampleNode\"}]}}}"
            + ",{\"name\":\"subNodes2\",\"type\":\n" +
"       {\"type\":\"array\",\"items\": \"SampleNode\"}}"
           + ",{\"name\":\"subNodes3\",\"type\":\n" +
"       {\"type\":\"array\",\"items\": \"SampleNode\"}} ]}";


    @Test
    public void testValidSymbolTree() throws IOException {
        Schema schema = new Schema.Parser().parse(SCHEMA);

        Symbol root = Symbol.root(new ResolvingGrammarGenerator()
                .generate(schema, schema, new HashMap<ValidatingGrammarGenerator.LitS, Symbol>()));
        validateNonNull(root, new HashSet<Symbol>());

        Schema samplePairSchema = schema.getField("subNodes").schema().getElementType();
        Schema methodSchema = samplePairSchema.getField("method").schema();

        GenericData.Record method = new GenericData.Record(methodSchema);
        method.put("methodName", "m1");
        method.put("declaringClass", "c1");

        GenericData.Record samplePair = new GenericData.Record(samplePairSchema);
        samplePair.put("method", method);

        GenericData.Record sampleNode1 = new GenericData.Record(schema);
        sampleNode1.put("subNodes", Collections.EMPTY_LIST);
        sampleNode1.put("subNodes2", Collections.EMPTY_LIST);
        sampleNode1.put("subNodes3", Collections.EMPTY_LIST);
        sampleNode1.put("count", 0);
        sampleNode1.put("otherNode", null);

        samplePair.put("node", sampleNode1);
        samplePair.put("otherNode", sampleNode1);

        GenericData.Record sampleNode2 = new GenericData.Record(schema);
        sampleNode2.put("subNodes", Arrays.asList(samplePair));
        sampleNode2.put("subNodes2", Arrays.asList(sampleNode1));
        sampleNode2.put("subNodes3", Arrays.asList(sampleNode1));
        sampleNode2.put("count", 0);
        sampleNode2.put("otherNode", null);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GenericDatumWriter writer = new GenericDatumWriter(schema);
        BinaryEncoder directBinaryEncoder = new EncoderFactory().directBinaryEncoder(bos, null);
        writer.write(sampleNode2, directBinaryEncoder);

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        GenericDatumReader reader = new GenericDatumReader(schema);
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(bis, null);
        Object read = reader.read(null, binaryDecoder);

        System.out.println(read);
        Assert.assertEquals(read, sampleNode2);

    }


/**
 * Test IDL:
 *
@namespace("test")
protocol NodeTest {

    record Node {

        string id = "";

        array<NodePair> nodes = [];

    }

    record NodePair {
       Node node1;
       Node node2;

    }

}
*/

    public static final Schema NODE_SCHEMA = new Schema.Parser().parse("{\"type\":\"record\",\"name\":"
            + "\"Node\",\"namespace\":\"test\",\"doc\":\"test node\",\"fields\":[{\"name\":\"id\","
            + "\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"identifier\",\"default\":\"\"},"
            + "{\"name\":\"nodes\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"NodePair\","
            + "\"doc\":\"test node pair\",\"fields\":[{\"name\":\"node1\",\"type\":\"Node\",\"doc\":\"node 1\"},"
            + "{\"name\":\"node2\",\"type\":\"Node\",\"doc\":\"node 2\"}]}},\"doc\":\"sub nodes\",\"default\":[]}]}");
    public static final Schema NOPAIR_SCHEMA = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"NodePair\","
            + "\"namespace\":\"test\",\"doc\":\"test node pair\",\"fields\":[{\"name\":\"node1\","
            + "\"type\":{\"type\":\"record\",\"name\":\"Node\",\"doc\":\"test node\",\"fields\":[{\"name\":\"id\","
            + "\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"identifier\",\"default\":\"\"},"
            + "{\"name\":\"nodes\",\"type\":{\"type\":\"array\",\"items\":\"NodePair\"},\"doc\":\"sub nodes\","
            + "\"default\":[]}]},\"doc\":\"node 1\"},{\"name\":\"node2\",\"type\":\"Node\",\"doc\":\"node 2\"}]}");


   @Test
    public void testValidSymbolTree2() throws IOException {
 
        Symbol root = Symbol.root(new ResolvingGrammarGenerator()
                .generate(NODE_SCHEMA, NODE_SCHEMA, new HashMap<ValidatingGrammarGenerator.LitS, Symbol>()));
        validateNonNull(root, new HashSet<Symbol>());
        root = Symbol.root(new ResolvingGrammarGenerator()
                .generate(NOPAIR_SCHEMA, NOPAIR_SCHEMA, new HashMap<ValidatingGrammarGenerator.LitS, Symbol>()));
        validateNonNull(root, new HashSet<Symbol>());

        GenericData.Record node1 = new GenericData.Record(NODE_SCHEMA);
        node1.put("id", "1");
        node1.put("nodes", Collections.EMPTY_LIST);

        GenericData.Record node2 = new GenericData.Record(NODE_SCHEMA);
        node2.put("id", "1");
        node2.put("nodes", Collections.EMPTY_LIST);


        GenericData.Record pair = new GenericData.Record(NOPAIR_SCHEMA);
        pair.put("node1", node1);
        pair.put("node2", node2);

        GenericData.Record node = new GenericData.Record(NODE_SCHEMA);
        node.put("id", "Root");
        node.put("nodes", Arrays.asList(pair));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GenericDatumWriter writer = new GenericDatumWriter(NODE_SCHEMA);
        BinaryEncoder directBinaryEncoder = new EncoderFactory().directBinaryEncoder(bos, null);
        writer.write(node, directBinaryEncoder);

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        GenericDatumReader reader = new GenericDatumReader(NODE_SCHEMA);
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(bis, null);
        Object read = reader.read(null, binaryDecoder);

        System.out.println(read);
        Assert.assertEquals(read, node);

    }




    private static void validateNonNull(final Symbol symb, Set<Symbol> seen) {
        if (seen.contains(symb)) {
            return;
        } else {
            seen.add(symb);
        }
        if (symb.production != null) {
            for (Symbol s : symb.production) {
                if (s == null) {
                    Assert.fail("invalid parsing tree should not contain nulls");
                }
                if (s.kind != Symbol.Kind.ROOT) {
                    validateNonNull(s, seen);;
                }
            }
        }
    }

}
