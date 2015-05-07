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
package org.apache.avro.joda;

import com.google.common.primitives.Ints;
import org.apache.avro.buckets.GpBucket;
import org.apache.avro.buckets.GpBucketSet;
import org.apache.avro.nodes.GpDateCompareType;
import org.apache.avro.nodes.GpNodeCriteria;
import org.apache.avro.nodes.GpNodeSequence;
import org.apache.avro.nodes.GpNodeType;
import org.apache.avro.nodes.GpNodes;
import org.apache.avro.rules.GpCalcSet;
import org.apache.avro.rules.GpECNettingMatch;
import org.apache.avro.rules.GpECNettingSet;
import org.apache.avro.rules.GpFactor;
import org.apache.avro.rules.GpMetric;
import org.apache.avro.rules.GpNodeMetric;
import org.apache.avro.rules.GpRule;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.specific.ExtendedSpecificDatumReader;
import org.apache.avro.specific.ExtendedSpecificDatumWriter;
import org.joda.time.LocalDate;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author zfarkas
 */
public class TestComplex {


  @Test
  public void test() throws IOException {
      GpRule rule = createRule(0);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DatumWriter writer = new ExtendedSpecificDatumWriter(rule.getClass());
      Encoder encoder = new ExtendedJsonEncoder(rule.getSchema(), bos);
      writer.write(rule, encoder);
      encoder.flush();
      System.out.println("Json:" + bos.toString("UTF-8"));
      ExtendedSpecificDatumReader<GpRule> reader = new ExtendedSpecificDatumReader<GpRule>(rule.getSchema());
      Decoder decoder = new ExtendedJsonDecoder(rule.getSchema(), new ByteArrayInputStream(bos.toByteArray()), reader);
      GpRule read = reader.read(null, decoder);
      Assert.assertEquals(rule, read);
  }

  private static GpRule createRule(final int i) {
      GpNodes gpNodes = GpNodes.newBuilder().setAsOfDtValue(new LocalDate())
              .setAttributeId(i)
              .setAttributeIds(Ints.asList(1, 2, 3))
              .setGpNodeType(GpNodeType.DATE)
              .setDtComparisonType(GpDateCompareType.DATE_RANGE)
              .build();
      GpNodeSequence ns = GpNodeSequence.newBuilder().setNodeList(Arrays.asList(gpNodes)).build();
      GpNodeCriteria nc = GpNodeCriteria.newBuilder().setNodeSequenceList(Arrays.asList(ns)).build();
      GpBucket bucket = GpBucket.newBuilder().setBucketId(i)
              .setNodesCriteria(nc).build();
      GpBucketSet bucketSet = GpBucketSet.newBuilder().setBucketName("bucket " + i)
              .setBucketSetId(i)
              .setDescription("description " + i)
              .setBuckets(Arrays.asList(bucket)).build();


      GpMetric m = GpMetric.newBuilder().setFormula("1 + 1").setName("metric" + i).build();
      GpNodeMetric nm = GpNodeMetric.newBuilder().setMetric(m).build();

      GpCalcSet cs = GpCalcSet.newBuilder().setCalcSetId(i)
              .setName("cs " + i)
              .setNodeMetrics(Arrays.asList(nm)).build();

      GpFactor factor = GpFactor.newBuilder().setFactorId(i)
              .setRule(GpRule.newBuilder().setIsPercentageRule(Boolean.TRUE).build()).build();
      GpECNettingMatch ecnm = GpECNettingMatch.newBuilder().setExposureMetric(m)
              .setFactor(factor)
              .build();

      GpECNettingSet ecns = GpECNettingSet.newBuilder().setEcNettingMatch(Arrays.asList(ecnm)).build();
      return GpRule.newBuilder().setAcctNum(i)
              .setBucketSet(bucketSet)
              .setCalcSet(cs)
              .setEcNettingSet(ecns)
              .build();
    }



}
