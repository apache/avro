package org.apache.avro.ipc.trace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestTracingUtil {
  @SuppressWarnings("unchecked")
  @Test
  public void testListSampling() {
    List<Long> in1 = (List<Long>) 
      Arrays.asList(new Long[] {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L});
    List<Long> out1 = Util.sampledList(in1, 4);
    assertTrue(Arrays.equals(out1.toArray(), new Long[] {0L, 3L, 6L }));
    
    List<Long> in2 = new ArrayList<Long>(50000);
    for (int i = 0; i < 50000; i++) {
      in2.add((long)i);
    }
    
    List<Long> out2 = Util.sampledList(in2, 1000);
    assertEquals(1000, out2.size());
  }
}
