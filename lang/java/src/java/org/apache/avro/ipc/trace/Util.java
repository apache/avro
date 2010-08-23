/**
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

package org.apache.avro.ipc.trace;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/**
 * Utility methods for common tasks in Avro tracing. Mostly consists of
 * static methods which we can't put in auto-generated classes.
 */
class Util {
  final private static Random RANDOM = new Random();
  final private static int NANOS_PER_MILI = 1000000;
  
  /**
   * Get all SpanEvents contained in Span s.
   */
  public static EnumSet<SpanEvent> getAllEvents(Span s) {
    EnumSet<SpanEvent> foundEvents = EnumSet.noneOf(SpanEvent.class);
    for (TimestampedEvent event: s.events) {
      if (event.event instanceof SpanEvent) {
        foundEvents.add((SpanEvent) event.event);
      }
    }
    return foundEvents;
  }
  
  /**
   * Get the size of an RPC payload.
   */
  public static int getPayloadSize(List<ByteBuffer> payload) {
    if (payload == null) {
      return 0;
    }
    int size = 0;
    for (ByteBuffer bb: payload) {
      size = size + bb.limit();
    }
    return size;
  }
  
  /**
   * Create a span without any events. If traceID or spanID is null, randomly
   * generate them. If parentSpanID is null, assume this is a root span.
   */
  public static Span createEventlessSpan(ID traceID, ID spanID, ID parentSpanID) {
    Span span = new Span();
    span.complete = false;
    
    if (traceID == null) {
      byte[] traceIDBytes = new byte[8];
      RANDOM.nextBytes(traceIDBytes);
      span.traceID = new ID();
      span.traceID.bytes(traceIDBytes);
    } else {
      span.traceID = new ID();
      span.traceID.bytes(traceID.bytes().clone());
    }
    
    if (spanID == null) {
      byte[] spanIDBytes = new byte[8];
      RANDOM.nextBytes(spanIDBytes);
      span.spanID = new ID();
      span.spanID.bytes(spanIDBytes);
    } else {
      span.spanID = new ID();
      span.spanID.bytes(spanID.bytes().clone());
    }

    if (parentSpanID != null) {
      span.parentSpanID = new ID();
      span.parentSpanID.bytes(parentSpanID.bytes().clone());
    }
    
    span.events = new GenericData.Array<TimestampedEvent>(
        10, Schema.createArray(TimestampedEvent.SCHEMA$));
    return span;
  }
  
  /**
   * Add a TimestampedEvent to a Span using the current time. 
   */
  public static void addEvent(Span span, SpanEvent eventType) {
    TimestampedEvent ev = new TimestampedEvent();
    ev.event = eventType;
    ev.timeStamp = System.currentTimeMillis() * NANOS_PER_MILI;
    span.events.add(ev);
  }
  
  /**
   * Get the long value from a given ID object.
   */
  public static long longValue(ID in) {
    if (in == null) { 
      throw new IllegalArgumentException("ID cannot be null");
    }
    if (in.bytes() == null) {
      throw new IllegalArgumentException("ID cannot be empty");
    }
    if (in.bytes().length != 8) {
      throw new IllegalArgumentException("ID must be 8 bytes");
    }
    ByteBuffer buff = ByteBuffer.wrap(in.bytes());
    return buff.getLong();
  }
  
  /**
   * Get an ID associated with a given long value. 
   */
  public static ID idValue(long in) {
    byte[] bArray = new byte[8];
    ByteBuffer bBuffer = ByteBuffer.wrap(bArray);
    LongBuffer lBuffer = bBuffer.asLongBuffer();
    lBuffer.put(0, in);
    ID out = new ID();
    out.bytes(bArray);
    return out;
  }
  
  /**
   * Verify the equality of ID objects. Both being null references is
   * considered equal.
   */
  public static boolean idsEqual(ID a, ID b) {
    if (a == null && b == null) { return true; }
    if (a == null || b == null) { return false; }
    
    return Arrays.equals(a.bytes(), b.bytes());
  }
  
  /**
   * Convert a timeStamp (in nanoseconds) to a pretty string.
   */
  public static String printableTime(long stamp) {
    String out = "";
    double milliseconds = (double) stamp / (double) NANOS_PER_MILI;
    return String.format("%.2fms", milliseconds);
  }
  
  /**
   * Convert a bytes count to a pretty string.
   */
  public static String printableBytes(long bytes) {
    if (bytes < 1024) {
      return Long.toString(bytes) + "b";
    } else if (bytes < (1024 * 1024)) {
      double kb = (double) bytes / 1024.0;
      return String.format("%.2fkb", kb);
    } else {
      double mb = (double) bytes / (1024.0 * 1024.0);
      return String.format("%.2fmb", mb);
    }
  }

  /**
   * Tests if a span occurred between start and end.
   */
  public static boolean spanInRange(Span s, long start, long end) {
    long startTime = 0;
    long endTime = 0;
    
    for (TimestampedEvent e: s.events) {
      if (e.event instanceof SpanEvent) {
        SpanEvent ev = (SpanEvent) e.event;
        switch (ev) {
          case CLIENT_SEND: startTime = e.timeStamp;
          case SERVER_RECV: startTime = e.timeStamp;
          case CLIENT_RECV: endTime = e.timeStamp;
          case SERVER_SEND: endTime = e.timeStamp;
        }      
      }
    }
    if (startTime > start && endTime < end) { return true; }
    return false;
  }
}
