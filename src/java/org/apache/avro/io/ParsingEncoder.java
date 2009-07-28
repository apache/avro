package org.apache.avro.io;

import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.AvroTypeException;

/** Base class for <a href="parsing/package-summary.html">parser</a>-based
 * {@link Encoder}s. */
public abstract class ParsingEncoder extends Encoder {
  /**
   * Tracks the number of items that remain to be written in
   * the collections (array or map).
   */
  private long[] counts = new long[10];
  
  protected int pos = -1;

  @Override
  public void setItemCount(long itemCount) throws IOException {
    if (counts[pos] != 0) {
      throw new AvroTypeException("Incorrect number of items written. " +
          counts[pos] + " more required.");
    }
    counts[pos] = itemCount;
  }

  @Override
  public void startItem() throws IOException {
    counts[pos]--;
  }

  /** Push a new collection on to the stack. */
  protected final void push() {
    if (pos == counts.length) {
      counts = Arrays.copyOf(counts, pos + 10);
    }
    counts[++pos] = 0;
  }
  
  protected final void pop() {
    if (counts[pos] != 0) {
      throw new AvroTypeException("Incorrect number of items written. " +
          counts[pos] + " more required.");
    }
    pos--;
  }
  
  protected final int depth() {
    return pos;
  }
}

