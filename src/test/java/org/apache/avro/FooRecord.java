/**
 * 
 */
package org.apache.avro;

public class FooRecord {
  private int fooCount;

  public FooRecord() {
  }

  public FooRecord(int fooCount) {
    this.fooCount = fooCount;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof FooRecord) {
      return this.fooCount == ((FooRecord) that).fooCount;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return fooCount;
  }

  @Override
  public String toString() {
    return FooRecord.class.getSimpleName() + "{count=" + fooCount + "}";
  }
}