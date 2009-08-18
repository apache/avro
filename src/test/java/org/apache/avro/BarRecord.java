/**
 * 
 */
package org.apache.avro;

import org.apache.avro.util.Utf8;

public class BarRecord {
  private Utf8 beerMsg;

  public BarRecord() {
  }

  public BarRecord(String beerMsg) {
    this.beerMsg = new Utf8(beerMsg);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof BarRecord) {
      return this.beerMsg.equals(((BarRecord) that).beerMsg);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return beerMsg.hashCode();
  }

  @Override
  public String toString() {
    return BarRecord.class.getSimpleName() + "{msg=" + beerMsg + "}";
  }
}