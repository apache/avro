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
package org.apache.avro.io.fastreader.readers;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.fastreader.steps.ExecutionStep;

public class RecordReader<D extends IndexedRecord> implements FieldReader<D> {

  public enum Stage {
    NEW,
    INITIALIZING,
    INITIALIZED
  }

  private ExecutionStep[] readSteps;
  private ExecutionStep[] defaultingSteps;

  private Supplier<D> supplier;
  private Stage stage = Stage.NEW;

  public Stage getInitializationStage() {
    return this.stage;
  }

  public void reset() {
    this.stage = Stage.NEW;
  }

  public void startInitialization() {
    this.stage = Stage.INITIALIZING;
  }

  @SuppressWarnings("unchecked")
  public void finishInitialization(
      ExecutionStep[] readSteps,
      ExecutionStep[] defaultingSteps,
      Supplier<? extends IndexedRecord> newInstanceSupplier) {
    this.readSteps = readSteps;
    this.defaultingSteps = defaultingSteps;
    this.supplier = (Supplier<D>) newInstanceSupplier;
    this.stage = Stage.INITIALIZED;
  }

  @Override
  public boolean canReuse() {
    return true;
  }

  @Override
  public D read(D reuse, Decoder decoder) throws IOException {
    try {
      D object = reuse != null ? reuse : supplier.get();
      for (ExecutionStep thisStep : readSteps) {
        thisStep.execute(object, decoder);
      }
      for (ExecutionStep thisStep : defaultingSteps) {
        thisStep.execute(object, decoder);
      }
      return object;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    for (ExecutionStep thisStep : readSteps) {
      thisStep.skip(decoder);
    }
  }
}
