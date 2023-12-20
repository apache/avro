/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.path;

/**
 * Selects items based on their "path" (name of a property under which they are
 * stored) relative to the context.
 */
public class LocationStep implements PathElement {
  /**
   * selector part of location step. either "." or ".."
   */
  private final String selector;
  /**
   * name of a property to select
   */
  private final String propertyName;

  public LocationStep(String selector, String propertyName) {
    this.selector = selector;
    this.propertyName = propertyName;
  }

  @Override
  public String toString() {
    if (propertyName == null || propertyName.isEmpty()) {
      return selector;
    }
    return selector + propertyName;
  }
}
