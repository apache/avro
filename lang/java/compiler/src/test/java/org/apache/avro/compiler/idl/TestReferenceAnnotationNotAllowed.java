/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compiler.idl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestReferenceAnnotationNotAllowed {

  @Test
  public void testReferenceAnnotationNotAllowed() {

    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Idl idl = new Idl(cl.getResourceAsStream("AnnotationOnTypeReference.avdl"), "UTF-8");

    try {
      idl.CompilationUnit();

      fail("Compilation should fail: annotations on type references are not allowed.");
    } catch (ParseException e) {
      assertEquals("Type references may not be annotated, at line 29, column 17", e.getMessage());
    }
  }
}
