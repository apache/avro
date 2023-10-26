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
package org.apache.avro;

public interface NameValidator {

  class Result {
    private final String errors;

    public Result(final String errors) {
      this.errors = errors;
    }

    public boolean isOK() {
      return this == NameValidator.OK;
    }

    public String getErrors() {
      return errors;
    }
  }

  Result OK = new Result(null);

  default Result validate(String name) {
    return OK;
  }

  NameValidator NO_VALIDATION = new NameValidator() {
  };

  NameValidator UTF_VALIDATOR = new NameValidator() {
    @Override
    public Result validate(final String name) {
      if (name == null) {
        return new Result("Null name");
      }
      int length = name.length();
      if (length == 0) {
        return new Result("Empty name");
      }
      char first = name.charAt(0);
      if (!(Character.isLetter(first) || first == '_')) {
        return new Result("Illegal initial character: " + name);
      }
      for (int i = 1; i < length; i++) {
        char c = name.charAt(i);
        if (!(Character.isLetterOrDigit(c) || c == '_')) {
          return new Result("Illegal character in: " + name);
        }
      }
      return OK;
    }
  };

  NameValidator STRICT_VALIDATOR = new NameValidator() {
    @Override
    public Result validate(final String name) {
      if (name == null) {
        return new Result("Null name");
      }
      int length = name.length();
      if (length == 0) {
        return new Result("Empty name");
      }
      char first = name.charAt(0);
      if (!(isLetter(first) || first == '_')) {
        return new Result("Illegal initial character: " + name);
      }
      for (int i = 1; i < length; i++) {
        char c = name.charAt(i);
        if (!(isLetter(c) || isDigit(c) || c == '_')) {
          return new Result("Illegal character in: " + name);
        }
      }
      return OK;
    }

    private boolean isLetter(char c) {
      return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private boolean isDigit(char c) {
      return c >= '0' && c <= '9';
    }

  };

}
