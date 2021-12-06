/**
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
 *
 * Some portions of this file were modeled after the example Java 1.5
 * parser included with JavaCC. The following license applies to those
 * portions:
 *
 * Copyright (c) 2006, Sun Microsystems, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Sun Microsystems, Inc. nor the names of its
 *       contributors may be used to endorse or promote products derived from
 *       this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.avro.compiler.idl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class with {@code ThreadLocal} fields that allow the generated
 * classes {@link Idl} and {@link IdlTokenManager} to exchange documentation
 * comments without forcing explicit parsing of documentation comments.
 *
 * The reason this works is that all calls to this class happen within a call to
 * the method {@link Idl#CompilationUnit()} (either directly or indirectly).
 */
public class DocCommentHelper {
  /**
   * Pattern to match the common whitespace indents in a multi-line String.
   * Doesn't match a single-line String, fully matches any multi-line String.
   *
   * To use: match on a {@link String#trim() trimmed} String, and then replace all
   * newlines followed by the group "indent" with a newline.
   */
  private static final Pattern WS_INDENT = Pattern.compile("(?U).*\\R(?<indent>\\h*).*(?:\\R\\k<indent>.*)*");
  /**
   * Pattern to match the whitespace indents plus common stars (1 or 2) in a
   * multi-line String. If a String fully matches, replace all occurrences of a
   * newline followed by whitespace and then the group "stars" with a newline.
   *
   * Note: partial matches are invalid.
   */
  private static final Pattern STAR_INDENT = Pattern.compile("(?U)(?<stars>\\*{1,2}).*(?:\\R\\h*\\k<stars>.*)*");

  private static final ThreadLocal<DocComment> DOC = new ThreadLocal<>();
  private static final ThreadLocal<List<String>> WARNINGS = ThreadLocal.withInitial(ArrayList::new);

  /**
   * Return all warnings that were encountered while parsing, once. Subsequent
   * calls before parsing again will return an empty list.
   */
  static List<String> getAndClearWarnings() {
    List<String> warnings = WARNINGS.get();
    WARNINGS.remove();
    return warnings;
  }

  static void setDoc(Token token) {
    DocComment newDocComment = new DocComment(token);
    DocComment oldDocComment = DOC.get();
    if (oldDocComment != null) {
      WARNINGS.get().add(String.format(
          "Found documentation comment at line %d, column %d. Ignoring previous one at line %d, column %d: \"%s\"\n"
              + "A common cause is to use documentation comments ( /** ... */ ) instead of multiline comments ( /* ... */ ).",
          newDocComment.line, newDocComment.column, oldDocComment.line, oldDocComment.column, oldDocComment.text));
    }
    DOC.set(newDocComment);
  }

  static void clearDoc() {
    DocComment oldDocComment = DOC.get();
    if (oldDocComment != null) {
      WARNINGS.get().add(String.format("Ignoring out-of-place documentation comment at line %d, column %d: \"%s\"\n"
          + "A common cause is to use documentation comments ( /** ... */ ) instead of multiline comments ( /* ... */ ).",
          oldDocComment.line, oldDocComment.column, oldDocComment.text));
    }
    DOC.remove();
  }

  static String getDoc() {
    DocComment docComment = DOC.get();
    DOC.remove();
    return docComment == null ? null : docComment.text;
  }

  /* Package private to facilitate testing */
  static String stripIndents(String doc) {
    Matcher starMatcher = STAR_INDENT.matcher(doc);
    if (starMatcher.matches()) {
      return doc.replaceAll("(?U)(?:^|(\\R)\\h*)\\Q" + starMatcher.group("stars") + "\\E\\h?", "$1");
    }

    Matcher whitespaceMatcher = WS_INDENT.matcher(doc);
    if (whitespaceMatcher.matches()) {
      return doc.replaceAll("(?U)(\\R)" + whitespaceMatcher.group("indent"), "$1");
    }

    return doc;
  }

  private static class DocComment {
    private final String text;
    private final int line;
    private final int column;

    DocComment(Token token) {
      // The token is everything after the initial '/**', including all
      // whitespace and the ending '*/'
      int tokenLength = token.image.length();
      this.text = stripIndents(token.image.substring(0, tokenLength - 2).trim());
      this.line = token.beginLine;
      // The preceding token was "/**", and the current token includes
      // everything since (also all whitespace). Thus, we can safely subtract 3
      // from the token column to get the start of the doc comment.
      this.column = token.beginColumn - 3;
    }
  }
}
