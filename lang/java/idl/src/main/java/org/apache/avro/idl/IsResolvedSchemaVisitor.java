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
package org.apache.avro.idl;

import org.apache.avro.Schema;

/**
 * This visitor checks if the current schema is fully resolved.
 */
public final class IsResolvedSchemaVisitor implements SchemaVisitor<Boolean> {
  boolean hasUnresolvedParts;

  IsResolvedSchemaVisitor() {
    hasUnresolvedParts = false;
  }

  @Override
  public SchemaVisitorAction visitTerminal(Schema terminal) {
    hasUnresolvedParts = SchemaResolver.isUnresolvedSchema(terminal);
    return hasUnresolvedParts ? SchemaVisitorAction.TERMINATE : SchemaVisitorAction.CONTINUE;
  }

  @Override
  public SchemaVisitorAction visitNonTerminal(Schema nonTerminal) {
    hasUnresolvedParts = SchemaResolver.isUnresolvedSchema(nonTerminal);
    if (hasUnresolvedParts) {
      return SchemaVisitorAction.TERMINATE;
    }
    if (nonTerminal.getType() == Schema.Type.RECORD && !nonTerminal.hasFields()) {
      // We're still initializing the type...
      return SchemaVisitorAction.SKIP_SUBTREE;
    }
    return SchemaVisitorAction.CONTINUE;
  }

  @Override
  public SchemaVisitorAction afterVisitNonTerminal(Schema nonTerminal) {
    return SchemaVisitorAction.CONTINUE;
  }

  @Override
  public Boolean get() {
    return !hasUnresolvedParts;
  }
}
