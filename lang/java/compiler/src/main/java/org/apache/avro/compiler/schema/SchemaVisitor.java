/*
* Copyright (c) 2001 - 2016, Zoltan Farkas All Rights Reserved.
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 2.1 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this program; if not, write to the Free Software
* Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package org.apache.avro.compiler.schema;

import org.apache.avro.Schema;

public interface SchemaVisitor<T> {

  /**
   * Invoked for schemas that do not have "child" schemas (like string, int ...)
   * or for a previously encountered schema with children,
   * which will be treated as a terminal. (to avoid circular recursion)
   *
   * @param terminal
   * @return
   */
  SchemaVisitorAction visitTerminal(Schema terminal);

  /**
   * Invoked for schema with children before proceeding to visit the children.
   *
   * @param nonTerminal
   * @return
   */
  SchemaVisitorAction visitNonTerminal(Schema nonTerminal);

  /**
   * Invoked for schemas with children after its children have been visited.
   *
   * @param nonTerminal
   * @return
   */
  SchemaVisitorAction afterVisitNonTerminal(Schema nonTerminal);


  /**
   * Invoked when visiting is complete.
   *
   * @return a value which will be returned by the visit method.
   */
  T get();

}
