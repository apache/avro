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

public enum SchemaVisitorAction {

  /**
   * continue visit.
   */
  CONTINUE,
  /**
   * terminate visit.
   */
  TERMINATE,
  /**
   * when returned from pre non terminal visit method the children of the non terminal are skipped.
   * afterVisitNonTerminal for the current schema will not be invoked.
   */
  SKIP_SUBTREE,
  /**
   * Skip visiting the  siblings of this schema.
   */
  SKIP_SIBLINGS;

}
