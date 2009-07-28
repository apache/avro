/**
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
package org.apache.avro.io.parsing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Symbol is the base of all symbols (terminals and non-terminals) of
 * the grammar.
 */
public abstract class Symbol {
  /*
   * The type of symbol.
   */
  public enum Kind {
    /** terminal symbols which have no productions */
    TERMINAL,
    /** Start symbol for some grammar */
    ROOT,
    /** non-terminal symbol which is a sequence of one or more other symbols */
    SEQUENCE,
    /** non-termial to represent the contents of an array or map */
    REPEATER,
    /** non-terminal to represent the union */
    ALTERNATIVE,
    /** non-terminal action symbol which are automatically consumed */
    IMPLICIT_ACTION,
    /** non-terminal action symbol which is explicitly consumed */
    EXPLICIT_ACTION
  };

  /// The kind of this symbol.
  public final Kind kind;

  /**
   * The production for this symbol. If this symbol is a terminal
   * this is <tt>null</tt>. Otherwise this holds the the sequence of
   * the symbols that forms the production for this symbol. The
   * sequence is in the reverse order of production. This is useful
   * for easy copying onto parsing stack.
   * 
   * Please note that this is a final. So the production for a symbol
   * should be known before that symbol is constructed. This requirement
   * cannot be met for those symbols which are recursive (e.g. a record that
   * holds union a branch of which is the record itself). To resolve this
   * problem, we initialize the symbol with an array of nulls. Later we
   * fill the symbols. Not clean, but works. The other option is to not have
   * this field a final. But keeping it final and thus keeping symbol immutable
   * gives some confort. See various generators how we generate records.
   */
  public final Symbol[] production;
  /**
   * Constructs a new symbol of the given kind <tt>kind</tt>.
   */
  protected Symbol(Kind kind) {
    this(kind, null);
  }
    
    
  protected Symbol(Kind kind, Symbol[] production) {
    this.production = production;
    this.kind = kind;
  }

  /**
   * A convenience method to construct a root symbol.
   */
  static Symbol root(Symbol... symbols) {
    return new Root(symbols);
  }
  /**
   * A convenience method to construct a sequence.
   * @param production  The constituent symbols of the sequence.
   */
  static Symbol seq(Symbol... production) {
    return new Sequence(production);
  }

  /**
   * A convenience method to construct a repeater.
   * @param symsToRepeat The symbols to repeat in the repeater.
   */
  static Symbol repeat(Symbol endSymbol, Symbol... symsToRepeat) {
    return new Repeater(endSymbol, symsToRepeat);
  }

  /**
   *  A convenience method to construct a union.
   */
  static Symbol alt(Symbol[] symbols, String[] labels) {
    return new Alternative(symbols, labels);
  }

  /**
   * A convenience method to construct an ErrorAction.
   * @param e
   * @return
   */
  static Symbol error(String e) {
    return new ErrorAction(e);
  }
  
  /**
   * A convenience method to construct a ResolvingAction.
   * @param w The writer symbol
   * @param r The reader symbol
   */
  static Symbol resolve(Symbol w, Symbol r) {
    return new ResolvingAction(w, r);
  }
  
  private static class Terminal extends Symbol {
    private final String printName;
    public Terminal(String printName) {
      super(Kind.TERMINAL);
      this.printName = printName;
    }
    public String toString() { return printName; }
  }

  private static class ImplicitAction extends Symbol {
    private ImplicitAction() {
      super(Kind.IMPLICIT_ACTION);
    }
  }
  
  protected static class Root extends Symbol {
    private Root(Symbol... symbols) {
      super(Kind.ROOT, makeProduction(symbols));
      production[0] = this;
    }

    private static Symbol[] makeProduction(Symbol[] symbols) {
      Symbol[] result = new Symbol[symbols.length + 1];
      System.arraycopy(symbols, 0, result, 1, symbols.length);
      return result;
    }
  }
  protected static class Sequence extends Symbol implements Iterable<Symbol> {
    private Sequence(Symbol[] productions) {
      super(Kind.SEQUENCE, productions);
    }

    public Symbol get(int index) {
      return production[index];
    }
    
    public int size() {
      return production.length;
    }
    
    public Iterator<Symbol> iterator() {
      return new Iterator<Symbol>() {
        private int pos = production.length;
        
        public boolean hasNext() {
          return 0 < pos;
        }
        
        public Symbol next() {
          if (0 < pos) {
            return production[--pos];
          } else {
            throw new NoSuchElementException();
          }
        }
        
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  public static class Repeater extends Symbol {
    public final Symbol end;
   
    private Repeater(Symbol end, Symbol... sequenceToRepeat) {
      super(Kind.REPEATER, makeProduction(sequenceToRepeat));
      this.end = end;
      production[0] = this;
    }
    
    private static Symbol[] makeProduction(Symbol[] p) {
      Symbol[] result = new Symbol[p.length + 1];
      System.arraycopy(p, 0, result, 1, p.length);
      return result;
    }
  }
    
  public static class Alternative extends Symbol {
    public final Symbol[] symbols;
    public final String[] labels;
    private Alternative(Symbol[] symbols, String[] labels) {
      super(Kind.ALTERNATIVE);
      this.symbols = symbols;
      this.labels = labels;
    }
    
    public Symbol getSymbol(int index) {
      return symbols[index];
    }
    
    public String getLabel(int index) {
      return labels[index];
    }
    
    public int size() {
      return symbols.length;
    }

    public int findLabel(String label) {
      if (label != null) {
        for (int i = 0; i < labels.length; i++) {
          if (label.equals(labels[i])) {
            return i;
          }
        }
      }
      return -1;
    }
  }

  public static class ErrorAction extends ImplicitAction {
    public final String msg;
    private ErrorAction(String msg) {
      this.msg = msg;
    }
  }

  public static class IntCheckAction extends Symbol {
    public final int size;
    public IntCheckAction(int size) {
      super(Kind.EXPLICIT_ACTION);
      this.size = size;
    }
  }

  public static class EnumAdjustAction extends IntCheckAction {
    public final Object[] adjustments;
    public EnumAdjustAction(int rsymCount, Object[] adjustments) {
      super(rsymCount);
      this.adjustments = adjustments;
    }
  }

  public static class WriterUnionAction extends ImplicitAction {
  }

  public static class ResolvingAction extends ImplicitAction {
    public final Symbol writer;
    public final Symbol reader;
    private ResolvingAction(Symbol writer, Symbol reader) {
      this.writer = writer;
      this.reader = reader;
    }
  }
  
  public static class SkipAction extends ImplicitAction {
    public final Symbol symToSkip;
    public SkipAction(Symbol symToSkip) {
      this.symToSkip = symToSkip;
    }
  }

  public static class FieldAdjustAction extends ImplicitAction {
    public final int rindex;
    public final String fname;
    public FieldAdjustAction(int rindex, String fname) {
      this.rindex = rindex;
      this.fname = fname;
    }
  }
  
  public static class DefaultStartAction extends ImplicitAction {
    public final Symbol root;
    public final byte[] contents;
    public DefaultStartAction(Symbol root, JsonNode defaultValue)
      throws IOException {
      this.root = root;
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      JsonGenerator g = new JsonFactory().createJsonGenerator(os,
          JsonEncoding.UTF8);
      new ObjectMapper().writeTree(g, defaultValue);
      g.flush();
      this.contents = os.toByteArray();
    }
  }

  public static class UnionAdjustAction extends ImplicitAction {
    public final int rindex;
    public final Symbol symToParse;
    public UnionAdjustAction(int rindex, Symbol symToParse) {
      this.rindex = rindex;
      this.symToParse = symToParse;
    }
  }

  /** For JSON. */
  public static class EnumLabelsAction extends IntCheckAction {
    public final List<String> symbols;
    public EnumLabelsAction(List<String> symbols) {
      super(symbols.size());
      this.symbols = symbols;
    }
    
    public String getLabel(int n) {
      return symbols.get(n);
    }
    
    public int findLabel(String l) {
      if (l != null) {
        for (int i = 0; i < symbols.size(); i++) {
          if (l.equals(symbols.get(i))) {
            return i;
          }
        }
      }
      return -1;
    }
  }

  /**
   * The terminal symbols for the grammar.
   */
  public static final Symbol NULL = new Symbol.Terminal("null");
  public static final Symbol BOOLEAN = new Symbol.Terminal("boolean");
  public static final Symbol INT = new Symbol.Terminal("int");
  public static final Symbol LONG = new Symbol.Terminal("long");
  public static final Symbol FLOAT = new Symbol.Terminal("float");
  public static final Symbol DOUBLE = new Symbol.Terminal("double");
  public static final Symbol STRING = new Symbol.Terminal("string");
  public static final Symbol BYTES = new Symbol.Terminal("bytes");
  public static final Symbol FIXED = new Symbol.Terminal("fixed");
  public static final Symbol ENUM = new Symbol.Terminal("enum");
  public static final Symbol UNION = new Symbol.Terminal("union");

  public static final Symbol ARRAY_START = new Symbol.Terminal("array-start");
  public static final Symbol ARRAY_END = new Symbol.Terminal("array-end");
  public static final Symbol MAP_START = new Symbol.Terminal("map-start");
  public static final Symbol MAP_END = new Symbol.Terminal("map-end");
  public static final Symbol END = new Symbol.Terminal("end");
  public static final Symbol ITEM_END = new Symbol.Terminal("item-end");

  /* a pseudo terminal used by parsers */
  public static final Symbol CONTINUE = new Symbol.Terminal("continue");
  public static final Symbol FIELD_ACTION =
    new Symbol.Terminal("field-action");

  public static final Symbol RECORD_START = new ImplicitAction();
  public static final Symbol RECORD_END = new ImplicitAction();
  public static final Symbol UNION_END = new ImplicitAction();
  
  public static final Symbol DEFAULT_END_ACTION = new ImplicitAction();
  public static final Symbol MAP_KEY_MARKER =
    new Symbol.Terminal("map-key-marker");
}

