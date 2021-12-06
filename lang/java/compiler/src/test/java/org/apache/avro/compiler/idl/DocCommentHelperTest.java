package org.apache.avro.compiler.idl;

import junit.framework.TestCase;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class DocCommentHelperTest extends TestCase {
  public void testNoWarnings() {
    DocCommentHelper.getAndClearWarnings(); // Clear warnings

    DocCommentHelper.setDoc(token(1, 1, "This is a token."));
    assertEquals(DocCommentHelper.getDoc(), "This is a token.");
    DocCommentHelper.clearDoc(); // Should be a no-op. If not, it adds a warning.
    assertEquals("There should be no warnings", emptyList(), DocCommentHelper.getAndClearWarnings());
  }

  /**
   * Create a doc comment token. Does not include the initial '/**'.
   *
   * @param line               the line where the comment starts
   * @param column             the column where the comment starts (the position
   *                           of the '/**')
   * @param tokenWithoutSuffix the comment content (without the trailing
   *                           '<span>*</span>/')
   * @return a mock token
   */
  private Token token(int line, int column, String tokenWithoutSuffix) {
    final Token token = new Token();
    token.image = tokenWithoutSuffix + "*/";
    token.beginLine = line;
    token.beginColumn = column + 3;
    return token;
  }

  public void testWarningAfterSecondDoc() {
    DocCommentHelper.getAndClearWarnings(); // Clear warnings

    DocCommentHelper.setDoc(token(3, 2, "This is the first token."));
    DocCommentHelper.setDoc(token(5, 4, "This is the second token."));
    assertEquals(DocCommentHelper.getDoc(), "This is the second token.");
    assertEquals("There should be no warnings", singletonList(
        "Found documentation comment at line 5, column 4. Ignoring previous one at line 3, column 2: \"This is the first token.\"\n"
            + "A common cause is to use documentation comments ( /** ... */ ) instead of multiline comments ( /* ... */ )."),
        DocCommentHelper.getAndClearWarnings());
  }

  public void testWarningAfterUnusedDoc() {
    DocCommentHelper.getAndClearWarnings(); // Clear warnings

    DocCommentHelper.setDoc(token(3, 2, "This is a token."));
    DocCommentHelper.clearDoc();
    assertNull(DocCommentHelper.getDoc());
    assertEquals("There should be no warnings",
        singletonList("Ignoring out-of-place documentation comment at line 3, column 2: \"This is a token.\"\n"
            + "A common cause is to use documentation comments ( /** ... */ ) instead of multiline comments ( /* ... */ )."),
        DocCommentHelper.getAndClearWarnings());
  }

  public void testStripIndentsFromDocCommentWithStars() {
    String parsedComment = "* First line\n\t  * Second Line\n\t * * Third Line\n\t  *\n\t  * Fifth Line";
    String schemaComment = "First line\nSecond Line\n* Third Line\n\nFifth Line";
    assertEquals(schemaComment, DocCommentHelper.stripIndents(parsedComment));
  }

  public void testStripIndentsFromDocCommentWithoutStars() {
    String parsedComment = "First line\n\t Second Line\n\t  * Third Line\n\t  \n\t  Fifth Line";
    String schemaComment = "First line\nSecond Line\n * Third Line\n \n Fifth Line";
    assertEquals(schemaComment, DocCommentHelper.stripIndents(parsedComment));
  }
}
