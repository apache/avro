<!--

*Thank you very much for contributing to Apache Avro - we are happy that you want to help us improve Avro. To help the community review your contribution in the best possible way, please go through the checklist below, which will get the contribution into a shape in which it can be best reviewed.*

*Please understand that we do not do this to make contributions to Avro a hassle. In order to uphold a high standard of quality for code contributions, while at the same time managing a large number of contributions, we need contributors to prepare the contributions well, and give reviewers enough contextual information for the review. Please also understand that contributions that do not follow this guide will take longer to review and thus typically be picked up with lower priority by the community.*

## Contribution Checklist

  - Make sure that the pull request corresponds to a [JIRA issue](https://issues.apache.org/jira/projects/AVRO/issues). Exceptions are made for typos in JavaDoc or documentation files, which need no JIRA issue.
  
  - Name the pull request in the form "AVRO-XXXX: [component] Title of the pull request", where *AVRO-XXXX* should be replaced by the actual issue number. 
    The *component* is optional, but can help identify the correct reviewers faster: either the language ("java", "python") or subsystem such as "build" or "doc" are good candidates.  

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.
  
  - Make sure that the change passes the automated tests. You can [build the entire project](https://github.com/apache/avro/blob/master/BUILD.md) or just the [language-specific SDK](https://avro.apache.org/project/how-to-contribute/#unit-tests).

  - Each pull request should address only one issue, not mix up code from multiple issues.
  
  - Each commit in the pull request has a meaningful commit message (including the JIRA id)

  - Every commit message references Jira issues in their subject lines. In addition, commits follow the guidelines from [How to write a good git commit message](https://chris.beams.io/posts/git-commit/)
    1. Subject is separated from body by a blank line
    1. Subject is limited to 50 characters (not including Jira issue reference)
    1. Subject does not end with a period
    1. Subject uses the imperative mood ("add", not "adding")
    1. Body wraps at 72 characters
    1. Body explains "what" and "why", not "how"

-->

## What is the purpose of the change

*(For example: This pull request improves file read performance by buffering data, fixing AVRO-XXXX.)*


## Verifying this change

*(Please pick one of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
- *Extended interop tests to verify consistent valid schema names between SDKs*
- *Added test that validates that Java throws an AvroRuntimeException on invalid binary data*
- *Manually verified the change by building the website and checking the new redirect*


## Documentation

- Does this pull request introduce a new feature? (yes / no)
- If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)
