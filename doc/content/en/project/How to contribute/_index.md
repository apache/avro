---
title: "How to contribute"
linkTitle: "How to contribute"
weight: 3
---

<!--

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

-->

## Getting the source code

First of all, you need the Avro source code.

The easiest way is to clone or fork the GitHub mirror:

```shell
git clone https://github.com/apache/avro.git -o github
```


## Making Changes

Before you start, file an issue in [JIRA](https://issues.apache.org/jira/browse/AVRO) or discuss your ideas on the [Avro developer mailing list](http://avro.apache.org/mailing_lists.html). Describe your proposed changes and check that they fit in with what others are doing and have planned for the project. Be patient, it may take folks a while to understand your requirements.

Modify the source code and add some (very) nice features using your favorite IDE.

But take care about the following points

**All Languages**
- Contributions should pass existing unit tests.
- Contributions should document public facing APIs.
- Contributions should add new tests to demonstrate bug fixes or test new features.

**Java**

- All public classes and methods should have informative [Javadoc comments](https://www.oracle.com/fr/technical-resources/articles/java/javadoc-tool.html).
- Do not use @author tags.
- Java code should be formatted according to [Oracle's conventions](https://www.oracle.com/java/technologies/javase/codeconventions-introduction.html), with one exception:
  - Indent two spaces per level, not four.
- [JUnit](http://www.junit.org/) is our test framework:
- You must implement a class whose class name starts with Test.
- Define methods within your class and tag them with the @Test annotation. Call JUnit's many assert methods to verify conditions; these methods will be executed when you run mvn test.
- By default, do not let tests write any temporary files to /tmp. Instead, the tests should write to the location specified by the test.dir system property.
- Place your class in the src/test/java/ tree.
- You can run all the unit tests with the command mvn test, or you can run a specific unit test with the command mvn -Dtest=<class name, fully qualified or short name> test (for example mvn -Dtest=TestFoo test)


## Code Style (Autoformatting)

For Java code we use [Spotless](https://github.com/diffplug/spotless/) to format the code to comply with Avro's code style conventions (see above). Automatic formatting relies on [Avro's Eclipse JDT formatter definition](https://github.com/apache/avro/blob/master/lang/java/eclipse-java-formatter.xml). You can use the same definition to auto format from Eclipse or from IntelliJ configuring the Eclipse formatter plugin.

If you use maven code styles issues are checked at the compile phase. If your code breaks because of bad formatting, you can format it automatically by running the command:
```shell
mvn spotless:apply
```

## Unit Tests

Please make sure that all unit tests succeed before constructing your patch and that no new compiler warnings are introduced by your patch. Each language has its own directory and test process.

<details><summary>Java</summary>

```shell
cd avro-trunk/lang/java
mvn clean test
```
</details>

<details><summary>Python</summary>

```shell
cd avro-trunk/lang/py
ant clean test
```
</details>

<details><summary>Python3</summary>

```shell
cd avro-trunk/lang/py3
./setup.py build test
```
</details>

<details><summary>C</summary>

```shell
cd avro-trunk/lang/c
./build.sh clean
./build.sh test
```
</details>

<details><summary>C++</summary>

```shell
cd avro-trunk/lang/c++
./build.sh clean test
```
</details>

<details><summary>Ruby</summary>

```shell
cd avro-trunk/lang/ruby
gem install echoe
rake clean test
```
</details>

<details><summary>PHP</summary>

```shell
cd avro-trunk/lang/php
./build.sh clean
./build.sh test
```

</details>

<details><summary>Documentation</summary>

Please also check the documentation.
Java

```shell
mvn compile
mvn javadoc:aggregate
firefox target/site/apidocs/index.html
```

Examine all public classes you've changed to see that documentation is complete, informative, and properly formatted. Your patch must not generate any javadoc warnings.
</details>

## Contributing your code

Contribution can be made directly via github with a Pull Request, or via a patch.

**Via Github**

Method is to create a [pull request](https://help.github.com/articles/using-pull-requests/).

On your fork, create a branch named with JIRA (avro-1234_fixNpe for example) 
On source, go to it
```shell
git pull
git switch avro-1234_fixNpe
```

code your changes (following preceding recommendations)

check and add updated sources
```shell
git status

# Add any new or changed files with:
git add src/.../MyNewClass.java
git add src/.../TestMyNewClass.java
```

Finally, create a commit with your changes and a good log message, and push it:
```shell
git commit -m "AVRO-1234: Fix NPE by adding check to ..."
git push
```
On your github fork site, a button will propose you to build the Pull Request.
Click on it, fill Conversation form, and create it.
Link this PR to the corresponding JIRA ticket (on JIRA ticket, add PR to "Issue Links" chapter, and add label 'pull-request-available' to it .


<details><summary><b>Via Patch</b> (if you don't have github account)</summary>
<blockquote>
<details><summary><b>Clone avro repository</b></summary> 

```shell
git clone https://github.com/apache/avro.git
```
</details>
code your changes (following preceding recommendations)
<details><summary><b>Creating a patch</b></summary>

In order to create a patch, type:
git diff > AVRO-1234.patch

This will report all modifications done on Avro sources on your local disk and save them into the AVRO-1234.patch file. Read the patch file.
Make sure it includes ONLY the modifications required to fix a single issue.

Please do not:
```
reformat code unrelated to the bug being fixed: formatting changes should be separate patches/commits.
comment out code that is now obsolete: just remove it.
insert comments around each change, marking the change: folks can use subversion to figure out what's changed and by whom.
make things public which are not required by end users.
```
Please do:
```
try to adhere to the coding style of files you edit;
comment code whose function or rationale is not obvious;
update documentation (e.g., package.html files, this wiki, etc.)
name the patch file after the JIRA – AVRO-<JIRA#>.patch
```
</details>

<details><summary><b>Applying a patch</b></summary>

To apply a patch either you generated or found from JIRA, you can issue

patch -p0 < AVRO-<JIRA#>.patch

if you just want to check whether the patch applies you can run patch with --dry-run option

patch -p0 --dry-run < AVRO-<JIRA#>.patch

If you are an Eclipse user, you can apply a patch by:

    Right click project name in Package Explorer
    Team -> Apply Patch

Finally, patches should be ''attached'' to an issue report in JIRA via the '''Attach File''' link on the issue's Jira. Please add a comment that asks for a code review following our code review checklist.
</details>
<details><summary><b>Contributing your patch</b></summary>

When you believe that your patch is ready to be committed, select the '''Submit Patch''' link on the issue's Jira.

Folks should run tests before selecting '''Submit Patch'''. Tests should all pass. Javadoc should report '''no''' warnings or errors. Submitting patches that fail tests is frowned on (unless the failure is not actually due to the patch).

If your patch involves performance optimizations, they should be validated by benchmarks that demonstrate an improvement.

If your patch creates an incompatibility with the latest major release, then you must set the '''Incompatible change''' flag on the issue's Jira 'and' fill in the '''Release Note''' field with an explanation of the impact of the incompatibility and the necessary steps users must take.

If your patch implements a major feature or improvement, then you must fill in the '''Release Note''' field on the issue's Jira with an explanation of the feature that will be comprehensible by the end user.

Once you have submitted your patch, a committer should evaluate it within a few days and either: commit it; or reject it with an explanation.

Please be patient. Committers are busy people too. If no one responds to your patch after a few days, please make friendly reminders. Please incorporate other's suggestions into your patch if you think they're reasonable. Finally, remember that even a patch that is not committed is useful to the community.

Should your patch be rejected, select the '''Resume Progress''' on the issue's Jira, upload a new patch with necessary fixes, and then select the **Submit Patch** link again.

In many cases a patch may need to be updated based on review comments. In this case the updated patch should be re-attached to the Jira with the name name. Jira will archive the older version of the patch and make the new patch the active patch. This will enable a history of patches on the Jira. As stated above patch naming is generally AVRO-#.patch where AVRO-# is the id of the Jira issue.

Committers: for non-trivial changes, it is best to get another committer to review your patches before commit. Use Submit Patch link like other contributors, and then wait for a "+1" from another committer before committing. Please also try to frequently review things in the patch queue.

</details>

<details><summary><b>Committing Guidelines for committers</b></summary>

Apply the patch uploaded by the user or check out their pull request. Edit the CHANGES.txt file, adding a description of the change, including the bug number it fixes. Add it to the appropriate section - BUGFIXES, IMPROVEMENTS, NEW FEATURES. Please follow the format in CHANGES.txt file. While adding an entry please add it to the end of a section. Use the same entry for the first line of the git commit message.

Changes are normally committed to master first, then, if they're backward-compatible, cherry-picked to a branch.

When you commit a change, resolve the issue in Jira. When resolving, always set the fix version and assign the issue. Set the fix version to either to the next minor release if the change is compatible and will be merged to that branch, or to the next major release if the change is incompatible and will only be committed to trunk. Assign the issue to the primary author of the patch. If the author is not in the list of project contributors, edit their Jira roles and make them an Avro contributor.
</details>
</blockquote>
</details>

## Jira Guidelines

Please comment on issues in [Jira](https://issues.apache.org/jira/projects/AVRO/issues), making your concerns known. Please also vote for issues that are a high priority for you.

Please refrain from editing descriptions and comments if possible, as edits spam the mailing list and clutter Jira's "All" display, which is otherwise very useful. Instead, preview descriptions and comments using the preview button (on the right) before posting them. Keep descriptions brief and save more elaborate proposals for comments, since descriptions are included in Jira's automatically sent messages. If you change your mind, note this in a new comment, rather than editing an older comment. The issue should preserve this history of the discussion.

## Stay involved

Contributors should join the Avro mailing lists. In particular, the commit list (to see changes as they are made), the dev list (to join discussions of changes) and the user list (to help others).

## See Also

- [Apache contributor documentation](http://www.apache.org/dev/contributors.html)
- [Apache voting documentation](http://www.apache.org/foundation/voting.html)
