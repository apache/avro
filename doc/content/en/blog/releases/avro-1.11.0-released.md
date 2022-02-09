
---
title: "Avro 1.11.0"
linkTitle: "Avro 1.11.0"
date: 2021-10-31
---

The Apache Avro community is pleased to announce the release of Avro 1.11.0!

All signed release artifacts, signatures and verification instructions can
be found <a href="{{< relref "/project/download" >}}">here</a>

This release includes 120 Jira issues, including some interesting features:

* Specification: AVRO-3212 Support documentation tags for FIXED types
* C#: AVRO-2961 Support dotnet framework 5.0
* C#: AVRO-3225 Prevent memory errors when deserializing untrusted data
* C++: AVRO-2923 Logical type corrections
* Java: AVRO-2863 Support Avro core on android
* Javascript: AVRO-3131 Drop support for node.js 10
* Perl: AVRO-3190 Fix error when reading from EOF
* Python: AVRO-2906 Improved performance validating deep record data
* Python: AVRO-2914 Drop Python 2 support
* Python: AVRO-3004 Drop Python 3.5 support
* Ruby: AVRO-3108 Drop Ruby 2.5 support

For the first time, the 1.11.0 release includes experimental support for
**Rust**. Work is continuing on this donated SDK, but we have not versioned and
published official artifacts for this release.

**Python**: The avro package fully supports Python 3. We will no longer publish a
separate avro-python3 package

And of course upgraded dependencies to latest versions, CVE fixes and more:
https://issues.apache.org/jira/issues/?jql=project%3DAVRO%20AND%20fixVersion%3D1.11.0

The link to all fixed JIRA issues and a brief summary can be found at:
https://github.com/apache/avro/releases/tag/release-1.11.0

In addition, language-specific release artifacts are available:

* C#: https://www.nuget.org/packages/Apache.Avro/1.11.0
* Java: from Maven Central,
* Javascript: https://www.npmjs.com/package/avro-js/v/1.11.0
* Perl: https://metacpan.org/release/Avro
* Python 3: https://pypi.org/project/avro/1.11.0
* Ruby: https://rubygems.org/gems/avro/versions/1.11.0

Thanks to everyone for contributing!

