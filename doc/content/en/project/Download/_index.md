---
title: "Download"
linkTitle: "Download"
weight: 1
---

## Download
Releases may be downloaded from Apache mirrors: [Download](https://www.apache.org/dyn/closer.cgi/avro/)

The latest release is: Avro {{< avro_version >}} (3.4M, source, [pgp](https://downloads.apache.org/avro/avro-{{< avro_version >}}/avro-src-{{< avro_version >}}.tar.gz.asc), [sha512](https://downloads.apache.org/avro/avro-{{< avro_version >}}/avro-src-{{< avro_version >}}.tar.gz.sha512))

* C#: https://www.nuget.org/packages/Apache.Avro/{{< avro_version >}}
* Java: from Maven Central,
* Javascript: https://www.npmjs.com/package/avro-js/v/{{< avro_version >}}
* Perl: https://metacpan.org/release/Avro
* Python 3: https://pypi.org/project/avro/{{< avro_version >}}
* Ruby: https://rubygems.org/gems/avro/versions/{{< avro_version >}}


## Release Notes
Release notes for Avro releases are available in [Jira](https://issues.apache.org/jira/browse/AVRO?report=com.atlassian.jira.plugin.system.project:changelog-panel#selectedTab=com.atlassian.jira.plugin.system.project%3Achangelog-panel)

##Verifying a release
It is essential that you verify the integrity of the downloaded files using the PGP signatures or SHA512 checksums. Please read [How to verify downloaded](https://www.apache.org/info/verification.html) files for more information on why you should verify our releases.

The PGP signatures can be verified using PGP or GPG. First download the [KEYS](https://downloads.apache.org/avro/KEYS) file as well as the .asc signature files for the relevant release packages. Make sure you get these files from the main distribution directory, rather than from a mirror. Then verify the signatures using:

```shell
% gpg --import KEYS
% gpg --verify downloaded_file.asc downloaded_file
```

or

```shell
% pgpk -a KEYS
% pgpv downloaded_file.asc
```

or

```shell
% pgp -ka KEYS
% pgp downloaded_file.asc
```
Alternatively, you can verify the hash on the file.

Hashes can be calculated using GPG:
```shell
% gpg --print-md SHA256 downloaded_file
```
The output should be compared with the contents of the SHA256 file. Similarly for other hashes (SHA512, SHA1, MD5 etc) which may be provided.

Windows 7 and later systems should all now have certUtil:
```shell
% certUtil -hashfile pathToFileToCheck
```
HashAlgorithm choices: _MD2 MD4 MD5 SHA1 SHA256 SHA384 SHA512_

Unix-like systems (and macOS) will have a utility called _md5_, _md5sum_ or _shasum_.
