---
title: "Contribution Guidelines"
linkTitle: "Contribution Guidelines"
weight: 1000
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

## How to contribute to Apache Avro

You can contribute to Apache Avro in many ways, so join us and make Avro better!

### Ways to Contribute to Apache Avro

A lot of work goes into maintaining a project like Avro and we can use your help!

You can help us in many ways:

* Help out other users at our [user](mailto:user@avro.apache.org) mailing list
* Answer questions on [Stackoverflow](http://stackoverflow.com/questions/tagged/avro)
* Report [your bugs](https://issues.apache.org/jira/browse/AVRO)
* Implement new features or fix bugs
* Improve this site

If you want to help out with one of our bugs or want to implement a feature, consider using our [Github mirror](https://github.com/apache/avro).

---

### Github Mirror for Easy Contributing

Avro is mirrored on github so you can contribute with ease:

1. Fork Avro
1. Implement your feature or fix the bug, and
1. Send a pull request.

The Github/Apache integration will send the pull request to our developer list, and we will look at your pull request as soon as we can.

### Fork Avro to your Account

So you have an account registered at Github and are ready to start hacking at Avro. First you need to fork the code to your own Github account.

You can find the official mirror for the Apache Avro project at the following location in the official Apache Software Foundation organisation at Github:

    Apache Avro Github mirror: https://github.com/apache/avro

On this page you will find a button with the label _“Fork”_. Click it or use the button below.

<button><i class="fas fa-code-branch"></i> [Fork Avro on GitHub](https://github.com/apache/avro/fork)</button>

Now you have your own copy of Avro to hack on. You can edit directly in the Github web interface–good for minor fixes like documentation errors or clone the project to your workstation.

### Clone Avro to your Workstation

You can use the tooling for Github to get a copy on your workstation or use the commandline:
```shell
$ git clone git@github.com:<your userid>/Avro.git
```
NB Ensure that you replace `<your userid>` with your actual Github user id, otherwise your clone command will fail.

This shell command will create a local checkout of the git repository.

When you are ready with your change you can ask us to review your changes and to integrate it into Avro by creating a Pull Request.

### Create a Pull Request

Make sure you have pushed your changes to your Github repository. To create a pull request on GitHub follow the instructions you can find [here](https://help.github.com/articles/creating-a-pull-request/).

The Github–Apache integration will automatically send a message to the Avro project that a new pull request is waiting for us.

#### Testing

All pull requests automatically trigger various tests at [Github Actions](https://github.com/apache/avro/actions) on Linux x86_64 and at [Travis CI](https://app.travis-ci.com/github/apache/avro) on Linux ARM64.

## Improve this website

Click on the _Edit this page_ link at the top-right corner of any page. It will navigate you to the source code 
of that page at [Github][https://github.com/apache/avro-website]. Make your edits and send us a Pull Request!
