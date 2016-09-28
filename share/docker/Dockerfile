# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Dockerfile for installing the necessary dependencies for building Avro.
# See BUILD.txt.

FROM java:7-jdk

WORKDIR /root

# Add the repository for node.js 4.x
RUN curl -sL https://deb.nodesource.com/setup_4.x | bash -

# Install dependencies from packages
RUN apt-get update && apt-get install --no-install-recommends -y \
  git subversion curl ant make maven \
  gcc cmake libjansson-dev asciidoc source-highlight \
  g++ flex bison libboost-all-dev doxygen \
  mono-devel mono-gmcs nunit \
  nodejs \
  perl \
  php5 phpunit php5-gmp bzip2 \
  python python-setuptools python3-setuptools \
  ruby ruby-dev rake \
  libsnappy1 libsnappy-dev

# Install Forrest in /usr/local/apache-forrest
RUN cd /usr/local/ && \
    wget "http://archive.apache.org/dist/forrest/apache-forrest-0.9-dependencies.tar.gz" && \
    wget "http://archive.apache.org/dist/forrest/apache-forrest-0.9-sources.tar.gz"

# Unpack Apache Forrest
RUN cd /usr/local/ && \
    tar xzf apache-forrest-0.9-sources.tar.gz && \
    tar xzf apache-forrest-0.9-dependencies.tar.gz && \
    mv apache-forrest-0.9 apache-forrest
RUN cd /usr/local/apache-forrest/main && ./build.sh

# The solution for https://issues.apache.org/jira/browse/PIG-3906
RUN mkdir -p /usr/local/apache-forrest/plugins       && chmod a+rwX -R /usr/local/apache-forrest/plugins
RUN mkdir -p /usr/local/apache-forrest/build/plugins && chmod a+rwX -R /usr/local/apache-forrest/build/plugins

# Configure where forrest can be found
RUN echo 'forrest.home=/usr/local/apache-forrest' > build.properties
ENV FORREST_HOME /usr/local/apache-forrest

# Install Perl modules
RUN curl -L https://cpanmin.us | perl - --self-upgrade # non-interactive cpan
RUN cpanm install Module::Install Module::Install::ReadmeFromPod \
  Module::Install::Repository \
  Math::BigInt JSON::XS Try::Tiny Regexp::Common Encode \
  IO::String Object::Tiny Compress::Zlib Test::More \
  Test::Exception Test::Pod

# Install Ruby modules
RUN gem install echoe yajl-ruby multi_json snappy

# Install global Node modules
RUN npm install -g grunt-cli
