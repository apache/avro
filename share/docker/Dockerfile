# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Dockerfile for installing the necessary dependencies for building Avro.
# See BUILD.txt.

FROM openjdk:8

WORKDIR /root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Add the repository for node.js 6.x
RUN curl -sL https://deb.nodesource.com/setup_6.x | bash -

# Register Microsoft key and feed for .NET SDK
# https://dotnet.microsoft.com/download/linux-package-manager/debian8/sdk-current
RUN wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.asc.gpg && \
  mv microsoft.asc.gpg /etc/apt/trusted.gpg.d/ && \
  wget -q https://packages.microsoft.com/config/debian/9/prod.list && \
  mv prod.list /etc/apt/sources.list.d/microsoft-prod.list && \
  chown root:root /etc/apt/trusted.gpg.d/microsoft.asc.gpg && \
  chown root:root /etc/apt/sources.list.d/microsoft-prod.list

RUN curl https://packages.sury.org/php/apt.gpg | apt-key add --no-tty - && \
      echo "deb https://packages.sury.org/php/ $(lsb_release -sc) main" > /etc/apt/sources.list.d/php.list

# Install dependencies from packages
RUN apt-get -qq update && \
  apt-get -qq install --no-install-recommends -y \
    ant \
    asciidoc \
    bison \
    bzip2 \
    cmake \
    curl \
    dotnet-sdk-2.2 \
    doxygen \
    flex \
    g++ \
    gcc \
    git \
    libboost-all-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libglib2.0-dev \
    libjansson-dev \
    libsnappy-dev \
    libsnappy1v5 \
    make \
    maven \
    nodejs \
    perl \
    php5.6 \
    php5.6-gmp \
    pycodestyle \
    python \
    python-pip \
    python-setuptools \
    python-snappy \
    python-wheel \
    python3-pip \
    python3-setuptools \
    python3-snappy \
    python3-wheel \
    rake \
    ruby \
    ruby-dev \
    source-highlight \
    subversion \
    valgrind && \
  apt-get -qq clean && \
  rm -rf /var/lib/apt/lists/*

# Install Perl modules
RUN curl -L https://cpanmin.us | perl - --self-upgrade && \
  cpanm --mirror https://www.cpan.org/ install \
  Module::Install Module::Install::ReadmeFromPod \
  Module::Install::Repository \
  Math::BigInt JSON::XS Try::Tiny Regexp::Common Encode \
  IO::String Object::Tiny Compress::Zlib Test::More \
  Test::Exception Test::Pod

# Install PHPUnit
RUN wget -O /usr/local/bin/phpunit https://phar.phpunit.de/phpunit-5.6.phar && chmod +x /usr/local/bin/phpunit

# Install Python2 packages
RUN python2 -m pip install --upgrade pip setuptools \
 && python2 -m pip install zstandard

# Install Python3 packages
RUN python3 -m pip install --upgrade pip setuptools \
 && python3 -m pip install zstandard

# Install Ruby modules
RUN gem install echoe yajl-ruby multi_json snappy

# Install global Node modules
RUN npm install -g grunt-cli

CMD ["/avro/share/docker/run-tests.sh"]
