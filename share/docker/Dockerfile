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

# Despite this base, JDK8 is used by default.  See below.
FROM openjdk:11-stretch
WORKDIR /root

ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=isolemnlysweariamuptonogood \
    DEBIAN_FRONTEND=noninteractive

# Install dependencies from vanilla system packages
RUN apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends ant \
                                                 apt-transport-https \
                                                 asciidoc \
                                                 bison \
                                                 bzip2 \
                                                 bundler \
                                                 cmake \
                                                 curl \
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
                                                 openjdk-8-jdk \
                                                 perl \
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
                                                 valgrind \
 && apt-get -qqy clean \
 && rm -rf /var/lib/apt/lists

# Install nodejs 6
RUN curl -sSL https://deb.nodesource.com/setup_10.x \
  | bash - \
 && apt-get -qqy install nodejs \
 && apt-get -qqy clean \
 && rm -rf /var/lib/apt/lists \
 && npm install -g grunt-cli \
 && npm cache clean --force

# Install PHP
# TODO: Remove php5-related packages when we drop PHP 5.6 support in the future
# TODO: Consolidate the php versions into a single one (7.x) when we drop PHP 5.6 support in the future
ENV PHP5_VERSION=5.6 \
    PHP7_VERSION=7.1
RUN curl -sSL https://packages.sury.org/php/apt.gpg \
  | apt-key add --no-tty - \
 && echo "deb https://packages.sury.org/php/ $(lsb_release -sc) main" > /etc/apt/sources.list.d/php.list \
 && apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends php${PHP5_VERSION} \
                                                 php${PHP5_VERSION}-gmp \
                                                 php${PHP7_VERSION} \
                                                 php${PHP7_VERSION}-gmp \
 && apt-get -qqy clean \
 && rm -rf /var/lib/apt/lists
# Install PHPUnit 5.7, which is the only version that supports both PHP 5.6 and 7.x
# TODO: use PHPUnit 7.x instead of 5.7 when we drop PHP 5.6 support in the future
RUN curl -sSL https://phar.phpunit.de/phpunit-5.7.phar > /usr/local/bin/phpunit \
 && chmod +x /usr/local/bin/phpunit

# Install Perl modules
RUN apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends libcompress-raw-zlib-perl \
                                                 libencode-perl \
                                                 libio-string-perl \
                                                 libjson-xs-perl \
                                                 libmodule-install-perl \
                                                 libmodule-install-readmefrompod-perl \
                                                 libobject-tiny-perl \
                                                 libperl-critic-perl \
                                                 libregexp-common-perl \
                                                 libtest-exception-perl \
                                                 libtest-pod-perl \
                                                 libtry-tiny-perl \
 && apt-get -qqy clean \
 && rm -rf /var/lib/apt/lists
RUN curl -sSL https://cpanmin.us \
  | perl - --self-upgrade \
 && cpanm --mirror https://www.cpan.org/ install Compress::Zstd \
                                                 Module::Install::Repository \
                                                 Test::Pod \
 && rm -rf .cpanm

# Install Python packages
ENV PIP_NO_CACHE_DIR=off

# Install Python2 packages
RUN python2 -m pip install --upgrade pip setuptools \
 && python2 -m pip install zstandard

# Install Python3 packages
RUN python3 -m pip install --upgrade pip setuptools \
 && python3 -m pip install tox zstandard

# Install .NET SDK
RUN curl -sSLO https://packages.microsoft.com/config/ubuntu/16.04/packages-microsoft-prod.deb \
 && dpkg -i packages-microsoft-prod.deb \
 && rm packages-microsoft-prod.deb \
 && apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends dotnet-sdk-3.1 \
 && apt-get -qqy clean \
 && rm -rf /var/lib/apt/lists

# Install Ruby modules
COPY lang/ruby/Gemfile /tmp
RUN bundle install --gemfile=/tmp/Gemfile

# Note: This "ubertool" container has two JDK versions:
# - OpenJDK 8 (installed as a package and available at /usr/bin/java)
# - OpenJDK 11 (installed from the base image, and prioritized in the PATH)
# - The root build.sh script switches between the versions according to
#   the JAVA environment variable.

# Since want the JDK8 as a default, we have to re-prepend it to the PATH.
# The /usr/local/openjdk-8 link is created to be consistent with the
# openjdk:8 image.

RUN ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
ENV PATH $JAVA_HOME/bin:$PATH

CMD ["/bin/bash", "-i"]
