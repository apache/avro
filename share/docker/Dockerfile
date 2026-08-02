# syntax=docker/dockerfile:1
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
# See BUILD.md.

ARG BUILDPLATFORM

FROM --platform=$BUILDPLATFORM ubuntu:24.04
WORKDIR /root

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies from vanilla system packages
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends software-properties-common \
 && add-apt-repository ppa:deadsnakes/ppa \
 && add-apt-repository ppa:longsleep/golang-backports \
 && apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends ant \
                                                 apt-utils \
                                                 asciidoc \
                                                 bison \
                                                 build-essential \
                                                 bzip2 \
                                                 cmake \
                                                 cppcheck \
                                                 curl \
                                                 doxygen \
                                                 flex \
                                                 git \
                                                 gnupg2 \
                                                 golang-go \
                                                 hugo \
                                                 libboost-dev \
                                                 libboost-math-dev \
                                                 libboost-random-dev \
                                                 libboost-test-dev \
                                                 libbz2-dev \
                                                 libfontconfig1-dev \
                                                 libfreetype6-dev \
                                                 libglib2.0-dev \
                                                 libjansson-dev \
                                                 libreadline-dev \
                                                 libsnappy-dev \
                                                 libssl-dev \
                                                 libyaml-dev \
                                                 libzstd-dev \
                                                 openjdk-11-jdk \
                                                 openjdk-17-jdk \
                                                 openjdk-21-jdk \
                                                 perl \
                                                 source-highlight \
                                                 subversion \
                                                 tmux \
                                                 unzip \
                                                 valgrind \
                                                 vim \
                                                 wget

# Install a maven release  -------------------------------------------
# Inspired from https://github.com/apache/accumulo-docker/blob/bbb9892e165d40fb35fa19f38929effc5d0c709b/Dockerfile#L30
ARG MAVEN_VERSION=3.9.11
# Ideally we use the 1st URL only, but if the version is outdated (or we're grabbing the .asc file), we might have to pull from the dist/archive :/
ARG APACHE_DIST_URLS="https://www.apache.org/dyn/closer.cgi?action=download&filename= \
  https://www-us.apache.org/dist/ \
  https://www.apache.org/dist/ \
  https://archive.apache.org/dist/"
RUN set -eux; \
  download() { \
    local f="$1"; shift; \
    local distFile="$1"; shift; \
    local success=; \
    local distUrl=; \
    for distUrl in $APACHE_DIST_URLS; do \
      if wget -nv -O "$f" "$distUrl$distFile"; then \
        success=1; \
        break; \
      fi; \
    done; \
    [ -n "$success" ]; \
  }; \
  download "maven.tar.gz" "maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"; \
  tar xzf "maven.tar.gz" -C /tmp/; \
  mv /tmp/apache-maven-$MAVEN_VERSION /opt/maven; \
  rm "maven.tar.gz"
ENV PATH="/opt/maven/bin:${PATH}"

# Install nodejs
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/root/.npm \
    curl -fsSL -o /tmp/nodesource_setup.sh https://deb.nodesource.com/setup_24.x \
 && bash /tmp/nodesource_setup.sh \
 && rm /tmp/nodesource_setup.sh \
 && apt-get -qqy install --no-install-recommends nodejs \
 && npm install -g grunt-cli \
 && npm install -g browserify

# Install PHP
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends php \
                                                 php-bz2 \
                                                 php-curl \
                                                 php-dev \
                                                 php-gmp \
                                                 php-mbstring \
                                                 php-xml \
                                                 php-zip
# Install PHP-snappy and PHP-zstd
ARG PHP8_VERSION=8.3
RUN mkdir -p "/etc/php/${PHP8_VERSION}/cli/conf.d" \
 && mkdir -p /tmp/lang/php && cd /tmp/lang/php \
 && git clone --recursive --depth=1 https://github.com/kjdev/php-ext-zstd.git \
 && cd php-ext-zstd \
 && phpize \
 && ./configure \
 && make \
 && make install \
 && echo "extension=zstd.so" > "/etc/php/${PHP8_VERSION}/cli/conf.d/10-zstd.ini" \
 && cd .. && rm -rf php-ext-zstd \
 && git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git \
 && cd php-ext-snappy \
 && phpize \
 && ./configure \
 && make \
 && make install \
 && echo "extension=snappy.so" > "/etc/php/${PHP8_VERSION}/cli/conf.d/10-snappy.ini" \
 && cd .. && rm -rf php-ext-snappy \
 && php -m \
 && cd / && rm -rf /tmp/lang/php

RUN curl -sS https://getcomposer.org/installer | php -- --version=2.8.12 --install-dir=/usr/local/bin --filename=composer

# Install Perl modules
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends libcompress-raw-zlib-perl \
                                                 libcpan-uploader-perl \
                                                 libencode-perl \
                                                 libio-string-perl \
                                                 libjson-maybexs-perl \
                                                 libmodule-install-perl \
                                                 libmodule-install-readmefrompod-perl \
                                                 libobject-tiny-perl \
                                                 libperl-critic-perl \
                                                 libregexp-common-perl \
                                                 libtest-exception-perl \
                                                 libtest-pod-perl \
                                                 libtry-tiny-perl

RUN --mount=type=cache,target=/root/.cpanm \
    curl -fsSL -o /usr/local/bin/cpanm https://cpanmin.us \
 && chmod +x /usr/local/bin/cpanm \
 && cpanm --mirror https://www.cpan.org/ Compress::Zstd \
                                         Module::Install::Repository

# Install Python3
ENV PATH="${PATH}:/opt/pypy3.11/bin:/opt/pypy3.10/bin"

# https://docs.docker.com/engine/reference/builder/#automatic-platform-args-in-the-global-scope
ARG BUILDARCH
RUN case "${BUILDARCH:?}" in \
      arm64) pypyarch=aarch64;; \
      *) pypyarch=linux64;; \
    esac \
 && cd /opt \
 && for url in \
    https://downloads.python.org/pypy/pypy3.11-v7.3.20-"$pypyarch".tar.bz2 \
    https://downloads.python.org/pypy/pypy3.10-v7.3.19-"$pypyarch".tar.bz2 \
    ; do curl -fsSL "$url" | tar -xjpf -; \
    done \
 && ln -s pypy3.11* pypy3.11 && ln -s pypy3.10* pypy3.10

# Note: python3-distutils was removed with Python 3.12; see here for migration advise: https://peps.python.org/pep-0632/#migration-advice
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends mypy \
                                                 python3 \
                                                 python3.10 \
                                                 python3.11 \
                                                 python3.12 \
                                                 python3.13 \
                                                 python3.14 \
                                                 python3.14-dev \
 && curl -fsSL -o /tmp/uv-install.sh https://astral.sh/uv/0.11.23/install.sh \
 && sh /tmp/uv-install.sh \
 && rm /tmp/uv-install.sh

# Install Ruby
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends ruby ruby-dev

# Install .NET SDK
RUN set -eux && \
    wget -nv -O /opt/dotnet-install.sh https://dot.net/v1/dotnet-install.sh && \
    bash /opt/dotnet-install.sh --channel "6.0" --install-dir "/opt/dotnet" && \
    bash /opt/dotnet-install.sh --channel "7.0" --install-dir "/opt/dotnet" && \
    bash /opt/dotnet-install.sh --channel "8.0" --install-dir "/opt/dotnet" && \
    rm /opt/dotnet-install.sh

ENV PATH=$PATH:/opt/dotnet:/root/.local/bin

# Since we want the JDK21 as a default, we have to re-prepend it to the PATH.
RUN update-java-alternatives -s "java-1.21.*"

# Install Ruby gems
RUN --mount=type=bind,source=lang/ruby,target=/mnt/ruby \
    --mount=type=bind,source=share/VERSION.txt,target=/tmp/share/VERSION.txt \
    --mount=type=cache,target=/usr/local/bundle/cache \
    cp -a /mnt/ruby /tmp/lang/ruby && \
    mkdir -p /tmp/lang/ruby/lib/avro && \
    gem install bundler --no-document && \
    cd /tmp/lang/ruby && bundle install && \
    rm -rf /tmp/lang/ruby

CMD ["/bin/bash", "-i"]
