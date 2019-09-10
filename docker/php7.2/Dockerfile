FROM phpdockerio/php72-fpm:latest

ENV DEBIAN_FRONTEND noninteractive

# Packages URL - Ubuntu 18.04 LTS (Bionic Beaver)
# https://packagecloud.io/phalcon/stable
# https://github.com/datastax/php-driver.git
# https://downloads.datastax.com/cpp-driver/ubuntu/
# https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/
# https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		curl \
        wget \
        cron \
        vim \
        gcc \
        git \
        libtool \
        libpcre3-dev \
        libgmp-dev \
        make \
        cmake \
        automake \
        openssh-server \
        bzip2 \
        libfontconfig \
        g++

RUN curl -s https://packagecloud.io/install/repositories/phalcon/stable/script.deb.sh | bash


RUN apt-get update \
    	&& apt-get -y --no-install-recommends install  \
        php7.2-mysql \
        php7.2-phalcon \
        php7.2-dev \
        php7.2-json \
        php7.2-curl \
        php7.2-cli \
        php7.2-mbstring \
    && apt-get clean; rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /usr/share/doc/*

ENV CASSANDRA_CPP_DRIVER_VERSION=2.13.0 \
    CASSANDRA_PHP_DRIVER_VERSION=v1.3.2 \
    LIBUV_VERSION=1.29.1 \
    LIBUV_NAME=libuv1

RUN wget http://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v"${CASSANDRA_CPP_DRIVER_VERSION}"/cassandra-cpp-driver_"${CASSANDRA_CPP_DRIVER_VERSION}"-1_amd64.deb \
  && wget http://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v"${CASSANDRA_CPP_DRIVER_VERSION}"/cassandra-cpp-driver-dev_"${CASSANDRA_CPP_DRIVER_VERSION}"-1_amd64.deb \
  && wget http://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v"${LIBUV_VERSION}"/"${LIBUV_NAME}"_"${LIBUV_VERSION}"-1_amd64.deb \
  && wget http://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v"${LIBUV_VERSION}"/"${LIBUV_NAME}"-dev_"${LIBUV_VERSION}"-1_amd64.deb \
  && dpkg -i \
    cassandra-cpp-driver_"${CASSANDRA_CPP_DRIVER_VERSION}"-1_amd64.deb \
    cassandra-cpp-driver-dev_"${CASSANDRA_CPP_DRIVER_VERSION}"-1_amd64.deb \
    "${LIBUV_NAME}"_"${LIBUV_VERSION}"-1_amd64.deb \
    "${LIBUV_NAME}"-dev_"${LIBUV_VERSION}"-1_amd64.deb

RUN git clone https://github.com/datastax/php-driver.git /usr/src/datastax-php-driver && \
    cd /usr/src/datastax-php-driver && \
    git fetch --all && \
    git checkout tags/"${CASSANDRA_PHP_DRIVER_VERSION}" && \
    cd ext && \
    phpize && \
    ./configure && \
    make && \
    make install && \
    echo extension=cassandra.so > /etc/php/7.2/mods-available/cassandra.ini

RUN phpenmod cassandra
