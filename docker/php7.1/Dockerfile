FROM phpdockerio/php71-fpm:latest

RUN apt-get update \
	&& apt-get install -y \
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

# Install libuv v1.11.0 from source
RUN git clone https://github.com/libuv/libuv.git && \
    cd libuv && \
    git fetch --all && \
    git checkout tags/v1.11.0 && \
    sh autogen.sh && ./configure && \
    make && make check && make install

RUN apt-get update \
    	&& apt-get -y --no-install-recommends install  \
        php7.1-mysql \
        php7.1-phalcon \
        php7.1-dev \
        php7.1-json \
        php7.1-curl \
        php7.1-mcrypt \
        php7.1-cli \
        php7.1-mbstring \
    && apt-get clean; rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /usr/share/doc/*

# Install datastax php-driver for cassandra
RUN git clone https://github.com/datastax/php-driver.git /usr/src/datastax-php-driver && \
    cd /usr/src/datastax-php-driver && \
    git fetch --all && \
    git checkout tags/v1.3.1 && \
    git submodule update --init && \
    cd ext && \
    ./install.sh && \
    echo extension=cassandra.so > /etc/php/7.1/mods-available/cassandra.ini

RUN phpenmod cassandra
