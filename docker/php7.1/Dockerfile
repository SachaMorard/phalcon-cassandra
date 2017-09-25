FROM phpdockerio/php71-fpm:latest

ENV DEBIAN_FRONTEND noninteractive

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
        php7.1-mysql \
        php7.1-phalcon \
        php7.1-dev \
        php7.1-json \
        php7.1-curl \
        php7.1-mcrypt \
        php7.1-cli \
        php7.1-mbstring \
    && apt-get clean; rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /usr/share/doc/*

# Install libuv v1.11.0 from source
#RUN git clone https://github.com/libuv/libuv.git && \
#    cd libuv && \
#    git fetch --all && \
#    git checkout tags/v1.13.1 && \
#    sh autogen.sh && ./configure && \
#    make && make check && make install

RUN wget http://downloads.datastax.com/cpp-driver/ubuntu/16.04/cassandra/v2.7.0/cassandra-cpp-driver_2.7.0-1_amd64.deb && \
    wget http://downloads.datastax.com/cpp-driver/ubuntu/16.04/dependencies/libuv/v1.13.1/libuv_1.13.1-1_amd64.deb && \
    wget http://downloads.datastax.com/cpp-driver/ubuntu/16.04/cassandra/v2.7.0/cassandra-cpp-driver-dev_2.7.0-1_amd64.deb && \
    wget http://downloads.datastax.com/cpp-driver/ubuntu/16.04/dependencies/libuv/v1.13.1/libuv-dev_1.13.1-1_amd64.deb && \
    dpkg -i cassandra-cpp-driver_2.7.0-1_amd64.deb libuv_1.13.1-1_amd64.deb cassandra-cpp-driver-dev_2.7.0-1_amd64.deb libuv-dev_1.13.1-1_amd64.deb

RUN git clone https://github.com/datastax/php-driver.git /usr/src/datastax-php-driver && \
    cd /usr/src/datastax-php-driver && \
    git fetch --all && \
    git checkout tags/v1.3.2 && \
    cd ext && \
    phpize && \
    ./configure && \
    make && \
    make install && \
    echo extension=cassandra.so > /etc/php/7.1/mods-available/cassandra.ini

RUN phpenmod cassandra
