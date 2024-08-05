FROM ubuntu:16.04

# Set environment variable to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# Update package list and install essential dependencies including Python
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    cmake \
    m4 \
    software-properties-common

# Install GCC 8.5.0
RUN add-apt-repository ppa:ubuntu-toolchain-r/test && \
    apt-get update && apt-get install -y gcc-8 g++-8 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 60 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-8 60

# Download and patch Bison 3.0.4
RUN wget https://ftp.gnu.org/gnu/bison/bison-3.0.4.tar.gz && \
    tar -xzf bison-3.0.4.tar.gz && \
    cd bison-3.0.4 && \
    echo '#include <stdio.h>\n#include <errno.h>\nvoid fseterr(FILE *stream) { if (ferror(stream)) { errno = EIO; } else { clearerr(stream); } }' > lib/fseterr.c && \
    ./configure --prefix=/usr --docdir=/usr/share/doc/bison-3.0.4 && \
    make && make install && \
    cd .. && rm -rf bison-3.0.4 bison-3.0.4.tar.gz

# Download and install Flex 2.6.1
RUN wget https://github.com/westes/flex/releases/download/v2.6.1/flex-2.6.1.tar.gz && \
    tar -xzf flex-2.6.1.tar.gz && \
    cd flex-2.6.1 && \
    ./configure && \
    make && make install && \
    cd .. && rm -rf flex-2.6.1 flex-2.6.1.tar.gz

# Set the default GCC to version 8
RUN update-alternatives --set gcc /usr/bin/gcc-8 && \
    update-alternatives --set g++ /usr/bin/g++-8
