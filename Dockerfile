# Builder stage
FROM gcc:11.4 as builder

# Update package lists and install Bison, Flex, and CMake
RUN apt-get update && \
    apt-get install -y \
    bison \
    flex \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /MyDB

# Copy the current directory contents into the container at /MyDB
COPY . /MyDB

# Remove any existing CMake cache files
RUN rm -rf CMakeCache.txt CMakeFiles

# Run CMake in the current directory
RUN cmake .

# Run Make
RUN make

# Final stage (only used when building the full application, not for testing)
FROM builder as final

# Set the working directory to /MyDB/Build
WORKDIR /MyDB/Build

# Set the entry point to run sqlProgram
ENTRYPOINT ["./bin/sqlProgram"]

# Set default command line arguments
CMD ["catalogFile", "."]