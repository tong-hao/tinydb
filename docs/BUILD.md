# Build Instructions

## Prerequisites

- CMake 3.14 or higher
- GCC 9+ or Clang 10+ (C++17 support)
- Google Test (optional, for tests)
- Flex and Bison (for SQL parser generation)

### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    libgtest-dev \
    flex \
    bison \
    libreadline-dev
```

### macOS

```bash
brew install cmake
brew install googletest
brew install flex
brew install bison
brew install readline
```

### CentOS/RHEL

```bash
sudo yum install -y \
    gcc-c++ \
    cmake3 \
    gtest-devel \
    flex \
    bison \
    readline-devel
```

## Build Steps

### 1. Clone Repository

```bash
git clone <repository-url>
cd tinydb
```

### 2. Create Build Directory

```bash
mkdir build
cd build
```

### 3. Configure with CMake

```bash
cmake ..
```

For debug build:
```bash
cmake -DCMAKE_BUILD_TYPE=Debug ..
```

For release build:
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
```

### 4. Build

```bash
make -j$(nproc)
```

For verbose output:
```bash
make VERBOSE=1
```

## Build Outputs

After successful build, the following binaries will be created:

```
build/
├── bin/
│   ├── tinydb-server          # Database server
│   └── tinydb-cli        # Interactive CLI client
└── tests/
    ├── test_lexer
    ├── test_protocol
    ├── test_transaction
    ├── test_lock_manager
    ├── test_update_delete
    ├── test_expression
    ├── test_executor
    ├── test_wal
    ├── test_btree_index
    ├── test_index_ddl
    ├── test_statistics
    ├── test_optimizer
    ├── test_join
    ├── test_phase4_integration
    └── test_phase5_integration
```

## Running Tests

```bash
# Run all tests
make test

# Or using ctest
ctest --output-on-failure

# Run specific test
./tests/test_phase5_integration

# Run with verbose output
./tests/test_phase5_integration --gtest_verbose
```

## Installation

```bash
sudo make install
```

Default install path: `/usr/local`

To install to custom location:
```bash
cmake -DCMAKE_INSTALL_PREFIX=/your/path ..
make
make install
```

## Troubleshooting

### Flex/Bison not found

```bash
# Ubuntu/Debian
sudo apt-get install flex bison

# macOS
brew install flex bison
```

### Google Test not found

```bash
# Build from source
cd /usr/src/gtest
sudo cmake .
sudo make
sudo cp lib/*.a /usr/lib/
```

### Readline not found

```bash
# Ubuntu/Debian
sudo apt-get install libreadline-dev

# macOS
brew install readline
```

## Platform Notes

### Linux

- Tested on Ubuntu 20.04, 22.04
- GCC 9+ recommended

### macOS

- Tested on macOS 12+ (Monterey)
- Apple Clang 13+ recommended
- May need to specify Flex/Bison paths:
  ```bash
  cmake -DFLEX_EXECUTABLE=/usr/local/opt/flex/bin/flex \
        -DBISON_EXECUTABLE=/usr/local/opt/bison/bin/bison ..
  ```

### Windows

Currently not supported. Windows support is planned for future releases.

## Docker Build

```dockerfile
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libgtest-dev \
    flex \
    bison

WORKDIR /app
COPY . .

RUN mkdir build && cd build && cmake .. && make -j$(nproc)

EXPOSE 5432

CMD ["./build/bin/tinydb-server", "-p", "5432"]
```

Build and run:
```bash
docker build -t tinydb .
docker run -p 5432:5432 tinydb
```
