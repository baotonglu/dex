# DEX: Scalable Range Indexing on Disaggregated Memory

## What's included

- DEX - Proposed DEX-enabled B+-Tree
- Benchmark framework

## Building

### Dependencies
1. We tested our build with Linux Kernel 6.3.2 and GCC 13.1.1.
2. Mellanox ConnectX-5 NICs
3. RDMA Driver: MLNX_OFED_LINUX-5**
4. memcached (to exchange QP information)
5. cityhash

### Compiling
Assuming to compile under a `build` directory:
```bash
git clone https://github.com/baotonglu/dex.git
cd dex
./script/hugepage.sh
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. 
make -j
cp ../script/restartMemc.sh .
cp ../script/run.sh .
cp ../script/run_other.sh .
```

## Running benchmark
1. configure ../memcached.conf, where the 1st line is memcached IP, the 2nd is memcached port
2. In one server, execute `sudo ./newbench/`