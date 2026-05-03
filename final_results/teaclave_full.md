<a id="teaclave-apache-org-trustzone-sdk-docs-index"></a>

# Teaclave TrustZone SDK Documentation | Apache Teaclave™

- [](/)

On this page

# Teaclave TrustZone SDK Documentation

## Quick Start

- [Quick Emulation And Development in Docker](/trustzone-sdk-docs/emulate-and-dev-in-docker.md)
- [Developing TAs with Rust Standard Library in Docker](/trustzone-sdk-docs/emulate-and-dev-in-docker-std.md)

## Development

- [TA Development Modes](/trustzone-sdk-docs/ta-development-modes.md)
- [Overview of OP-TEE Rust Examples](/trustzone-sdk-docs/overview-of-optee-rust-examples)
- [Writing Rust TAs using optee-utee-build](/trustzone-sdk-docs/writing-rust-tas-using-optee-utee-build.md)
- [Building Rust CA as Android ELF](/trustzone-sdk-docs/building-rust-ca-as-android-elf.md)

## Advanced Topics

- [Advanced Setup](/trustzone-sdk-docs/advanced-setup.md)
- [Expanding TA Secure Memory on QEMUv8](/trustzone-sdk-docs/expanding-ta-secure-memory-on-qemuv8.md)
- [Migrating to New Building Environment](/trustzone-sdk-docs/migrating-to-new-building-env.md)
- [Debugging OP-TEE TA](/trustzone-sdk-docs/debugging-optee-ta.md)

## Maintenance

- [Operation Guide for Integration with Each OP-TEE Release](/teaclave-trustzone-sdk/docs/operation-guide-for-integration-with-each-optee-release)

---

<a id="teaclave-apache-org-trustzone-sdk-docs-advanced-setup-md-index"></a>

# Advanced Setup: Customize Your Build Environment | Apache Teaclave™

- [](/)
- Advanced Topics
- Advanced Setup

On this page

# Advanced Setup: Customize Your Build Environment

## Platforms

To get started with Teaclave TrustZone SDK, you could choose either [QEMU for
Armv8-A](#teaclave-apache-org-trustzone-sdk-docs-advanced-setup-md-index--develop-with-qemuv8) (QEMUv8) or [other
platforms](#teaclave-apache-org-trustzone-sdk-docs-advanced-setup-md-index--develop-on-other-platforms) ([platforms OP-TEE
supported](https://optee.readthedocs.io/en/latest/general/platforms.html)) as
your development environment.

### Develop with QEMUv8

The OP-TEE libraries are needed when building Rust applications, so you should
finish the [Quick start with the OP-TEE Repo for
QEMUv8](#teaclave-apache-org-trustzone-sdk-docs-advanced-setup-md-index--quick-start-with-the-op-tee-repo-for-qemuv8) part first. Then
initialize the building environment in Teaclave TrustZone SDK, build Rust
applications and copy them into the target's filesystem.

Teaclave TrustZone SDK is located in `[YOUR_OPTEE_DIR]/optee_rust/`. Teaclave
TrustZone SDK in OP-TEE repo is pinned to the release version. Alternatively,
you can try the develop version using `git pull`:

```bash
cd [YOUR_OPTEE_DIR]/optee_rust/git pull github master
```

### Develop on Other Platforms

If you are building trusted applications for other platforms ([platforms OP-TEE
supported](https://optee.readthedocs.io/en/latest/general/platforms.html)). QEMU
and the filesystem in the OP-TEE repo are not needed. You can follow these
steps to clone the project and build applications independently from the
complete OP-TEE repo. In this case, the necessary OP-TEE libraries are
initialized in the setup process.

1. The complete list of prerequisites can be found here: [OP-TEE
   Prerequisites](https://optee.readthedocs.io/en/latest/building/prerequisites.html).

   ```bash
   # install dependenciessudo apt-get install android-tools-adb android-tools-fastboot autoconf \automake bc bison build-essential ccache cscope curl device-tree-compiler \expect flex ftp-upload gdisk iasl libattr1-dev libc6:i386 libcap-dev \libfdt-dev libftdi-dev libglib2.0-dev libhidapi-dev libncurses5-dev \libpixman-1-dev libssl-dev libstdc++6:i386 libtool libz1:i386 make \mtools netcat python-crypto python3-crypto python-pyelftools \python3-pycryptodome python3-pyelftools python-serial python3-serial \rsync unzip uuid-dev xdg-utils xterm xz-utils zlib1g-dev
   ```

   Alternatively, you can use a docker container built with our
   [Dockerfile](/Dockerfile).
2. After installing dependencies or building the Docker image, fetch the source
   code from the official GitHub repository:

   ```bash
   git clone https://github.com/apache/teaclave-trustzone-sdk.gitcd teaclave-trustzone-sdk
   ```

## Setup Building Environment

Currently, we support building on both `aarch64` and `x86_64` host machines, and
they share the same steps.

1. Install the Rust environment and toolchains:

   ```bash
   ./setup.sh
   ```
2. Build OP-TEE libraries

   By default, the `OPTEE_DIR` is
   `teaclave-trustzone-sdk/optee/`. OP-TEE submodules
   (`optee_os` and `optee_client` for QEMUv8) will be initialized
   automatically by executing:

   ```bash
   ./build_optee_libraries.sh optee/
   ```
3. Before building applications, set up the configuration:

   a. By default, the target platform is `aarch64` for both CA and TA. If
   you want to build for the `arm` target, you can set up `ARCH`:

   ```bash
   export ARCH_HOST=armexport ARCH_TA=arm
   ```

   b. By default, the build is for `no-std` TA. If you want to enable
   `std` TA, set the `STD` variable:

   ```bash
   export STD=y
   ```
4. Run this script to set up all toolchain and library paths:

   ```bash
   source environment
   ```

## Build Examples

Run this command to build all Rust examples:

```bash
make examples
```

Or build your own CA and TA:

```bash
make -C examples/[YOUR_APPLICATION]
```

Besides, you can collect all example CAs and TAs to
`/teaclave-trustzone-sdk/out`:

```bash
make examples-install
```

## Run Rust Applications

Considering the platform has been chosen
([QEMUv8](#teaclave-apache-org-trustzone-sdk-docs-advanced-setup-md-index--run-rust-applications-in-qemuv8) or
[other](#teaclave-apache-org-trustzone-sdk-docs-advanced-setup-md-index--run-rust-applications-on-other-platforms)), the ways to run the Rust
applications are different.

### Run Rust Applications in QEMUv8

1. The shared folder is needed to share CAs and TAs with the QEMU guest system.
   Recompile QEMU in OP-TEE to enable QEMU VirtFS:

```bash
(cd $OPTEE_DIR/build && make QEMU_VIRTFS_ENABLE=y qemu)
```

2. Copy all the Rust examples or your own applications to the shared folder:

```bash
mkdir shared_foldercd [YOUR_OPTEE_DIR]/optee_rust/ && make examples-install)cp -r [YOUR_OPTEE_DIR]/optee_rust/out/* shared_folder/
```

3. Run QEMU:

```bash
(cd $OPTEE_DIR/build && make run-only QEMU_VIRTFS_ENABLE=yQEMU_VIRTFS_HOST_DIR=$(pwd)/shared_folder)
```

4. After the QEMU has been booted, you need to mount the shared folder in the
   QEMU guest system (username: root), in order to access the compiled CA/TA from
   QEMU. Run the command as follows in the QEMU guest terminal:

```bash
mkdir shared && mount -t 9p -o trans=virtio host shared
```

5. Then run CA and TA as [this
   documentation](https://optee.readthedocs.io/en/latest/building/optee_with_rust.html)
   describes.

### Run Rust Applications on Other Platforms

Copy the applications to your platform and run.

---

<a id="teaclave-apache-org-trustzone-sdk-docs-building-rust-ca-as-android-elf-md-index"></a>

# Building Rust CA as Android ELF | Apache Teaclave™

- [](/)
- Development
- Building Rust CA as Android ELF

# Building Rust CA as Android ELF

In Teaclave TrustZone SDK, example CAs are built as ARM64 Linux ELF by default.
Besides, you can follow these steps to build Rust CAs running on the Android
platform:

1. Download Android NDK toolchain

```text
$ wget https://dl.google.com/android/repository/android-ndk-r21e-linux-x86_64.zip$ unzip android-ndk-r21e-linux-x86_64.zip
```

2. Add the android target

```text
$ rustup target add aarch64-linux-android
```

3. Set toolchains for the target. Add PATH env:

```text
export PATH=$PATH:/your/path/to/android-ndk-r21e/toolchains/llvm/prebuilt/linux-x86_64/bin/
```

4. Edit `teaclave-trustzone-sdk/.cargo/config`, add:

```text
[target.aarch64-linux-android]linker = "aarch64-linux-android28-clang"ar = "aarch64-linux-android-ar"
```

5. Copy Android libteec.so to
   `/teaclave-trustzone-sdk/optee/optee_client/out/export/usr/lib`.

- Note: If you've not built the libteec.so of Android, you can build it using:

```text
$ cd /path/to/optee/optee_client/$ ndk-build APP_BUILD_SCRIPT=./Android.mk NDK_PROJECT_PATH=. NDK_LOG=1 APP_PLATFORM=android-29
```

6. Modify CA's Makefile:

```text
NAME := hello_world-rsTARGET := aarch64-linux-androidOPTEE_DIR ?= ../../../opteeOUT_DIR := $(CURDIR)/target/$(TARGET)/releaseall: hosthost:        @cargo build --target $(TARGET) --release --verboseclean:        @cargo clean
```

7. build:

```text
$ make -C examples/hello_world-rs/host
```

---

<a id="teaclave-apache-org-trustzone-sdk-docs-debugging-optee-ta-md-index"></a>

# Debugging OP-TEE TA | Apache Teaclave™

- [](/)
- Advanced Topics
- Debugging OP-TEE TA

# Debugging OP-TEE TA

When developing applications, it is inevitable that there will be a need for
debugging. This tutorial introduces how to configure the debug environment in
OP-TEE enabled QEMU environment. You may also check
[OP-TEE documentation](https://optee.readthedocs.io/en/latest/building/devices/qemu.html)
for more information about running QEMU for Arm v8.

To debug TEE core running QEMU with GDB, it is necessary to disable TEE ASLR with
`CFG_CORE_ASLR ?= n` in `OP-TEE/optee_os/mk/config.mk`. Note that you need to
recompile QEMU with `make run` again. You can also choose to add the compilation
information directly at compile time.

```bash
$ make run CFG_CORE_ASLR=n
```

Since we will debug the TA remotely with a `gdb` server, please also add the
`GDBSERVER=y` flag when compiling QEMU.

To debug a TA, you need to first start a gdb on the host machine. Then run
`target remote :1234` to connect to the remote QEMU GDB server.

```bash
$ ./path/to/qemu-v8-project/out-br/host/bin/aarch64-buildroot-linux-gnu-gdb(gdb) target remote :1234Remote debugging using :1234warning: No executable has been specified and target does not supportdetermining executable automatically.  Try using the "file" command.0xffffb30b00ea12b4 in ?? ()
```

Next, in the GDB console, load the symbol table of the TEE core library.

```bash
(gdb) symbol-file /path/to/qemu-v8-project/optee_os/out/arm/core/tee.elf
```

Taking `hello_world-rs` as an example, you can get the start address of the text
section from the log in the secure world console, which is 0x40014000.

```bash
D/LD:  ldelf:168 ELF (133af0ca-bdab-11eb-9130-43bf7873bf67) at 0x40014000
```

Then, you can load symbols from TA file (in debug build) to the address.

```bash
(gdb) add-symbol-file /path/to/examples/hello_world-rs/ta/target/aarch64-unknown-linux-gnu/debug/ta 0x40014000
```

Now, you can add breakpoints according to your own needs in the corresponding
functions or addresses.

```bash
(gdb) b invoke_commandBreakpoint 2 at 0xe11bb08: invoke_command. (6 locations)
```

Last, initiate the boot. You can execute `hello_world-rs` in the normal world
console, and will see that the breakpoint we set was hit.

```bash
(gdb) cContinuing.[Switching to Thread 1.2]Thread 2 hit Breakpoint 2, ta::invoke_command (cmd_id=0, params=0x4010ff00) at src/main.rs:5050	    trace_println!("[+] TA invoke command");
```

---

<a id="teaclave-apache-org-trustzone-sdk-docs-emulate-and-dev-in-docker-std-md-index"></a>

# 🚀 Developing TAs with Rust Standard Library in Docker | Apache Teaclave™

- [](/)
- Quick Start
- Developing TAs with Rust Standard Library in Docker

On this page

# 🚀 Developing TAs with Rust Standard Library in Docker

This guide covers the **dev-env with std support** that enables **developing TA
using Rust standard library (std)**, compared to the regular no-std environment
documented in [emulate-and-dev-in-docker.md](/trustzone-sdk-docs/emulate-and-dev-in-docker.md).

The **dev-env with std support** provides a complete setup for building TAs that
can use Rust's standard library features like collections, networking, etc.

> 📖 **Prerequisites**: Read the [original Docker development
> guide](/trustzone-sdk-docs/emulate-and-dev-in-docker.md) first. This document focuses only on
> std-specific differences and capabilities.

## What the Dev-Env with Std Support Provides

The **dev-env with std support** enables **developing TA using Rust std** by
providing:

- **Flexible configuration management** - Switch between std/no-std modes and
  architectures dynamically
- **Rust standard library tailored for OP-TEE** - Build TAs using collections,
  networking, serialization capabilities
- **Mixed development support** - Combine different host and TA architectures,
  including switching between no-std/std in the same project

## 1. Setting Up the Dev-Env with Std Support

### Pull the Docker Image

```bash
# Pull the dev-env with std support for developing TA using Rust std$ docker pull teaclave/teaclave-trustzone-emulator-std-expand-memory:latest# Launch the dev-env container$ docker run -it --rm \  --name teaclave_dev_env \  -v $(pwd):/root/teaclave_sdk_src \  -w /root/teaclave_sdk_src \  teaclave/teaclave-trustzone-emulator-std-expand-memory:latest
```

### One-Time Setup Inside Container

```bash
# Create symbolic link to make it compatiable with existing SDK examples$ ln -s $RUST_STD_DIR rust
```

> 📝 **Note**: This symlink is required for current SDK examples due to
> hardcoded std dependency paths in Cargo.toml. Your own projects may organize
> std files differently.

## 2. Configuration Management System

The key difference is the **unified configuration system** that allows switching
between std/no-std modes and different architectures on demand.

And [cargo-optee](/cargo-optee/README.md#configuration-system) is available as
an alternative to the original configuration management tool: switch\_config.

### If you use switch\_config:

#### Check Available Configurations

```bash
# Show current active configuration$ switch_config --status# List all supported configurations$ switch_config --list
```

**TA Configurations Available:**

- `std/aarch64`, `std/arm32` - With Rust standard library
- `no-std/aarch64`, `no-std/arm32` - Without standard library

**Host Configurations Available:** `aarch64`, `arm32`

**Default Configuration:** Host=`aarch64`, TA=`std/aarch64`

#### Switching Between Configurations

```bash
# Switch TA configurations$ switch_config --ta std/aarch64     # Enable std for 64-bit TA$ switch_config --ta std/arm32       # Enable std for 32-bit TA  $ switch_config --ta no-std/aarch64  # Disable std, use 64-bit no-std# Switch host architecture$ switch_config --host arm32         # Use 32-bit host# Mixed development example: 32-bit host + 64-bit std TA$ switch_config --host arm32 && switch_config --ta std/aarch64
```

### If you use cargo-optee:

You can see the [cargo-optee configuration
system](/cargo-optee/README.md#configuration-system) for details.

## 3. Building and Target Differences

Follow the [original building
instructions](/trustzone-sdk-docs/emulate-and-dev-in-docker.md#2-build-the-hello-world-example), but
note these important target differences:

| Configuration | TA Target | Build Tool | Host Target |
| --- | --- | --- | --- |
| std/* | *-unknown-optee | xargo | *-unknown-linux-gnu |
| no-std/* | *-unknown-linux-gnu | cargo | *-unknown-linux-gnu |

**Example std build output:**

```bash
TA=ta/target/aarch64-unknown-optee/release/133af0ca-bdab-11eb-9130-43bf7873bf67.ta
```

## 4. Hello World Example: Std vs No-Std

### Build with Default Std Configuration

```bash
# Build hello world with std/aarch64 (default configuration)$ cd examples/hello_world-rs/$ make
```

**Result:** TA built with std enabled, targeting `aarch64-unknown-optee`:

```bash
TA=ta/target/aarch64-unknown-optee/release/133af0ca-bdab-11eb-9130-43bf7873bf67.ta
```

### Switch to No-Std and Rebuild

```bash
# Switch TA to no-std mode and rebuild$ switch_config --ta no-std/aarch64$ make clean && make
```

**Result:** TA now targets `aarch64-unknown-linux-gnu` (no-std):

```bash
TA=ta/target/aarch64-unknown-linux-gnu/release/133af0ca-bdab-11eb-9130-43bf7873bf67.ta
```

If you are using cargo-optee, the relevant workflow is already clearly
documented in the [cargo-optee appendix](/cargo-optee/README.md#appendix), so
it will not be repeated here.

## 5. Emulation and Execution

The emulation process is identical to the no-std environment. Follow [sections
3-6 of the original
guide](/trustzone-sdk-docs/emulate-and-dev-in-docker.md#3-make-the-artifacts-accessible-to-the-emulator)
for complete emulation setup instructions.

---

<a id="teaclave-apache-org-trustzone-sdk-docs-emulate-and-dev-in-docker-md-index"></a>

# 🚀 Quick Start For QEMU Emulation | Apache Teaclave™

- [](/)
- Quick Start
- Quick Emulation And Development in Docker

On this page

# 🚀 Quick Start For QEMU Emulation

This guide walks you through building and running QEMU emulation using the
Teaclave TrustZone SDK.

We provide a Docker image with prebuilt QEMU and OP-TEE images to streamline the
entire Trusted Application (TA) development workflow. The image allows
developers to build TAs and emulate a guest virtual machine (VM) that includes
both the Normal World and Secure World environments.

## 1. Pull Development Docker Image

**Terminal A** (Main development terminal):

```bash
# Pull the pre-built development environment$ docker pull teaclave/teaclave-trustzone-emulator-nostd-expand-memory:latest# Clone the repository$ git clone https://github.com/apache/teaclave-trustzone-sdk.git && \  cd teaclave-trustzone-sdk# Launch the development container$ docker run -it --rm \  --name teaclave_dev_env \  -v $(pwd):/root/teaclave_sdk_src \  -w /root/teaclave_sdk_src \  teaclave/teaclave-trustzone-emulator-nostd-expand-memory:latest
```

## 2. Build the Hello World Example

Update: cargo-optee is available as an alternative to the original Makefile
system. See the [cargo-optee documentation](/cargo-optee/README.md) for
details.

### If you use Makefile:

**Still in Terminal A** (inside the Docker container):

```bash
# Build the Hello World example (both CA and TA)make -C examples/hello_world-rs/
```

Under the hood, the Makefile builds both the Trusted Application (TA) and the
Host Application separately. After a successful build, you'll find the resulting
binaries in the `hello_world-rs` directory:

```bash
TA=ta/target/aarch64-unknown-linux-gnu/release/133af0ca-bdab-11eb-9130-43bf7873bf67.taHOST_APP=host/target/aarch64-unknown-linux-gnu/release/hello_world-rs
```

### If you use cargo-optee:

You can see the [cargo-optee build
commands](/cargo-optee/README.md#build-commands) for details.

## 3. Make the Artifacts Accessible to the Emulator

After building the Hello World example, the next step is to make the compiled
artifacts accessible to the emulator.

There are **two approaches** to do this. You can choose either based on your
preference:

- 📦 **Manual sync**: Explicitly sync host and TA binaries to the emulator
- ⚙️ **Makefile integration**: Use `make emulate` to build and sync in one step
  (only when you use Makefile for building)

#### Option 1: Manual Sync via `sync_to_emulator`

We provide a helper command called `sync_to_emulator`, which simplifies the
process of syncing the build outputs to the emulation environment. Run the
following commands inside the container:

```bash
sync_to_emulator --ta $TAsync_to_emulator --host $HOST_APP
```

Run `sync_to_emulator -h` for more usage options.

#### Option 2: Integrate sync with TA's Makefile

For convenience during daily development, the sync invocation can be integrated
into the Makefile. In the `hello_world-rs` example, an `emulate` target is
provided. This helps automatically build the artifacts and sync them to the
emulator in one step:

```bash
make -C examples/hello_world-rs/ emulate
```

## 4. Multi-Terminal Execution

The emulation workflow requires three additional terminals to monitor various
aspects of the system:

- **Terminal B**: 🖥️ **Normal World Listener** - Provides access to the guest
  VM shell
- **Terminal C**: 🔒 **Secure World Listener** - Monitors Trusted Application
  output logs
- **Terminal D**: 🚀 **QEMU Control** - Controls the QEMU emulator

Built-in commands are provided in the Docker image. These commands are located
in `/opt/teaclave/bin/` and are included in the default user's $PATH.

You may use `bash -l` or the full path when executing with docker exec.

**Terminal B** (Guest VM Shell):

```bash
# Connect to the guest VM shell for running commands inside the emulated environment$ docker exec -it teaclave_dev_env bash -l -c listen_on_guest_vm_shell# Alternative: Use full path$ docker exec -it teaclave_dev_env /opt/teaclave/bin/listen_on_guest_vm_shell
```

**Terminal C** (Secure World Output Monitor):

```bash
# Monitor Trusted Application output logs in real-time$ docker exec -it teaclave_dev_env bash -l -c listen_on_secure_world_log# Alternative: Use full path  $ docker exec -it teaclave_dev_env /opt/teaclave/bin/listen_on_secure_world_log
```

## 5. Start the Emulation

After the listeners are set up, we can start the QEMU emulator.

**Terminal D** (QEMU Control):

```bash
# Launch QEMU emulator with debug output and connect to monitoring ports$ docker exec -it teaclave_dev_env bash -l -c "LISTEN_MODE=ON start_qemuv8"
```

> ⏳ **Wait for the QEMU environment to fully boot...** You should see boot
> messages in Terminal D and the guest VM shell prompt in Terminal B.

After QEMU in Terminal D successfully launches, switch to Terminal B, which
provides shell access to the guest VM's normal world.

**Terminal B** (Inside Guest VM): From this shell, you'll find that the
artifacts synced in **Step 3** are already available in the current working
directory. Additionally, the `ta/` and `plugin/` subdirectories are
automatically mounted to be used by TEE OS during TA execution and plugin
loading.

For more details on the mount configuration, refer to the
`listen_on_guest_vm_shell` command in the development environment.

```bash
# tree.|-- host|   `-- hello_world-rs|-- plugin`-- ta    `-- 133af0ca-bdab-11eb-9130-43bf7873bf67.ta3 directories, 2 files
```

This makes it especially convenient for iterative development and frequent code
updates.

Now we are ready to interact with the TA from normal world shell.

```bash
# Execute the Hello World Client Application$ ./host/hello_world-rs
```

The secure world logs, including TA debug messages, are displayed in **Terminal
C**.

## 6. Iterative Development with Frequent Code Updates and Execution

During active development and debugging, you can leave Terminals B, C, and D
open to avoid restarting them each time. Simply return to Terminal A, and repeat
Step 2 (build) and Step 3 (sync) to rebuild and update the artifacts. Once
synced, switch to Terminal B to re-run the client application. This setup
streamlines iterative development and testing.

## Summary

By following this guide, you can emulate and debug Trusted Applications using
our pre-configured Docker-based development environment.

- **Terminal A** serves as the main interface for building and syncing
  artifacts.
- **Terminal B** gives access to the normal world inside the guest VM, where you
  can run client applications like the Hello World example.
- **Terminal C** captures logs and debug output from the secure world, making it
  easy to trace TA behavior.
- **Terminal D** controls the QEMU emulator and shows system-level logs during
  boot and runtime.

Together, these terminals provide a complete and efficient workflow for
TrustZone development and emulation.

### Development Environment Details

The setup scripts and built-in commands can be found in `/opt/teaclave/`. Please
refer to the Dockerfile in the SDK source repository for more information about
how we set up the development environment.

---

<a id="teaclave-apache-org-trustzone-sdk-docs-expanding-ta-secure-memory-on-qemuv8-md-index"></a>

# Expanding TA Secure Memory on QEMUv8 | Apache Teaclave™

- [](/)
- Advanced Topics
- Expanding TA Secure Memory on QEMUv8

# Expanding TA Secure Memory on QEMUv8

Since some Rust examples such as `tls_server-rs` and `tls_client-rs` require
larger TA memory (about 18M heap), we've expanded TA secure memory on OP-TEE
QEMUv8 platform. On QEMUv8 platform it supports 7M TA memory originally, after
expanding it supports 27M TA memory at most.

We modified the firmware and configuration of QEMU, ATF and OPTEE. You can
download the pre-built image from
[https://nightlies.apache.org/teaclave/teaclave-trustzone-sdk/](https://nightlies.apache.org/teaclave/teaclave-trustzone-sdk/) or patch the code
and build the images by yourself.

For details on the modifications, please refer to the
[Patches](https://github.com/apache/teaclave-trustzone-sdk/.patches/test_qemu)

Finally, build images:

```text
$ cd optee-repo/build$ make
```

---

<a id="teaclave-apache-org-trustzone-sdk-docs-migrating-to-new-building-env-md-index"></a>

# Migrating To New Building ENV | Apache Teaclave™

- [](/)
- Advanced Topics
- Migrating to New Building Environment

On this page

# Migrating To New Building ENV

> After optee-utee-build release, this doc is keeping for developers
> who intend to know the detail of building process, we suggest use
> [optee-utee-build](/trustzone-sdk-docs/writing-rust-tas-using-optee-utee-build.md) for building
> instead.

## Migration Guide: Moving from `master` to `main` Branch (Post-Oct 2024)

Since the `main` branch (after October 2024) introduces breaking changes
to the build environment, if users of the legacy `master` branch want to
keep upstream or use a new version of the Rust toolchain, they will need
to migrate their TA to the new environment.

Note that the migration is mainly for building scripts to support both
`no-std` and `std` building for TA, no need for modifying your application
code.

### Current Structure

We have retained almost the same structure as the original but removed
`ta_arm.lds` and `ta_aarch64.lds` from the directory structure. Besides
we have some modification on `ta/ta_static.rs`, `ta/build.rs` and all
`Makefile`s. (See the explanation in next part).

For example the current `examples/acipher-rs/`:

```text
examples/acipher-rs/├── host│   ├── Cargo.toml│   ├── Makefile│   └── src│       └── main.rs├── Makefile├── proto│   ├── build.rs│   ├── Cargo.toml│   └── src│       └── lib.rs├── ta│   ├── build.rs│   ├── Cargo.toml│   ├── Makefile│   ├── src│   │   └── main.rs│   ├── ta_static.rs│   └── Xargo.toml└── uuid.txt
```

### Changes in Build Scripts

1. **TA linking script**: `ta_arm.lds` and `ta_aarch64.lds`.  
   These linking scripts define the low-level TA ELF sections arrangement
   (e.g., `.text` section in ELF). They have been removed, and we now use
   the `lds` file from OP-TEE's TA dev-kit, for example, located in
   `optee_os/out/arm-plat-vexpress/export-ta_arm64/src/ta.ld.S`. This
   change helps to stay upstream with OP-TEE OS and makes it more stable
   when running on OP-TEE OS.
2. **`ta_static.rs`**: C FFI primitives, such as `ta_heap_size` and `ta_props`.  
   This file helps to set TA properties in a C-like manner in the TA ELF
   for OP-TEE OS to load.  
   The change involves modifying imports of primitives, e.g., from
   `libc::c_int` to `core::c_int`, and from `std::u64::MAX` to
   `core::primitive::u64::MAX`. This helps ensure support for both `no_std`
   and `std`-based environments.
3. **`build.rs`**:  
   Since TA is not a normal ELF, it has a header before the ELF sections.  
   This file is the main entry point for building a TA as ELF and adding
   the specific header. It uses configurations such as `ta_static.rs`,
   `user_ta_header.rs`, and the linking script `ta.ld.S`. It also defines
   the linking with OP-TEE's C libraries (`libutee` and `libutils`) from
   OP-TEE's TA dev-kit.

   The changes are:

   a. Move linking parameters from the original
   [`/.cargo/config`](https://github.com/apache/teaclave-trustzone-sdk/blob/master/.cargo/config):
   This change is primarily designed to accommodate more complex build targets.
   For standard TAs, the specific build targets are `aarch64-unknown-optee-trustzone`
   and `arm-unknown-optee-trustzone`.
   For example, in the no-std mode for aarch64, both no-std TAs and CAs are built
   with the `aarch64-unknown-linux-gnu` target. However, in std mode, TAs are
   built with the `aarch64-unknown-optee-trustzone` target, while CAs remain
   built with the `aarch64-unknown-linux-gnu` target.
   This change allows us to decouple TA's linking parameters from the target, as
   they are now defined within TA's `build.rs`.

   b. Add `cargo:rustc-link-arg=--no-warn-mismatch` to work around
   the EABI version mismatch linking error: symbols.o with EABI version 0
   and other objects are EABI version 5.
4. **ENV variables**:  
   The original script for setting the toolchain path has some modifications.
   Due to the more complex building options mentioned above, `CROSS_COMPILE_{HOST, TA}`
   and `TARGET_{HOST, TA}` should be set by `source environment`.
   You should also set whether you want to build in `STD` mode (`export STD=y`)
   and specify the target architecture (`ARM32` or `AArch64`) for both CA and TA.
   Running `source environment` will set up all toolchains and libraries.
5. **Makefile Polishing**:  
   a. Top-level Makefile (`examples/*/Makefile`): Reads the `CROSS_COMPILE_{HOST, TA}`
   and `TARGET_{HOST, TA}`.  
   b. `host/Makefile`: Simplified and polished for the changes in ENV variables.  
   c. `ta/Makefile`: For `std` TAs, checks if the `STD` environment variable is set,
   and further simplifications and polish are done.

### Step 1: Migrating Projects

#### Case 1: Default Migration (No Custom Modifications to Build Scripts)

If you have developed based on one of our example structures and haven't
modified the build scripts mentioned above, you can simply copy a current
example and move your code into it.  
Note that the `Makefile` for `std` TAs has tiny differences from the `no_std`
one. If you are using a `no_std` TA, refer to `hello_world-rs`. For `std` TAs,
refer to `serde-rs`.

We provide a shell script to assist with this migration (you may need to make
small adjustments based on whether you are building in `no_std` or `std` mode).
Here is an example for `no_std`:

```bash
TARGET_EXAMPLE="your_project"OLD_ROOT_PATH="/path/to/old/sdk"NEW_PATH="/path/to/current/sdk"# Duplicate the hello-world example in the new path as a templatecp -r ${NEW_PATH}/examples/hello_world-rs ${NEW_PATH}/examples/${TARGET_EXAMPLE}# Remove the source code directory and copy from the old path to the new path# including: src/ and Cargo.toml in host, ta, proto(cd ${NEW_PATH}/examples/${TARGET_EXAMPLE}/host && rm -rf src/ Cargo.* && \cp -r ${OLD_ROOT_PATH}/examples/${TARGET_EXAMPLE}/host/src . && \cp ${OLD_ROOT_PATH}/examples/${TARGET_EXAMPLE}/host/Cargo.toml .)(cd ${NEW_PATH}/examples/${TARGET_EXAMPLE}/ta && rm -rf src/ Cargo.* && \cp -r ${OLD_ROOT_PATH}/examples/${TARGET_EXAMPLE}/ta/src . && \cp ${OLD_ROOT_PATH}/examples/${TARGET_EXAMPLE}/ta/Cargo.toml .)(cd ${NEW_PATH}/examples/${TARGET_EXAMPLE}/proto && rm -rf src/ Cargo.* && \cp -r ${OLD_ROOT_PATH}/examples/${TARGET_EXAMPLE}/proto/src . && \cp ${OLD_ROOT_PATH}/examples/${TARGET_EXAMPLE}/proto/Cargo.toml .)# Copy the UUID file from the old path to the new pathcp ${OLD_ROOT_PATH}/examples/${TARGET_EXAMPLE}/uuid.txt \${NEW_PATH}/examples/${TARGET_EXAMPLE}/uuid.txt# Update binary names in host/Cargo.toml and host/Makefilesed -i "s/hello_world-rs/${TARGET_EXAMPLE}/g" \${NEW_PATH}/examples/${TARGET_EXAMPLE}/host/Cargo.tomlsed -i "s/hello_world-rs/${TARGET_EXAMPLE}/g" \${NEW_PATH}/examples/${TARGET_EXAMPLE}/host/Makefile
```

#### Case 2: Custom Migration (With Modified Build Scripts)

If you have made changes to your build scripts, follow the steps below to
manually migrate those files:

1. **TA linking script `ta_arm.lds` and `ta_aarch64.lds`**:  
   Usually, developers don't need to modify those files. If you have made any
   changes, compare the diff between your file and
   `optee_os/out/arm-plat-vexpress/export-ta_{arm64, arm32}/src/ta.ld.S` in the
   current SDK.
   This `ta.ld.S` file is currently not included in SDK but in OPTEE\_OS repo.
2. **`ta_static.rs`**:  
   Usually, developers don't need to modify this file. If you have made
   modifications to this file, compare them with the latest version here:  
   [ta\_static.rs diff](https://github.com/apache/teaclave-trustzone-sdk/compare/cd19ac2e1c3cb1a848d5131d4af8138d84be8708..9e3906e9d82f0471e96bf892afe0df37dd90a86e#diff-c0cdd7b28f558bd417069b8e60ed35b70ac1cd01e68e3c0ba6c7311a5a444e22)
3. **`build.rs`**:  
   Usually, developers don't need to modify this file. If you have made
   changes to link other libraries or dependencies in `build.rs`, compare
   the two versions and migrate accordingly:  
   [build.rs diff](https://github.com/apache/teaclave-trustzone-sdk/compare/cd19ac2e1c3cb1a848d5131d4af8138d84be8708..9e3906e9d82f0471e96bf892afe0df37dd90a86e#diff-c07432a8a8ecbc1f00799a2bd008bd8dcbba9d58fd0a9e5815b835e4ed425e86)
4. **Makefiles**:  
   You may have modified some of the Makefiles. Please compare them
   with the current versions to ensure compatibility:

- **For `no_std` builds**:

  - [Top-level Makefile](https://github.com/apache/teaclave-trustzone-sdk/compare/cd19ac2e1c3cb1a848d5131d4af8138d84be8708..dc1523cbcf6c716213854d9a16d39b8d498a9bb6#diff-df315bfec3c0b8e84c64b31e4450660ea66c33aa833f5b1b9d76250481c15887)
  - [Host Makefile](https://github.com/apache/teaclave-trustzone-sdk/compare/cd19ac2e1c3cb1a848d5131d4af8138d84be8708..dc1523cbcf6c716213854d9a16d39b8d498a9bb6#diff-96468cc392cceb21806dbfb2dd24007d772f19992955ed81c4979a45f753378a)
  - [TA Makefile](https://github.com/apache/teaclave-trustzone-sdk/compare/cd19ac2e1c3cb1a848d5131d4af8138d84be8708..dc1523cbcf6c716213854d9a16d39b8d498a9bb6#diff-29c530c8f83308f34fae9b3516015f07fa80c1b879cc9a8834c4dfaa497af1a5)
- **For `std` builds**:

  - [Top-level Makefile](https://github.com/apache/teaclave-trustzone-sdk/compare/cd19ac2e1c3cb1a848d5131d4af8138d84be8708..dc1523cbcf6c716213854d9a16d39b8d498a9bb6#diff-15685120d44f0ca4ea11ac90799a621f19378cebf5b018792ebc25bee68c3824)
  - [Host Makefile](https://github.com/apache/teaclave-trustzone-sdk/compare/cd19ac2e1c3cb1a848d5131d4af8138d84be8708..dc1523cbcf6c716213854d9a16d39b8d498a9bb6#diff-dfb3cbc25e6b4bad652b716b9d051c9fb7c45d2d8303caa936666774c49a624a)
  - [TA Makefile](https://github.com/apache/teaclave-trustzone-sdk/compare/cd19ac2e1c3cb1a848d5131d4af8138d84be8708..dc1523cbcf6c716213854d9a16d39b8d498a9bb6#diff-e0618a8a49e0ac65dd1acd48a0108c280a3821bcfb233f46f4baa56c77369001)

### Step 2: **Update `Cargo.toml`**

You may need to update your `Cargo.toml` file to include newer
versions of crates that depend on the new Rust toolchain. Refer to
the `rust-toolchain.toml` file to verify the current toolchain. If
you update any crates, be prepared for potential code changes to
accommodate new interfaces.

### Step 3: **Build and Resolve Errors**

After updating the necessary files, rebuild the project. During the
process, errors might arise due to crate version mismatches or
other updates. Make sure to resolve these errors by adjusting your
code accordingly.

---

<a id="teaclave-apache-org-trustzone-sdk-docs-ta-development-modes-md-index"></a>

# TA Development Modes | Apache Teaclave™

- [](/)
- Development
- TA Development Modes

On this page

# TA Development Modes

## Comparison

### `no-std`

- **Pros**:

  - Reuses standard Rust tier-1 toolchain targets (`aarch64-unknown-linux-gnu`,
    `arm-unknown-linux-gnueabihf`).
  - Significant performance improvements.
  - Substantial reduction in binary size.
- **Cons**:

  - Limited support for third-party crates. In the no-std mode, Trusted
    Applications (TAs) are unable to utilize crates dependent on the standard
    library (std).

### `std`

- **Pros**:

  - Enables the utilization of more third-party crates, including those
    requiring `std`, such as `rustls`, which are essential for functionality.
- **Cons**:

  - Manual porting of `std` with infrequent updates. Currently using `std`
    version `1.80.0` and `Rust` version `nightly-2024-05-14`, which might not
    meet the MSRV requirements of some crates.

## Supported Examples

- **Common**: See
  [Overview of OP-TEE Rust Examples](#teaclave-apache-org-trustzone-sdk-docs-overview-of-optee-rust-examples-index).
- **`no-std`**: Excludes `test_tls_client`, `test_tls_server`, `test_secure_db_abstraction`.
- **`std`**: Excludes `test_mnist_rs`, `test_build_with_optee_utee_sys`.

---

<a id="teaclave-apache-org-trustzone-sdk-docs-writing-rust-tas-using-optee-utee-build-md-index"></a>

# Writing Rust TAs Using optee-utee-build | Apache Teaclave™

- [](/)
- Development
- Writing Rust TAs using optee-utee-build

On this page

# Writing Rust TAs Using optee-utee-build

Currently we provide a `optee-utee-build` crate to simplify the compilcated
building process of TA, and we recommend everyone use it in future developement.

- For legacy app structures migrating to use this crate, refer to [Migration
  Guide](#teaclave-apache-org-trustzone-sdk-docs-writing-rust-tas-using-optee-utee-build-md-index--migration-guide)
- If you're new to development, start with [Minimal Example](#teaclave-apache-org-trustzone-sdk-docs-writing-rust-tas-using-optee-utee-build-md-index--minimal-example)
- To customize the build process, see [Customization](#teaclave-apache-org-trustzone-sdk-docs-writing-rust-tas-using-optee-utee-build-md-index--customization)

# Minimal Example

Assuming currently we are developing a `hello_world` TA, and we want to build it
with `optee-utee-build` crate, we can do it by following steps.

Firstly, we should add `optee-utee-build` in `build-dependencies`:

```bash
cargo add --build optee-utee-build
```

Secondly, we set a `ta_config` and call `optee-utee-build::build` with it in
build.rs:

```rust
use proto;use optee_utee_build::{TaConfig, Error, RustEdition};fn main() -> Result<(), Error> {    let ta_config = TaConfig::new_default_with_cargo_env(proto::UUID)?;    optee_utee_build::build(RustEdition::Before2024, ta_config)}
```

It will generate a `user_ta_header.rs` file and setup all the required
configurations of the linker of rustc.

Finally, we include the generated `user_ta_header.rs` in the source codes,
normally we put it in `src/main.rs`.

```rust
// src/main.rsinclude!(concat!(env!("OUT_DIR"), "/user_ta_header.rs"));
```

After that, everything finished, we can start building the TA now.

For full codes, you can check the [`hello_world-rs example`](https://github.com/apache/teaclave-trustzone-sdk/tree/main/examples/hello_world-rs/ta)

## Explaination of Minimal Example

### 1. The TaConfig

This is a struct that use for the configuration of the TA we are developing, it
has some public fields:

1. **uuid**: the identifier of TA.
2. **ta\_flags**: combination of some bitflags.  
   for available values, you may check [user\_ta\_header.h in optee\_os](https://github.com/OP-TEE/optee_os/blob/c2e42a8f03a5bb6b894ef85ae409f54760c1f50e/lib/libutee/include/user_ta_header.h#L13-L53)
3. **ta\_data\_size**: the size in bytes of the TA allocation pool.
4. **ta\_stack\_size**: the size in bytes of the stack used for TA execution.
5. **ta\_version**: a version string of TA, should be in semver format.
6. **ta\_description**: the desciption of TA.
7. **trace\_level**: the default trace level of TA.  
   for available values, you may check [trace\_levels.h in optee\_os](https://github.com/OP-TEE/optee_os/blob/c2e42a8f03a5bb6b894ef85ae409f54760c1f50e/lib/libutils/ext/include/trace_levels.h#L26-L31)
8. **trace\_ext**: an extra prefix string when output trace log.
9. **ta\_framework\_stack\_size**: the size in bytes of the stack used for Trusted
   Core Framework.  
   currently used for trace framework and invoke command, should not be less
   than 2048.
10. **ext\_properties**: the extra custom properties.

We can construct the `TaConfig` by providing all of the public fields manually,
or use our standard constructor:

1. **new\_default**: construct a default TaConfig by providing uuid, ta\_version
   and ta\_description, with other configurations set to suggested values, you can
   update those configurations later.
2. **new\_default\_with\_cargo\_env**: it's a constructor wrapped with new\_default,
   but take `version` and `description` from cargo.toml so simply providing a uuid
   as parameter is enough.

### 2. The RustEdition

The generated `user_ta_header.rs` must be different between `edition of 2024`
and `edition before 2024`, and currently there is no official stable way to know
what edition we are compiling with, so we provide a argument to set with.

> #### What’s the difference?
>
> the generated `user_ta_header.rs` file include some const variables and global
> functions tagged with `no_mangle` and `link_section`, start from rust edition of
> 2024, they must be wrapped with unsafe, or rustc will output a compilation error
> (while before edition of 2024 it must not, or rustc will output a syntax error).

# Customization

`optee-utee-build` provide some structs for flexible use.

### 1. Builder

Instead of calling the `build` function directly, you can use Builder for
customization.

Usage:

```rust
use proto;use optee_utee_build::{TaConfig, Builder, Error, RustEdition, LinkType};fn main() -> Result<(), Error> {    let ta_config = TaConfig::new_default_with_cargo_env(proto::UUID)?;    Builder::new(RustEdition::Before2024, ta_config)      .out_dir("/tmp")      .header_file_name("my_generated_user_ta_header.rs")      .link_type(LinkType::CC)      .build()}
```

As you can see from the codes, there are some customizations of the builder:

1. **out\_dir**: change directory of output files.  
   default to OUT\_DIR by cargo.
2. **header\_file\_name**: change name of output header file.  
   default to `user_ta_header.rs`
3. **link\_type**: set link\_type manually.  
   there are some difference in parameters in
   linkers between `CC` and `LD` types, for example, `--sort-section` in `CC` types
   of linkers changes to `-Wl,--sort-section`, we will try to detect current linker
   that cargo using, you can use this function to set it manually if you think our
   detection mismatch.

### 2. Linker

For developers who prefer to use a hand-written `user_ta_header.rs` and only
want `optee-utee-build` to handle the linking process, they can use the
`Linker`, otherwise, try `Builder` instead.

Usage:

```rust
use optee_utee_build::{Linker, Error};use std::env;fn main() -> Result<(), Error> {  let out_dir = env::var("OUT_DIR")?;  Linker::auto().link_all(out_dir)?;  Ok(())}
```

When linking manually, developers construct a `Linker` and calling the
`link_all` method by providing the out\_dir, and linker will generate some
required files (link script, etc, used by linker) into out\_dir and handle all
the linking stuff.

In above codes, we use `auto` to construct the linker, it will detect current
linker that cargo using automatically, you can use `new` function to construct
the linker manually if you think our detection mismatch.

```rust
use optee_utee_build::{Linker, Error, LinkType};use std::env;fn main() -> Result<(), Error> {  let out_dir = env::var("OUT_DIR")?;  Linker::new(LinkerType::CC).link_all(out_dir)?;  Ok(())}
```

### 3. HeaderFileGenerator

For developers who prefer to do the linking themselves and only want
`optee-utee-build` to generate the header file, they can use the
`HeaderFileGenerator`, otherwise, try `Builder` instead.

Usage:

```rust
use optee_utee_build::{HeaderFileGenerator, TaConfig, RustEdition, Error};fn main() -> Result<(), Error> {  const UUID: &str = "26509cec-4a2b-4935-87ab-762d89fbf0b0";  let ta_config = TaConfig::new_default(UUID, "0.1.0", "example")?;  let codes = HeaderFileGenerator::new(RustEdition::Before2024).generate(&ta_config)?;  Ok(std::io::Write("/tmp/user_ta_header.rs", codes.as_bytes())?)}
```

# Migration Guide

For developers still using `const configuration values` in `src/main.rs` and
`custom build scripts` in `build.rs`(described in [[migrating-to-new-building-env]](https://github.com/apache/teaclave-trustzone-sdk/blob/main/docs/migrating-to-new-building-env.md)),
they can upgrade to `optee-utee-build` by following step:

Firstly, add `optee-utee-build` as `build-dependencies`:

```bash
cargo add --build optee-utee-build
```

Secondly, in `build.rs`, remove codes of `custom build scripts`, and use
`optee_utee_build::build` instead:

```rust
// ... other importsuse optee_utee_build::{TaConfig, Error}fn main() -> Result<(), Error> {  // should customize the ta_config with the same as const configuration values  // in your src/main.rs  let ta_config = TaConfig::new_default_with_cargo_env(proto::UUID)?    .ta_stack_size(10 * 1024);   optee_utee_build::build(RustEdition::Before2024, ta_config)?;  // ... other build scripts}
```

Thirdly, remove `const configuration values` in `src/main.rs`, keep the line of
`include user_ta_header.rs`.

```rust
/// ... other codes in src/main.rs/* remove const configuration values, move them to TaConfig in src/main.rs// TA configurationsconst TA_FLAGS: u32 = 0;const TA_DATA_SIZE: u32 = 32 * 1024;const TA_STACK_SIZE: u32 = 2 * 1024;const TA_VERSION: &[u8] = b"0.1\0";const TA_DESCRIPTION: &[u8] = b"This is a hello world example.\0";const EXT_PROP_VALUE_1: &[u8] = b"Hello World TA\0";const EXT_PROP_VALUE_2: u32 = 0x0010;const TRACE_LEVEL: i32 = 4;const TRACE_EXT_PREFIX: &[u8] = b"TA\0";const TA_FRAMEWORK_STACK_SIZE: u32 = 2048;*/include!(concat!(env!("OUT_DIR"), "/user_ta_header.rs"));  // keep this line
```

Finally, delete the useless `ta_static.rs` and start building now.

---

<a id="teaclave-apache-org-trustzone-sdk-docs-overview-of-optee-rust-examples-index"></a>

# Overview of OP-TEE Rust Examples | Apache Teaclave™

- [](/)
- Development
- Overview of OP-TEE Rust Examples

# Overview of OP-TEE Rust Examples

All OP-TEE Rust examples are suffixed with `-rs`, which work as standalone host
application and corresponding TA (Trusted Application) and can be found in
separate directories.

To install all examples in `SDK_ROOT_DIR/out/`, run `make examples-install`
after `make examples`.

To compile one of the examples, run `make -C examples/EXAMPLE_DIR`.

| Host application name | TA UUID | Description | Std/No-std Support |
| --- | --- | --- | --- |
| acipher-rs | 057f4b66-bdab-11eb-96cf-33d6e41cc849 | Generate an RSA key pair,  encrypt a supplied string and decrypt it. | both |
| aes-rs | 0864c8ec-bdab-11eb-8926-c7fa47a8c92d | Run an AES encryption and decryption. | both |
| authentication-rs | 0a5a06b2-bdab-11eb-add0-77f29de31296 | Run AES-CCM authenticated encryption / decryption. | both |
| big_int-rs | 0bef16a2-bdab-11eb-94be-6f9815f37c21 | Do mathematical operations of big integers, such as addition, subtraction, multiplication, division, etc. | both |
| diffie_hellman-rs | 0e6bf4fe-bdab-11eb-9bc5-3f4ecb50aee7 | Run Diffie-Hellman key exchange to derive shared secrets. | both |
| digest-rs | 10de87e2-bdab-11eb-b73c-63fec73e597c | Calculate the hash of the message using SHA256 digest algorithm. | both |
| error_handling-rs | ec59c1fc-b9e0-4c3c-8756-0a3cc48f0088 | Demonstrate error handling patterns in Trusted Applications. | both |
| hello_world-rs | 133af0ca-bdab-11eb-9130-43bf7873bf67 | Increment and decrement an integer value. | both |
| hotp-rs | 1585d412-bdab-11eb-ba91-3b085fd2601f | Generate HMAC based One Time Password which is  described inRFC4226. | both |
| inter_ta-rs | fa9ea860-ef3b-4d59-8457-5564a60c0379 | Demonstrate inter-TA communication patterns. | both |
| message_passing_interface-rs | 17556a46-bdab-11eb-b325-d38c9a9af725 | Passing serde json message between host application and TA, which is more convenient to send structured data. | std |
| random-rs | 197c710c-bdab-11eb-8f3f-17a5f698d23b | Generate a random UUID. | both |
| property-rs | a3859d33-b540-4a69-8d29-696dde9115cc | Demonstrate property-based testing in Trusted Applications. | both |
| secure_storage-rs | 1cd6d392-bdab-11eb-9082-abc902ac5cd4 | Read / write / delete raw data from / into the OP-TEE secure storage. | both |
| serde-rs | 1ed47816-bdab-11eb-9ebd-3ffe0648da93 | Invoke third party crateserdefor serialization and deserialization. | std |
| supp_plugin-rs | 255fc838-de89-42d3-9a8e-d044c50fa57c | TA actively invokes a command defined in normal world plugins. Do interaction between host <-> TA <-> plugin. The plugin is identified by UUID: ef620757-fa2b-4f19-a1c4-6e51cfe4c0f9. | both |
| tcp_client-rs | 59db8536-e5e6-11eb-8e9b-a316ce7a6568 | Do HTTP connection from Trusted Application. | both |
| time-rs | 21b1a1da-bdab-11eb-b614-275a7098826f | Set / get TEE time. | both |
| udp_socket-rs | 87c2d78e-eb7b-11eb-8d25-df4d5338f285 | Do UDP socket connection from Trusted Application. | both |
| signature_verification-rs | c7e478c2-89b3-46eb-ac19-571e66c3830d | Sign a message and verify the signature using the third party cratering. | both |
| tls_client-rs | ec55bfe2-d9c7-11eb-8b0e-f3f8fad927f7 | Do TLS connection from Trusted Application. | std |
| tls_server-rs | 69547de6-f47e-11eb-994e-f34e88d5c2b4 | Set up the TLS server in Trusted Application. | std |
| secure_db_abstraction-rs | e55291e1-521c-4dca-aa24-51e34ab32ad9 | An abstraction of database base on Secure Storage. | std |
| mnist-rs | Train:1b5f5b74-e9cf-4e62-8c3e-7e41da6d76f6Infer:ff09aa8a-fbb9-4734-ae8c-d7cd1a3f6744 | Training and Performing Inference in Trusted Application. | no-std |
| client_pool-rs | c9d73f40-ba45-4315-92c4-cf1255958729 | Generic Client Session Pool. | both |
| build_with_optee_utee_sys-rs | bcac6292-5b9d-4b20-a2e5-b389d5e8ae2f | Usingoptee_utee_sysasbuild-dependencies, requiresworkspace.resolver = "2", which is not supported in xargo, so no_std only. | no-std |