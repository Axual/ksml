# GraalVM versions in KSML, and how to upgrade them

KSML runs Python code with GraalVM. GraalVM reaches KSML in two ways:

| Part | Where the version is set | Comes from |
|---|---|---|
| Java libraries (`org.graalvm.polyglot`, `org.graalvm.python`, `org.graalvm.truffle`) | `<graalvm.version>` in `pom.xml` | Maven Central |
| The Java runtime that the Docker image and CI run on | `.graalvm-jdk-version` | GitHub releases of `graalvm/graalvm-ce-builds` |

Use the same version for both. Mixing releases can fail: with libraries 25.0.5.0.3 on runtime
25.5.5.0.3, every Python context fails to start (`UnsatisfiedLinkError` in `LibGraalScope`).
`scripts/graalvm-tarballs.sh` stops the build when the two files do not agree.

We ship GraalVM Community Edition (GPLv2 with Classpath Exception). Oracle GraalVM has another
licence (GFTC). Do not switch to it by accident: in GitHub Actions, the distribution is
`graalvm-community`, not `graalvm`.

## Version names since GraalVM 25.1

Up to 25.0.2, GraalVM Community was released as `jdk-<version>`, for example `jdk-25.0.2`.
After that, GraalVM Community only has "innovation" releases, tagged `graal-<version>`:

| Release | OpenJDK inside | File name |
|---|---|---|
| `graal-25.1.3` | 25.0.3 | `graalvm-community-jdk-25i1-25.0.3_linux-x64_bin.tar.gz` |
| `graal-25.5.5.0.3` | 25.0.5.0.3 | `graalvm-community-jdk-25i5-25.0.5.0.3_linux-x64_bin.tar.gz` |

Maven Central also has library versions such as `25.0.3` and `25.0.5.0.3`. These belong to the
Oracle GraalVM 25.0 line. There is no GraalVM Community runtime for them, so `.graalvm-jdk-version`
cannot hold them.
The innovation releases have the same version on Maven Central and on GitHub (for example
`25.5.5.0.3`), so both files can hold the same number.

`actions/setup-java` skips the new file names, so CI uses `graalvm/setup-graalvm` instead.

## What changed in GraalPy, and what it means for KSML

- GraalPy 25.3 and later implement Python 3.13 (25.0 to 25.2: Python 3.12). Python 3.13 removed
  some old standard modules (for example `cgi`, `telnetlib`, `imghdr`). User code that imports them
  breaks. Mention the Python version in the release notes when it changes.
- GraalPy 25.1 and later load `ctypes` as a real C extension, not as a Java module. This has two
  effects:
  - `ctypes` imports `sysconfig`, and `sysconfig` asks for the current folder. KSML's sandbox
    denies that by default, so `PythonContext` uses `DenyAllFileSystem`, which answers `/`. It
    also allows read-only access to GraalVM's resource cache (`FileSystem.allowInternalResources`),
    which holds the files of all GraalVM languages, not only GraalPy.
  - Only one Python context in a process can load C extensions (`ctypes`, `sqlite3`, `termios`,
    which `getpass`, `tty` and `pty` load). A second context gets `SystemError: Option
    python.IsolateNativeModules is set to 'false' and a second GraalPy context attempted to load
    a native module`. KSML makes one context for the pipelines and one for the producers of each
    definition file, and both load all functions of the file. So with `allowNativeAccess: true`,
    even one file with pipelines and producers that imports `ctypes` in `globalCode` stops KSML at
    startup. The definitions are kept in a `HashMap`, so which context is first does not follow
    the runner config.
    A failed attempt also counts: a context without native access that tries to import `ctypes`
    takes the place. Normal modules (`json`, `re`, `datetime`, `hashlib`, ...) are not C
    extensions and work in every context.
- `python.IsolateNativeModules` would lift that limit, but only on Linux (on macOS it fails with
  `modifying Mach-O files is not yet supported`), and it needs extra file and process access.
  KSML does not turn it on.

## How to upgrade GraalVM

1. Pick a release from <https://github.com/graalvm/graalvm-ce-builds/releases>.
2. Set it in `.graalvm-jdk-version` and in `<graalvm.version>` in `pom.xml`. The workflows take the
   Java version from the first number of `.graalvm-jdk-version`. Dependabot sends GraalVM library
   bumps as their own pull request (group `graalvm`). Add the `.graalvm-jdk-version` change to that
   pull request.
3. Run `scripts/graalvm-tarballs.sh check`. It checks that both versions agree and that both
   Linux files exist.
4. Run the full test suite on the new runtime. `PythonContextTest` covers the sandbox. Its
   native access test runs each case in a new JVM, because of the one-context limit above.
5. Build the Docker image (`build-local-docker.sh`) and run a definition with Python in it, on
   `linux/arm64` and `linux/amd64`.

## History

KSML stayed on GraalVM 25.0.x after the Dependabot pull requests #688 (25.2.4) and #701 (25.3.4.1)
failed: GraalPy 25.1 and 25.2 could not find `_ctypes` on arm64 and macOS, and 25.3 broke `ctypes`
in the sandbox. The move to 25.5.5.0.3 fixed the sandbox part and accepted the one-context limit.
