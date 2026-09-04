# Why GraalVM is pinned below 25.1, and what to check before moving past it

KSML (Axual's project) is deliberately staying on an older version of GraalVM, the
Java runtime that lets KSML run Python code, instead of moving to the newest one.
Dependabot is configured to never even suggest a GraalVM upgrade past this point
(`.github/dependabot.yaml`). This doc explains why.

If you are reading this because Dependabot's ignore range needs to change, or because
you are about to try a GraalVM upgrade by hand, read this whole file first, especially
the checklist at the end.

## How this was found

Dependabot opened [PR #701](https://github.com/Axual/ksml/pull/701), a routine
dependency bump that also raised GraalVM. CI failed. That failure was investigated in
full, including downloading real, matching GraalVM builds (not just the mismatched pair
CI happened to use) and testing locally, on both macOS and a real Linux container (the
actual deployment target for KSML's Docker image). PR #701 was closed/rejected as a
result - it should not be merged.

## Two separate blockers

### 1. A download/tooling problem (easy, just not done yet)

GraalVM changed how it names its release files starting with newer versions. The old
script that downloads the right GraalVM runtime (`scripts/graalvm-tarballs.sh`) can't
figure out the new file names any more.

This is just a scripting fix: instead of guessing the filename from a fixed pattern,
the script needs to ask GitHub's API what the real filename is. For example, GraalVM
`25.3.4.1` is tagged `graal-25.3.4.1` (not `jdk-25.3.4.1`, the old style), and its real
file is named `graalvm-community-jdk-25i3-25.0.4.1_linux-x64_bin.tar.gz` - a version
number that cannot be guessed from `25.3.4.1` alone. This was confirmed directly: both
the Linux and macOS files for `25.3.4.1` were downloaded and used for local testing in
this investigation.

Nobody has done this fix yet, but it is not hard.

### 2. A real bug that breaks a KSML feature (hard, not fixed)

This is the actual dealbreaker. Here is the chain:

- KSML loads each "definition" (a piece of user-supplied config/code) into its own
  isolated Python sandbox, with almost no file access allowed.
- On the new GraalVM version, some Python modules (like `ctypes`) need a little file
  access just to start up, even more than they used to need. There is a tested fix for
  this part: give them a fake "current folder" answer, and read-only access to
  GraalVM's own bundled files. This part works fine and was verified with 790 passing
  tests, using a genuinely matched GraalVM runtime and library pair.
- But there is a second, deeper problem: if a KSML setting called `allowNativeAccess`
  is turned on, and two or more definitions each try to import the same native module
  (like `ctypes`), the second one crashes. Since KSML normally runs multiple
  definitions at once (`TopologyBuildContext.java`: every KSML definition file gets its
  own private Python context; `KafkaProducerRunner.java` creates one context per loaded
  definition), this is not a rare edge case. It would happen regularly.
- GraalVM has a setting meant to fix this crash (`python.IsolateNativeModules`), but
  testing it showed:
  - On Mac: it just does not work at all. GraalVM has not finished building that
    feature for Mac yet (`modifying Mach-O files is not yet supported`).
  - On Linux (what KSML's Docker image actually uses, confirmed by testing inside a
    real Linux container with a genuinely matching GraalVM build): it technically
    could work, but only if KSML allows Python code to launch an external system
    program, `patchelf` - something KSML currently forbids on purpose, for security
    reasons, and a tool KSML's own Docker image does not even include today. Enabling
    that would be a much bigger, riskier change to KSML's sandbox, deserving its own
    security review, not something to sneak in with a version bump.

## Bottom line

KSML stays on the old GraalVM version until either GraalVM fixes this natively
(especially the Mac issue), or someone deliberately decides, after a proper security
review, to let KSML's Python sandbox launch that one external program.

## Checklist for whoever revisits this

1. Check back later if GraalVM fixes this upstream - especially whether native module
   isolation works without needing external process execution, and whether it works on
   Mac. Re-run the exact same tests used here (`PythonContextTest`, especially
   `testNativeAccess`, then the full `ksml` module suite) against a genuinely matched
   runtime and library pair, on both macOS and Linux, before trusting the result.
2. Fix the download script: update `scripts/graalvm-tarballs.sh` (and
   `.graalvm-jdk-version`) together, using GitHub's release API instead of a fixed URL
   pattern, for any version `25.1.0` and above.
3. Reapply the already-tested sandbox fix for part 1 (the fake "current folder" answer
   plus read-only access to GraalVM's bundled files) to `PythonContext.java`. It is
   independent of the native-module-isolation problem and already tested clean; do not
   skip re-testing it against whatever new version is being considered, in case
   something else has changed.
4. Only then let Dependabot suggest GraalVM upgrades again: narrow the `ignore` range
   for `org.graalvm.*:*` in `.github/dependabot.yaml` back down (or remove it).
