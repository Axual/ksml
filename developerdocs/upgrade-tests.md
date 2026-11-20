# Upgrade tests: does state from older KSML versions still read correctly?

The upgrade tests check that state written by an older KSML version reads correctly
after the upgrade to this version. The older version runs as a container from
`registry.axual.io`, and this version then runs on the same state directory and topics.

The tests are the integration tests whose class name ends in `UpgradeIT`, in
`ksml-integration-tests/src/test/java/io/axual/ksml/integration/`. For example,
`WindowStoreUpgradeIT` checks the window stores written by KSML 1.1.0 and 1.3.2.

## When they run

They take about 2 to 3 minutes and pull older KSML images, so they don't run with the
other integration tests in the normal build. Instead:

- the workflow `.github/workflows/upgrade-tests-nightly.yml` runs them every night on `main`;
- run that workflow by hand before a release, and when you change store or upgrade code:
  "Run workflow" on the Actions tab, or `gh workflow run upgrade-tests-nightly.yml --ref <branch>`.

## Run them locally

The Maven profile `upgrade-tests` runs only the upgrade tests:

```shell
mvn verify -pl ksml-integration-tests -am -Pupgrade-tests -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false
```

`-Dtest=none` skips the unit tests of the modules that are built. Docker must be running.

## Add an upgrade test

Name the class `<Something>UpgradeIT`. The name alone moves it out of the normal build
and into the `upgrade-tests` profile.
