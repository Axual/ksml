# Contributing

Thank you for considering contributing to this project! Before making any changes, please open an [issue](https://github.com/Axual/ksml/issues) to discuss the proposed modifications.

Please also make sure to read and follow our [Code of Conduct](CODE_OF_CONDUCT.md) in all interactions with the project.

## Pull Request Process

1. Fork the repository and create a feature or bugfix branch.
2. Ensure your code follows the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).
2. Document your code clearly and thoroughly.
3. Include a unit or integration test that reproduces any bug you're fixing.
4. Keep changes focused and concise. Submit separate pull requests for unrelated changes, but you may combine minor bug fixes and tests.
6. Update the [`README.md`](README.md) when you introduce interface changes (e.g., new environment variables, file paths, etc.).
7. Submit a pull request from your fork to the main repository.
8. A pull request may be merged after approval from two developers. If you do not have merge permissions, ask the second reviewer to merge it for you.
## Before Sending for Review

The following two steps are **mandatory** before asking someone to review:

1. Run the SonarCloud analysis in your IDE and resolve any reported issues. See [Sonar IDE integration](#sonar-ide-integration) below.
2. Run `diff-cover` as described above and confirm that coverage of your changes is above 60%. See [Checking Diff Coverage](#checking-diff-coverage-coverage-of-changed-lines-only) below.

## Setting Up the Project for Local Development

To run the project locally:

1. **Clone the repository**:

   ```bash
   git clone https://github.com/Axual/ksml.git
   cd ksml
   ```

2. **Minimum requirements**:
  - Docker Engine 20.10.13+
  - Docker Compose v2 plugin: 2.17.0+
  - Java 21.0.7 with GraalVM runtime

3. **Prepare the environment**:
  - Ensure ports `8080`,`8081`,`9999` are open.
  - Comment out the `example-producer` container in `docker-compose.yml`.

4. **Start services**:

   ```bash
   docker compose up -d
   ```

5. **Set up the workspace**:
  - Create the folder: `ksml/workspace/local`
  - Add the following files:
    - `ksml-data-generator-local.yaml`
    - `ksml-runner-local.yaml`
  - To experiment with different examples, uncomment definitions in `ksml.definitions`.

6. **Run the KSML Runner**:
  - For the data generator:

    ```bash
    java io.axual.ksml.runner.KSMLRunner workspace/local/ksml-data-generator-local.yaml
    ```

    IntelliJ IDEA run configuration: `ksml/.run/KSML example producer LOCAL.run.xml`

  - For the processor:

    ```bash
    java io.axual.ksml.runner.KSMLRunner workspace/local/ksml-runner-local.yaml
    ```

    IntelliJ IDEA run configuration: `ksml/.run/KSML example processor LOCAL.run.xml`

### Metrics

KSML exposes runtime metrics in Prometheus format.
- Metrics are available at [http://localhost:9999/metrics](http://localhost:9999/metrics) by default.
- The metrics endpoint and other Prometheus settings can be configured by changing `ksml.prometheus` values.
- All custom KSML metrics start with `ksml_`
  - Metric labels (e.g., namespace, pipeline name) are defined within the KSML source code and can be modified
- Metrics that start with `kafka_` are native to Kafka and originate from its internal `kafka.producer`, `kafka.consumer`, and `kafka.streams` domains.
  -  These cannot be modified at the source level, but they can be transformed or filtered during post-processing

## Running Tests

To run tests and validate code coverage:

```shell
mvn clean test
```

- Ensure at least 60% code coverage.
- Use the following custom annotations when writing tests for definitions:
  - `@ExtendWith(KSMLTestExtension.class)`
  - `@KSMLTopic(topic = "xxx")`
  - `@KSMLTest(topology = "pipelines/xxx.yaml")`

### Checking Coverage Locally (same numbers as SonarCloud)

`mvn clean test` only generates per-module reports, which show lower numbers because each module only counts its own unit tests. To get the full aggregate report that matches what SonarCloud sees, run:

```shell
mvn clean verify --no-transfer-progress
```

This runs all tests across all modules and then generates the aggregate coverage report at:

```
ksml-reporting/target/site/jacoco-aggregate/index.html
```

Open that file in a browser to see the project-wide numbers.

The `Cov.` percentage shown at the top of that page is **instruction coverage** (bytecode-level), which as of this moment reads around 66%. That is not the number SonarCloud shows. To get the numbers that match SonarCloud, look at the **Total** row at the bottom of the table and read the `Lines` and `Missed` columns:

- **Line coverage** = (Lines - Missed Lines) / Lines
- **Branch coverage** = (Branches - Missed Branches) / Branches

For example, if the Total row shows 4,125 missed out of 12,679 lines and 2,787 missed out of 6,676 branches:

```
Line coverage:   (12679 - 4125) / 12679 = 67.5%
Branch coverage: (6676  - 2787) / 6676  = 58.3%
```

Those two numbers will match what SonarCloud reports.

Per-module reports (lower numbers, own tests only) are still at `<module>/target/site/jacoco/index.html` if you need them.

#### Checking Diff Coverage (coverage of changed lines only)

To check test coverage only for the lines changed in your branch compared to `main`, use [diff-cover](https://github.com/Bachmann1234/diff_cover). Install it with [pipx](https://pipx.pypa.io), which keeps CLI tools in isolated environments and puts them on your `PATH` automatically:

```shell
pipx install diff-cover
```

If you don't have `pipx`, install it first:

```shell
brew install pipx
```

Make sure you have run `mvn clean verify --no-transfer-progress` first to produce the per-module JaCoCo reports, then run:

```shell
diff-cover ksml-reporting/target/site/jacoco-aggregate/jacoco.xml \
  --compare-branch origin/main \
  --src-roots **/src/main/java \
  --format html:diff-coverage.html
```

Open the report:

```shell
open diff-coverage.html
```

This generates an HTML report showing test coverage for every line added or modified in your branch. Aim for **>60% coverage** on the changed code before submitting a pull request.

## Sonar IDE Integration

SonarCloud can analyse your branch against `main` directly inside IntelliJ IDEA, giving you instant feedback without pushing to CI.

### Generating a SonarCloud token

1. Sign in at [sonarcloud.io](https://sonarcloud.io).
2. Click your avatar in the top-right corner and select **My Account**.
3. Go to the **Security** tab.
4. Under **Generate Tokens**, enter a name (e.g. `local-ide`) and click **Generate**.
5. Copy the token, it is shown only once.

### IntelliJ IDEA

1. Install the **SonarQube for IDE** plugin (*Settings → Plugins → Marketplace*).
2. Open *Settings → Tools → SonarQube for IDE → Settings*.
3. Under **SonarQube / SonarCloud connections**, click **+** and set a name for the new connection and choose **SonarCloud**.
4. Paste your token (or generate a new one through the `Generate Token` option)
5. Proceed until the end
6. Bind the project: in the same settings page open **Project Settings**, enable **Bind to SonarQube / SonarCloud**, and select the `ksml` project.
7. Click `Analyze All Project Files` and then click `Show Filters`
8. Enable `New Code` option. This will compare the current branch with the previous released version e.g. 1.3.0. As a result, the issues shown reflect only what your branch introduces relative to the base.

> **Note:** To keep this happening, every issue should be resolved before merging back to the main branch. In different case, we will end up seeing issues which are not introduced by our code but from a previous commit after the latest release.

## Next Steps

Explore examples and advanced use cases in [`ksml-blog.md`](ksml-blog.md).