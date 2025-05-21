# Contributing

Thank you for considering contributing to this project! Before making any changes, please open an [issue](https://github.com/Axual/ksml/issues) to discuss the proposed modifications.

Please also make sure to read and follow our [Code of Conduct](CODE_OF_CONDUCT.md) in all interactions with the project.

## Merge Request Process

1. Fork the repository and create a feature or bugfix branch.
2. Ensure your code follows the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).
2. Document your code clearly and thoroughly.
3. Maintain at least 70% test coverage. Include a unit or integration test that reproduces any bug you're fixing.
4. Keep changes focused and concise. Submit separate pull requests for unrelated changes, but you may combine minor bug fixes and tests.
6. Update the [`README.md`](README.md) when you introduce interface changes (e.g., new environment variables, file paths, etc.).
7. Submit a pull request from your fork to the main repository.
8. A pull request may be merged after approval from two developers. If you do not have merge permissions, ask the second reviewer to merge it for you.

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

- Ensure at least 70% code coverage.
- Use the following custom annotations when writing tests for definitions:
  - `@ExtendWith(KSMLTestExtension.class)`
  - `@KSMLTopic(topic = "xxx")`
  - `@KSMLTest(topology = "pipelines/xxx.yaml")`

## Next Steps

Explore examples and advanced use cases in [`ksml-blog.md`](ksml-blog.md).