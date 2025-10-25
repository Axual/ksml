## To achieve this: https://github.com/Axual/ksml/issues/314

## New Dockerfile Changes

We need to write a new Dockerfile.

### In new Dockerfile:

1. We remove the maven stage, because the Java compilation will now happen in GitHub Actions with GraalVM.
   This allows us to leverage GitHub's Maven caching which saves many minutes per build. The maven stage was downloading 800MB of dependencies on every build without any caching.

2. We modify KSML stage to accept build artifacts from a `build-output/` directory, because GitHub Actions will pre-compile the Java code and upload the artifacts. The Docker build then only needs to copy these pre-built JARs into the image, reducing Docker build time. This also means we only compile once even when building for multiple architectures (amd64 + arm64).

3. We use COPY to bring in pre-built JARs from GitHub Actions artifacts, because this separates concerns: Java compilation happens where it's optimized (GitHub Actions with caching), and Docker build focuses only on containerization. This makes the build pipeline faster, more cacheable, and allows better visibility into test results.

### Dockerfile.local for Local Development

But we also need the old Dockerfile(renamed Dockerfile.local) because want to have the option to do these testing steps during our release procedure:

```bash
docker buildx create --name ksml
docker buildx --builder ksml build --load -t axual/ksml:local --target ksml -f Dockerfile .
docker compose up -d
./run.sh
```

The new command to test locally will be `docker buildx --builder ksml build --load -t axual/ksml:local --target ksml -f Dockerfile.local .`

### Detailed Dockerfile Changes:

1. We remove the maven stage, because the Java compilation will now happen in GitHub Actions with GraalVM. This allows us to leverage GitHub's Maven caching which saves 3-5 minutes per build. The maven stage was downloading 800MB of dependencies on every build without any caching.

2. We modify ksml stage to accept build artifacts from a build-output/ directory, because GitHub Actions will pre-compile the Java code and upload the artifacts. The Docker build then only needs to copy these pre-built JARs into the image, reducing Docker build time from 10 minutes to 2-3 minutes. This also means we only compile once even when building for multiple architectures (amd64 + arm64).

3. We use COPY to bring in pre-built JARs from GitHub Actions artifacts, because this separates concerns: Java compilation happens where it's optimized (GitHub Actions with caching), and Docker build focuses only on containerization. This makes the build pipeline faster, more cacheable, and allows better visibility into test results.

---

## Changes to GitHub Workflows

### New Workflow: `maven-build.yml`

I will create new Github workflow `maven-build.yml` that does these steps:

1. Checkout code
2. Setup GraalVM with Maven caching
3. Run Maven build (package)
4. Run Maven tests
5. (Optional) SonarQube scan
6. Upload these build artifacts to Github:
   - `ksml-runner/target/ksml-runner*.jar`
   - `ksml-runner/target/libs/`
   - `ksml-runner/NOTICE.txt`
   - `LICENSE.txt`
7. Publish test results to GitHub


### Modified Workflow: `build-push-docker.yml`

I will change existing Github workflow `build-push-docker.yml`

**Currently `build-push-docker.yml`:**

1. Installs QEMU and Docker Buildx
2. Builds arm64 and amd64 images from Dockerfile (which includes Maven build)
3. Uploads arm64 and amd64 to GHCR, Docker Hub, Harbor(Axual Registry)
4. Publishes helm chart with snapshot version (0.0.0-snapshot)

**New `build-push-docker.yml` will:**

1. First call the maven-build.yml workflow as a reusable workflow to:
   - Build Java artifacts with Maven + GraalVM
   - Run tests and publish results
   - Upload artifacts (ksml-runner*.jar, libs/, NOTICE.txt, LICENSE.txt)

2. Then in the Docker build job:
   - Checkout the code to get Dockerfile
   - Download the Maven artifacts to build-output/ directory
   - Install QEMU and Docker Buildx
   - Build Docker images for linux/amd64 and linux/arm64 using the pre-built artifacts
   - Push images to GHCR, Docker Hub, and Axual Registry with 'snapshot' tag
   - Trigger helm chart packaging with snapshot version

This approach compiles Java code only once (instead of twice for multi-arch), leverages Maven caching, and provides better visibility into the build process.


### Modified Workflow: `release-push-docker.yml`

I will similarly change `release-push-docker.yml` which will:

1. First call the maven-build.yml workflow as a reusable workflow to build the Java artifacts
2. Download the build artifacts (build-artifacts-${{ github.sha }}) to build-output/ directory
3. Checkout the code to get the Dockerfile
4. Build Docker images for both linux/amd64 and linux/arm64 platforms using the pre-built artifacts
5. Push images to all three registries (Docker Hub, GHCR, Axual Registry) with the release tag and 'latest' tag
6. Trigger helm chart packaging with the release version

This ensures releases are built consistently with the same artifacts for all architectures, and the build is faster because Maven only runs once regardless of how many Docker architectures we build.

### Modified Workflow: `build-and-test.yml`

I will change `build-and-test.yml`

**Currently it:**

1. Starts a local Docker registry container inside the same job
2. pulls the KSML repo into the runner
3. Sets up QEMU emulation that allows Docker to build multi-architecture images (amd64, arm64)
4. Set up Docker Buildx that connects to local registry
5. builds Docker image to local registry
6. prints metadata (architectures, layers, etc.) about the built image

**I will change it so it:**

1. First runs the maven-build.yml workflow which:
   - Sets up GraalVM with Maven caching enabled
   - Runs mvn clean package to compile the code
   - Runs mvn test to execute unit tests
   - Publishes test results to GitHub UI so developers can see which tests passed/failed
   - Optionally runs SonarQube scan (if SONAR_TOKEN is configured) for code quality checks
   - Uploads build artifacts for the Docker build step

2. Then runs a Docker build test job which:
   - Downloads the build artifacts from the maven-build job
   - Starts a local Docker registry container (for testing without pushing to external registries)
   - Sets up QEMU for multi-architecture support
   - Sets up Docker Buildx configured to use the local registry
   - Builds the Docker image using the new Dockerfile (which uses pre-built artifacts from build-output/)
   - Pushes to local registry
   - Inspects the image to verify it was built correctly

**This split allows us to:**

- See test results in GitHub's UI instead of buried in Docker logs
- Cache Maven dependencies between builds (3-5 min savings)
- Run SonarQube for code quality without modifying Dockerfile
- Verify both the Java build and Docker build work correctly in CI

---

## Optional Future Enhancement (Phase 2)

I can add a new GH workflow: `integration-tests.yml` that runs integration tests with TestContainers. This would:

- Run after PR merge or on a schedule (nightly)
- Use TestContainers to spin up real Kafka containers
- Execute integration tests that were previously skipped (skipITs=true)
- Publish integration test results to GitHub
- Provide confidence that the full system works end-to-end

This is optional because the current changes already provide significant value, and integration tests can be added in a future phase.