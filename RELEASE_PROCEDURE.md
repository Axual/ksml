# KSML Release Procedure

## Branching Strategy

- **`main` branch**: Development branch for new features and upcoming major/minor releases
- **`release/<major>.<minor>.x` branches**: Used for initial releases and subsequent patch releases
- All releases are built from `release/<major>.<minor>.x` branches
- Release candidates for new major/minor versions are tested in `main` before creating a release branch

## Release Types

### Major/Minor Release (e.g., 1.1.0, 2.0.0)

1. **Prepare Release Candidate in Main**
  - Ensure all features for the release are merged to `main`
  - Tests have been run

2. **Create Release Branch**
   ```bash
   git checkout main
   git checkout -b release/<major>.<minor>.x
   # Example: git checkout -b release/1.1.x
   ```

3. **Update GitHub Actions in Release Branch**

   a. Update `.github/workflows/build-push-docker.yml`:
  - Set Docker tags to `<major>.<minor>-snapshot`
  - Set Chart app-version to `<major>.<minor>-snapshot`
  - Set Chart version to `<major>.<minor>.0-snapshot`

   b. Update `.github/workflows/release-push-docker.yml`:
  - Add additional Docker tag `<major>.<minor>` alongside version-specific tags

   c. Update `.github/workflows/package-push-helm.yml`:
  - Set default app-version and version to `<major>.<minor>.0-snapshot`

4. **Continue with Standard Release Process** (see below)

### Patch Release (e.g., 1.0.9, 1.1.1)

1. **Work in Existing Release Branch**
   ```bash
   git checkout release/<major>.<minor>.x
   # Example: git checkout release/1.0.x
   ```

2. **Apply Fixes**
  - Cherry-pick fixes from `main` or create fixes directly in release branch
  - Run tests

3. **Continue with Standard Release Process** (see below)

## Standard Release Process

This process applies to both major/minor and patch releases after the appropriate branch is prepared.

### 1. Prepare Version Strings

Replace all occurrences of `<major>.<minor>.<patch>-SNAPSHOT` with `<major>.<minor>.<patch>` in the release branch.

Example locations:
- `pom.xml` files
- Any version configuration files

### 2. Build and Update Files

```bash
mvn clean package -DskipTests
```

This ensures `NOTICE.txt` files and other generated files are updated with the correct version.

### 3. Build Docker Image Locally

```bash
docker buildx create --name ksml
docker buildx --builder ksml build --load -t axual/ksml:local --target ksml -f Dockerfile .
```

### 4. Test Local Image

a. Update test configurations to use local image:
- Modify `run.sh` to use `axual/ksml:local`
- Update `docker-compose.yml` to use `axual/ksml:local` for all KSML services

b. Start test environment:
   ```bash
   docker compose up -d
   ```

c. Verify data generation:
   ```bash
   docker compose logs example-generator
   ```

### 5. Verify KSML Runner

a. Run the test script:
   ```bash
   ./run.sh
   ```

b. Verify:
- Correct version appears in logs: `Starting KSML Runner <major>.<minor>.<patch> (2025-...)`
- No errors occur after ~2 minutes of running examples
- Stop the script after verification

### 6. Set Release Version

```bash
mvn versions:set -DgenerateBackupPoms=false
```

When prompted, enter the release version (e.g., `1.1.0` or `1.0.9`)

### 7. Final Build

```bash
mvn clean package -DskipTests
```

### 8. Commit Changes

```bash
git add -A
git commit -m "Release <major>.<minor>.<patch>"
# Example: git commit -m "Release 1.1.0"
```

**Do not push yet!**

### 9. Create and Push Tag

```bash
git tag <major>.<minor>.<patch> -m "Release <major>.<minor>.<patch>" -a
git push origin <major>.<minor>.<patch>
# Example: git tag 1.1.0 -m "Release 1.1.0" -a
# Example: git push origin 1.1.0
```

Note: Pushing the tag first allows for easier rollback if issues occur.

### 10. Create GitHub Release

1. Navigate to GitHub → Releases → "Draft new release"
2. Select the tag you just pushed
3. Select the appropriate previous tag for comparison
4. Generate release notes
5. Structure the release notes with:
  - **What's Changed**: Manual summary of major changes
  - **Commits**: Auto-generated commit list

### 11. Final Push

Monitor the GitHub Actions for the tag. If the build succeeds:

```bash
git push origin release/<major>.<minor>.x
# Example: git push origin release/1.1.x
```

### 12. Prepare for Next Development Cycle

In the release branch, update version strings to the next snapshot version:

```bash
mvn versions:set -DgenerateBackupPoms=false
# Enter next snapshot version, e.g., 1.1.1-SNAPSHOT
git add -A
git commit -m "Prepare for next development iteration"
git push origin release/<major>.<minor>.x
```

## Version Numbering Guidelines

- **Major version**: Breaking changes or significant architectural changes
- **Minor version**: New features, backward compatible
- **Patch version**: Bug fixes and minor improvements
- **Snapshot versions**:
  - Development versions use `-SNAPSHOT` suffix
  - Docker images use `<major>.<minor>-snapshot` tags
  - Helm charts require semantic versioning, use `<major>.<minor>.0-snapshot`

## Troubleshooting

If something goes wrong after tagging:
1. Delete the tag locally: `git tag -d <version>`
2. Delete the tag remotely: `git push origin :refs/tags/<version>`
3. Fix the issues
4. Start again from step 9