# KSML Release Procedure

## Overview

- **`main` branch**: Development branch for new features
- **`release/<major>.<minor>.x` branches**: Used for releases and patches
- All releases are built from release branches
- Changelog is maintained through GitHub Releases
- For major/minor releases: RC versions and the actual release are created on main, then release branch is created from the final release tag

## Complete Release Process

### Step 1: Prepare Release Candidates (Major/Minor Releases Only)

For major/minor releases (e.g., 1.1.0, 2.0.0):

1. Ensure all features are merged to `main`
2. Set RC version in `main` branch:
   ```bash
   mvn versions:set -DgenerateBackupPoms=false
   # Enter version like: 1.1.0-RC1
   ```
3. Run `mvn clean package -DskipTests` to update NOTICE.txt files
4. Build, test, commit with message `Release 1.1.0-RC1`
5. Tag: `git tag 1.1.0-RC1 -m "Release 1.1.0-RC1" -a`
6. Push tag and test RC `git push origin 1.1.0-RC1`. For example by running e2e tests in Staging Cloud.
7. Create additional RCs (RC2, RC3) as needed
8. Go to GitHub -> Releases -> "Draft new release" -> "Generate release notes" -> uncheck "latest release" -> check "pre-release"-> OK
9. Test the RC1 image from Harbor(for example in e2e tests in Staging Cloud)

### Step 2: Release Major/Minor Version (Major/Minor Releases Only)

Still on `main` branch:

1. Set the release version:
   ```bash
   mvn versions:set -DgenerateBackupPoms=false
   # Enter version like: 1.1.0
   ```

2. Update NOTICE.txt files:
   ```bash
   mvn clean package -DskipTests
   ```

3. Build Docker Image Locally:
   ```bash
   docker buildx create --name ksml
   docker buildx --builder ksml build --load -t axual/ksml:local --target ksml -f Dockerfile .
   ```

4. Test the Release:
   - Modify `run.sh` and `docker-compose.yml` to use `axual/ksml:local`
   - Start environment: `docker compose up -d`
   - Verify data generator: `docker compose logs example-producer`
   - Execute `./run.sh` and verify:
      - Correct version appears: `Starting KSML Runner x.x.x (2025-...)`
      - Wait ~2 minutes for examples to run without errors
      - Stop the script after verification

5. Commit changes:
   ```bash
   git add pom.xml **/NOTICE.txt
   git commit -m "Release 1.1.0"
   ```

6. Create and push tag:
   ```bash
   git tag 1.1.0 -m "Release 1.1.0" -a
   git push origin 1.1.0
   ```

### Step 3: Create Release Branch and Update GitHub Actions (Major/Minor Releases Only)

After the release tag is created on main:

1. **Create release branch from release tag:**
   ```bash
   git checkout -b release/1.1.x 1.1.0
   ```

   **Important**: The release branch is created from the **final release tag** (e.g., 1.1.0), not from any RC tag. This ensures the release branch contains all fixes and changes made during the RC process.

2. **Update GitHub Actions on the release branch:**

   a. Update `.github/workflows/build-push-docker.yml`:
   - Change Docker tags from `snapshot` to `<major>.<minor>-snapshot`
   - Update helm-chart-release job: `app-version: <major>.<minor>-snapshot`, `version: <major>.<minor>.0-snapshot`

   b. Update `.github/workflows/package-push-helm.yml`:
   - Change default `version` parameter to `<major>.<minor>.0-snapshot` in both workflow_dispatch and workflow_call

   c. Update `.github/workflows/release-push-docker.yml`:
   - Add `<major>.<minor>` tag to all Docker registries (axual/ksml, ghcr.io, registry.axual.io)

3. **Commit and push the release branch:**
   ```bash
   git add .github/workflows/*.yml
   git commit -m "Update GitHub Actions for release/<major>.<minor>.x branch"
   git push origin release/<major>.<minor>.x
   ```

### Step 4: Create GitHub Release

1. Go to GitHub -> Releases -> "Draft new release"
2. Select the new tag (e.g., `1.1.0`)
3. Set previous tag for comparison (e.g., `1.0.8`)
4. Click "Generate release notes"
5. Structure the release notes:
   - **"What's Changed"**: Write concise summary of key changes
   - **"Full Changelog"**: Keep auto-generated commit list
6. Publish the release
7. Upload ksml-language-spec.json

### Step 5: Monitor Build

1. Go to GitHub -> Actions -> Monitor release workflow
2. Ensure the build completes successfully

### Step 6: Set Next Development Version

1. In release branch:
   ```bash
   git checkout release/1.1.x
   mvn versions:set -DgenerateBackupPoms=false
   # Enter next patch snapshot: 1.1.1-SNAPSHOT
   mvn clean package -DskipTests # To generate NOTICE.TXT
   ```
   Update `Chart.yaml` version and appVersion, i.e. "version: 1.1.1-SNAPSHOT" and "appVersion: "1.1-snapshot""
   Commit: `git commit -m "Prepare for next development iteration"`
   Push: `git push origin release/1.1.x`

2. In main branch:
   ```bash
   git checkout main
   mvn versions:set -DgenerateBackupPoms=false
   # Enter next minor/major snapshot: 1.2.0-SNAPSHOT
   mvn clean package -DskipTests # To generate NOTICE.TXT
   ```
   Update `Chart.yaml` version and appVersion, i.e. "version: 1.2.0-SNAPSHOT" and "appVersion: "1.2-snapshot""
   Commit: `git commit -m "Prepare for next development iteration"`
   Push: `git push origin main`

## Patch Release Process

When doing a patch release (e.g., 1.0.9, 1.1.1):

**Note**: GitHub Actions do not need to be updated for patch releases as they already contain the correct snapshot versions for the release branch.

### Step 1: Prepare Patch

1. Checkout existing release branch:
   ```bash
   git checkout release/1.0.x
   ```

2. Apply fixes by either:
   - Cherry-picking from main: `git cherry-pick <commit-hash>`
   - Creating fixes directly in release branch

### Step 2: Set Release Version

```bash
mvn versions:set -DgenerateBackupPoms=false
# Enter version like: 1.0.9
```

### Step 3: Build and Update Files

1. Update NOTICE.txt files:
   ```bash
   mvn clean package -DskipTests
   ```

### Step 4: Build Docker Image Locally

```bash
docker buildx create --name ksml
docker buildx --builder ksml build --load -t axual/ksml:local --target ksml -f Dockerfile .
```

### Step 5: Test the Release

- Modify `run.sh` and `docker-compose.yml` to use `axual/ksml:local`
- Start environment: `docker compose up -d`
- Verify data generator: `docker compose logs example-producer`
- Execute `./run.sh` and verify:
   - Correct version appears: `Starting KSML Runner x.x.x (2025-...)`
   - Wait ~2 minutes for examples to run without errors
   - Stop the script after verification

### Step 6: Commit Changes

```bash
git add pom.xml **/NOTICE.txt
git commit -m "Release 1.0.9"
```

### Step 7: Create and Push Tag

```bash
git tag 1.0.9 -m "Release 1.0.9" -a
git push origin 1.0.9
```

### Step 8: Create GitHub Release

1. Go to GitHub -> Releases -> "Draft new release"
2. Select the new tag (e.g., `1.0.9`)
3. Set previous tag for comparison (e.g., `1.0.8`)
4. Click "Generate release notes"
5. Structure the release notes:
   - **"What's Changed"**: Write concise summary of key changes
   - **"Full Changelog"**: Keep auto-generated commit list
6. Publish the release
7. Upload ksml-language-spec.json

### Step 9: Monitor Build and Push Branch

1. Go to GitHub -> Actions -> Monitor release workflow
2. **Only after successful build**, push the branch:
   ```bash
   git push origin release/1.0.x
   ```

### Step 10: Set Next Development Version

```bash
mvn versions:set -DgenerateBackupPoms=false
# Enter next patch snapshot: 1.0.10-SNAPSHOT
mvn clean package -DskipTests # To generate NOTICE.TXT
```
Update `Chart.yaml` version and appVersion, i.e. "version: 1.0.10-SNAPSHOT" and "appVersion: "1.0-snapshot""
Commit: `git commit -m "Prepare for next development iteration"`
Push: `git push origin release/1.0.x`

## Version Numbering

- **Major**: Breaking changes
- **Minor**: New features (backward compatible)
- **Patch**: Bug fixes
- **Snapshot**: Development versions (`-SNAPSHOT`)
- **RC**: Release candidates (`-RC<number>`)

## Rollback Procedure

If issues found after tagging but before release build:
1. Delete remote tag: `git push origin :refs/tags/<version>`
2. Delete local tag: `git tag -d <version>`
3. Fix issues and restart from appropriate step