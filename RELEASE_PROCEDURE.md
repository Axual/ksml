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
6. Push tag and test RC
7. Create additional RCs (RC2, RC3) as needed

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

### Step 3: Create Release Branch (Major/Minor Releases Only)

After the release tag is created on main:

```bash
git checkout -b release/1.1.x 1.1.0
git push origin release/1.1.x
```

**Important**: The release branch is created from the **final release tag** (e.g., 1.1.0), not from any RC tag. This ensures the release branch contains all fixes and changes made during the RC process.

### Step 4: Create GitHub Release

1. Go to GitHub -> Releases -> "Draft new release"
2. Select the new tag (e.g., `1.1.0`)
3. Set previous tag for comparison (e.g., `1.0.8`)
4. Click "Generate release notes"
5. Structure the release notes:
   - **"What's Changed"**: Write concise summary of key changes
   - **"Full Changelog"**: Keep auto-generated commit list
6. Publish the release

### Step 5: Monitor Build

1. Go to GitHub -> Actions -> Monitor release workflow
2. Ensure the build completes successfully

### Step 6: Update GitHub Actions (Major/Minor Only)

For major/minor releases, update the release branch:
1. Checkout the release branch: `git checkout release/1.1.x`
2. Set Docker tags to `<major>.<minor>-snapshot`
3. Set Chart versions to `<major>.<minor>.0-snapshot`
4. Add `<major>.<minor>` Docker tag in release workflow
5. Commit and push changes

### Step 7: Set Next Development Version

1. In release branch:
   ```bash
   git checkout release/1.1.x
   mvn versions:set -DgenerateBackupPoms=false
   # Enter next patch snapshot: 1.1.1-SNAPSHOT
   ```
   Commit: `git commit -m "Prepare for next development iteration"`
   Push: `git push origin release/1.1.x`

2. In main branch:
   ```bash
   git checkout main
   mvn versions:set -DgenerateBackupPoms=false
   # Enter next minor/major snapshot: 1.2.0-SNAPSHOT
   ```
   Commit: `git commit -m "Prepare for next development iteration"`
   Push: `git push origin main`

## Patch Release Process

When doing a patch release (e.g., 1.0.9, 1.1.1):

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

Same as Step 2.4 in major/minor releases

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

Same as Step 4 in major/minor releases

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
```
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