# KSML Release Procedure

## Overview

- **`main` branch**: Development branch for new features
- **`release/<major>.<minor>.x` branches**: Used for releases and patches
- All releases are built from release branches
- Changelog is maintained through GitHub Releases

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
8. Create release branch from first RC:
   ```bash
   git checkout -b release/1.1.x <first-RC-commit>
   ```

### Step 2: Set Release Version

1. Switch to appropriate branch:
   - **Major/Minor**: Use newly created `release/<major>.<minor>.x`
   - **Patch**: Use existing `release/<major>.<minor>.x`

2. Set the release version:
   ```bash
   mvn versions:set -DgenerateBackupPoms=false
   # Enter version like: 1.0.8
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

1. Modify `run.sh` and `docker-compose.yml` to use `axual/ksml:local`
2. Start environment:
   ```bash
   docker compose up -d
   ```
3. Verify data generator:
   ```bash
   docker compose logs example-producer
   ```
4. Execute `./run.sh` and verify:
   - Correct version appears: `Starting KSML Runner x.x.x (2025-...)`
   - Wait ~2 minutes for examples to run without errors
   - Stop the script after verification

### Step 6: Commit Changes

```bash
git add pom.xml **/NOTICE.txt
git commit -m "Release 1.0.8"
```
**Do not push yet** - tag must be created first

### Step 7: Create and Push Tag

```bash
git tag 1.0.8 -m "Release 1.0.8" -a
git push origin 1.0.8
```

### Step 8: Create GitHub Release

1. Go to GitHub → Releases → "Draft new release"
2. Select the new tag (e.g., `1.0.8`)
3. Set previous tag for comparison (e.g., `1.0.7`)
4. Click "Generate release notes"
5. Structure the release notes:
   - **"What's Changed"**: Write concise summary of key changes
   - **"Full Changelog"**: Keep auto-generated commit list
6. Publish the release

### Step 9: Monitor Build and Push Branch

1. Go to GitHub → Actions → Monitor release workflow
2. **Only after successful build**, push the branch:
   ```bash
   git push origin release/1.0.x
   ```

### Step 10: Update GitHub Actions (Major/Minor Only)

For major/minor releases, update the release branch:
1. Set Docker tags to `<major>.<minor>-snapshot`
2. Set Chart versions to `<major>.<minor>.0-snapshot`
3. Add `<major>.<minor>` Docker tag in release workflow

### Step 11: Set Next Development Version

1. In release branch:
   ```bash
   mvn versions:set -DgenerateBackupPoms=false
   # Enter next patch snapshot: 1.0.9-SNAPSHOT
   ```
2. Commit: `git commit -m "Prepare for next development iteration"`
3. Push: `git push origin release/1.0.x`

4. For major/minor releases, also update main branch:
   ```bash
   git checkout main
   mvn versions:set -DgenerateBackupPoms=false
   # Enter next minor/major snapshot: 1.2.0-SNAPSHOT
   ```
   Commit and push main branch

## Key Differences for Patch Releases

When doing a patch release (e.g., 1.0.9, 1.1.1):

1. **Skip Step 1** - No release candidates needed
2. **Use existing release branch** - Don't create a new one
3. **Skip Step 10** - GitHub Actions already configured
4. **Skip main branch update in Step 11** - Only update release branch

Apply fixes by either:
- Cherry-picking from main: `git cherry-pick <commit-hash>`
- Creating fixes directly in release branch

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
3. Fix issues and restart from Step 2