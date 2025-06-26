# KSML Release Procedure

## 1. Prepare Release Branch
- Check out `release/1.0.x`
- Change strings `"x.x.x-SNAPSHOT"` to `"x-x-x"` in `release/1.0.x`
  - Example: https://github.com/Axual/ksml/commit/d9bea98a3a14d1c4c16e6384902f35c6cc9c771a

## 2. Build and Update Files
- Run `mvn package -DskipTests` to make sure `NOTICE.txt` files are updated
- Note: Local tests in `release/1.0.x` are not important to run, only the tests that the docker build runs are important

## 3. Build Docker Image
```bash
docker buildx create --name ksml
docker buildx --builder ksml build --load -t axual/ksml:local --target ksml -f Dockerfile .
```

## 4. Test Local Image
- Change `run.sh` and `docker-compose.yml` to use the local image
  - Make sure `run.sh` and `example-producer` in `docker-compose.yml` are using local image: `axual/ksml:local`
- Run `docker compose up -d` (activates the data generator)
  - Check that `example-producer` is producing correctly with `docker compose logs example-generator`

## 5. Verify KSML Runner
- Run `./run.sh`
  - Make sure this datetime is correct in the logs: `Starting KSML Runner x.x.x-SNAPSHOT (2025-...)`
  - Wait ~2 min and see that no errors when running all examples, then stop the script

## 6. Set New Version
- Run this command in root directory to generate new version:
```bash
mvn versions:set -DgenerateBackupPoms=false
```
- Enter the new version to set, for example `"1.0.8"`
- Run `mvn clean package -DskipTests`

## 7. Commit Changes
- Commit all the changes to `pom.xml` and `NOTICE.txt` files
- Commit message: `"Release 1.0.8"`
- **Don't push yet**

## 8. Create and Push Tag
- Create annotated tag:
```bash
git tag 1.0.8 -m "Release 1.0.8" -a
```
- Push the tag:
```bash
git push origin 1.0.8
```
- Note: This makes it easier to revert if something goes wrong with the tag

## 9. Create GitHub Release
- Go to GitHub → Releases → "Draft new release"
- Choose a tag → `1.0.8`
- Previous tag → `1.0.7`
- Generate release notes
- Make 2 sections:
    - **"What's changed"** (freetext summary)
    - **"Commits"** (automatically generated)

## 10. Final Steps
- Go to GitHub → Actions → `1.0.8`
- If this succeeds, do `git push` to `release/1.0.x`