# Local SonarQube Setup

Run a SonarQube instance locally to analyse the project without pushing to SonarCloud.

## 1. Start SonarQube

```bash
docker run -d --name sonarqube -p 9000:9000 sonarqube:community
```

Wait for it to be ready (~60 s):

```bash
until curl -s http://localhost:9000/api/system/status | grep -q '"status":"UP"'; do
  echo "Waiting..."; sleep 5
done && echo "SonarQube is UP"
```

## 2. Generate a token

```bash
curl -s -u admin:admin -X POST "http://localhost:9000/api/user_tokens/generate" \
  -d "name=local-scan" \
  | grep -o '"token":"[^"]*"' | cut -d'"' -f4
```

Default credentials are `admin` / `admin`.

## 3. Run the Maven analysis

```bash
mvn verify sonar:sonar \
  -DskipTests \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.projectKey=ksml \
  -Dsonar.organization= \
  -Dsonar.token=<token-from-step-2>
```

- `-Dsonar.projectKey=ksml` matches the key defined in `pom.xml`.
- `-Dsonar.organization=` (empty value) overrides the SonarCloud org set in `pom.xml` so the local server accepts the scan.
- `-DskipTests` speeds up the build but omits coverage data — remove it if you want full results.

## 4. View results

Open <http://localhost:9000> — the project appears once the server has processed the report (a few seconds after the build finishes).

## Cleanup

```bash
docker stop sonarqube && docker rm sonarqube
```

> If you used a different container name (e.g. `docker run --name my-sonar ...`), substitute that name in the stop/rm commands above.
