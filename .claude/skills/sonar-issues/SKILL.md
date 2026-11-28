---
name: sonar-issues
description: Look up SonarCloud (SonarQube Cloud) issues, quality gate status and new-code measures for this repo - for a pull request, a branch, or the whole project - by calling the SonarCloud web API directly. Use when the user asks about "sonar issues", "sonar findings", "why is the quality gate failing", "code smells", or wants the Sonar result for a PR without opening CI logs or the browser.
---

# Look up SonarCloud issues without CI

This project is analysed by SonarCloud. You can read the results straight from the web API, so there is no need to open the GitHub Actions logs or the SonarCloud web UI.

## Setup

- Host: `https://sonarcloud.io`
- Organization: `axual-github`
- Project key: `ksml` (display name is `ksml-parent`)
- Token: your own SonarCloud user token, in the file `.sonar-token` (one line) in the root of your checkout. In a worktree, the file in the main checkout also works, so all worktrees can share one file. Git ignores this file. Create a token at https://sonarcloud.io/account/security. As an alternative, set the environment variable `SONAR_TOKEN`.

These values come from the root `pom.xml` (`sonar.organization`, `sonar.host.url`, and `sonar.projectKey` inside the `sonarqube` profile). If a lookup fails, read those properties again first.

Pass the token as the HTTP basic auth user name with an empty password:

```bash
for d in "$(git rev-parse --show-toplevel)" "$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"; do
  [ -f "$d/.sonar-token" ] && TOKEN=$(tr -d '\r\n' < "$d/.sonar-token") && break
done
TOKEN=${SONAR_TOKEN:-$TOKEN}
curl -s -u "$TOKEN:" "https://sonarcloud.io/api/authentication/validate"
# -> {"valid":true}
```

If no file is found and `SONAR_TOKEN` is not set, stop and ask the user to create the file. Do not look for a token anywhere else.

Never print the token, and never commit it.

## Step 1: find the analysis you want

For a pull request, list every PR that SonarCloud has analysed. This also gives the commit SHA that was analysed, so you can check that it matches your local `HEAD`:

```bash
curl -s -u "$TOKEN:" "https://sonarcloud.io/api/project_pull_requests/list?project=ksml" \
  | python3 -m json.tool
```

Each entry has `key` (the GitHub PR number), `branch`, `analysisDate`, `commit.sha` and a `status` block with `qualityGateStatus`, `bugs`, `vulnerabilities` and `codeSmells`. For `https://github.com/Axual/ksml/pull/669` the key is `669`.

Compare `commit.sha` with `git rev-parse HEAD`. If they differ, the Sonar result is older than your working tree. Say so in your answer, and do not present old findings as current.

For a long-lived branch (`main`, release branches) use:

```bash
curl -s -u "$TOKEN:" "https://sonarcloud.io/api/project_branches/list?project=ksml" \
  | python3 -m json.tool
```

## Step 2: fetch the issues

Every read endpoint takes either `pullRequest=<number>` or `branch=<name>`. Leave both out to query the main branch.

Filter with `resolved=false`. Without it, Sonar also returns closed and resolved issues, which makes the list look much worse than it is:

```bash
curl -s -u "$TOKEN:" \
  "https://sonarcloud.io/api/issues/search?componentKeys=ksml&pullRequest=669&resolved=false&ps=100" \
  | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('unresolved:', d['total'])
for i in d['issues']:
    path = i['component'].split(':',1)[1]
    print(f\"- [{i['severity']}/{i.get('type')}] {i['rule']} {path}:{i.get('line','-')}\")
    print(f\"  {i['message']}\")
"
```

For a PR that SonarCloud does not have, this call returns `total: 0` with no error. Check Step 1 first, so you do not report "0 issues" by mistake.

Useful extra query parameters:

- `types=BUG,VULNERABILITY,CODE_SMELL` - filter by kind
- `severities=BLOCKER,CRITICAL,MAJOR` - filter by severity
- `rules=java:S107` - all hits of one rule
- `componentKeys=ksml:ksml/src/main/java/...` - one file only (note the `ksml:` prefix before the path relative to the repository root)
- `ps=100` and `p=2` - page size and page number

The full response is large. Pipe it through `python3 -c` and print only what you need.

## Step 3: explain why the quality gate failed

The issue list does not tell you which gate condition broke. Ask for the gate directly:

```bash
curl -s -u "$TOKEN:" \
  "https://sonarcloud.io/api/qualitygates/project_status?projectKey=ksml&pullRequest=669" \
  | python3 -m json.tool
```

Each condition shows `metricKey`, `comparator`, `errorThreshold` and `actualValue`, so you can name the exact failing rule. For example, one new bug sets `new_reliability_rating` to `3` against a threshold of `1`.

For the numbers behind the gate:

```bash
curl -s -u "$TOKEN:" \
  "https://sonarcloud.io/api/measures/component?component=ksml&pullRequest=669&metricKeys=new_bugs,new_vulnerabilities,new_code_smells,new_coverage,new_lines,new_reliability_rating,new_maintainability_rating,new_security_rating,new_duplicated_lines_density" \
  | python3 -m json.tool
```

Ratings are numbers: `1.0` = A, `2.0` = B, `3.0` = C, and so on.

Security hotspots have their own endpoint and are not part of `api/issues/search`:

```bash
curl -s -u "$TOKEN:" "https://sonarcloud.io/api/hotspots/search?projectKey=ksml&pullRequest=<number>&status=TO_REVIEW" \
  | python3 -m json.tool
```

`status=TO_REVIEW` leaves out the hotspots that a person already marked as reviewed. If the answer is "Project 'ksml' doesn't exist", SonarCloud has no analysis for that PR (old PRs are removed). Check Step 1.

## Step 4: read the flagged code

The `component` field is `ksml:<path relative to the repository root>`. Remove the `ksml:` prefix and read the file at the reported line, so you can explain the finding and suggest a fix instead of only repeating Sonar's message.

## Rule explanations

To get the description of a rule:

```bash
curl -s -u "$TOKEN:" \
  "https://sonarcloud.io/api/rules/show?key=java:S6218&organization=axual-github" \
  | python3 -c "
import json,sys
r=json.load(sys.stdin)['rule']
print(r['key'], '-', r['name'])
"
```

This endpoint needs `organization=axual-github`. Without it the response has no `rule` field.

## Quality profile

- `ksml` uses the Java quality profile **Axual Java**, which inherits from "Sonar way comprehensive" and is used by `ksml` only. Read a rule's settings with `api/rules/show?key=java:<rule>&organization=axual-github&actives=true`.
- A profile rule applies to main and test code alike. To limit a rule to production code, add the project setting `sonar.issue.ignore.multicriteria` (rule key + `**/src/test/**`).
- Changing the profile or the project settings needs admin rights in SonarCloud. Give the user the steps instead of trying it yourself.

## Reporting back

Give the user:

1. The quality gate result and which condition failed.
2. The unresolved issues, most severe first, each with file, line, rule and message.
3. A note when the analysed commit is not your current `HEAD`.

A passing quality gate does not mean 0 issues. The gate (and the green `SonarCloud Code Analysis` check on GitHub) can pass while minor code smells are still open on new code. When the user wants the PR clean, report the number of open issues next to the gate result.

Do not report resolved or closed issues as if they still need work.

## History

- 2026-11-28 (#790): The hotspots call needs `projectKey=ksml` and `status=TO_REVIEW`.
- 2026-11-28 (#790): Read `.sonar-token` from the checkout, then from the main checkout, so worktrees can share one file.
