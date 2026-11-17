# Review: branch `punctuate-operation`

Scope: all commits on `punctuate-operation` since `main` (9 commits), plus the uncommitted change in `examples/21-example-punctuate.yaml`.
Only problems are listed, from the most to the least important.

| # | Priority | Title |
|---|----------|-------|
| 1 | HIGH | `forward(to=...)` skips a repartition |
| 2 | HIGH | A Python float without a type becomes a long or fails |
| 3 | HIGH | Unrelated changes are mixed into this branch |
| 4 | MEDIUM | Example comment is wrong after the `streamTime` change |
| 5 | MEDIUM | `stores` is no longer a global in Python |
| 6 | MEDIUM | Result conversion is skipped for all functions |
| 7 | MEDIUM | Changed test: produce block schema allows `messages` and `generator` together |
| 8 | MEDIUM | Line-ending changes are mixed with code changes |
| 9 | MEDIUM | Core `FatalError` now imports an Apicurio class |
| 10 | MEDIUM | Example 21 is missing from `examples/ksml-runner.yaml` |
| 11 | MEDIUM | The openspec change is not archived |
| 12 | LOW | Changed test: byte overflow error message |
| 13 | LOW | Exception cause is lost in `ProcessorContextProxy` |
| 14 | LOW | Extra `stores` field only on the punctuate operation |
| 15 | LOW | Apicurio client factory uses reflection and keeps a dead v2 path |
| 16 | LOW | JSON Schema and Protobuf notations now create the Apicurio client early |
| 17 | LOW | Injected parameter lists can be changed from outside |
| 18 | LOW | Naming: `PYTHON_PROXY` does not match `INJECT_STORES` |
| 19 | LOW | Old Javadoc in `PythonContext.registerGlobalCode` |
| 20 | LOW | Docs: wrong function count, too much detail in the operation reference |
| 21 | LOW | Leftover `log.warn` in the example |
| 22 | LOW | Old lines in openspec `tasks.md` |
| 23 | LOW | Missing tests |
| 24 | NITPICK | Formatting changes not related to the feature |
| 25 | NITPICK | Comments that are too long |
| 26 | NITPICK | Small code clean-ups |
| 27 | NITPICK | Small doc and test-resource fixes |

---

## 1. HIGH: `forward(to=...)` skips a repartition

**Location:** `ksml/src/main/java/io/axual/ksml/operation/OperationChain.java:50`, `PunctuateOperation.java:129-134`

**PR code**
```java
if (!punctuates.isEmpty() && cursor instanceof KStreamWrapper stream && isNamed(operation)) {
    for (final var punctuate : punctuates) stream = punctuate.sidePathTo((BaseOperation) operation, stream);
    cursor = stream;
}
```

**Suggested change**
```java
// A repartition starts a new sub-topology; a side path must not jump over it
if (operation instanceof RepartitionOperation) punctuates.clear();
if (!punctuates.isEmpty() && cursor instanceof KStreamWrapper stream && isNamed(operation)) {
```
Or reject a `to=` target that comes after a repartition.

**Reason:** With `punctuate -> repartition -> peek(name: after)`, the input source and the repartition source end up in the same sub-topology. Records sent with `to="after"` do not go through the repartition, so they can be in the wrong partition for later stateful operations. The real effect in a cluster was not tested. No test covers a target after a repartition.

---

## 2. HIGH: A Python float without a type becomes a long or fails

**Location:** `ksml/src/main/java/io/axual/ksml/python/PythonNativeMapper.java:113`

**PR code**
```java
// Without a numeric expected type, read an int as a long and a float as a double. ...
if (object.fitsInLong() || object.fitsInBigInteger()) return object.asLong();
return object.asDouble();
```

**Suggested change**
```java
// Without a numeric expected type, a Python float becomes a double and an int a long
if (object.fitsInLong()) {
    final var isFloat = "float".equals(object.getMetaObject().getMetaSimpleName());
    return isFloat ? object.asDouble() : object.asLong();
}
if (object.fitsInBigInteger() && !"float".equals(object.getMetaObject().getMetaSimpleName()))
    return object.asLong(); // fails with a clear error: int too large for a long
return object.asDouble();
```

**Reason:** I checked this in GraalPy. The Python float `3.0` gives `fitsInLong() == true`, so it becomes a long, while `2.5` becomes a double. So the type changes per value. The Python float `1e20` gives `fitsInBigInteger() == true`, so `asLong()` fails, although the comment says floats become doubles. The new test `toDataObjectNumberWithoutExpectedType` only tests `1.5` and `7`. Add `3.0` and `1e20`.

---

## 3. HIGH: Unrelated changes are mixed into this branch

**Location:** commit `0bda20c1` and commit `51d6a5e5`

**PR code**
- `ksml-data-apicurio/.../ApicurioRegistryClientFactory.java` (+ test), changes in the three Apicurio notation providers, `docs/migration-to-2.0.0.md` (TLS section), `docs/release-notes.md` (mTLS line)
- `ksml/.../execution/FatalError.java`: `case ProblemDetails pd -> ...`
- 11 `NOTICE.txt` files with new dependency versions (for example `kafka-clients:4.3.1` -> `4.4.0`)

**Suggested change:** Move the Apicurio mTLS work and the `FatalError` change to their own branch and PR. Drop the `NOTICE.txt` changes, or regenerate them in a separate commit on `main`.

**Reason:** This is a punctuate PR. Unrelated features make the review harder, and they can not be reverted on their own. The `NOTICE.txt` files are generated by the build and change by themselves when dependencies change.

---

## 4. MEDIUM: Example comment is wrong after the `streamTime` change

**Location:** `examples/21-example-punctuate.yaml:3-5`, `:70` (uncommitted change)

**PR code**
```yaml
# ... every minute it emits the totals and clears the store, also when no new records arrive.
      - type: punctuate
        punctuationType: streamTime
```

**Suggested change**
```yaml
        punctuationType: wallClockTime
```
Or keep `streamTime` and change the comment to: `# When the record timestamps move on by a minute, it emits the totals and clears the store.`

**Reason:** Stream time only moves when records arrive, so "also when no new records arrive" is not true any more. openspec tasks 5.1 and 5.4 say the same wrong thing. The uncommitted change also moved `name` from the first field to the last field. Other examples put `name` first.

---

## 5. MEDIUM: `stores` is no longer a global in Python

**Location:** `ksml/src/main/java/io/axual/ksml/python/PythonFunction.java:262`

**PR code**
```java
final var value = "_ksml_injected_parameters.get(\"" + param.name() + "\")";
result.append("  ").append(param.name()).append(" = ");
```
(On `main`: `global stores` and `stores = globalVars["stores"]` in every function, and `stores = None` at module level.)

**Suggested change:** Keep `stores` working in the old places, or write it down as a breaking change in `docs/migration-to-2.0.0.md`:
```markdown
`stores` is now a local variable of the function. Helper functions in `globalCode` can not read it any more; pass the store as an argument.
```

**Reason:** On `main`, `stores` was a module global. A helper function in `globalCode` could use it, and every function had it (an empty dict when no store was declared). Now it only exists as a local variable, and only when the function declares stores. Existing user code can now fail with `NameError`. No test covers this.

---

## 6. MEDIUM: Result conversion is skipped for all functions

**Location:** `ksml/src/main/java/io/axual/ksml/python/PythonFunction.java:140`

**PR code**
```java
if (converter != null && resultType.dataType().isAssignableFrom(result).isNotAssignable())
    result = converter.convert(DEFAULT_NOTATION, result, resultType);
```

**Suggested change:** Remove it from this branch (keep `if (converter != null)`), or move it to its own PR with tests for structs with a schema, unions and nested lists.

**Reason:** This changes the result of every Python function, not only punctuators. `ConvertUtil.convert` also goes into lists, maps and structs, and runs notation converters. Skipping it when the outer type "fits" can give different nested values than before. No test checks that the results are the same as before.

---

## 7. MEDIUM: Changed test: produce block schema allows `messages` and `generator` together

**Location:** `ksml-test-runner/src/main/java/io/axual/ksml/testrunner/ProduceBlock.java:38`, `ksml-test-runner/src/test/java/io/axual/ksml/testrunner/TestDefinitionSchemaGeneratorTest.java:140`, `docs/ksml-test-spec.json`

**PR code**
```java
anyOfRequired = {MESSAGES, GENERATOR, ADVANCE_WALL_CLOCK_TIME}
```
```java
-    void produceBlockOneOfBetweenMessagesAndGenerator() {
+    void produceBlockAnyOfMessagesGeneratorAndTimeStep() {
```

**Suggested change:** Keep `anyOf`, and add a rule that `messages` and `generator` can not both be set:
```json
"not": { "required": ["messages", "generator"] }
```
Keep a test that checks this rule.

**Reason:** Before, `oneOf` rejected a block with both `messages` and `generator`. Now the schema accepts it, and only the parser rejects it. The existing test was rewritten, so nothing checks this rule any more. The docs still say "not both".

---

## 8. MEDIUM: Line-ending changes are mixed with code changes

**Location:** commit `010e316e` (`PythonContext.java` and others: 90 lines changed only for line endings), commit `dd34e40b` (`PythonDataObjectMapper.java`: 181 lines in the diff, about 5 real)

**PR code:** CRLF -> LF conversions together with real changes in the same commit.

**Suggested change:** Do all CRLF -> LF conversions in the separate `Fix line endings` commit (`ae0220b9`), and keep only the real changes in the other commits. Or squash-merge the PR.

**Reason:** `CLAUDE.md` says line-ending fixes go in a separate commit, never mixed with code changes. Mixed commits hide the real change in `git log -p` and `git blame`.

---

## 9. MEDIUM: Core `FatalError` now imports an Apicurio class

**Location:** `ksml/src/main/java/io/axual/ksml/execution/FatalError.java:23`, `:55`

**PR code**
```java
import io.apicurio.registry.rest.client.models.ProblemDetails;
...
case ProblemDetails pd -> pd.getDetail() + "(" + pd.getStatus() + ")";
```

**Suggested change:** Remove it from this branch (see #3). If you keep it, make it generic, and add the missing space:
```java
case ProblemDetails pd -> pd.getDetail() + " (" + pd.getStatus() + ")";
```

**Reason:** This is the only `io.apicurio` import in the main code of the core `ksml` module, so a core class now depends on one registry client. There is no test, and the message has no space before `(`.

---

## 10. MEDIUM: Example 21 is missing from `examples/ksml-runner.yaml`

**Location:** `examples/ksml-runner.yaml:139`

**PR code**
```yaml
    windowed-avro-state-store: 20-example-windowed-avro-state-store.yaml
```

**Suggested change**
```yaml
    windowed-avro-state-store: 20-example-windowed-avro-state-store.yaml
    punctuate: 21-example-punctuate.yaml
```

**Reason:** All other numbered examples are listed. Without this line, `run-local.sh` and docker compose do not run the new example.

---

## 11. MEDIUM: The openspec change is not archived

**Location:** `openspec/changes/add-punctuate-operation/**`

**PR code:** `openspec/changes/add-punctuate-operation/specs/punctuation/spec.md`, `.../specs/test-definition-format/spec.md`

**Suggested change:** Move it to `openspec/changes/archive/<date>-add-punctuate-operation/`. Merge the specs into `openspec/specs/test-definition-format/spec.md` and a new `openspec/specs/punctuation/spec.md`.

**Reason:** On `main`, earlier changes (for example #505 and #750) were only committed as archived changes, with their specs merged. No unarchived change exists on `main`.

---

## 12. LOW: Changed test: byte overflow error message

**Location:** `ksml/src/test/java/io/axual/ksml/python/PythonDataObjectMapperTest.java:279`

**PR code**
```java
-                .hasMessageContaining("exceeds BYTE range");
+                .hasMessageContaining("300 does not fit in expected type byte");
```

**Suggested change:** No code change needed, but add a line about it in `docs/release-notes.md`.

**Reason:** The test was changed because the error now comes earlier, from `PythonNativeMapper`. The check is still as strong. But the error text that users see has changed, and people who search logs for the old text will not find it.

---

## 13. LOW: Exception cause is lost in `ProcessorContextProxy`

**Location:** `ksml/src/main/java/io/axual/ksml/proxy/context/ProcessorContextProxy.java:130-132`, `:145-147`

**PR code**
```java
} catch (
        RuntimeException e) {
    throw PythonErrors.typeError("forward " + role + ...);
}
```

**Suggested change**
```java
} catch (RuntimeException e) {
    throw PythonErrors.typeError("forward " + role + ...: " + e.getMessage());
}
```

**Reason:** The original conversion error is dropped, so the user does not see why the value did not fit. Line 255 already passes the cause (`valueError(..., e)`).

---

## 14. LOW: Extra `stores` field only on the punctuate operation

**Location:** `ksml/src/main/java/io/axual/ksml/operation/parser/PunctuateOperationParser.java:59`

**PR code**
```java
FieldParsers.optional(FieldParsers.listField(Punctuate.STORES, "store-name", "store", "The names of the state stores the punctuator uses, in addition to those of the function", ...))
```

**Suggested change:** Remove it, and use only the `stores` of the function, like `transformValue`, `peek` and the other operations.

**Reason:** No other operation has a `stores` field next to its function. Two ways to say the same thing means more syntax and more docs to keep up to date.

---

## 15. LOW: Apicurio client factory uses reflection and keeps a dead v2 path

**Location:** `ksml-data-apicurio/src/main/java/io/axual/ksml/data/notation/apicurio/ApicurioRegistryClientFactory.java:78`, `:105`

**PR code**
```java
final var method = RegistryClientFacadeFactory.class.getDeclaredMethod("buildClientOptions", SchemaResolverConfig.class, Vertx.class);
method.setAccessible(true);
...
if (isV2(config))
    return new RegistryClientFacadeImpl_v2(RegistryV2ClientFactory.create(options));
```

**Suggested change:** Remove the v2 branch. Add a test that fails when `buildClientOptions` is gone, so an Apicurio upgrade shows the problem at build time:
```java
@Test
void apicurioStillHasBuildClientOptions() {
    assertThatCode(() -> RegistryClientFacadeFactory.class.getDeclaredMethod("buildClientOptions", SchemaResolverConfig.class, Vertx.class))
            .doesNotThrowAnyException();
}
```

**Reason:** The release notes say v2 URLs are no longer supported, so the v2 branch is dead code. The reflection call depends on a private Apicurio method that can change in any release. Today the tests only find this indirectly.

---

## 16. LOW: JSON Schema and Protobuf notations now create the Apicurio client early

**Location:** `ApicurioJsonSchemaNotationProvider.java`, `ApicurioProtobufNotationProvider.java` (`createNotation`)

**PR code**
```java
final var srClient = registryClient != null ? registryClient : ApicurioRegistryClientFactory.create(MapUtil.stringKeys(context.serdeConfigs()));
```

**Suggested change:** Create the client here only when a key store is set, and leave the other cases to the serde as before. Or write the change down in the release notes.

**Reason:** Before, these serdes built their own client when they were configured. Now a client is built when the notation is created, also when the notation is never used. A wrong registry setting now fails at startup instead of on first use. This is a change in behaviour.

---

## 17. LOW: Injected parameter lists can be changed from outside

**Location:** `ksml/src/main/java/io/axual/ksml/user/UserFunction.java:55`, `ksml/src/main/java/io/axual/ksml/definition/FunctionDefinition.java:63`, `:75`

**PR code**
```java
public final List<InjectedParameter> injectedParameters = new LinkedList<>();
...
injectedParameters != null ? new ArrayList<>(injectedParameters):new ArrayList<>();
...
this.injectedParameters = definition.injectedParameters;
```

**Suggested change**
```java
this.injectedParameters = injectedParameters != null ? List.copyOf(injectedParameters) : List.of();
```
In `UserFunction`, build the list locally and store `List.copyOf(...)`.

**Reason:** The getter and the public field return lists that anyone can change, and the copy constructor shares the same list. Three different list types (`LinkedList`, `ArrayList` and the copy) are used for the same thing.

---

## 18. LOW: Naming: `PYTHON_PROXY` does not match `INJECT_STORES`

**Location:** `ksml/src/main/java/io/axual/ksml/proxy/context/ProcessorContextProxy.java:95`

**PR code**
```java
public static final InjectedParameter PYTHON_PROXY = new InjectedParameter(PROXY_NAME, PROXY_CLASS_NAME, PROXY_CLASS_CODE);
```

**Suggested change**
```java
public static final InjectedParameter INJECT_CONTEXT = new InjectedParameter(PROXY_NAME, PROXY_CLASS_NAME, PROXY_CLASS_CODE);
```

**Reason:** `UserFunction.INJECT_STORES` names what is injected. `PYTHON_PROXY` does not say that it is the `context` injection, and other proxies may get their own injections later.

---

## 19. LOW: Old Javadoc in `PythonContext.registerGlobalCode`

**Location:** `ksml/src/main/java/io/axual/ksml/python/PythonContext.java:144`

**PR code**
```java
* Register global code that is used to initialize the loggerBridge and metricsBridge variables, and the Python
* class that wraps the processor context.
```

**Suggested change**
```java
* Register global code that is used to initialize the loggerBridge and metricsBridge variables.
```

**Reason:** The Python class for the context is now added to the code of each function by `PythonFunction`, not here.

---

## 20. LOW: Docs: wrong function count, too much detail in the operation reference

**Location:** `docs/reference/function-reference.md:291`, `docs/reference/operation-reference.md:1092-1094`

**PR code**
```markdown
Below is a table with all 21 function types in KSML.
```
```markdown
KSML connects every possible target to the `punctuate` operation when it builds the topology, with a pass-through node and a merge right before the target. These nodes are added for every named operation after `punctuate`, also when the punctuator never sends to it. Records from the input still pass every operation once.
```

**Suggested change**
```markdown
Below is a table with all 22 function types in KSML.
```
```markdown
Records from the input still pass every operation once.
```

**Reason:** The table now has 22 types. Users do not need to know about the node wiring. Its only visible effect, the changed names of later operations, is already explained under "Things to keep in mind".

---

## 21. LOW: Leftover `log.warn` in the example

**Location:** `examples/21-example-punctuate.yaml:48` (uncommitted change)

**PR code**
```python
log.warn("Emitted {} totals at {}", len(keys), timestamp)
```

**Suggested change**
```python
log.info("Emitted {} totals at {}", len(keys), timestamp)
```

**Reason:** This looks like a local debug change. A normal event should not be logged as a warning, and the other examples use `log.info`.

---

## 22. LOW: Old lines in openspec `tasks.md`

**Location:** `openspec/changes/add-punctuate-operation/tasks.md:11`, `:40`

**PR code**
```markdown
built-in parameters `timestamp: long` and `context`
- Review the proposal, design and specs with the team before implementing.
```

**Suggested change**
```markdown
built-in parameter `timestamp: long`; `context` is injected
```
Delete the "Review the proposal ..." line.

**Reason:** `context` is injected, not a parameter (see `design.md`). The work is already done, so "before implementing" is not true any more.

---

## 23. LOW: Missing tests

**Location:** several

**PR code:** none (tests are missing)

**Suggested change:** Add tests for:
- a `to=` target after `repartition`, after a join that changes the key, and after `groupBy -> aggregate -> toStream` (see #1)
- Python floats `3.0` and `1e20` without an expected type (see #2)
- a helper function in `globalCode` that uses `stores` (see #5)
- the `FatalError` `ProblemDetails` branch (see #9)
- `ErrorHandling` with no record and `logPayload: false` (`ErrorHandling.java:124-125`)
- the behaviour of example 21, not only that its topology builds (`ExampleTopologyTest.java:71`)
- `advanceWallClockTime` as a plain number of milliseconds

**Reason:** These paths are new or changed, and today no test runs them.

---

## 24. NITPICK: Formatting changes not related to the feature

**Location:** `PythonContext.java:119, 283, 297`, `PythonFunction.java:148`, `ProcessorContextProxy.java:130, 145, 252`, `Invoker.java`, `UserFunction.java`, `FunctionDefinition.java`, `PythonFunction.java`, `OperationTestSupport.java`, `UserTestSupport.java`, `FatalError.java:37-38`

**PR code**
```java
} catch (
        Exception e) {
```
```java
if (closed > 0)
    log.debug(...);
```
Also: methods moved (`Invoker.withOptionalArgument`, the `PythonFunction.forXxx` factories, the `FunctionDefinition.withXxx` methods), one-line Javadocs split over three lines, `LOG` replaced by `@Slf4j`, and the text block in `PythonContext.addModulePath` re-indented.

**Suggested change**
```java
} catch (Exception e) {
```
Undo the other formatting changes and moves.

**Reason:** The `catch (` split looks like a formatter mistake, and the rest of the code base does not do it. Moving code and changing the format makes the diff larger and hides the real changes.

---

## 25. NITPICK: Comments that are too long

**Location:** `ksml/src/main/java/io/axual/ksml/definition/InjectedParameter.java:23-26`, `ksml/src/main/java/io/axual/ksml/python/PythonFunction.java:223-225`

**PR code**
```java
// Formally this is not a Definition in the sense that it does not show up in the KSML Language
// Specification. Therefore, it does not have the Definition suffix in the name. It does belong
// in this package because it is a crucial element during the generation of Python code for
// parameters that do not implement the DataObject interface.
```
```java
// The caller sets only the variables it is given, and restores them when it returns: a call can run while
// another one is busy, for example a filter after a punctuator calls context.forward, and must not leave its
// values to the outer call. Its own names start with _ksml_, so they do not hide a function or parameter.
```

**Suggested change**
```java
// A Python variable that KSML injects into a function, such as context. Not part of the KSML language spec.
```
```java
// Saves and restores the injected values, because calls can nest (a punctuator's context.forward runs a filter)
```

**Reason:** Shorter comments say the same thing and are easier to read.

---

## 26. NITPICK: Small code clean-ups

**Location:** `Invoker.java:121`, `ProcessorContextProxy.java:113`, `BranchOperation.java:74-75`

**PR code**
```java
verify(type != null && expected.isAssignableFrom(type.dataType()).isAssignable(), "... but " + (type != null ? type.dataType() : "null") + " instead");
```
```java
this.namedTargets = new TreeMap<>(namedTargets);
```
```java
OperationChain.apply(branchCursor, ..., context, (operation, result) -> {
});
```

**Suggested change**
```java
verify(expected.isAssignableFrom(type.dataType()).isAssignable(), "... but " + type.dataType() + " instead");
```
```java
// Build the sorted, read-only map once in PunctuateProcessor.init() and pass it in
```
```java
OperationChain.apply(branchCursor, ..., context);   // new overload without a callback
```

**Reason:** The line before already checks that `type` is not null. A new map is copied on every punctuation, but it is only needed for sorted error messages. An empty lambda is hard to read.

---

## 27. NITPICK: Small doc and test-resource fixes

**Location:** `ProduceBlock.java:78`, `ksml-test-runner/README.md:121`, `docs/getting-started/testing-your-pipeline.md:109`, `advance-wall-clock-negative.yaml:1`, `TestDefinitionSchemaGenerator.java:34`, `.gitattributes`

**PR code**
```java
// Validate that the produce block has either messages or a generator, but not both.
```
```markdown
Duration, such as `1m` or `30s`.
```
```yaml
# ... with an negative advanceWallClockTime
```
`import java.time.Duration;` comes after `java.util.ArrayList`. `.gitattributes` has no rule for `examples/run.cmd`.

**Suggested change**
```java
// Validate that the produce block has messages, a generator or advanceWallClockTime, and not both messages and a generator.
```
```markdown
Duration, such as `1m`, `30s` or `500` (milliseconds).
```
```yaml
# ... with a negative advanceWallClockTime
```
Sort the imports. Add `*.cmd text eol=crlf` to `.gitattributes`.

**Reason:** The comment and the docs no longer match the code. A Windows batch file may not work well with LF line endings.
