# Parser Cleanup and YAML Schema Generation

## Summary

Refactor the KSML test runner's TestDefinitionParser to be more maintainable while adding YAML schema generation for improved editor support. This change uses a hybrid approach that preserves excellent error messages while enabling auto-completion and validation in editors.

## Context

The current TestDefinitionParser in ksml-test-runner works well but has maintenance challenges:

1. **Repetitive boilerplate** - Manual JsonNode navigation and validation repeated throughout
2. **Default value inconsistency** - README documents keyType/valueType as optional with "string" defaults, but code treats them as required
3. **No editor support** - Test authors have no auto-completion or validation when writing YAML files
4. **Hard to extend** - Adding new fields requires copying validation patterns

The parser currently has ~192 lines of parsing logic with good error messages but repetitive patterns.

## Goals

1. **Cleaner, more maintainable parser** - Extract repetitive patterns into reusable utilities
2. **Fix default value handling** - Implement documented defaults for keyType/valueType
3. **Enable editor support** - Generate YAML schema for auto-completion and validation
4. **Preserve excellent error messages** - Keep current user-friendly error reporting
5. **Easy extensibility** - Make adding new fields trivial

## Non-Goals

- Changing the YAML test definition format (remains backward compatible)
- Switching to a different parsing technology (keeping JsonNode approach)
- Modifying validation logic or requirements

## Success Criteria

- Parser code reduced by ~60% while maintaining functionality
- Default values work as documented
- VS Code and IntelliJ provide auto-completion for test YAML files
- Error messages remain clear and contextual
- Adding new fields requires only 1-2 lines of code

## Risks

- **Development time** - Estimated 4-5 hours total work across phases
- **Testing complexity** - Need to verify schema generation produces valid JSON Schema
- **Editor setup** - Users need to configure editors to use generated schema

## Alternative Considered

**Full Jackson annotation approach** - Replace JsonNode parsing with Jackson @JsonProperty annotations. Rejected because:
- Loses rich, contextual error messages
- Harder to debug (annotation magic vs explicit flow)
- Similar complexity, just moved to different places
- No significant maintainability improvement over clean JsonNode approach