# Sample Tutorial: Building Your First KSML Data Pipeline

This document provides an outline for a sample tutorial that demonstrates the proposed documentation structure. This tutorial would be part of the "Beginner Tutorials" section in the new documentation structure.

## Tutorial Overview

This tutorial guides new users through building their first KSML data pipeline, from setup to execution. By the end, users will understand the basic components of KSML and how to create a simple but functional data processing application.

## Prerequisites

- Basic understanding of Kafka concepts (topics, messages)
- Docker installed for running the example environment
- No prior Kafka Streams or KSML experience required

## Tutorial Structure

### 1. Introduction (What You'll Build)

- Brief explanation of what the tutorial will cover
- Visual diagram of the pipeline we'll build:
  - Source topic → Filter → Transform → Destination topic
- Expected outcomes and skills learned

### 2. Setting Up Your Environment

- Using Docker Compose to start a local Kafka cluster
- Verifying the environment is running correctly
- Understanding the example data we'll be working with

### 3. Understanding KSML Basics

- What is a KSML definition file?
- Key components: streams, functions, pipelines
- How KSML translates to Kafka Streams topologies

### 4. Creating Your First KSML Pipeline

#### Step 1: Define Your Streams
```yaml
streams:
  input_stream:
    topic: tutorial_input
    keyType: string
    valueType: json
  output_stream:
    topic: tutorial_output
    keyType: string
    valueType: json
```

- Explanation of stream definitions
- Understanding key and value types
- How KSML handles serialization/deserialization

#### Step 2: Create a Simple Function
```yaml
functions:
  log_message:
    type: forEach
    parameters:
      - name: message_type
        type: string
    code: log.info("{} message - key={}, value={}", message_type, key, value)
```

- How functions work in KSML
- Python code integration
- Parameter passing and types

#### Step 3: Build Your Pipeline
```yaml
pipelines:
  tutorial_pipeline:
    from: input_stream
    via:
      - type: filter
        if:
          expression: value.get('temperature') > 70
      - type: mapValues
        mapper:
          expression: {"sensor": key, "temp_fahrenheit": value.get('temperature'), "temp_celsius": (value.get('temperature') - 32) * 5/9}
      - type: peek
        forEach:
          code: log_message(key, value, message_type="Processed")
    to: output_stream
```

- Breaking down each operation
- Understanding the data flow
- How operations are chained together

### 5. Running Your Pipeline

- Starting the KSML runner
- Producing test messages to the input topic
- Observing the filtered and transformed output

### 6. Exploring and Modifying

- Suggestions for modifications to try
- Adding more operations to the pipeline
- Troubleshooting common issues

### 7. Next Steps

- Links to more advanced tutorials
- Related concepts to explore
- Reference documentation for operations used

## Teaching Approach

This tutorial uses a hands-on, step-by-step approach with explanations at each stage:

1. **What**: Clear description of what we're doing
2. **Why**: Explanation of why this approach is useful
3. **How**: Detailed instructions with code examples
4. **Result**: What to expect when the code runs
5. **Understanding**: Deeper explanation of what's happening behind the scenes

## Sample Code Snippets with Explanations

### Example: Explaining the Filter Operation

```yaml
- type: filter
  if:
    expression: value.get('temperature') > 70
```

**What's happening here:**
- The `filter` operation examines each message and decides whether to keep it or discard it
- The `if` parameter takes a Python expression that evaluates to a boolean (True/False)
- In this case, we're only keeping messages where the temperature value is greater than 70°F
- Messages that don't meet this condition are dropped from the pipeline
- This is a stateless operation - it doesn't remember anything about previous messages

**When to use filters:**
- To reduce the volume of data flowing through your pipeline
- To focus on specific conditions or events of interest
- As an early step to eliminate irrelevant data before more expensive operations

**Common patterns:**
- Filtering by field values
- Filtering based on message keys
- Combining multiple conditions with logical operators (and, or, not)

## Conclusion

This sample tutorial outline demonstrates how the new documentation structure would provide a more educational and intuitive approach for beginners. It combines practical, hands-on examples with conceptual explanations and context, helping users not just learn the syntax but understand the underlying concepts and best practices.