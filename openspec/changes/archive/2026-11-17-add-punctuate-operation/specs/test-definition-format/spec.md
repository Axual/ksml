## ADDED Requirements

### Requirement: Advancing wall-clock time after a produce block
Each entry in a test's `produce` list SHALL accept an optional `advanceWallClockTime` field with a KSML duration. After producing the block's messages, the runner SHALL advance the wall-clock time of the test's `TopologyTestDriver` by that duration, so that wall-clock punctuations that become due run before the next produce block and before the assertions.

#### Scenario: Wall-clock punctuation in a test
- **WHEN** the pipeline has a `wallClockTime` punctuator with interval `1m`, and a produce block sends 3 messages and sets `advanceWallClockTime: 1m`
- **THEN** the punctuator SHALL run once after the 3 messages are processed, and records it emits SHALL be visible to the test's assertions

#### Scenario: Interleaved produce and time steps
- **WHEN** a test has two produce blocks to the same stream, the first with `advanceWallClockTime: 1m`
- **THEN** a punctuation SHALL run after the first block's messages and before the second block's messages

#### Scenario: Produce block without messages
- **WHEN** a produce block sets `to` and `advanceWallClockTime` but has no messages
- **THEN** the runner SHALL accept it and only advance the wall-clock time

#### Scenario: Invalid duration
- **WHEN** `advanceWallClockTime` is not a valid KSML duration, or is negative
- **THEN** the parser SHALL reject the file with an error identifying the produce block
