## ADDED Requirements

### Requirement: Runner exits with a non-zero code on a runtime failure
The KSML runner process SHALL terminate with a non-zero exit code when the Kafka Streams runner or the producer runner fails while running, so that a container orchestrator can detect the failure and restart the process. The process SHALL NOT remain alive after the failure has been reported.

#### Scenario: Kafka Streams goes to ERROR
- **WHEN** a pipeline function throws an unhandled exception and Kafka Streams moves to state `ERROR`
- **THEN** the runner SHALL log the failure and the process SHALL exit with a non-zero exit code

#### Scenario: Producer runner fails
- **WHEN** the producer runner completes exceptionally while the runner is running
- **THEN** the runner SHALL stop the other runner and the process SHALL exit with a non-zero exit code

#### Scenario: Normal completion
- **WHEN** all runners complete without failure (for example a producer-only definition whose producers finish)
- **THEN** the process SHALL exit with exit code 0

#### Scenario: Schema output only
- **WHEN** the runner is started only to print the KSML or runner configuration schema
- **THEN** it SHALL write the schema and return from `main` without calling `System.exit`

### Requirement: Graceful shutdown on termination signal
When the JVM receives a termination signal (SIGTERM), the runner SHALL stop the producer runner and the Kafka Streams runner and SHALL wait until they have finished before the JVM exits, so that Kafka Streams commits the offsets of processed records and closes its client.

#### Scenario: SIGTERM after processing
- **WHEN** the runner has processed records and has not yet reached a regular commit, and the process receives SIGTERM
- **THEN** the runner SHALL log `Stopping Kafka Streams`, Kafka Streams SHALL commit the offsets of the processed records and close, and only then SHALL the JVM exit

#### Scenario: No reprocessing after restart
- **WHEN** the process is stopped with SIGTERM after processing records and is started again with the same application id
- **THEN** the already processed records SHALL NOT be processed again

#### Scenario: Producer is stopped on SIGTERM
- **WHEN** a producer runner is active and the process receives SIGTERM
- **THEN** the producer runner SHALL stop scheduling new messages and close its Kafka producer before the JVM exits

### Requirement: Shutdown is bounded in time
The wait for the runners during shutdown SHALL be bounded by a fixed shutdown timeout of 30 seconds (plus a small margin for the hook), so that a stuck runner cannot prevent the process from exiting.

#### Scenario: Runner does not finish in time
- **WHEN** a runner has not finished within the shutdown timeout after SIGTERM
- **THEN** the runner SHALL log that the shutdown timed out and the shutdown hook SHALL return, allowing the JVM to exit

#### Scenario: Shutdown hook removal during JVM shutdown
- **WHEN** the runners finish because of a SIGTERM and the runner tries to remove its shutdown hook while the JVM shutdown is in progress
- **THEN** the runner SHALL tolerate this without raising an error

### Requirement: Kafka Streams client is closed in every terminal state
The Kafka Streams runner SHALL close its Kafka Streams client when its run loop ends, whether it ended because of a stop request or because Kafka Streams reached a failed state, using a bounded close timeout. A failed state SHALL still be reported to the caller as a runner failure after the client is closed.

#### Scenario: Close after stop request
- **WHEN** the Kafka Streams runner is running and `stop()` is called
- **THEN** the runner SHALL close the Kafka Streams client and return normally

#### Scenario: Close after ERROR
- **WHEN** Kafka Streams is in state `ERROR` and the runner's run loop ends
- **THEN** the runner SHALL close the Kafka Streams client and then throw a runner exception reporting the failed state

### Requirement: Runner threads do not outlive a failure
The runner SHALL shut down the executor that runs the producer and streams runners when the runners complete, including when one of them completed exceptionally, so that no runner worker thread keeps the JVM alive.

#### Scenario: Executor shut down after runner failure
- **WHEN** the streams runner completes exceptionally
- **THEN** the runner SHALL shut down its executor service before propagating the failure
