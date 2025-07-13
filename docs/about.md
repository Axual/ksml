# About KSML

## Kafka Streams

The Kafka Streams framework is at the heart of many organisations' event sourcing, analytical processing, real-time,
batch or ML workloads. It has a beautiful Java DSL that allows developers to focus on what their applications need to
do, not how to do it.

But the Java requirement for Kafka Streams also holds people back. If you don’t know Java, does that mean you cannot
use Kafka Streams?

## KSML
Enter KSML.

KSML is a wrapper language and interpreter around Kafka Streams that lets you express any topology in a YAML syntax.
Simply define your topology as a processing pipeline with a series of steps that your data passes through. Your custom
functions can be expressed inline in Python. KSML will read your definition and construct the topology dynamically via
the Kafka Streams DSL and run it in GraalVM.

## Documentation
This documentation takes you for a deep-dive into KSML. It covers the basic concepts, how to run, and of course contains
lots of useful and near-real-world examples. After going through these docs, you will understand how easily and quickly
you can develop Kafka Streams applications without writing a single line of Java.
