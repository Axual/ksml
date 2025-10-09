# Quick Start with KSML

Get KSML running in 5 minutes! This guide shows you how to create your first **real-time analytics application** using only YAML - no Java required. You'll build an IoT analytics system that processes sensor data in real-time.

## What We'll Build

A **real-time device health monitoring system** that:

- **Filters** offline devices from processing
- **Analyzes** device health from battery, signal, and error metrics
- **Categorizes** devices as "healthy" or "needs attention"  
- **Alerts** on specific issues like low battery or poor signal

All with just YAML configuration and clear Python logic.

## Prerequisites

You'll need:

- **Docker Compose** installed ([installation guide](https://docs.docker.com/compose/install/))
- 5 minutes of your time

## Choose Your Setup Method

**Option A: Quick Start (Recommended)**
Download the pre-configured setup and run immediately:

1. Download and extract: [local-docker-compose-setup-quick-start.zip](../local-docker-compose-setup/local-docker-compose-setup-quick-start.zip)
2. Navigate to the extracted folder
3. Run `docker compose up -d`
4. Skip to [Step 6: See It In Action!](#step-6-see-it-in-action)

**Option B: Step-by-Step Setup**
Follow the detailed instructions below to create everything from scratch.

---

## Step 1: Set Up Your Environment

> **Note**: Skip this step if you chose Option A above.

- Create a new directory for your KSML project:

```bash
mkdir my-ksml-project
cd my-ksml-project
mkdir examples
```

- Create a `docker-compose.yml` file:

??? info "Docker Compose Configuration (click to expand)"

    ```yaml
    {%
      include "../local-docker-compose-setup/docker-compose.yml"
    %}
    ```

- Create `kowl-ui-config.yaml` for Kafka UI:

??? info "Kafka UI Configuration (click to expand)"

    ```yaml
    {%
      include "../local-docker-compose-setup/kowl-ui-config.yaml"
    %}
    ```

## Step 2: Start Docker Services

> **Note**: Skip this step if you chose Option A above.

```bash
docker compose up -d
```

This starts:

- **Kafka** broker (port 9092)
- **KSML** runner for your stream processing  
- **Kowl (Kafka UI)** for monitoring (port 8080)
- Automatic topic creation

## Step 3: Verify Everything Started

```bash
docker compose ps
```

You should see:

- **Kafka broker** and **Kafka UI** are running  
- **Topic setup container** has exited successfully (this creates the required topics)
- **KSML runner** will have exited (missing config - we'll fix this next)

Check the Kafka UI at [http://localhost:8080](http://localhost:8080) to see your topics.

## Step 4: Create Your First KSML Application

> **Note**: If you chose Option A (zip download), these files already exist - you can skip to [Step 5](#step-5-start-your-application).

Now let's create a smart device health monitoring application!

In the `examples/` directory, create `iot-analytics.yaml`:

??? info "KSML Device Health Monitoring processing definition (click to expand)"

       > This is a definition for a demo KSML application showing device health monitoring. It is not required at this point to understand this YAML & Python syntax, we will explain what it does and how it works later.

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/quick-start/processor.yaml" %}
    ```

Now create the KSML runner configuration file `ksml-runner.yaml`:

??? info "KSML Runner Configuration (click to expand)"

    ```yaml
    kafka:
      bootstrap.servers: broker:9093
      application.id: io.ksml.example.producer
      security.protocol: PLAINTEXT
      acks: all

    ksml:
      definitions:
        iot: iot-analytics.yaml
    ```

## Step 5: Start Your Application

Restart the KSML runner:

```bash
docker compose restart ksml
```

Check the logs to see when KSML is ready to receive messages:

```bash
docker compose logs ksml -f
```

KSML is ready to receive messages when you see a message:

```
Pipeline processing state change. Moving from old state 'REBALANCING' to new state 'RUNNING'
```

## Step 6: See It In Action!

Send some test data to see your pipeline work:

```bash
docker compose exec broker kafka-console-producer.sh --bootstrap-server broker:9093 --topic iot_sensor_data --property "parse.key=true" --property "key.separator=:"
```

Paste these device health metrics (press Enter after each):

```
sensor-001:{"battery_percent":15,"signal_strength":80,"error_count":1,"status":"online"}
sensor-002:{"battery_percent":75,"signal_strength":30,"error_count":2,"status":"online"}
sensor-003:{"battery_percent":90,"signal_strength":85,"error_count":0,"status":"online"}
sensor-004:{"battery_percent":60,"signal_strength":70,"error_count":8,"status":"online"}
sensor-005:{"battery_percent":50,"signal_strength":60,"error_count":1,"status":"offline"}
```

Press `Ctrl+C` to exit.

## What Just Happened?

Your KSML pipeline performed **real-time device health monitoring** in just a few lines of YAML:

1. **Filtered** offline devices (only processes devices with status "online")
2. **Analyzed** device health from battery, signal strength, and error metrics
3. **Categorized** devices as "healthy" or "needs attention" 
4. **Identified** specific issues (low battery, poor signal, high errors)

Check the KSML logs to see the real-time analysis:

```bash
docker compose logs ksml -f
```

You'll see device health reports like:
```
sensor-003: Healthy (Battery: 90%)
sensor-001: Needs attention - low_battery
sensor-002: Needs attention - poor_signal
sensor-004: Needs attention - high_errors
```

**This is real-time device monitoring!** Each device status is instantly analyzed and categorized as data flows through the pipeline.

### Example messages

**INPUT** (to `iot_sensor_data` topic):

- key: `sensor-001`
- value:
```json
{
  "battery_percent": 15,
  "signal_strength": 80,
  "error_count": 1,
  "status": "online"
}
```

**OUTPUT** (to `device_health_alerts` topic):

- key: `sensor-001`
- value:
```json
{
  "device_id": "sensor-001",
  "battery_percent": 15,
  "signal_strength": 80,
  "error_count": 1,
  "health_status": "needs_attention",
  "issue_reason": "low_battery"
}
```

Open [http://localhost:8080](http://localhost:8080) to explore your topics and see the transformed data!

## Congratulations!

You just built a **real-time device health monitoring system** with:

- **Smart device analysis** with battery, signal, and error monitoring
- **Real-time filtering and categorization** of device health status
- **No compilation** or complex infrastructure needed

## What Makes KSML Powerful?

**Traditional stream processing requires:**

- Java/Scala expertise for Kafka Streams
- Complex filtering and transformation code
- Build and deployment pipelines

**With KSML you get:**

- Simple YAML configuration for data processing
- Built-in filtering and transformation operations
- Optional Python for custom business logic
- Instant deployment with containers

## Next Steps

Ready to learn more?

1. **[Schema Validation](schema-validation.md)** - Set up IDE validation for error-free development
2. **[Understanding KSML](introduction.md)** - Learn the concepts
3. **[KSML Basics Tutorial](basics-tutorial.md)** - Build more advanced pipelines