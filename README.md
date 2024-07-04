# metrics-gen

A tool to generate metrics data points and upload them to a [Prometheus Remote Write](https://prometheus.io/docs/specs/remote_write_spec/) endpoint.

### Build

The [protobuf compiler](https://github.com/protocolbuffers/protobuf?tab=readme-ov-file#protobuf-compiler-installation) is a required external dependency.

####

```bash
cargo build --release
```

### Usage

1. Gather metrics outputs from all exporters and save them in text files in a directory, let's call it `./metrics`.

2. Create a config.yml file:
```yaml
metricsDir: "./metrics"
destination:
  type: remoteWrite
  url: "https://example.com/api/v1/receive"
  headers:
      - ["Authorization", "Basic EXAMPLE"]
uploadCooldown: 500msec
generationPeriod: 15s
uploadInterval: 300s
labels:
  - name: "resellerId"
    count: 1
  - name: "customerId"
    count: 2
  - name: "deviceId"
    count: 3
startDate: "2024-01-01T00:00:00Z"
endDate: "2024-06-01T00:00:00Z"
randomization:
  factor: 0.5

```

With this config file, it iterates between the start date and the end date, generates metrics data points timestamped every 15 seconds and it uploads groups of 20. There is a 500ms cooldown period between each upload. It attaches 3 labels to each metric data point, with the labels being randomly generated. The last label's count determines how many times this program will generate metrics from start to the end. In this case, it will do it three times for 3 devices.

3. Run metrics-gen:

```bash
metrics-gen -c ./config.yml --concurrency 1
```

You can try to increase the concurrency depending on how capable your server is.

