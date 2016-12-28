# Beamium - Promotheus to Warp10 metrics forwarder
[![version](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/runabove/beamium)
[![Build Status](https://travis-ci.org/runabove/beamium.svg?branch=master)](https://travis-ci.org/runabove/beamium)

Beamium collect metrics from Promotheus endpoints and forward them to Warp10 data platform.

Beamium is written in Rust to ensure efficiency, a very low footprint and deterministic performances.

Beamium key points:
 - **Simple**: Beamium fetch Promotheus metrics and so benefits from an awesome community.
 - **Reliable**: Beamium handle network failure. Never lose data. We guarantee void proof graph ;)
 - **Versatile**: Beamium can also fetch metrics from a directory.
 - **Powerfull**: Beamium is able to filter metrics and to send them to multiple Warp10 platforms.

## Status
Beamium is currently under development.

## Building
Beamium is pretty easy to build.
 - Clone the repository
 - Setup a minimal working config (see bellow)
 - Build and run `cargo run`

## Configuration
Beamium come with a [sample config file](config.sample.yaml). Simply copy the sample to *config.yaml*, replace `WARP10_ENDPOINT` and `WARP10_TOKEN`, launch Beamiun and you are ready to go!

### Definitions
Config is composed of four parts:

#### Sources
Beamium can have none to many Promotheus endpoints. A *source* is defined as follow:
``` yaml
sources: # Sources definitions (Optional)
  source1:                             # Source name         (Required)
    url: http://127.0.0.1:9100/metrics # Promotheus endpoint (Required)
    period: 10000                      # Polling interval    (Required)
    format: prometheus                 # Polling format      (Optional, default: prometheus, value: [prometheus, sensision])
```

#### Sinks
Beamium can have none to many Warp10 endpoints. A *sink* is defined as follow:
``` yaml
sinks: # Sinks definitions (Optional)
  source1:                             # Sink name                    (Required)
    url: https://warp.io/api/v0/update # Warp10 endpoint              (Required)
    token: mywarp10token               # Warp10 write token           (Required)
    token-header: X-Custom-Token       # Warp10 token header name     (Optional, default: X-Warp10-Token)
    selector: metrics.*                # Regex used to filter metrics (Optional, default: None)
```

#### Labels
Beamium can add static labels to collected metrics. A *label* is defined as follow:
``` yaml
labels: # Labels definitions (Optional)
  label_name: label_value # Label definition             (Required)
```

#### Labels
Beamium can be customized through parameters. See available parameters bellow:
``` yaml
parameters: # Parameters definitions (Optional)
  source-dir: sources # Beamer data source directory                    (Optional, default: sources)
  sink-dir: sinks       # Beamer data sink directory                    (Optional, default: sinks)
  scan-period: 1000     # Delay(ms) between source/sink scan            (Optional, default: 1000)
  batch-count: 250      # Maximum number of files to process in a batch (Optional, default: 250)
  batch-size: 200000    # Maximum batch size                            (Optional, default: 250)
  log-file: beamium.log # Log file                                      (Optional, default: beamium.log)
  log-level: 4          # Log level                                     (Optional, default: info)
```

## Contributing
Instructions on how to contribute to Beamium are available on the [Contributing][Contributing] page.

## Get in touch

- Twitter: [@notd33d33](https://twitter.com/notd33d33)

[Contributing]: CONTRIBUTING.md
