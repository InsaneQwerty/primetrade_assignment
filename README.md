# MLOps Pipeline

A batch pipeline that reads crypto OHLCV data, computes a rolling mean on the close price, generates trading signals, and outputs metrics as JSON. Runs locally or inside Docker.

## Project Structure

```
run.py            - main script
config.yaml       - config (seed, window, version)
data.csv          - 10k rows of crypto OHLCV data
requirements.txt  - python dependencies
Dockerfile        - container setup
metrics.json      - example output
run.log           - example log
README.md         - this file
```

## Setup

```bash
pip install -r requirements.txt
```

Dependencies: `pandas`, `numpy`, `pyyaml`

## Running Locally

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

## Docker

```bash
# build
docker build -t mlops-task .

# run
docker run --rm mlops-task
```

## Output

On success, `metrics.json` looks like:

```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4987,
  "latency_ms": 34,
  "seed": 42,
  "status": "success"
}
```

On error:

```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Input file not found: missing.csv"
}
```

## How it Works

1. Loads config from `config.yaml` (seed, window size, version)
2. Sets the random seed for reproducibility
3. Reads the CSV and checks that the `close` column exists
4. Computes rolling mean on `close` with the configured window
5. Generates signals: `1` if close > rolling mean, `0` otherwise
6. Writes metrics to JSON and logs everything

## Error Handling

Handles missing files, bad CSV format, empty data, missing columns, and bad config. Errors get logged and written to the output JSON with `status: "error"`.
