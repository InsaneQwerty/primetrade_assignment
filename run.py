import os
import sys
import time
import json
import logging
import argparse

import numpy as np
import pandas as pd
import yaml



def setup_logging(log_file):
    logger = logging.getLogger('mlops_pipeline')
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # file handler
    fh = logging.FileHandler(log_file, mode='w')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    # stdout for docker
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger


def parse_arguments():
    parser = argparse.ArgumentParser(description='MLOps Pipeline')
    parser.add_argument('--input', type=str, required=True, help='Input CSV file')
    parser.add_argument('--config', type=str, required=True, help='Config YAML file')
    parser.add_argument('--output', type=str, required=True, help='Output metrics JSON')
    parser.add_argument('--log-file', type=str, required=True, help='Log file path')
    return parser.parse_args()


def load_config(config_path, logger):
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f'Configuration file not found: {config_path}')

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    if not config or not isinstance(config, dict):
        raise ValueError('Invalid config file: empty or malformed')

    for key in ['seed', 'window', 'version']:
        if key not in config:
            raise ValueError(f'Missing required config key: {key}')

    if not isinstance(config['seed'], int):
        raise ValueError('seed must be an integer')
    if not isinstance(config['window'], int) or config['window'] < 1:
        raise ValueError('window must be a positive integer')
    if not isinstance(config['version'], str):
        raise ValueError('version must be a string')

    logger.info(
        'Config loaded: seed=%d, window=%d, version=%s',
        config['seed'], config['window'], config['version']
    )
    return config


def load_data(input_path, logger):
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f'Input file not found: {input_path}')

    try:
        df = pd.read_csv(input_path)
    except pd.errors.EmptyDataError:
        raise ValueError('Input CSV file is empty')
    except pd.errors.ParserError:
        raise ValueError('Invalid CSV format')

    if df.empty:
        raise ValueError('CSV file contains no data rows')

    if 'close' not in df.columns:
        raise ValueError(f'Missing required column: close. Found: {list(df.columns)}')

    logger.info('Data loaded: %d rows', len(df))
    return df


def compute_rolling_mean(df, window, logger):
    rolling_mean = df['close'].rolling(window=window).mean()
    logger.info('Rolling mean calculated with window=%d', window)
    return rolling_mean


def generate_signals(df, rolling_mean, logger):
    signals = (df['close'] > rolling_mean).astype(int)
    signals = signals.fillna(0).astype(int)  # NaN rows get 0
    logger.info('Signals generated')
    return signals


def calculate_metrics(signals, total_rows, config, latency_ms, logger):
    signal_rate = round(float(signals.mean()), 4)

    metrics = {
        'version': config['version'],
        'rows_processed': total_rows,
        'metric': 'signal_rate',
        'value': signal_rate,
        'latency_ms': latency_ms,
        'seed': config['seed'],
        'status': 'success'
    }

    logger.info('Metrics: signal_rate=%.4f, rows_processed=%d', signal_rate, total_rows)
    return metrics


def write_output(data, output_path):
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)


def main():
    start_time = time.time()
    args = parse_arguments()
    logger = setup_logging(args.log_file)
    logger.info('Job started')

    version = 'v1'

    try:
        config = load_config(args.config, logger)
        version = config['version']

        np.random.seed(config['seed'])
        logger.info('Random seed set to %d', config['seed'])

        df = load_data(args.input, logger)

        rolling_mean = compute_rolling_mean(df, config['window'], logger)
        signals = generate_signals(df, rolling_mean, logger)

        elapsed_ms = int((time.time() - start_time) * 1000)
        metrics = calculate_metrics(signals, len(df), config, elapsed_ms, logger)

        write_output(metrics, args.output)
        logger.info('Metrics written to %s', args.output)

        print(json.dumps(metrics, indent=2))
        logger.info('Job completed successfully in %dms', elapsed_ms)
        return 0

    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        logger.error('Pipeline failed: %s', str(e))

        error_out = {
            'version': version,
            'status': 'error',
            'error_message': str(e)
        }
        write_output(error_out, args.output)
        logger.info('Job failed after %dms', elapsed_ms)
        return 1


if __name__ == '__main__':
    sys.exit(main())
