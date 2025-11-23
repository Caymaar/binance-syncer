# Binance Syncer

**Binance Syncer** is a high-performance synchronization tool for downloading and managing Binance historical data (klines, trades, etc.) asynchronously. It supports both local and S3 storage with intelligent data optimization (monthly vs daily preference).

![Version](https://img.shields.io/github/v/tag/Caymaar/binance-syncer?label=version)

## Features

- **Asynchronous synchronization**: Concurrent downloading optimized for large volumes
- **Multi-market support**: SPOT, FUTURES (UM/CM), and OPTIONS markets
- **Various data types**: KLINES, TRADES, AGG_TRADES, BOOK_DEPTH, etc.
- **Flexible storage**: Local filesystem or cloud (AWS S3)
- **Intelligent optimization**: Automatic preference for monthly data
- **CLI interface**: Complete command-line interface with dry-run mode
- **Error handling**: Automatic retry and robust SSL management
- **Progress tracking**: Progress bar with rich console
- **Advanced logging**: Structured logs with automatic rotation

## Installation

### From pip

```bash
pip install binance-syncer
```

### From uv

```bash
uv tool install binance-syncer
```

### From source
```bash
git clone https://github.com/caymaar/binance-syncer.git
cd binance-syncer
pip install .
```

## Configuration

### Automatic configuration
On first launch, the syncer automatically creates:

```
~/utilities/config/binance_syncer.ini
~/utilities/logs/binance_syncer/
```

### Manual configuration
Edit `~/.config/binance_syncer/config.ini` and replace with your configuration:

```ini
[LOCAL]
PATH = ~/binance-vision

[S3]
BUCKET = my-binance-data-bucket
PREFIX = binance-vision

[SETTINGS]
MAX_CONCURRENT_DOWNLOADS = 100
SYMBOL_CONCURRENCY = 10
```

### AWS environment variables (for S3)

To set your S3:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

## Usage

### Command Line Interface

#### Basic commands

```bash
# Sync all SPOT KLINES 1d data
binance-syncer --market-type spot --data-type klines --interval 1d

# Specific symbols with progress bar
binance-syncer --market-type spot --data-type klines --interval 1d \
  --symbols BTCUSDT ETHUSDT ADAUSDT --progress

# S3 storage
binance-syncer --market-type spot --data-type klines --interval 1d \
  --storage s3 --progress

# Dry-run mode (simulation)
binance-syncer --market-type spot --data-type klines --interval 1d \
  --dry-run

# Trade data (no interval required)
binance-syncer --market-type spot --data-type trades --storage local
```

#### Advanced options

```bash
# Verbose logging
binance-syncer --market-type spot --data-type klines --interval 1d \
  --verbose --progress

# Futures with 4h interval
binance-syncer --market-type futures/um --data-type klines --interval 4h \
  --storage s3 --progress
```

### Programmatic usage

```python
import asyncio
from binance_syncer import BinanceDataSync, MarketType, DataType, KlineInterval

async def main():
    # Configure syncer
    syncer = BinanceDataSync(
        storage_mode="local",  # or "s3"
        market_type=MarketType.SPOT,
        data_type=DataType.KLINES,
        interval=KlineInterval.D1,
        progress=True
    )
    
    # Sync specific symbols
    await syncer.sync(["BTCUSDT", "ETHUSDT"])
    
    # Or sync all available symbols
    # await syncer.sync()

# Execute
asyncio.run(main())
```

## Data Structure

### Local storage
```
~/binance-vision/
├── data/
│   ├── spot/
│   │   ├── klines/
│   │   │   ├── BTCUSDT/
│   │   │   │   ├── 1d/
│   │   │   │   │   ├── 2024-01.parquet    # Monthly data
│   │   │   │   │   ├── 2024-02-15.parquet # Daily data
│   │   │   │   │   └── ...
│   │   │   │   └── 1h/
│   │   │   └── ETHUSDT/
│   │   └── trades/
│   ├── futures/
│   └── option/
```

### S3 storage
```
s3://your-bucket/
├── binance-vision/
│   ├── data/
│   │   ├── spot/
│   │   │   ├── klines/
│   │   │   │   ├── BTCUSDT/
│   │   │   │   │   ├── 1d/
│   │   │   │   │   │   ├── 2024-01.parquet
│   │   │   │   │   │   └── ...
```

## Supported Data Types

| Data Type | Market Types | Interval Required |
|-----------|--------------|-------------------|
| KLINES | SPOT, FUTURES_UM, FUTURES_CM | Yes |
| TRADES | SPOT, FUTURES_UM, FUTURES_CM | No |
| AGG_TRADES | SPOT, FUTURES_UM, FUTURES_CM | No |
| BOOK_DEPTH | SPOT, FUTURES_UM, FUTURES_CM | No |
| BOOK_TICKER | SPOT, FUTURES_UM, FUTURES_CM | No |
| EOH_SUMMARY | OPTION | No |

### Supported intervals
- **Seconds**: 1s
- **Minutes**: 1m, 3m, 5m, 15m, 30m
- **Hours**: 1h, 2h, 4h, 6h, 8h, 12h
- **Days**: 1d
- **Weeks**: 1w

## Logging and Monitoring

### Log files
Logs are automatically created in `~/utilities/logs/binance_syncer/`

### Automatic rotation
- Retention: 7 days
- Levels: DEBUG, INFO, WARNING, ERROR

## Performance and Optimizations

### Concurrency
- **Downloads**: 100 simultaneous tasks per symbol
- **Symbols**: 10 symbols processed in parallel
- **Batches**: Processing in batches of 20 to avoid overload

### Network optimizations
- **SSL**: Robust configuration with fallback
- **Timeouts**: Configurable (connection: 30s, read: 120s, total: 300s)
- **Retry**: Automatic retry logic with exponential backoff
- **Chunked reading**: Reading in 8KB chunks for large files

### Data optimization
- **Monthly preference**: Automatic removal of daily data when monthly data is available
- **Deduplication**: Avoids downloading existing files
- **Compression**: Parquet format with Snappy compression

## Troubleshooting

### Common SSL issues
```bash
# On macOS, install Python certificates
/Applications/Python\ 3.11/Install\ Certificates.command

# Check SSL configuration
python -c "import ssl, certifi; print(certifi.where())"
```

### S3 issues
```bash
# Check AWS credentials
aws configure list
aws s3 ls s3://your-bucket/
```
