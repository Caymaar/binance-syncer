# Binance Syncer

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
![Version](https://img.shields.io/badge/version-3.0.0-green.svg)
[![Downloads](https://pepy.tech/badge/binance-syncer)](https://pepy.tech/project/binance-syncer)

**Binance Syncer** is a high-performance synchronization tool for downloading and managing Binance historical data (klines, trades, etc.) asynchronously. It supports both local and S3 storage with intelligent data optimization (monthly vs daily preference).

## Features

- **Asynchronous synchronization**: Concurrent downloading optimized for large volumes
- **Multi-market support**: SPOT, FUTURES (UM/CM), and OPTIONS markets
- **Various data types**: KLINES, TRADES, AGG_TRADES, BOOK_DEPTH, METRICS, and more
- **Flexible storage**: Local filesystem or cloud (AWS S3)
- **Intelligent optimization**: Automatic preference for monthly data over daily files
- **CLI interface**: Complete Click-based CLI with dry-run mode
- **DuckDB-powered loader**: Fast data loading with predicate pushdown
- **Error handling**: Automatic retry and robust SSL management
- **Progress tracking**: Rich progress bars with detailed statistics
- **Advanced logging**: Structured logs with automatic rotation

## Installation

### From pip

```bash
pip install binance-syncer
```

### From uv

```bash
uv pip install binance-syncer
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
Edit `~/utilities/config/binance_syncer.ini`:

```ini
[LOCAL]
PATH = ~/binance-vision

[S3]
BUCKET = my-binance-data-bucket
PREFIX = binance-vision

[SETTINGS]
MAX_CONCURRENT_DOWNLOADS = 100
SYMBOL_CONCURRENCY = 10
BATCH_SIZE_SYNC = 20
BATCH_SIZE_DELETE = 1000
```

### AWS environment variables (for S3)

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

## Usage

### Command Line Interface

The CLI is built with Click and provides a user-friendly interface for data synchronization.

#### Basic commands

```bash
# Sync all SPOT KLINES 1d data
binance-syncer --market-type spot --data-type klines --interval 1d

# Specific symbols with progress bar
binance-syncer --market-type spot --data-type klines --interval 1d \
  --symbols BTCUSDT --symbols ETHUSDT --symbols ADAUSDT --progress

# S3 storage
binance-syncer --market-type spot --data-type klines --interval 1d --s3 --progress

# Dry-run mode (shows what would be synced without downloading)
binance-syncer --market-type spot --data-type klines --interval 1d --dry-run

# Trade data (no interval required)
binance-syncer --market-type spot --data-type trades
```

#### Advanced options

```bash
# Futures with 4h interval
binance-syncer --market-type futures/um --data-type klines --interval 4h --progress

# Multiple symbols with S3 storage
binance-syncer --market-type spot --data-type klines --interval 1d \
  --symbols BTCUSDT --symbols ETHUSDT --s3 --progress

# Disable progress bar
binance-syncer --market-type spot --data-type klines --interval 1d --no-progress
```

#### CLI Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `--market-type` | Choice | Yes | Market type: `spot`, `futures/um`, `futures/cm`, `option` |
| `--data-type` | Choice | Yes | Data type: `klines`, `trades`, `aggTrades`, etc. |
| `--interval` | Choice | Conditional | Kline interval (required for klines): `1s`, `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w` |
| `--symbols` | Multiple | No | Specific symbols to sync (can be repeated). If not provided, all symbols are synced |
| `--progress/--no-progress` | Flag | No | Show/hide progress bar (default: `--progress`) |
| `--dry-run` | Flag | No | Show what would be synced without downloading |
| `--s3` | Flag | No | Use S3 storage instead of local |

### Programmatic Usage

#### Syncer API

```python
import asyncio
import binance_syncer as bs

async def main():
    # Configure syncer
    syncer = bs.Syncer(
        market_type=bs.MarketType.SPOT,
        data_type=bs.DataType.KLINES,
        interval=bs.KlineInterval.D1,
        progress=True,
        s3=False  # Set to True for S3 storage
    )
    
    # Sync specific symbols
    await syncer.sync(["BTCUSDT", "ETHUSDT"])
    
    # Or sync all available symbols
    all_symbols = await syncer.list_remote_symbols()
    print(f"Found {len(all_symbols)} symbols")
    await syncer.sync(all_symbols)

# Execute
asyncio.run(main())
```

#### Loader API

The Loader uses DuckDB for fast parquet file loading with predicate pushdown:

```python
import binance_syncer as bs

# Initialize loader
loader = bs.Loader(
    market_type=bs.MarketType.SPOT,
    data_type=bs.DataType.KLINES,
    interval=bs.KlineInterval.D1,
    s3=False  # Set to True for S3
)

# Load data for a specific symbol and date range
df = loader.load(
    symbol="BTCUSDT",
    start="2023-01-01",
    end="2023-06-01"
)

print(df)
```

#### Complete workflow example

```python
import asyncio
import binance_syncer as bs
from utilities import LoggingConfigurator

async def sync_and_load():
    # Configure logging
    LoggingConfigurator.configure(project="my_project", level="INFO")
    
    # 1. Sync data
    syncer = bs.Syncer(
        market_type=bs.MarketType.SPOT,
        data_type=bs.DataType.KLINES,
        interval=bs.KlineInterval.D1,
        progress=True,
        s3=False
    )
    
    await syncer.sync(["BTCUSDT"])
    
    # 2. Load data
    loader = bs.Loader(
        market_type=bs.MarketType.SPOT,
        data_type=bs.DataType.KLINES,
        interval=bs.KlineInterval.D1
    )
    
    df = loader.load("BTCUSDT", start="2024-01-01", end="2024-12-31")
    
    print(f"Loaded {len(df)} rows")
    print(df.head())

asyncio.run(sync_and_load())
```

#### Batch processing multiple markets

```python
import asyncio
import binance_syncer as bs

async def sync_all_markets():
    """Sync first symbol of every market type and data type."""
    
    for market_type, data_types in bs.SCHEMA.items():
        print(f"Market Type: {market_type}")
        
        for data_type in data_types:
            print(f"  Data Type: {data_type}")
            
            # Configure syncer
            syncer = bs.Syncer(
                market_type,
                data_type,
                bs.KlineInterval.D1 if data_type == bs.DataType.KLINES else None,
                progress=True
            )
            
            # Get available symbols
            symbols = await syncer.list_remote_symbols()
            print(f"    Available Symbols: {len(symbols)}")
            
            # Sync first symbol only
            if symbols:
                symbol = symbols[0]
                print(f"    Syncing: {symbol}")
                await syncer.sync([symbol])

asyncio.run(sync_all_markets()) # Warning, some market or type of data can be heavy
```

## Data Structure

### Local storage
```
~/binance-vision/
└── data/
    ├── spot/
    │   ├── daily/
    │   │   ├── klines/
    │   │   │   └── BTCUSDT/
    │   │   │       └── 1d/
    │   │   │           └── 2024-02-15.parquet
    │   │   └── trades/
    │   │       └── BTCUSDT/
    │   │           └── 2024-02-15.parquet
    │   └── monthly/
    │       └── klines/
    │           └── BTCUSDT/
    │               └── 1d/
    │                   └── 2024-01.parquet
    ├── futures/
    │   └── um/
    │       └── daily/
    │           └── klines/
    └── option/
        └── daily/
            └── BVOLIndex/
```

### S3 storage
```
s3://your-bucket/
└── binance-vision/
    └── data/
        ├── spot/
        │   ├── daily/
        │   │   └── klines/
        │   │       └── BTCUSDT/
        │   └── monthly/
        │       └── klines/
        │           └── BTCUSDT/
        └── futures/
```

## Supported Data Types

### Market Type: SPOT

| Data Type | Schema Columns | Interval Required |
|-----------|---------------|-------------------|
| `klines` | open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore | Yes |
| `trades` | id, price, qty, base_qty, time, is_buyer_maker, is_best_match | No |
| `aggTrades` | agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker, is_best_match | No |

### Market Type: FUTURES_UM / FUTURES_CM

| Data Type | Schema Columns | Interval Required |
|-----------|---------------|-------------------|
| `klines` | open_time, open, high, low, close, volume, close_time, quote_volume... | Yes |
| `trades` | id, price, qty, quote_qty/base_qty, time, is_buyer_maker | No |
| `aggTrades` | agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker | No |
| `bookDepth` | timestamp, percentage, depth, notional | No |
| `bookTicker` | update_id, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, transaction_time, event_time | No |
| `indexPriceKlines` | open_time, open, high, low, close, volume... | Yes |
| `markPriceKlines` | open_time, open, high, low, close, volume... | Yes |
| `premiumIndexKlines` | open_time, open, high, low, close, volume... | Yes |
| `liquidationSnapshot` | time, side, order_type, time_in_force, original_quantity, price, average_price, order_status... | No |
| `metrics` | create_time, symbol, sum_open_interest, sum_open_interest_value... | No |

### Market Type: OPTION

| Data Type | Schema Columns | Interval Required |
|-----------|---------------|-------------------|
| `BVOLIndex` | calc_time, symbol, base_asset, quote_asset, index_value | No |
| `EOHSummary` | date, hour, symbol, underlying, type, strike, open, high, low, close, volume_contracts, volume_usdt... | No |

### Supported Intervals

| Category | Values |
|----------|--------|
| **Seconds** | `1s` |
| **Minutes** | `1m`, `3m`, `5m`, `15m`, `30m` |
| **Hours** | `1h`, `2h`, `4h`, `6h`, `8h`, `12h` |
| **Days** | `1d` |
| **Weeks** | `1w` |

## Enums Reference

```python
from binance_syncer import MarketType, DataType, KlineInterval

# Market Types
MarketType.SPOT           # "spot"
MarketType.FUTURES_UM     # "futures/um"
MarketType.FUTURES_CM     # "futures/cm"
MarketType.OPTION         # "option"

# Data Types
DataType.KLINES                   # "klines"
DataType.TRADES                   # "trades"
DataType.AGG_TRADES              # "aggTrades"
DataType.BOOK_DEPTH              # "bookDepth"
DataType.BOOK_TICKER             # "bookTicker"
DataType.INDEX_PRICE_KLINES      # "indexPriceKlines"
DataType.MARK_PRICE_KLINES       # "markPriceKlines"
DataType.PREMIUM_INDEX_KLINES    # "premiumIndexKlines"
DataType.LIQUIDATION_SNAPSHOT    # "liquidationSnapshot"
DataType.METRICS                 # "metrics"
DataType.BVOL_INDEX              # "BVOLIndex"
DataType.EOH_SUMMARY             # "EOHSummary"

# Kline Intervals
KlineInterval.S1    # "1s"
KlineInterval.M1    # "1m"
KlineInterval.M3    # "3m"
KlineInterval.M5    # "5m"
KlineInterval.M15   # "15m"
KlineInterval.M30   # "30m"
KlineInterval.H1    # "1h"
KlineInterval.H2    # "2h"
KlineInterval.H4    # "4h"
KlineInterval.H6    # "6h"
KlineInterval.H8    # "8h"
KlineInterval.H12   # "12h"
KlineInterval.D1    # "1d"
KlineInterval.W1    # "1w"
```

## Logging and Monitoring

### Log configuration

Logs are managed by the `utilities-toolkit` package and automatically created:

```
~/utilities/logs/binance_syncer/
├── binance_syncer_2024-02-15.log
├── binance_syncer_2024-02-14.log
└── ...
```

### Log levels

```python
from utilities import LoggingConfigurator

# Configure logging for your project
LoggingConfigurator.configure(
    project="binance_syncer",
    level="INFO",         # DEBUG, INFO, WARNING, ERROR
    retention_days=7      # Automatic cleanup after 7 days
)
```

### Automatic features
- **Rotation**: Daily log rotation
- **Retention**: Configurable retention period (default: 7 days)
- **Format**: Structured JSON-like format with timestamps
- **Levels**: DEBUG, INFO, WARNING, ERROR with color coding

## Performance and Optimizations

### Concurrency settings

Default settings (configurable in `config.ini`):

```ini
[SETTINGS]
MAX_CONCURRENT_DOWNLOADS = 100  # Concurrent downloads per symbol
SYMBOL_CONCURRENCY = 10         # Symbols processed in parallel
BATCH_SIZE_SYNC = 20           # Files processed per batch
BATCH_SIZE_DELETE = 1000       # Files deleted per batch
```

### Network optimizations
- **SSL**: Robust configuration with certifi fallback
- **Timeouts**: 
  - Connection: 30s
  - Read: 120s
  - Total: 300s
- **Retry logic**: Automatic exponential backoff
- **Chunked reading**: 8KB chunks for large files
- **Connection pooling**: Reused HTTP connections via aiohttp

### Data optimization
- **Monthly preference**: Automatically prefers monthly files over daily when available
- **Deduplication**: Skips already downloaded files
- **Compression**: Parquet format with Snappy compression
- **Predicate pushdown**: DuckDB pushdown for efficient date filtering

### Storage optimization

The syncer intelligently manages data by:
1. **Checking for monthly files**: Downloads monthly aggregated data when available
2. **Removing redundant daily files**: Deletes daily files when monthly data covers the period
3. **Incremental updates**: Only downloads missing or updated files

Example optimization:
```
Before:  BTCUSDT-1d-2024-01-01.parquet (30 files for January)
After:   BTCUSDT-1d-2024-01.parquet (1 monthly file)
Result:  29 files removed, faster loading
```

## Troubleshooting

### Common SSL issues

#### macOS Certificate Error
```bash
# Install Python certificates
/Applications/Python\ 3.11/Install\ Certificates.command

# Or via pip
pip install --upgrade certifi

# Verify SSL configuration
python -c "import ssl, certifi; print(certifi.where())"
```

#### SSL Diagnostic Mode
The CLI automatically performs SSL diagnostics on startup:
```bash
binance-syncer --market-type spot --data-type klines --interval 1d
# Output will show:
# === SSL Diagnostic ===
# Certifi path: /path/to/cacert.pem
# SSL default paths: ...
# ✅ SSL test connection successful
```

### S3 Configuration Issues

#### Check AWS credentials
```bash
# List configuration
aws configure list

# Test S3 access
aws s3 ls s3://your-bucket/

# Check environment variables
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
echo $AWS_DEFAULT_REGION
```

#### Verify S3 bucket configuration
```bash
# Test with dry-run first
binance-syncer --market-type spot --data-type klines --interval 1d --s3 --dry-run

# Check bucket exists and is accessible
aws s3 ls s3://my-binance-data-bucket/binance-vision/
```

### Connection Timeout Issues

If experiencing timeout issues:

```python
# Increase timeout in your code
import aiohttp

timeout = aiohttp.ClientTimeout(
    total=600,      # 10 minutes total
    connect=60,     # 1 minute to connect
    sock_read=300   # 5 minutes to read
)
```

### Memory Issues

For large datasets:

```python
# Configure DuckDB memory limit
loader = bs.Loader(
    market_type=bs.MarketType.SPOT,
    data_type=bs.DataType.KLINES,
    interval=bs.KlineInterval.D1,
    memory_limit="4GB",    # Limit DuckDB memory usage
    threads=4              # Limit thread count
)
```

### Debugging Tips

#### Enable debug logging
```python
from utilities import LoggingConfigurator

LoggingConfigurator.configure(
    project="binance_syncer",
    level="DEBUG"  # Show all debug information
)
```

#### Check file integrity
```python
import pandas as pd

# Verify parquet file
df = pd.read_parquet("path/to/file.parquet")
print(df.info())
print(df.head())
```

#### Test individual components
```python
import asyncio
import binance_syncer as bs

async def test_connection():
    syncer = bs.Syncer(
        market_type=bs.MarketType.SPOT,
        data_type=bs.DataType.KLINES,
        interval=bs.KlineInterval.D1
    )
    
    # Test listing symbols
    symbols = await syncer.list_remote_symbols()
    print(f"Found {len(symbols)} symbols")
    
    # Test dates computation for one symbol
    if symbols:
        dates = await syncer.compute_dates_cover(symbols[0])
        print(f"Symbol: {symbols[0]}")
        print(f"Months to download: {len(dates['M_DL'])}")
        print(f"Days to download: {len(dates['D_DL'])}")
        print(f"Days to remove: {len(dates['D_RM'])}")

asyncio.run(test_connection())
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Data provided by [Binance Public Data](https://data.binance.vision/)
- Built with [aiohttp](https://docs.aiohttp.org/), [DuckDB](https://duckdb.org/), [Click](https://click.palletsprojects.com/), and [Rich](https://rich.readthedocs.io/)
- Configuration management via [utilities-toolkit](https://github.com/your-repo/utilities-toolkit)

## Support

- **Issues**: [GitHub Issues](https://github.com/caymaar/binance-syncer/issues)
- **Documentation**: This README
- **Examples**: See [examples/examples.ipynb](examples/examples.ipynb)

## Changelog

### v3.0.0
- Migrated CLI from argparse to Click
- Added `--s3` flag for simplified S3 storage mode
- Improved SSL diagnostics and error handling
- Enhanced DuckDB-based Loader with predicate pushdown
- Better progress tracking with Rich
- Updated dependencies and Python 3.9+ requirement

### v2.x
- Initial public release
- Basic sync and load functionality
- S3 and local storage support
