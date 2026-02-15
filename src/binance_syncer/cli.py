import asyncio
import sys
from typing import Optional, List
import click
from utilities import LoggingConfigurator

from binance_syncer.constant import MarketType, DataType, KlineInterval
from binance_syncer.syncer import Syncer

import logging

logger = logging.getLogger(__name__)


async def run_sync(
    market_type: str,
    data_type: str,
    interval: Optional[str],
    symbols: Optional[List[str]],
    progress: bool,
    dry_run: bool,
    s3: bool
) -> None:
    """Run the synchronization with the provided arguments."""
    # Convert string enums to enum objects
    try:
        market_type_enum = MarketType(market_type)
    except ValueError:
        click.echo(f"Error: Invalid market type: {market_type}", err=True)
        sys.exit(1)

    try:
        data_type_enum = DataType(data_type)
    except ValueError:
        click.echo(f"Error: Invalid data type: {data_type}", err=True)
        sys.exit(1)

    # Validate interval for klines
    interval_enum = None
    if data_type == "klines" or "klines" in data_type.lower():
        if not interval:
            click.echo("Error: --interval is required for klines data type", err=True)
            sys.exit(1)
        try:
            interval_enum = KlineInterval(interval)
        except ValueError:
            click.echo(f"Error: Invalid interval: {interval}", err=True)
            sys.exit(1)
    else:
        if interval:
            logger.warning("--interval is ignored for non-klines data types")

    # Validate and normalize symbols format
    if symbols:
        normalized_symbols = []
        for symbol in symbols:
            if not symbol.isupper():
                logger.warning(f"Symbol '{symbol}' should be uppercase. Converting to '{symbol.upper()}'")
                normalized_symbols.append(symbol.upper())
            else:
                normalized_symbols.append(symbol)
        symbols = normalized_symbols
    try:
        # DIAGNOSTIC SSL
        import ssl
        import certifi
        
        logger.debug("=== SSL Diagnostic ===")
        logger.debug(f"Certifi path: {certifi.where()}")
        logger.debug(f"SSL default paths: {ssl.get_default_verify_paths()}")
        
        # Test de connexion SSL basique
        try:
            import urllib.request
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            with urllib.request.urlopen('https://s3-ap-northeast-1.amazonaws.com', context=ssl_context, timeout=5) as _:
                logger.debug("✅ SSL test connection successful")
        except Exception as ssl_error:
            logger.debug(f"SSL test failed: {ssl_error}")
            logger.debug("Trying with alternative SSL configuration...")
        
        # Create syncer instance
        syncer = Syncer(
            market_type=market_type_enum,
            data_type=data_type_enum,
            interval=interval_enum,
            progress=progress,
            s3=s3
        )

        # Get symbols list
        if symbols:
            logger.info(f"Syncing {len(symbols)} specified symbols: {', '.join(symbols)}")
        else:
            logger.info("Fetching available symbols...")
            all_symbols = await syncer.list_remote_symbols()
            logger.info(f"Found {len(all_symbols)} available symbols")
            symbols = all_symbols

        # Dry run mode
        if dry_run:
            logger.info("\n=== DRY RUN MODE ===")
            logger.info(f"Would sync {len(symbols)} symbols:")
            logger.info(f"  Market Type: {market_type}")
            logger.info(f"  Data Type: {data_type}")
            logger.info(f"  Interval: {interval or 'N/A'}")
            logger.info(f"  Symbols: {symbols[:10]}{'...' if len(symbols) > 10 else ''}")
            
            # Show what would be synced for first symbol
            if symbols:
                logger.info(f"\nAnalyzing first symbol: {symbols[0]}")
                dates_dict = await syncer.compute_dates_cover(symbols[0])
                logger.info(f"  Months to download: {len(dates_dict['M_DL'])}")
                logger.info(f"  Days to download: {len(dates_dict['D_DL'])}")
                logger.info(f"  Days to remove: {len(dates_dict['D_RM'])}")
            return

        # Run actual sync
        logger.info("\nStarting synchronization...")
        logger.info(f"  Market Type: {market_type}")
        logger.info(f"  Data Type: {data_type}")
        logger.info(f"  Interval: {interval or 'N/A'}")
        logger.info(f"  Progress Bar: {'Enabled' if progress else 'Disabled'}")
        logger.info(f"  Symbols: {len(symbols)} total")

        await syncer.sync(symbols)
        logger.info("\nSynchronization completed successfully!")

    except KeyboardInterrupt:
        logger.info("\nSynchronization interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nSynchronization failed: {e}")
        sys.exit(1)


@click.command(
    help="Binance Data Synchronization Tool",
    epilog="""
Examples:

  # Sync SPOT KLINES with 1d interval for all symbols
  python -m binance_syncer.cli --market-type spot --data-type klines --interval 1d

  # Sync specific symbols with progress bar
  python -m binance_syncer.cli --market-type spot --data-type klines --interval 1d --symbols BTCUSDT ETHUSDT --progress

  # Sync to S3 storage
  python -m binance_syncer.cli --market-type spot --data-type klines --interval 1d --s3

  # Sync trades data (no interval needed)
  python -m binance_syncer.cli --market-type spot --data-type trades
    """
)
@click.option(
    '--market-type',
    type=click.Choice([market.value for market in MarketType], case_sensitive=False),
    required=True,
    help='Market type to sync (spot, futures/um, futures/cm, option)'
)
@click.option(
    '--data-type',
    type=click.Choice([data.value for data in DataType], case_sensitive=False),
    required=True,
    help='Data type to sync (klines, trades, aggTrades, etc.)'
)
@click.option(
    '--interval',
    type=click.Choice([interval.value for interval in KlineInterval], case_sensitive=False),
    default=None,
    help='Kline interval (required for klines data type)'
)
@click.option(
    '--symbols',
    multiple=True,
    help='Specific symbols to sync (e.g., BTCUSDT ETHUSDT). If not provided, all available symbols will be synced. Can be used multiple times: --symbols BTCUSDT --symbols ETHUSDT'
)
@click.option(
    '--progress/--no-progress',
    default=True,
    help='Show progress bar during synchronization'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be synced without actually downloading'
)
@click.option(
    '--s3',
    is_flag=True,
    help='Set storage mode to s3'
)
def main(
    market_type: str,
    data_type: str,
    interval: Optional[str],
    symbols: tuple,
    progress: bool,
    dry_run: bool,
    s3: bool
) -> None:
    """Main entry point for the CLI."""
    # Configure logging
    LoggingConfigurator.configure(project="binance_syncer", level="INFO", retention_days=7)

    # Convert symbols tuple to list
    symbols_list = list(symbols) if symbols else None

    # Run synchronization
    try:
        asyncio.run(run_sync(
            market_type=market_type,
            data_type=data_type,
            interval=interval,
            symbols=symbols_list,
            progress=progress,
            dry_run=dry_run,
            s3=s3
        ))
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(1)

if __name__ == "__main__":
    main()