import argparse
import asyncio
import sys
from utilities import LoggingConfigurator

from binance_syncer.constant import MarketType, DataType, KlineInterval
from binance_syncer.binance_data_sync import BinanceDataSync

import logging

logger = logging.getLogger(__name__)

def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Binance Data Synchronization Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync SPOT KLINES with 1d interval for all symbols
  python -m binance_syncer.cli --market-type spot --data-type klines --interval 1d

  # Sync specific symbols with progress bar
  python -m binance_syncer.cli --market-type spot --data-type klines --interval 1d --symbols BTCUSDT ETHUSDT --progress

  # Sync to S3 storage
  python -m binance_syncer.cli --market-type spot --data-type klines --interval 1d --storage s3

  # Sync trades data (no interval needed)
  python -m binance_syncer.cli --market-type spot --data-type trades --storage local
        """
    )

    # Required arguments
    parser.add_argument(
        "--market-type",
        type=str,
        required=True,
        choices=[market.value for market in MarketType],
        help="Market type to sync (spot, futures/um, futures/cm, option)"
    )

    parser.add_argument(
        "--data-type",
        type=str,
        required=True,
        choices=[data.value for data in DataType],
        help="Data type to sync (klines, trades, aggTrades, etc.)"
    )

    # Optional arguments
    parser.add_argument(
        "--interval",
        type=str,
        choices=[interval.value for interval in KlineInterval],
        help="Kline interval (required for klines data type)"
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        type=str,
        help="Specific symbols to sync (e.g., BTCUSDT ETHUSDT). If not provided, all available symbols will be synced"
    )

    parser.add_argument(
        "--storage",
        type=str,
        choices=["local", "s3"],
        default="local",
        help="Storage mode: local filesystem or S3 (default: local)"
    )

    parser.add_argument(
        "--progress",
        action="store_true",
        help="Show progress bar during synchronization"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without actually downloading"
    )

    return parser

def validate_args(args: argparse.Namespace) -> bool:
    """Validate command line arguments."""
    errors = []

    # Convert string enums to enum objects
    try:
        args.market_type_enum = MarketType(args.market_type)
    except ValueError:
        errors.append(f"Invalid market type: {args.market_type}")

    try:
        args.data_type_enum = DataType(args.data_type)
    except ValueError:
        errors.append(f"Invalid data type: {args.data_type}")

    # Validate interval for klines
    if args.data_type == "klines" or "klines" in args.data_type.lower():
        if not args.interval:
            errors.append("--interval is required for klines data type")
        else:
            try:
                args.interval_enum = KlineInterval(args.interval)
            except ValueError:
                errors.append(f"Invalid interval: {args.interval}")
    else:
        if args.interval:
            logger.info("Warning: --interval is ignored for non-klines data types")
        args.interval_enum = None

    # Validate symbols format
    if args.symbols:
        for symbol in args.symbols:
            if not symbol.isupper():
                logger.info(f"Warning: Symbol '{symbol}' should be uppercase. Converting to '{symbol.upper()}'")
                # Convert to uppercase
                args.symbols = [s.upper() for s in args.symbols]
                break

    if errors:
        for error in errors:
            logger.info(f"Error: {error}", file=sys.stderr)
        return False

    return True

async def run_sync(args: argparse.Namespace) -> None:
    """Run the synchronization with the provided arguments."""
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
        syncer = BinanceDataSync(
            storage_mode=args.storage,
            market_type=args.market_type_enum,
            data_type=args.data_type_enum,
            interval=args.interval_enum,
            progress=args.progress
        )

        # Get symbols list
        symbols = args.symbols
        if symbols:
            logger.info(f"Syncing {len(symbols)} specified symbols: {', '.join(symbols)}")
        else:
            logger.info("Fetching available symbols...")
            all_symbols = await syncer.list_remote_symbols()
            logger.info(f"Found {len(all_symbols)} available symbols")
            symbols = all_symbols

        # Dry run mode
        if args.dry_run:
            logger.info("\n=== DRY RUN MODE ===")
            logger.info(f"Would sync {len(symbols)} symbols:")
            logger.info(f"  Market Type: {args.market_type}")
            logger.info(f"  Data Type: {args.data_type}")
            logger.info(f"  Interval: {args.interval or 'N/A'}")
            logger.info(f"  Storage: {args.storage}")
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
        logger.info(f"  Market Type: {args.market_type}")
        logger.info(f"  Data Type: {args.data_type}")
        logger.info(f"  Interval: {args.interval or 'N/A'}")
        logger.info(f"  Storage: {args.storage}")
        logger.info(f"  Progress Bar: {'Enabled' if args.progress else 'Disabled'}")
        logger.info(f"  Symbols: {len(symbols)} total")

        await syncer.sync(symbols)
        logger.info("\nSynchronization completed successfully!")

    except KeyboardInterrupt:
        logger.info("\nSynchronization interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.info(f"\nSynchronization failed: {e}")
        sys.exit(1)

def main() -> None:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Configure logging
    LoggingConfigurator.configure(project="binance_syncer", level="INFO", retention_days=7)

    # Validate arguments
    if not validate_args(args):
        parser.logger.info_help()
        sys.exit(1)

    # Run synchronization
    try:
        asyncio.run(run_sync(args))
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(1)

if __name__ == "__main__":
    main()