from enum import Enum

BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

class MarketType(Enum):
    SPOT = "spot"
    FUTURES_CM = "futures/cm"
    FUTURES_UM = "futures/um"
    OPTION = "option"

class Frequency(Enum):
    DAILY = "daily"
    MONTHLY = "monthly"

class DataType(Enum):
    AGG_TRADES = "aggTrades"
    BOOK_DEPTH = "bookDepth"
    BOOK_TICKER = "bookTicker"
    INDEX_PRICE_KLINES = "indexPriceKlines"
    KLINES = "klines"
    LIQUIDATION_SNAPSHOT = "liquidationSnapshot"
    MARK_PRICE_KLINES = "markPriceKlines"
    METRICS = "metrics"
    PREMIUM_INDEX_KLINES = "premiumIndexKlines"
    TRADES = "trades"
    BVOL_INDEX = "BVOLIndex"
    EOH_SUMMARY = "EOHSummary"

class KlineInterval(Enum):
    S1 = "1s"
    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H8 = "8h"
    H12 = "12h"
    D1 = "1d"
    W1 = "1w"

SCHEMA = {
    MarketType.SPOT:       [DataType.AGG_TRADES, 
                            DataType.KLINES, 
                            DataType.TRADES],

    MarketType.OPTION :    [DataType.BVOL_INDEX, 
                            DataType.EOH_SUMMARY],

    MarketType.FUTURES_CM: [DataType.AGG_TRADES, 
                            DataType.BOOK_DEPTH, 
                            DataType.BOOK_TICKER, 
                            DataType.INDEX_PRICE_KLINES, 
                            DataType.KLINES, 
                            DataType.LIQUIDATION_SNAPSHOT,
                            DataType.MARK_PRICE_KLINES,
                            DataType.METRICS,
                            DataType.PREMIUM_INDEX_KLINES, 
                            DataType.TRADES],
                            
    MarketType.FUTURES_UM: [DataType.AGG_TRADES,
                            DataType.BOOK_DEPTH,
                            DataType.BOOK_TICKER,
                            DataType.INDEX_PRICE_KLINES,
                            DataType.KLINES,
                            DataType.MARK_PRICE_KLINES,
                            DataType.METRICS,
                            DataType.PREMIUM_INDEX_KLINES, 
                            DataType.TRADES],
}

class Headers(Enum):
   KLINES = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", "nb_trades", "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"]