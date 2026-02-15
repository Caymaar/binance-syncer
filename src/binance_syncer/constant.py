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
    MarketType.SPOT: {
        DataType.AGG_TRADES:            ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'transact_time', 'is_buyer_maker', 'is_best_match'],
        DataType.KLINES:                ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.TRADES:                ['id', 'price', 'qty', 'base_qty', 'time', 'is_buyer_maker', "is_best_match"]
    },
    MarketType.OPTION : {
        DataType.BVOL_INDEX:            ['calc_time', 'symbol', 'base_asset', 'quote_asset', 'index_value'],
        DataType.EOH_SUMMARY:           ['date', 'hour', 'symbol', 'underlying', 'type', 'strike', 'open', 'high', 'low', 'close', 'volume_contracts', 'volume_usdt', 'best_bid_price', 'best_ask_price', 'best_bid_qty', 'best_ask_qty', 'best_buy_iv', 'best_sell_iv', 'mark_price', 'mark_iv', 'delta', 'gamma', 'vega', 'theta', 'openinterest_contracts', 'openinterest_usdt']
    },
    MarketType.FUTURES_CM: {
        DataType.AGG_TRADES:            ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'transact_time', 'is_buyer_maker'],
        DataType.BOOK_DEPTH:            ['timestamp', 'percentage', 'depth', 'notional'],
        DataType.BOOK_TICKER:           ['update_id', 'best_bid_price', 'best_bid_qty', 'best_ask_price', 'best_ask_qty', 'transaction_time', 'event_time'],
        DataType.INDEX_PRICE_KLINES:    ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.KLINES:                ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.LIQUIDATION_SNAPSHOT:  ['time', 'side', 'order_type', 'time_in_force', 'original_quantity', 'price', 'average_price', 'order_status', 'last_fill_quantity', 'accumulated_fill_quantity'],
        DataType.MARK_PRICE_KLINES:     ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.METRICS:               ['create_time', 'symbol', 'sum_open_interest', 'sum_open_interest_value', 'count_toptrader_long_short_ratio', 'sum_toptrader_long_short_ratio', 'count_long_short_ratio', 'sum_taker_long_short_vol_ratio'],
        DataType.PREMIUM_INDEX_KLINES:  ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.TRADES:                ['id', 'price', 'qty', 'base_qty', 'time', 'is_buyer_maker']
    },
    MarketType.FUTURES_UM: {
        DataType.AGG_TRADES:            ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'transact_time', 'is_buyer_maker'],
        DataType.BOOK_DEPTH:            ['timestamp', 'percentage', 'depth', 'notional'],
        DataType.BOOK_TICKER:           ['update_id', 'best_bid_price', 'best_bid_qty', 'best_ask_price', 'best_ask_qty', 'transaction_time', 'event_time'],
        DataType.INDEX_PRICE_KLINES:    ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.KLINES:                ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.MARK_PRICE_KLINES:     ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.METRICS:               ['create_time', 'symbol', 'sum_open_interest', 'sum_open_interest_value', 'count_toptrader_long_short_ratio', 'sum_toptrader_long_short_ratio', 'count_long_short_ratio', 'sum_taker_long_short_vol_ratio'],
        DataType.PREMIUM_INDEX_KLINES:  ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'],
        DataType.TRADES:                ['id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker']
    }
}

TIME_COLUMNS = ['time', 'open_time', 'close_time', 'transact_time', 'event_time', 'transaction_time', 'calc_time']
TIMESTAMP_COLUMNS = ['timestamp', 'create_time']
DATE_COLUMNS = ['date']