from dataclasses import dataclass
import pandas as pd
from datetime import datetime

from binance_syncer.constant import MarketType, DataType, Frequency, KlineInterval, BASE_URL

import logging

logger = logging.getLogger(__name__)

@dataclass
class BinancePathBuilder:
    market_type: MarketType
    data_type: DataType
    interval: KlineInterval = None

    def build_download_path(self, frequency: Frequency, symbol: str, year: str, month: str, day: str = None) -> str:
        month = int(month)
        year = int(year)
        day = int(day) if day is not None else None
        assert 1 <= year <= 9999, "Year must be between 1 and 9999"
        assert 1 <= month <= 12, "Month must be between 1 and 12"
        date_part = f"{year}-{month:02d}"
        if frequency == Frequency.DAILY:
            assert day is not None, "Day must be provided for daily frequency"
            assert 1 <= day <= 31, "Day must be between 1 and 31"
            date_part += f"-{day:02d}"

        path_parts = [BASE_URL, "data", self.market_type.value, frequency.value, self.data_type.value]

        if self.data_type in [DataType.KLINES, DataType.INDEX_PRICE_KLINES, DataType.MARK_PRICE_KLINES, DataType.PREMIUM_INDEX_KLINES]:
            assert self.interval is not None, "Interval must be provided for Klines"
            path_parts.append(symbol)
            path_parts.append(self.interval.value)
            filename = f"{symbol}-{self.interval.value}-{date_part}.zip"
        else:
            path_parts.append(symbol)
            filename = f"{symbol}-{self.data_type.value}-{date_part}.zip"

        path_parts.append(filename)
        return "/".join(path_parts)
    
    def build_listing_files_path(self, frequency: Frequency, symbol: str) -> str:
        path_parts = [BASE_URL, "?prefix=data", self.market_type.value, frequency.value, self.data_type.value]
        path_parts.append(symbol)
        if self.data_type in [DataType.KLINES, DataType.INDEX_PRICE_KLINES, DataType.MARK_PRICE_KLINES, DataType.PREMIUM_INDEX_KLINES]:
            assert self.interval is not None, "Interval must be provided for Klines"
            path_parts.append(self.interval.value)
        return "/".join(path_parts)
    
    def build_listing_symbols_path(self) -> str:
        path_parts = [BASE_URL, "?prefix=data", self.market_type.value, Frequency.DAILY.value, self.data_type.value]
        return "/".join(path_parts)
    
    def build_save_path(self, prefix: str, symbol: str, filename: str = None) -> str:
        path_parts = [prefix, "data", self.market_type.value, self.data_type.value, symbol]
        if self.data_type in [DataType.KLINES, DataType.INDEX_PRICE_KLINES, DataType.MARK_PRICE_KLINES, DataType.PREMIUM_INDEX_KLINES]:
            assert self.interval is not None, "Interval must be provided for Klines"
            path_parts.append(self.interval.value)
        if filename:
            path_parts.append(filename)
        return "/".join(path_parts)

def safe_parse_time(series: pd.Series) -> pd.Series:
    sample = series.iloc[0]
    for unit in ["ms", "us", "ns"]:
        try:
            # Convert to numeric first to avoid FutureWarning
            ts = pd.to_datetime(pd.to_numeric(sample, errors='coerce'), unit=unit)
            if ts is not pd.NaT and 2000 < ts.year < datetime.now().year + 2:
                return pd.to_datetime(pd.to_numeric(series, errors='coerce'), unit=unit)
        except Exception:
            continue
    logger.error(f"Impossible to parse time for {series.name} with sample {sample}")
    raise ValueError(f"Cannot parse timestamp {sample}")