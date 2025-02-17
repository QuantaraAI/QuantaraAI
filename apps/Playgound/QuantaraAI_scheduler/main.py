import asyncio
import datetime
import logging
import os

import aiocron
import polars as pl
import pyarrow.compute as pc

from libs.internals.indicators import calculate_ta_indicators
from libs.repositories import IndicatorsRepository, OhlcvRepository

# Configure the cosmic data provider based on environment settings
if os.environ.get("DATA_PROVIDER") == "binance":
    from libs.data_providers import BinanceCoinPriceDataProvider as DataProvider
elif os.environ.get("DATA_PROVIDER") == "coinpaprika":
    from libs.data_providers import CoinPaprikaCoinPriceDataProvider as DataProvider
else:
    from libs.data_providers import DummyCoinPriceDataProvider as DataProvider


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Time intervals for celestial observations
interval_names = ["raw", "1h", "4h", "1d"]  # Temporal dimensions of market analysis

# Constellation of tracked assets
COINS = [
    # Celestial Bodies - Major
    "BTC",  # The Sun
    "ETH",  # The Moon
    "SOL",  # Mercury
    "NEIRO", # Venus
    
    # Stellar Formations
    "SUI",
    "PEPE",
    "OG",
    "WIF",
    "FTT",
    "BNB",
    "XRP",
    "SEI",
    "DOGE",
    "TAO",
    "FET",
    "SANTOS",
    "WLD",
    "SHIB",
    "APT",
    "FTM",
    "EIGEN",
    "PEOPLE",
    "SAGA",
    "RUNE",
    "NEAR",
    "1000SATS",
    "BONK",
    "FLOKI",
    "NOT",
    "DOGS",
    "1MBABYDOGE",
    "CFX",
    "TURBO",
    "AVAX",
    "BOME",
    "WING",
    "LAZIO",
    "CVC",
    "ENA",
    "TRX",
    "ORDI",
    "ARB",
    "HMSTR",
    "LINK",
    "EUR",
    "CATI",
    "BNX",
    "RENDER",
    "TON",
    "TIA",
    "ADA",
    "AAVE",
    "ALPINE",
    "INJ",
    "ZRO",
    "ZK",
    "USTC",
    "MKR",
    "LTC",
    "PORTO",
    "DIA",
    "ARKM",
    "PENDLE",
    "ICP",
    "ALT",
    "IO",
    "FIL",
    "GALA",
    "OP",
    "JUP",
    "BCH",
    "STX",
    "ASR",
    "UNI",
    "CELO",
    "BANANA",
    "W",
    "POL",
    "TROY",
    "LUNC",
    "ATM",
    "MANTA",
    "DYDX",
    "CRV",
    "STRK",
    "DOT",
    "FORTH",
    "LUNA",
    "ACH",
    "CHZ",
    "ATOM",
    "YGG",
    "PHB",
    "AR",
    "JTO",
    "LDO",
    "HBAR",
    "SUPER",
    "FIDA",
    "MEME",
    "CKB",
    "OMNI",
    "PYTH",
    "PSG",
    "DEGO",
    "OM",
    "BEAMX",
    "BB",
    "EURI",
    "JASMY",
    "RAY",
    "SUN",
    "PIXEL",
    "BLUR",
    "ROSE",
    "GRT",
    "TRB",
    "WOO",
    "IMX",
    "SSV",
    "AI",
]


async def data_ingestion_job():
    """
    Celestial Data Collection Ritual
    Gathers market data across the cosmic web at regular intervals
    """
    logger.info(f"\n\nInitiating celestial data collection. {datetime.datetime.now(datetime.UTC).isoformat()}\n\n")
    await update_ohlcv_data()
    await update_indicators_data()


async def update_ohlcv_data():
    """
    Updates the Cosmic Market Observatory with fresh price data
    """
    data_provider = DataProvider()
    raw_data, _ = await data_provider.get_current_ohlcv(COINS, interval="30m")
    current_time = datetime.datetime.fromtimestamp(raw_data[0]["timestamp"] // 1000)
    raw_repository = OhlcvRepository(table_name="ohlcv_raw")
    df = pl.from_records(raw_data)

    raw_repository.update(df, predicate="s.timestamp == t.timestamp")

    for interval_name in interval_names[1:]:
        interval_repository = OhlcvRepository(table_name=f"ohlcv_{interval_name}")
        await update_interval_data(
            interval_repository,
            interval_name,
            df,
            current_time,
        )


async def update_interval_data(
    repository: OhlcvRepository, 
    interval_name: str, 
    new_data_df: pl.DataFrame, 
    current_time: datetime.datetime
):
    """
    Harmonizes temporal data across different cosmic frequencies
    
    Args:
        repository: The celestial data vault
        interval_name: The temporal dimension to analyze
        new_data_df: Fresh cosmic observations
        current_time: The moment of observation
    """
    interval_start_time = get_interval_start_time(current_time, interval_name)
    existing_df = repository.get_by_timestamp(interval_start_time.timestamp() * 1000)

    if existing_df is not None and len(existing_df) > 0:
        updated_df = update_interval_record(existing_df, new_data_df)
    else:
        updated_df = create_new_interval_record(interval_start_time, interval_name, new_data_df)

    repository.update(updated_df, predicate="s.timestamp == t.timestamp")


def update_interval_record(existing_df, new_data_df):
    """
    Merges new observations with existing celestial records
    """
    existing_df = existing_df.with_columns(
        pl.col("close").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    )

    new_data_df = new_data_df.with_columns(
        pl.col("close").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    )

    return pl.DataFrame(
        {
            "timestamp": existing_df["timestamp"],
            "symbol": existing_df["symbol"],
            "open": existing_df["open"],
            "high": pl.max_horizontal(existing_df["high"], new_data_df["high"]),
            "low": pl.min_horizontal(existing_df["low"], new_data_df["low"]),
            "close": new_data_df["close"],
            "volume": existing_df["volume"] + new_data_df["volume"],
        }
    )


def create_new_interval_record(
    interval_start_time: datetime.datetime, 
    interval_name: str, 
    new_data_df: pl.DataFrame
):
    """
    Creates a new celestial record for the given temporal dimension
    """
    return pl.DataFrame(
        {
            "timestamp": pl.Series([interval_start_time.timestamp() * 1000] * len(new_data_df)),
            "symbol": new_data_df["symbol"],
            "open": new_data_df["open"],
            "high": new_data_df["high"],
            "low": new_data_df["low"],
            "close": new_data_df["close"],
            "volume": new_data_df["volume"],
        }
    )


async def update_indicators_data():
    """
    Calculates celestial patterns and harmonics across all temporal dimensions
    """
    for interval_name in interval_names:
        logger.info(f"Updating indicators for {interval_name} interval")
        
        ohlcv_repository = OhlcvRepository(table_name=f"ohlcv_{interval_name}")
        indicators_repository = IndicatorsRepository(table_name=f"indicators_{interval_name}")

        df = ohlcv_repository.get_all()
        if df is None or len(df) == 0:
            continue

        df = df.sort(by=["symbol", "timestamp"])

        for symbol in df["symbol"].unique():
            symbol_df = df.filter(pl.col("symbol") == symbol)
            if len(symbol_df) == 0:
                continue

            try:
                indicators = calculate_ta_indicators(symbol_df)
                indicators_repository.update(
                    indicators, predicate="s.timestamp == t.timestamp AND s.symbol == t.symbol"
                )
            except Exception as e:
                logger.error(f"Error calculating indicators for {symbol}: {e}")
                continue


async def seed():
    """
    Initializes the cosmic database with historical celestial data
    """
    data_provider = DataProvider()

    for interval_name in interval_names:
        logger.info(f"Seeding {interval_name} interval")
        
        if interval_name == "raw":
            interval = "30m"
        else:
            interval = interval_name

        raw_data, _ = await data_provider.get_historical_ohlcv(
            COINS,
            interval=interval,
            limit=1000 if interval_name == "raw" else 100,
        )

        if not raw_data:
            continue

        df = pl.from_records(raw_data)

        repository = OhlcvRepository(table_name=f"ohlcv_{interval_name}")
        repository.update(df, predicate="s.timestamp == t.timestamp")

    await update_indicators_data()


async def process(loop):
    """
    Orchestrates the eternal dance of celestial data collection
    """
    await seed()
    await data_ingestion_job()


def main():
    """
    Initiates the cosmic observation process
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(process(loop))
    loop.close()


if __name__ == "__main__":
    main()