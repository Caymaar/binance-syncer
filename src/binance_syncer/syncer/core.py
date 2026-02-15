import aiohttp
import asyncio
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from pathlib import Path
import xml.etree.ElementTree as ET
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from utilities import Config, LoggingConfigurator

from ..constant import MarketType, DataType, Frequency, KlineInterval, SCHEMA, TIME_COLUMNS, TIMESTAMP_COLUMNS, DATE_COLUMNS
from binance_syncer.utils import BinancePathBuilder, safe_parse_time

import logging

logger = logging.getLogger(__name__)

BINANCE_SYNCER_CONFIG_DICT = {
    'LOCAL': {
        "PATH": f"{Path.home()}/binance-vision"
        },
    'S3': {
        "BUCKET": "my-binance-data-bucket",
        "PREFIX": "binance-vision"
        },
    'SETTINGS': {
        "MAX_CONCURRENT_DOWNLOADS": 100,
        "SYMBOL_CONCURRENCY": 10,
        "BATCH_SIZE_SYNC": 20,
        "BATCH_SIZE_DELETE": 1000
        }
}

Config.ensure_initialized("binance_syncer", BINANCE_SYNCER_CONFIG_DICT)


class Syncer:

    LOCAL_PREFIX = Config.BINANCE_SYNCER.LOCAL.PATH
    S3_PREFIX = Config.BINANCE_SYNCER.S3.PREFIX
    S3_BUCKET = Config.BINANCE_SYNCER.S3.BUCKET

    MAX_CONCURRENT_TASKS = int(Config.BINANCE_SYNCER.SETTINGS.MAX_CONCURRENT_DOWNLOADS)        # download/convert concurrency per symbol
    SYMBOL_CONCURRENCY = int(Config.BINANCE_SYNCER.SETTINGS.SYMBOL_CONCURRENCY)                # number of symbols processed in parallel
    BATCH_SIZE_SYNC = int(Config.BINANCE_SYNCER.SETTINGS.BATCH_SIZE_SYNC)                      # number of files processed in a batch
    BATCH_SIZE_DELETE = int(Config.BINANCE_SYNCER.SETTINGS.BATCH_SIZE_DELETE)                  # number of files processed in a batch

    def __init__(self, market_type: MarketType, data_type: DataType, 
                    interval: KlineInterval = None, progress: bool = False, s3: bool = False):
            """
            Initialize the BinanceDataSync object.
            This method sets up the instance with the provided configuration parameters and
            initializes additional components such as the path builder and a robust SSL context.
            If the storage mode is set to 's3', it also initializes an S3 client.
            Parameters:
                market_type (MarketType): The type of market data being processed.
                data_type (DataType): The type of data to sync.
                interval (KlineInterval, optional): The interval at which the kline data is updated.
                                                        Defaults to None.
                progress (bool, optional): Flag indicating whether to display progress during operation.
                                            Defaults to False.
                s3 (bool, optional): Flag indicating whether to use S3 for storage. Defaults to False.
            """
        
            self.market_type = market_type
            self.data_type = data_type
            self.interval = interval
            self.progress = progress
            self.path_builder = BinancePathBuilder(market_type, data_type, interval)
            
            # Configuration SSL robuste
            self._ssl_context = self._create_ssl_context()
            
            if s3:
                import boto3
                self.s3 = boto3.client('s3')
            else:
                self.s3 = None

    def _create_ssl_context(self):
        """
        Creates and returns an SSL context with a robust fallback mechanism.
        The method attempts to create a secure SSL context using certifi's CA bundle. If that fails,
        it falls back to the system's default SSL context. If the system default also fails to create a usable
        context, it finally resorts to a permissive SSL context with disabled hostname verification and certificate checks.
        Returns:
            ssl.SSLContext: A configured SSL context object suitable for secure or fallback scenarios.
        """

        import ssl
        import certifi
        
        try:
            # Try to use certifi for SSL context
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            logger.debug(f"Using certifi SSL context: {certifi.where()}")
            return ssl_context
        except Exception as e:
            logger.warning(f"Certifi SSL failed: {e}, trying system certificates")
            
            try:
                # Fallback to system SSL context
                ssl_context = ssl.create_default_context()
                return ssl_context
            except Exception as e2:
                logger.warning(f"System SSL failed: {e2}, using permissive SSL")
                
                # Fallback to permissive SSL context
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                return ssl_context
            
    async def list_remote_symbols(self) -> list[str]:
        """
        Asynchronously fetch and return a sorted list of unique remote symbols.
        The function makes paginated HTTP GET requests to a remote endpoint to retrieve XML listings of symbols.
        It continues fetching pages until there are no more pages left (i.e., the response indicates it is not truncated).
        Each symbol is extracted from the XML response by finding the "s3:Prefix" element within each "s3:CommonPrefixes" element.
        If the response is truncated, a marker is updated to fetch the next page.
        The resulting symbols are deduplicated, sorted, and returned as a list.
        Returns:
            list[str]: A sorted list of unique symbol strings retrieved from the remote server.
        Raises:
            aiohttp.ClientError: If an HTTP-related exception occurs while retrieving data.
            Exception: For any other unexpected errors that occur during the data retrieval process.
        """
        
        symbols, marker = [], None
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

        # Use the predefined SSL context
        connector = aiohttp.TCPConnector(ssl=self._ssl_context)
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            while True:
                url = self.path_builder.build_listing_symbols_path() + "/&delimiter=/&max-keys=1000"
                if marker:
                    url += f"&marker={marker}"
                
                try:
                    logger.debug(f"Fetching symbols from: {url}")
                    resp = await session.get(url)
                    if resp.status != 200:
                        logger.warning(f"List symbols failed status {resp.status}")
                        break
                        
                    xml = await resp.text()
                    root = ET.fromstring(xml)
                    for p in root.findall("s3:CommonPrefixes", ns):
                        sym = p.find("s3:Prefix", ns).text.split(self.data_type.value)[-1].strip("/")
                        if sym:
                            symbols.append(sym)
                            
                    if root.find("s3:IsTruncated", ns) is not None and root.find("s3:IsTruncated", ns).text.lower() == "true":
                        next_marker = root.find("s3:NextMarker", ns)
                        marker = next_marker.text if next_marker is not None else symbols[-1]
                    else:
                        break
                        
                except aiohttp.ClientError as e:
                    logger.error(f"HTTP error while listing symbols: {e}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error while listing symbols: {e}")
                    raise
                    
        return list(sorted(set(symbols)))

    def list_local_dates(self, symbol: str) -> set[str]:
        """
        List available date identifiers from local storage or S3 for a given symbol.
        This function retrieves the dates for which data is available by either querying an S3 bucket or scanning a local directory. 
        For S3 storage, it uses a paginator to list objects with a specified prefix and extracts the date from each object's key. 
        For local storage, it scans the directory for files with a ".parquet" extension and extracts the stem (filename without extension).
        Parameters:
            symbol (str): The trading symbol for which to list the available data dates.
        Returns:
            set[str]: A set of date strings derived from file names or S3 object keys representing the available data dates.
        """

        if self.s3 is not None:
            prefix = self.path_builder.build_save_path(self.S3_PREFIX, symbol)
            paginator = self.s3.get_paginator("list_objects_v2")
            existing = set()
            for page in paginator.paginate(Bucket=self.S3_BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    existing.add(Path(obj["Key"]).stem)
        else:
            prefix = self.path_builder.build_save_path(self.LOCAL_PREFIX, symbol)
            path = Path(prefix)
            existing = {f.stem for f in path.glob("*.parquet") if f.is_file()}
        return existing
    
    async def compute_dates_cover(self, symbol: str):
        """
        Asynchronously computes the dates coverage for a given trading symbol by comparing local and remote data availability.
        Parameters:
            symbol (str): The trading symbol for which to determine the dates coverage. This is used to locate and
                          compare local and remote data files.
        Returns:
            dict: A dictionary with the following keys:
                  "M_DL": A set of month strings (formatted as "YYYY-MM") that are available remotely (from monthly files)
                          but missing locally.
                  "D_DL": A set of day strings (formatted as "YYYY-MM-DD") that are available remotely (from daily files)
                          but missing locally and for which there is no corresponding remote monthly data.
                  "D_RM": A set of day strings (formatted as "YYYY-MM-DD") representing local daily data dates that should be
                          removed because their corresponding month has remote monthly data available.
        Description:
            The function starts by listing local dates and dividing them into monthly (length 7) and daily (length 10)
            collections. Then, it asynchronously retrieves remote file lists for both monthly and daily frequencies and
            extracts the corresponding date parts from the file names. The comparisons between the local and remote dates
            determine which dates are missing and which local days should be removed when their monthly data is present
            remotely.
        Raises:
            Exceptions originating from self.list_local_dates or self.list_remote_files may be raised in case of errors
            during file retrieval or processing.
        """
        
        local_dates = self.list_local_dates(symbol)
        local_months = {d for d in local_dates if len(d) == 7}
        local_days = {d for d in local_dates if len(d) == 10}

        remote_months_files = await self.list_remote_files(Frequency.MONTHLY, symbol)
        remote_months = {'-'.join(file.split('/')[-1].replace('.zip', '').split('-')[2:]) for file in remote_months_files}

        remote_days_files = await self.list_remote_files(Frequency.DAILY, symbol)
        remote_days = {'-'.join(file.split('/')[-1].replace('.zip', '').split('-')[2:]) for file in remote_days_files}

        days_to_remove = {d for d in local_days if d[:7] in remote_months}
        days_to_have = {d for d in remote_days if d[:7] not in remote_months}

        return {"M_DL": remote_months - local_months, "D_DL": days_to_have - local_days, "D_RM": days_to_remove}

    async def list_remote_files(self, frequency: Frequency, symbol: str) -> list[str]:
        """
        Asynchronously retrieves a list of remote file keys for a specified frequency and symbol from a remote S3-style service.
        This method builds the request URL for listing files and iteratively fetches paginated XML-formatted responses. It extracts and
        collects file keys that end with ".zip" (excluding those ending with ".zip.CHECKSUM"). If the response indicates that the listing is
        truncated, it uses the last file's key as a marker to continue retrieving additional files.
        Parameters:
            frequency (Frequency): The frequency or interval defining the range of files to list.
            symbol (str): The symbol identifier for which the remote files are to be fetched.
        Returns:
            list[str]: A list of file keys that match the specified criteria.
        Raises:
            aiohttp.ClientError: If an HTTP-related error occurs while making the request.
            Exception: If any unexpected error occurs during processing.
        """

        files, marker = [], None
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        
        # Use the predefined SSL context
        connector = aiohttp.TCPConnector(ssl=self._ssl_context)
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            while True:
                url = self.path_builder.build_listing_files_path(frequency, symbol)
                if marker:
                    url += f"&marker={marker}"
                    
                try:
                    resp = await session.get(url)
                    if resp.status != 200:
                        logger.warning(f"Listing dates failed for {symbol} status {resp.status}")
                        break
                        
                    xml = await resp.text()
                    root = ET.fromstring(xml)
                    contents = root.findall("s3:Contents", ns)
                    if not contents:
                        break
                        
                    for c in contents:
                        key = c.find("s3:Key", ns).text
                        if key.endswith(".zip") and not key.endswith(".zip.CHECKSUM"):
                            files.append(key)
                            
                    if root.find("s3:IsTruncated", ns) is not None and root.find("s3:IsTruncated", ns).text.lower() == "true":
                        marker = contents[-1].find("s3:Key", ns).text
                    else:
                        break
                        
                except aiohttp.ClientError as e:
                    logger.error(f"HTTP error while listing files for {symbol}: {e}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error while listing files for {symbol}: {e}")
                    raise
                    
        return files

    async def sync_symbol(self, symbol: str):
        """
        Synchronize data for the given symbol by downloading missing and removing outdated files.
        This asynchronous method performs the following steps:
        1. Logs the initiation of the synchronization process for the provided symbol.
        2. Computes the dates that require data coverage (both for download and removal) using compute_dates_cover().
        3. If there are no dates requiring action, logs that the symbol is up-to-date and returns early.
        4. Logs the number of months and days to download as well as the number of days to remove.
        5. Constructs download paths for monthly and daily data files to be fetched.
        6. If there are files to remove:
            - Removes files from S3 if the storage mode is set to 's3' using batch_delete_s3().
            - Otherwise, deletes files asynchronously using delete_files_async().
        7. If there are files to download:
            - Sets up an aiohttp ClientSession with specified SSL, timeout, and connection parameters.
            - Processes downloads in batches, limiting concurrent downloads with a semaphore.
            - For each batch, schedules download_and_store tasks and awaits their completion.
            - Logs the success rate for each batch and warns if any downloads failed.
        Parameters:
             symbol (str): The symbol for which the data synchronization is performed.
        Returns:
             None
        """

        logger.info(f"=== Syncing {symbol} ===")
        dates_dict = await self.compute_dates_cover(symbol)
        
        if not any(dates_dict.values()):
            logger.info(f"{symbol} is up-to-date")
            return
            
        logger.info(f"{symbol}: {len(dates_dict['M_DL'])} months to download, "
                    f"{len(dates_dict['D_DL'])} days to download, "
                    f"{len(dates_dict['D_RM'])} days to remove")
        
        to_fetch = []
        for date in dates_dict['M_DL']:
            date_parts = date.split('-')
            to_fetch.append(self.path_builder.build_download_path(
                Frequency.MONTHLY, symbol, *date_parts))
        
        for date in dates_dict['D_DL']:
            date_parts = date.split('-')
            to_fetch.append(self.path_builder.build_download_path(
                Frequency.DAILY, symbol, *date_parts))

        if dates_dict['D_RM']:
            if self.s3 is not None:
                await self.batch_delete_s3(list(dates_dict['D_RM']), symbol)
            else:
                delete_semaphore = asyncio.Semaphore(20)
                await self.delete_files_async(list(dates_dict['D_RM']), symbol, delete_semaphore)
        
        # Download new files
        if to_fetch:
            download_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_TASKS // 2)
            
            connector = aiohttp.TCPConnector(
                ssl=self._ssl_context,
                limit=100,
                limit_per_host=20,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )
            
            timeout = aiohttp.ClientTimeout(
                total=300,
                connect=30,
                sock_read=120
            )
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sess:
                
                for i in range(0, len(to_fetch), self.BATCH_SIZE_SYNC):
                    batch_urls = to_fetch[i:i + self.BATCH_SIZE_SYNC]
                    
                    batch_tasks = [
                        self.download_and_store(sess, symbol, url, download_semaphore) 
                        for url in batch_urls
                    ]
                    
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    success_count = sum(1 for r in results if r is True)
                    failed_count = len(batch_tasks) - success_count
                    
                    logger.info(f"{symbol}: Batch {i//self.BATCH_SIZE_SYNC + 1}/{(len(to_fetch) + self.BATCH_SIZE_SYNC - 1)//self.BATCH_SIZE_SYNC} - "
                               f"{success_count}/{len(batch_tasks)} success")
                    if failed_count > 0:
                        logger.warning(f"{symbol}: {failed_count} downloads failed in this batch")

    async def download_and_store(self, session: aiohttp.ClientSession, symbol: str,
                                file_key: str, semaphore: asyncio.Semaphore, max_retries: int = 3) -> bool:
        """
        Downloads a zipped CSV file from the given file_key, processes its data, and stores it in either S3 or locally.
        This asynchronous function performs the following operations:
        1. Uses a semaphore to limit concurrent downloads.
        2. Checks if the file (derived from file_key) already exists in S3 (if storage_mode is 's3') and skips downloading if it does.
        3. Attempts to download the file using the provided aiohttp ClientSession.
        4. Reads the downloaded zip file, extracts the contained CSV, and loads it into a pandas DataFrame.
            - If the data type matches one of the predefined headers in Headers, the CSV is read without a header and the columns are set accordingly.
            - In the case of KLINES data, specific timestamp columns ('open_time' and 'close_time') are parsed safely.
        5. Depending on the storage_mode:
            - For S3: Converts the DataFrame to parquet format and uploads it to the specified S3 bucket.
            - For local storage: Saves the DataFrame as a parquet file to the local filesystem.
        6. Implements exponential backoff and retries the download process up to max_retries times if any error occurs.
        Parameters:
                session (aiohttp.ClientSession): The HTTP session used for making asynchronous requests.
                symbol (str): The trading symbol associated with the data.
                file_key (str): The URL or identifier of the zipped file to download.
                semaphore (asyncio.Semaphore): Semaphore to limit concurrent execution.
                max_retries (int, optional): Maximum number of download attempts. Defaults to 3.
        Returns:
                bool: True if the download and storage process completes successfully; otherwise, False.
        """

        async with semaphore:
            filename = f"{'-'.join(file_key.split('/')[-1].replace('.zip', '').split('-')[2:])}.parquet"

            if self.s3 is not None:
                file_path = self.path_builder.build_save_path(self.S3_PREFIX, symbol, filename)
                try:
                    self.s3.head_object(Bucket=self.S3_BUCKET, Key=file_path)
                    logger.debug(f"File already exists: {file_path}")
                    return True
                except self.s3.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] != "404":
                        logger.error(f"Head object error {symbol} {file_path}: {e}")
                        return False

            for attempt in range(max_retries + 1):
                try:
                    async with session.get(file_key) as resp:
                        resp.raise_for_status()
                        
                        data = BytesIO()
                        async for chunk in resp.content.iter_chunked(8192):
                            data.write(chunk)
                        
                        data.seek(0)

                        with ZipFile(data) as zf:
                            with zf.open(zf.namelist()[0]) as f:

                                # Read CSV into DataFrame (with appropriate headers)
                                cols = SCHEMA.get(self.market_type, {}).get(self.data_type, None)
                                if cols is not None:
                                    # Check if file has content
                                    first_line = f.readline()
                                    if not first_line:
                                        logger.warning(f"Empty file: {file_key}")
                                        return False
                                    
                                    f.seek(0)  # Reset file pointer
                                    
                                    header = pd.read_csv(f, header=None, nrows=1)
                                    if len(header) > 0 and len(header.columns) > 0 and all(isinstance(x, str) for x in header.iloc[0]):
                                        f.seek(0)
                                        df = pd.read_csv(f, header=None, skiprows=1)
                                    else:
                                        f.seek(0)
                                        df = pd.read_csv(f, header=None)
                                    
                                    if df.empty:
                                        logger.warning(f"Empty dataframe after reading: {file_key}")
                                        return False
                                    
                                    df.columns = cols
                                else:
                                    df = pd.read_csv(f)

                                # Convert time columns
                                for col in df.columns:
                                    if col in TIMESTAMP_COLUMNS:
                                        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S")
                                    elif col in DATE_COLUMNS:
                                        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")
                                    elif col in TIME_COLUMNS:
                                        df[col] = safe_parse_time(df[col])
                                    
                                if self.s3 is not None:
                                    out = BytesIO()
                                    df.to_parquet(out, compression="snappy", index=False)
                                    out.seek(0)
                                    self.s3.upload_fileobj(out, self.S3_BUCKET, file_path)
                                    logger.debug(f"Uploaded to S3: {file_path}")
                                else:
                                    local_path = Path(self.path_builder.build_save_path(self.LOCAL_PREFIX, symbol, filename))
                                    local_path.parent.mkdir(parents=True, exist_ok=True)
                                    df.to_parquet(local_path, compression="snappy", index=False)
                                    logger.debug(f"Saved locally: {local_path}")
                                
                                return True
                                
                except Exception as e:
                    logger.warning(f"Download failed for {symbol} {file_key} (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    if attempt == max_retries:
                        logger.error(f"Max retries reached for {symbol} {file_key}")
                        return False
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

            return False

    async def batch_delete_s3(self, files_to_delete: list[str], symbol: str):
        """
        Asynchronously deletes multiple S3 objects in batches.
        This method constructs S3 object keys based on the provided list of dates and symbol,
        using a path builder utility. It then deletes the objects in batches (up to 1000 per batch)
        by calling the S3 client's delete_objects method. The method logs the number of successfully
        deleted files for each batch and reports any errors encountered during deletion.
        Parameters:
            files_to_delete (list[str]): A list of date strings, each representing a file (without extension)
                                         to be deleted. The file names are appended with a '.parquet' extension.
            symbol (str): The symbol used to build the file path for each file to delete.
        Returns:
            None
        Notes:
            - If the files_to_delete list is empty, the method returns immediately without performing any actions.
            - Batch deletion errors are logged individually, and the process continues for remaining batches.
        """

        if not files_to_delete:
            return
            
        delete_objects = []
        for date in files_to_delete:
            file_path = self.path_builder.build_save_path(self.S3_PREFIX, symbol, f"{date}.parquet")
            delete_objects.append({'Key': file_path})
        
        for i in range(0, len(delete_objects), self.BATCH_SIZE_DELETE):
            batch = delete_objects[i:i + self.BATCH_SIZE_DELETE]
            try:
                response = self.s3.delete_objects(
                    Bucket=self.S3_BUCKET,
                    Delete={'Objects': batch, 'Quiet': False}
                )
                deleted_count = len(response.get('Deleted', []))
                logger.info(f"Batch deleted {deleted_count} files for {symbol}")
                
                for error in response.get('Errors', []):
                    logger.error(f"Error deleting {error['Key']}: {error['Message']}")
                    
            except Exception as e:
                logger.error(f"Batch delete failed for {symbol}: {e}")
            
    async def delete_files_async(self, files_to_delete: list[str], symbol: str, semaphore: asyncio.Semaphore):
        """
        Asynchronously deletes multiple files specified by file dates.
        This function processes a list of file identifiers (typically representing dates)
        and deletes each corresponding file concurrently. Depending on the storage_mode,
        it either performs deletion on an S3 bucket or on the local filesystem.
        Parameters:
            files_to_delete (list[str]): A list of strings representing file identifiers 
                                         (e.g., dates) that will be appended to build the file paths.
            symbol (str): A symbol used to customize the file path for deletion.
            semaphore (asyncio.Semaphore): A semaphore to limit the number of concurrent deletion 
                                           operations, ensuring controlled access to the resources.
        Behavior:
            - If the storage_mode is 's3':
                Constructs the remote file path using the S3_PREFIX, symbol, and file identifier.
                It then attempts to asynchronously delete the object from the specified S3 bucket.
                Errors during deletion are caught and logged.
            - For local storage:
                Constructs the local file path using the LOCAL_PREFIX, symbol, and file identifier.
                If the file exists, it is deleted asynchronously using an executor to avoid blocking
                the event loop.
        All file deletion tasks are gathered and executed concurrently while handling any 
        exceptions internally.
        """

        async def delete_single_file(file_date: str):
            """
            Asynchronously deletes a file for a given date based on the storage mode.
            This function determines whether to delete the file from an S3 bucket or from the local
            filesystem by checking the 'storage_mode'. It uses an asynchronous semaphore to limit the
            number of concurrent deletion operations.
            Parameters:
                file_date (str): The date string representing the file to be deleted. The file name is
                                 constructed by appending '.parquet' to this string.
            Raises:
                Exception: Any exceptions raised during the deletion process (e.g., S3 deletion errors)
                           are logged with an error message.
            Notes:
                - For S3 storage, the file path is built using a remote prefix and the corresponding
                  symbol, then deletion is attempted via the asynchronous S3 deletion method.
                - For local storage, the file path is built using a local prefix and the corresponding
                  symbol. It checks the file's existence before performing an asynchronous deletion using
                  an executor.
            """

            async with semaphore:
                if self.s3 is not None:
                    file_path = self.path_builder.build_save_path(self.S3_PREFIX, symbol, f"{file_date}.parquet")
                    try:
                        await self.s3_async.delete_object(Bucket=self.S3_BUCKET, Key=file_path)
                        logger.info(f"Deleted {file_path}")
                    except Exception as e:
                        logger.error(f"Error deleting {file_path}: {e}")
                else:
                    local_path = Path(self.path_builder.build_save_path(self.LOCAL_PREFIX, symbol, f"{file_date}.parquet"))
                    if local_path.exists():
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, local_path.unlink)
                        logger.info(f"Deleted {local_path}")
        
        tasks = [delete_single_file(date) for date in files_to_delete]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def sync(self, symbols: list[str] = None):
        """
        Asynchronously synchronizes symbol data.
        Parameters:
            symbols (list[str], optional): A list of symbols to be synchronized. If not provided, the method retrieves the symbols using the `list_remote_symbols` method.
        Behavior:
            - Logs the number of symbols along with market type, data type, and interval information.
            - If progress tracking is enabled (via the `progress` attribute), displays a progress bar that updates as each batch of symbols is processed.
            - Processes symbols in batches determined by the product of a concurrency constant (`SYMBOL_CONCURRENCY * 2`).
            - Uses `asyncio.gather` to concurrently execute the `sync_symbol` method for each symbol in the current batch.
            - If progress tracking is disabled, synchronizes symbols in batches and logs a completion message once done.
        Returns:
            Coroutine: This asynchronous method completes once all symbols have been synchronized.
        """

        if symbols is None:
            symbols = await self.list_remote_symbols()

        if isinstance(symbols, str):
            symbols = [symbols]

        logger.info(f"Syncing {len(symbols)} symbols for {self.market_type.value}{self.data_type.value} with interval {self.interval.value if self.interval else 'N/A'}")

        if self.progress:
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold green]Completed {task.completed}/{task.total} symbols[/bold green]"),
                BarColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=LoggingConfigurator.get_console(),
                transient=False
            ) as progress:
                
                task = progress.add_task("Syncing symbols...", total=len(symbols))
                for i in range(0, len(symbols), self.SYMBOL_CONCURRENCY * 2):
                    batch = symbols[i:i + self.SYMBOL_CONCURRENCY * 2]
                    await asyncio.gather(*(self.sync_symbol(s) for s in batch))
                    progress.update(task, advance=len(batch))
        else:
            for i in range(0, len(symbols), self.SYMBOL_CONCURRENCY * 2):
                batch = symbols[i:i + self.SYMBOL_CONCURRENCY * 2]
                await asyncio.gather(*(self.sync_symbol(s) for s in batch))

            logger.info("Sync complete")

