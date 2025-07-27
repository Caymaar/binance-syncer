import aiohttp
import asyncio
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from pathlib import Path
import xml.etree.ElementTree as ET
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from binance_syncer.utils.logger import console
from binance_syncer.utils.enums import MarketType, DataType, Frequency, KlineInterval, Headers
from binance_syncer.utils.utils import BinancePathBuilder, safe_parse_time, get_config

import logging

logger = logging.getLogger(__name__)



MAX_CONCURRENT_TASKS = 100        # download/convert concurrency per symbol
SYMBOL_CONCURRENCY = 10           # number of symbols processed in parallel

config = get_config()

class BinanceDataSync:

    LOCAL_PREFIX = config.get('DEFAULT', 'LOCAL_PREFIX')
    REMOTE_PREFIX = config.get('DEFAULT', 'REMOTE_PREFIX')
    S3_BUCKET = config.get('DEFAULT', 'S3_BUCKET')

    def __init__(self, storage_mode: str, market_type: MarketType, data_type: DataType, 
                    interval: KlineInterval = None, progress: bool = False):
            self.storage_mode = storage_mode
            self.market_type = market_type
            self.data_type = data_type
            self.interval = interval
            self.progress = progress
            self.path_builder = BinancePathBuilder(market_type, data_type, interval)
            
            # Configuration SSL robuste
            self._ssl_context = self._create_ssl_context()
            
            if storage_mode == 's3':
                import boto3
                self.s3 = boto3.client('s3')

    def _create_ssl_context(self):
        """Créer un contexte SSL robuste qui fonctionne en mode package."""
        import ssl
        import certifi
        
        try:
            # Essayer d'abord avec certifi
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            logger.debug(f"Using certifi SSL context: {certifi.where()}")
            return ssl_context
        except Exception as e:
            logger.warning(f"Certifi SSL failed: {e}, trying system certificates")
            
            try:
                # Fallback sur les certificats système
                ssl_context = ssl.create_default_context()
                return ssl_context
            except Exception as e2:
                logger.warning(f"System SSL failed: {e2}, using permissive SSL")
                
                # Dernier recours : SSL permissif (temporaire pour diagnostic)
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                return ssl_context
            
    async def list_remote_symbols(self) -> list[str]:
            """List all available symbols for the given market and data type."""
            symbols, marker = [], None
            ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            
            # Utiliser le contexte SSL prédéfini
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

    # async def list_remote_files(self, frequency: Frequency, symbol: str) -> list[str]:
    #     files, marker = [], None
    #     ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    #     timeout = aiohttp.ClientTimeout(total=60)
    #     async with aiohttp.ClientSession(timeout=timeout) as session:
    #         while True:
    #             url = self.path_builder.build_listing_files_path(frequency, symbol)
    #             if marker:
    #                 url += f"&marker={marker}"
    #             resp = await session.get(url)
    #             if resp.status != 200:
    #                 logger.warning(f"Listing dates failed for {symbol} status {resp.status}")
    #                 break
    #             xml = await resp.text()
    #             root = ET.fromstring(xml)
    #             contents = root.findall("s3:Contents", ns)
    #             if not contents:
    #                 break
    #             for c in contents:
    #                 key = c.find("s3:Key", ns).text
    #                 if key.endswith(".zip") and not key.endswith(".zip.CHECKSUM"):
    #                     files.append(key)
    #             if root.find("s3:IsTruncated", ns) is not None and root.find("s3:IsTruncated", ns).text.lower() == "true":
    #                 marker = contents[-1].find("s3:Key", ns).text
    #             else:
    #                 break
    #     return files

    def list_local_dates(self, symbol: str) -> set[str]:
        if self.storage_mode == 's3':
            prefix = self.path_builder.build_save_path(self.REMOTE_PREFIX, symbol)
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
        """List all remote files for a given frequency and symbol."""
        files, marker = [], None
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        
        # CORRECTION : Utiliser le même contexte SSL
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
        """Sync a single symbol."""
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

        # Supprimer les fichiers d'abord
        if dates_dict['D_RM']:
            if self.storage_mode == 's3':
                await self.batch_delete_s3(list(dates_dict['D_RM']), symbol)
            else:
                delete_semaphore = asyncio.Semaphore(20)
                await self.delete_files_async(list(dates_dict['D_RM']), symbol, delete_semaphore)
        
        # Télécharger les nouveaux fichiers
        if to_fetch:
            download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS // 2)
            
            # CORRECTION : Utiliser le même contexte SSL
            connector = aiohttp.TCPConnector(
                ssl=self._ssl_context,  # CHANGEMENT ICI
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
                # Traitement par batches avec création lazy des tâches
                batch_size = 20
                
                for i in range(0, len(to_fetch), batch_size):
                    batch_urls = to_fetch[i:i + batch_size]
                    
                    # Créer les tâches seulement pour ce batch
                    batch_tasks = [
                        self.download_and_store(sess, symbol, url, download_semaphore) 
                        for url in batch_urls
                    ]
                    
                    # Exécuter ce batch
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    # Compter les résultats
                    success_count = sum(1 for r in results if r is True)
                    failed_count = len(batch_tasks) - success_count
                    
                    logger.info(f"{symbol}: Batch {i//batch_size + 1}/{(len(to_fetch) + batch_size - 1)//batch_size} - "
                               f"{success_count}/{len(batch_tasks)} success")
                    if failed_count > 0:
                        logger.warning(f"{symbol}: {failed_count} downloads failed in this batch")

    # async def download_and_store(self, session: aiohttp.ClientSession, symbol: str,
    #                             file_key: str, semaphore: asyncio.Semaphore, max_retries: int = 3) -> bool:
    #     async with semaphore:
    #         filename = f"{'-'.join(file_key.split('/')[-1].replace('.zip', '').split('-')[2:])}.parquet"

    #         if self.storage_mode == 's3':
    #             file_path = self.path_builder.build_save_path(self.REMOTE_PREFIX, symbol, filename)
    #             try:
    #                 self.s3.head_object(Bucket=self.S3_BUCKET, Key=file_path)
    #                 logger.debug(f"File already exists: {file_path}")
    #                 return True
    #             except self.s3.exceptions.ClientError as e:
    #                 if e.response["Error"]["Code"] != "404":
    #                     logger.error(f"Head object error {symbol} {file_path}: {e}")
    #                     return False

    #         for attempt in range(max_retries + 1):
    #             try:
    #                 timeout = aiohttp.ClientTimeout(
    #                     total=120,      # Timeout total plus long
    #                     connect=30,     # Timeout de connexion
    #                     sock_read=60    # Timeout de lecture
    #                 )
                    
    #                 async with session.get(file_key, timeout=timeout) as resp:
    #                     resp.raise_for_status()
                        
    #                     # Lecture progressive pour éviter les timeouts
    #                     data = BytesIO()
    #                     async for chunk in resp.content.iter_chunked(8192):  # 8KB chunks
    #                         data.write(chunk)
                        
    #                     data.seek(0)
                        
    #                     with ZipFile(data) as zf:
    #                         with zf.open(zf.namelist()[0]) as f:
    #                             if self.data_type.name in Headers.__members__:
    #                                 df = pd.read_csv(f, header=None)
    #                                 df.columns = Headers[self.data_type.name].value
    #                                 for col in ['open_time', 'close_time']:
    #                                     if col in df.columns:
    #                                         df[col] = safe_parse_time(df[col])
    #                             else:
    #                                 df = pd.read_csv(f)
                                
    #                             if self.storage_mode == 's3':
    #                                 out = BytesIO()
    #                                 df.to_parquet(out, compression="snappy", index=False)
    #                                 out.seek(0)
    #                                 self.s3.upload_fileobj(out, self.S3_BUCKET, file_path)
    #                                 logger.info(f"Uploaded to S3: {file_path}")
    #                             else:
    #                                 local_path = Path(self.path_builder.build_save_path(self.LOCAL_PREFIX, symbol, filename))
    #                                 local_path.parent.mkdir(parents=True, exist_ok=True)
    #                                 df.to_parquet(local_path, compression="snappy", index=False)
    #                                 logger.info(f"Saved locally: {local_path}")
                                
    #                             return True
                                
    #             except asyncio.CancelledError:
    #                 logger.warning(f"Download cancelled for {symbol} {file_key} (attempt {attempt + 1}/{max_retries + 1})")
    #                 if attempt == max_retries:
    #                     logger.error(f"Max retries reached for {symbol} {file_key}")
    #                     return False
    #                 await asyncio.sleep(2 ** attempt)
                    
    #             except asyncio.TimeoutError:
    #                 logger.warning(f"Timeout for {symbol} {file_key} (attempt {attempt + 1}/{max_retries + 1})")
    #                 if attempt == max_retries:
    #                     logger.error(f"Max retries reached for {symbol} {file_key}")
    #                     return False
    #                 await asyncio.sleep(2 ** attempt)
                    
    #             except aiohttp.ClientError as e:
    #                 logger.warning(f"Client error for {symbol} {file_key}: {e} (attempt {attempt + 1}/{max_retries + 1})")
    #                 if attempt == max_retries:
    #                     logger.error(f"Max retries reached for {symbol} {file_key}")
    #                     return False
    #                 await asyncio.sleep(2 ** attempt)
                    
    #             except Exception as e:
    #                 logger.exception(f"Unexpected error processing {symbol} {file_key}: {e}")
    #                 return False
            
    #         return False
    
    # async def download_and_store(self, session: aiohttp.ClientSession, symbol: str,
    #                             file_key: str, semaphore: asyncio.Semaphore) -> bool:
    #     async with semaphore:
    #         filename = f"{'-'.join(file_key.split('/')[-1].replace('.zip', '').split('-')[2:])}.parquet"

    #         if self.storage_mode == 's3':
    #             file_path = self.path_builder.build_save_path(self.LOCAL_PREFIX, symbol, filename)
    #             try:
    #                 self.s3.head_object(Bucket=self.s3_bucket, Key=file_path)
    #                 return True
    #             except self.s3.exceptions.ClientError as e:
    #                 if e.response["Error"]["Code"] != "404":
    #                     logger.error(f"Head object error {symbol} {file_path}: {e}")
    #                     return False
    #         try:
    #             resp = await session.get(file_key)
    #             resp.raise_for_status()
    #             buf = BytesIO(await resp.read())
    #             with ZipFile(buf) as zf:
    #                 with zf.open(zf.namelist()[0]) as f:
    #                     if self.data_type.name in Headers.__members__:
    #                         df = pd.read_csv(f, header=None)
    #                         df.columns=Headers[self.data_type.name].value
    #                         for col in ['open_time', 'close_time']:
    #                             if col in df.columns:
    #                                 df[col] = safe_parse_time(df[col])
    #                     else:
    #                         df = pd.read_csv(f)
    #                     if self.storage_mode == 's3':
    #                         out = BytesIO()
    #                         df.to_parquet(out, compression="snappy", index=False)
    #                         out.seek(0)
    #                         self.s3.upload_fileobj(out, self.S3_BUCKET, file_path)
    #                         logger.info(f"Uploaded {file_path}")
    #                     else:
    #                         local_path = Path(self.path_builder.build_save_path(self.LOCAL_PREFIX, symbol, filename))
    #                         local_path.parent.mkdir(parents=True, exist_ok=True)
    #                         df.to_parquet(local_path, compression="snappy", index=False)
    #                         logger.info(f"Uploaded {local_path}")
    #             return True
    #         except asyncio.CancelledError:
    #             logger.warning(f"Timeout/cancelled {symbol} {file_key}")
    #             return False
    #         except Exception:
    #             logger.exception(f"Error processing {symbol} {file_key}")
    #             return False

    async def download_and_store(self, session: aiohttp.ClientSession, symbol: str,
                                file_key: str, semaphore: asyncio.Semaphore, max_retries: int = 3) -> bool:
        async with semaphore:
            filename = f"{'-'.join(file_key.split('/')[-1].replace('.zip', '').split('-')[2:])}.parquet"

            if self.storage_mode == 's3':
                file_path = self.path_builder.build_save_path(self.REMOTE_PREFIX, symbol, filename)
                try:
                    self.s3.head_object(Bucket=self.S3_BUCKET, Key=file_path)
                    logger.debug(f"File already exists: {file_path}")
                    return True
                except self.s3.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] != "404":
                        logger.error(f"Head object error {symbol} {file_path}: {e}")
                        return False

            # Retry logic pour les téléchargements
            for attempt in range(max_retries + 1):
                try:
                    # Configuration de timeout - utiliser la session passée en paramètre
                    # (qui utilise déjà le bon contexte SSL)
                    async with session.get(file_key) as resp:
                        resp.raise_for_status()
                        
                        # Lecture progressive pour éviter les timeouts
                        data = BytesIO()
                        async for chunk in resp.content.iter_chunked(8192):  # 8KB chunks
                            data.write(chunk)
                        
                        data.seek(0)
                        
                        with ZipFile(data) as zf:
                            with zf.open(zf.namelist()[0]) as f:
                                # Traitement des données selon le type
                                if self.data_type.name in Headers.__members__:
                                    df = pd.read_csv(f, header=None)
                                    df.columns = Headers[self.data_type.name].value
                                    
                                    # Traitement des colonnes de temps pour KLINES uniquement
                                    if self.data_type == DataType.KLINES:
                                        for col in ['open_time', 'close_time']:
                                            if col in df.columns:
                                                df[col] = safe_parse_time(df[col])
                                else:
                                    df = pd.read_csv(f)
                                
                                # Sauvegarde selon le mode de stockage
                                if self.storage_mode == 's3':
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
                    # Attendre avant de retry
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
            
            return False


    async def batch_delete_s3(self, files_to_delete: list[str], symbol: str):
        if not files_to_delete:
            return
            
        # Préparer les objets à supprimer
        delete_objects = []
        for date in files_to_delete:
            file_path = self.path_builder.build_save_path(self.REMOTE_PREFIX, symbol, f"{date}.parquet")
            delete_objects.append({'Key': file_path})
        
        batch_size = 1000
        for i in range(0, len(delete_objects), batch_size):
            batch = delete_objects[i:i + batch_size]
            try:
                response = self.s3.delete_objects(
                    Bucket=self.S3_BUCKET,
                    Delete={'Objects': batch, 'Quiet': False}
                )
                deleted_count = len(response.get('Deleted', []))
                logger.info(f"Batch deleted {deleted_count} files for {symbol}")
                
                # Log des erreurs
                for error in response.get('Errors', []):
                    logger.error(f"Error deleting {error['Key']}: {error['Message']}")
                    
            except Exception as e:
                logger.error(f"Batch delete failed for {symbol}: {e}")
            
    async def delete_files_async(self, files_to_delete: list[str], symbol: str, semaphore: asyncio.Semaphore):
        """Supprime les fichiers de manière asynchrone."""
        async def delete_single_file(file_date: str):
            async with semaphore:
                if self.storage_mode == 's3':
                    file_path = self.path_builder.build_save_path(self.REMOTE_PREFIX, symbol, f"{file_date}.parquet")
                    try:
                        # Utiliser aioboto3 pour les opérations async S3
                        await self.s3_async.delete_object(Bucket=self.S3_BUCKET, Key=file_path)
                        logger.info(f"Deleted {file_path}")
                    except Exception as e:
                        logger.error(f"Error deleting {file_path}: {e}")
                else:
                    local_path = Path(self.path_builder.build_save_path(self.LOCAL_PREFIX, symbol, f"{file_date}.parquet"))
                    if local_path.exists():
                        # Suppression locale dans un thread pool
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, local_path.unlink)
                        logger.info(f"Deleted {local_path}")
        
        # Créer les tâches pour toutes les suppressions
        tasks = [delete_single_file(date) for date in files_to_delete]
        await asyncio.gather(*tasks, return_exceptions=True)

    # async def sync_symbol(self, symbol: str):
    #     logger.info(f"=== Syncing {symbol} ===")
    #     dates_dict = await self.compute_dates_cover(symbol)
        
    #     if not any(dates_dict.values()):
    #         logger.info(f"{symbol} is up-to-date")
    #         return
            
    #     logger.info(f"{symbol}: {len(dates_dict['M_DL'])} months to download, "
    #                 f"{len(dates_dict['D_DL'])} days to download, "
    #                 f"{len(dates_dict['D_RM'])} days to remove")
        
    #     to_fetch = []
    #     for date in dates_dict['M_DL']:
    #         date_parts = date.split('-')
    #         to_fetch.append(self.path_builder.build_download_path(
    #             Frequency.MONTHLY, symbol, *date_parts))
        
    #     for date in dates_dict['D_DL']:
    #         date_parts = date.split('-')
    #         to_fetch.append(self.path_builder.build_download_path(
    #             Frequency.DAILY, symbol, *date_parts))

    #     # Supprimer les fichiers d'abord
    #     if dates_dict['D_RM']:
    #         if self.storage_mode == 's3':
    #             await self.batch_delete_s3(list(dates_dict['D_RM']), symbol)
    #         else:
    #             delete_semaphore = asyncio.Semaphore(20)
    #             await self.delete_files_async(list(dates_dict['D_RM']), symbol, delete_semaphore)
        
    #     # Télécharger les nouveaux fichiers
    #     if to_fetch:
    #         download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS // 2)  # Réduire la concurrence
            
    #         # Configuration SSL et timeout améliorée
    #         ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    #         connector = aiohttp.TCPConnector(
    #             ssl=ssl_ctx,
    #             limit=100,           # Limite de connexions
    #             limit_per_host=20,   # Limite par host
    #             keepalive_timeout=30,
    #             enable_cleanup_closed=True
    #         )
            
    #         timeout = aiohttp.ClientTimeout(
    #             total=300,      # 5 minutes total
    #             connect=30,     # 30s pour la connexion
    #             sock_read=120   # 2 minutes pour la lecture
    #         )
            
    #         async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sess:
    #             download_tasks = [
    #                 self.download_and_store(sess, symbol, k, download_semaphore) 
    #                 for k in to_fetch
    #             ]
                
    #             # Traiter par petits batches pour éviter la surcharge
    #             batch_size = 20
    #             for i in range(0, len(download_tasks), batch_size):
    #                 batch = download_tasks[i:i + batch_size]
    #                 results = await asyncio.gather(*batch, return_exceptions=True)
                    
    #                 # Logger les résultats
    #                 success_count = sum(1 for r in results if r is True)
    #                 failed_count = len(batch) - success_count
    #                 logger.info(f"{symbol}: Batch {i//batch_size + 1} - {success_count} success, {failed_count} failed")

    # async def sync_symbol(self, symbol: str):
    #     logger.info(f"=== Syncing {symbol} ===")
    #     dates_dict = await self.compute_dates_cover(symbol)
        
    #     if not any(dates_dict.values()):
    #         logger.info(f"{symbol} is up-to-date")
    #         return
            
    #     logger.info(f"{symbol}: {len(dates_dict['M_DL'])} months to download, "
    #                 f"{len(dates_dict['D_DL'])} days to download, "
    #                 f"{len(dates_dict['D_RM'])} days to remove")
        
    #     to_fetch = []
    #     for date in dates_dict['M_DL']:
    #         to_fetch.append(self.path_builder.build_download_path(
    #             Frequency.MONTHLY, symbol, *date.split('-')))
    #     for date in dates_dict['D_DL']:
    #         to_fetch.append(self.path_builder.build_download_path(
    #             Frequency.DAILY, symbol, *date.split('-')))

    #     delete_semaphore = asyncio.Semaphore(20) 
    #     download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        
    #     ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    #     timeout = aiohttp.ClientTimeout(total=60)
        
    #     async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_ctx), timeout=timeout) as sess:
    #         download_tasks = [self.download_and_store(sess, symbol, k, download_semaphore) for k in to_fetch]
    #         delete_task = self.delete_files_async(list(dates_dict['D_RM']), symbol, delete_semaphore)
            
    #         await asyncio.gather(
    #             delete_task,
    #             *download_tasks,
    #             return_exceptions=True
    #         )

    async def sync(self, symbols: list[str] = None):
        if symbols is None:
            symbols = await self.list_remote_symbols()
        logger.info(f"Syncing {len(symbols)} symbols for {self.market_type.value}{self.data_type.value} with interval {self.interval.value if self.interval else 'N/A'}")

        if self.progress:
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold green]Completed {task.completed}/{task.total} symbols[/bold green]"),
                BarColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=console,
                transient=False
            ) as progress:
                
                task = progress.add_task("Syncing symbols...", total=len(symbols))
                for i in range(0, len(symbols), SYMBOL_CONCURRENCY * 2):
                    batch = symbols[i:i + SYMBOL_CONCURRENCY * 2]
                    await asyncio.gather(*(self.sync_symbol(s) for s in batch))
                    progress.update(task, advance=len(batch))
        else:
            for i in range(0, len(symbols), SYMBOL_CONCURRENCY * 2):
                batch = symbols[i:i + SYMBOL_CONCURRENCY * 2]
                await asyncio.gather(*(self.sync_symbol(s) for s in batch))

            logger.info("Sync complete")

