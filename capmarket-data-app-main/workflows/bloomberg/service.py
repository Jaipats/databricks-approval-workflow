import copy
import gzip
import io
import json
import logging
import os
import secrets
import shutil
import tarfile
import tempfile
from datetime import datetime
from functools import partial
from itertools import dropwhile, takewhile
from multiprocessing import Pool, cpu_count
from typing import Dict, Optional
from typing import List
from urllib.parse import urljoin

from oauthlib.oauth2 import BackendApplicationClient
from pydantic import BaseModel
from sseclient import SSEClient

from bloomberg.session import DLRestApiSession

LOG = logging.getLogger(__name__)

HOST = 'https://api.bloomberg.com'
REQ_BWSLIB_CAT = "/eap/catalogs/"
REQ_BWSLIB_DAT = "/eap/catalogs/{}/requests/"
REQ_BWSLIB_RES = "/eap/catalogs/{}/content/responses/"
REQ_BWSLIB_SSE = "/eap/notifications/sse"
REQ_BWSLIB_FIL = "/eap/catalogs/{}/content/responses/{}"
RES_CONTENT_TYPE_CSV = "text/csv"
RES_CONTENT_TYPE_BBG = "text/vnd.blp.dl.std"
RES_CONTENT_TYPE_JSON = "application/json"
RES_CONTENT_TYPE_TAR = "application/x-tar"

REQ_MAX_AWAIT_MINUTES = 45
REQ_MAX_SIZE_PER_CORE = 64 * 1024 * 1024


class PartFileChunk(BaseModel):
    id: int
    begin: int
    end: int


class DataLicenseFile(BaseModel):
    identifier: str
    download_url: str
    output_dir: str
    content_type: str
    digest: str
    digest_algorithm: str


def generate_unique_identifier() -> str:
    now = datetime.now()
    formatted_date_time = now.strftime("%Y%m%d%H%M%S")
    random_text = secrets.token_hex(3)
    # Return a 21 character identifier
    return f'r{formatted_date_time}{random_text}'


def _download_part_file(byte_chunk: PartFileChunk, credentials: Dict[str, str], file_url: str, tmp_dir: str) -> str:
    headers = {'Range': f"bytes={byte_chunk.begin}-{byte_chunk.end}", "Accept-encoding": "gzip"}
    oauth2 = BackendApplicationClient(client_id=credentials['client_id'])
    session = DLRestApiSession(credentials, client=oauth2)
    tmp_file = os.path.join(tmp_dir, f'{byte_chunk.id}')
    with session.get(file_url, headers=headers, stream=True) as response:
        with open(tmp_file, 'wb') as f:
            shutil.copyfileobj(response.raw, f)
    session.close()
    return tmp_file


def _download_part_files(
        byte_chunks: List[PartFileChunk], credentials: Dict[str, str], bloomberg_file: DataLicenseFile):
    with tempfile.TemporaryDirectory() as tmp_dir:
        pool = Pool(cpu_count())
        download_func = partial(
            _download_part_file, file_url=bloomberg_file.download_url, credentials=credentials, tmp_dir=tmp_dir)
        tmp_files = pool.map(download_func, byte_chunks)
        tmp_files = sorted(tmp_files, key=lambda x: int(x.split('/')[-1]))
        output_stream = io.BytesIO()
        for tmp_file in tmp_files:
            with open(tmp_file, 'rb') as f:
                output_stream.write(f.read())
        pool.close()
        pool.join()
        output_stream.seek(0)
        return output_stream


def _split_size(
        expected_size: int, max_size_chunk: int, processed_size: int = 0, processed_chunks=None):
    if processed_chunks is None:
        processed_chunks = []
    chunks = copy.deepcopy(processed_chunks)
    assert max_size_chunk > 1, 'Maximum size of chunked response must be positive integer greater than 1'
    current_chunk_size = min(processed_size + max_size_chunk, expected_size)
    if processed_size == 0:
        begin = 0
    else:
        begin = processed_size + 1
    current_chunk = PartFileChunk(id=len(chunks) + 1, begin=begin, end=current_chunk_size - 1)
    chunks.append(current_chunk)
    if current_chunk.end == expected_size - 1:
        assert chunks[-1].end == expected_size - 1, 'All chunks must covered total file size'
        return chunks
    else:
        return _split_size(expected_size, max_size_chunk, current_chunk.end, chunks)


def _process_json_format(input_stream: io.BufferedIOBase, output_dir: str, identifier: str):
    LOG.info('Processing input stream for JSON format')
    json_string = io.BytesIO(input_stream.read())
    json_string = json_string.read().decode('utf-8')
    json_array = json.loads(json_string)
    with open(os.path.join(output_dir, 'results.json'), 'w') as f:
        for i, json_object in enumerate(json_array):
            if i > 0:
                f.write('\n')
            f.write(json.dumps(json_object))


def _process_csv_format(input_stream: io.BufferedIOBase, output_dir: str, identifier: str):
    LOG.info('Processing input stream for CSV format')
    with open(os.path.join(output_dir, 'results.csv'), 'wb') as f:
        f.write(input_stream.read())


def _process_tar_format(input_stream: io.BufferedIOBase, output_dir: str, identifier: str):
    LOG.info('Unpacking archive for input stream')
    with tarfile.open(fileobj=io.BytesIO(input_stream.read()), mode='r') as tar:
        for member in tar.getmembers():
            if member.isfile():
                LOG.info('Processing input stream for parquet format')
                f = tar.extractfile(member)
                if f:
                    with open(os.path.join(output_dir, member.name), 'wb') as f_out:
                        f_out.write(f.read())


def _process_bbg_format(input_stream: io.BufferedIOBase, output_dir: str, identifier: str):
    LOG.info('Processing input stream for BBG format')
    raw_text = io.BytesIO(input_stream.read())
    raw_text = raw_text.read().decode('utf-8').split('\n')
    result = list(dropwhile(lambda x: x != 'START-OF-DATA', raw_text))[1:]
    result = list(takewhile(lambda x: x != 'END-OF-DATA', result))
    result = list(filter(lambda x: not x.startswith('#'), result))
    with open(os.path.join(output_dir, 'results.txt'), 'w') as f:
        f.write('\n'.join(result))


class DataLicenseSvc:

    def __init__(self, credentials: Dict[str, str]):
        oauth2 = BackendApplicationClient(client_id=credentials['client_id'])
        self.session = DLRestApiSession(credentials, client=oauth2)
        self.credentials = credentials

    def download_file(
            self, bloomberg_file: DataLicenseFile, output_dir: str, max_size_per_core: int = REQ_MAX_SIZE_PER_CORE) -> (
            DataLicenseFile):

        # Get file statistics
        LOG.info(f'Retrieving file statistics for request [{bloomberg_file.identifier}]')
        with self.session.head(bloomberg_file.download_url) as response:
            content_length = int(response.headers['content-length'])
            is_compressed = response.headers.get('content-encoding') == 'gzip'

        # We might split request into multiple partial downloads
        chunks = _split_size(content_length, max_size_per_core)
        download_response = None
        try:
            if len(chunks) == 1:
                # Download individual file
                LOG.info(f'Downloading individual file for request [{bloomberg_file.identifier}]')
                headers = {"Accept-encoding": "gzip"}
                download_response = self.session.get(bloomberg_file.download_url, stream=True, headers=headers)
                if is_compressed:
                    input_stream = gzip.GzipFile(fileobj=download_response.raw)
                else:
                    input_stream = io.BufferedReader(download_response.raw)
            else:
                # Download multiple part files and recompose into 1
                LOG.info(
                    f'Downloading file for request [{bloomberg_file.identifier}] into {len(chunks)} partial requests')
                input_stream = _download_part_files(chunks, self.credentials, bloomberg_file)
                if is_compressed:
                    input_stream = gzip.GzipFile(fileobj=input_stream)

            content_type = bloomberg_file.content_type.split(';')[0]
            output_dir = os.path.join(output_dir, bloomberg_file.identifier)
            os.mkdir(output_dir)
            bloomberg_file.output_dir = output_dir
            if content_type == RES_CONTENT_TYPE_CSV:
                _process_csv_format(input_stream, output_dir, bloomberg_file.identifier)
            elif content_type == RES_CONTENT_TYPE_JSON:
                _process_json_format(input_stream, output_dir, bloomberg_file.identifier)
            elif content_type == RES_CONTENT_TYPE_BBG:
                _process_bbg_format(input_stream, output_dir, bloomberg_file.identifier)
            elif content_type == RES_CONTENT_TYPE_TAR:
                _process_tar_format(input_stream, output_dir, bloomberg_file.identifier)
            else:
                raise Exception(f'Unsupported format {bloomberg_file.content_type}')
        except Exception as e:
            raise e
        finally:
            if download_response:
                download_response.close()
            return bloomberg_file

    def get_available_catalogs(self) -> Dict[str, str]:
        LOG.info("Retrieving access catalogs")
        url = urljoin(HOST, REQ_BWSLIB_CAT)
        with self.session.get(url) as response:
            response.raise_for_status()
            catalogs = {}
            response_content = response.json()
            if 'contains' in response_content:
                for catalog in response_content['contains']:
                    catalogs[catalog['identifier']] = catalog['description']
            else:
                raise Exception('Could not parse response content')
            return catalogs

    def submit_data_request(self, catalog: str, json_request: dict) -> str:
        url = urljoin(HOST, REQ_BWSLIB_DAT.format(catalog))
        assert 'identifier' in json_request, "Request identifier must be specified"
        LOG.info(f"Submitting data request for identifier [{json_request['identifier']}]")

        assert '@type' in json_request, "Request type not specified"
        assert len(json_request['identifier']) <= 21, "Identifier must be between 1 and 21 characters long"
        with self.session.post(url, json=json_request) as response:
            response.raise_for_status()
            return json_request['identifier']

    def await_response(self, identifier: str) -> DataLicenseFile:
        LOG.info(f"Subscribing to SSE event for identifier [{identifier}]")
        url = urljoin(HOST, REQ_BWSLIB_SSE)
        headers = {'Accept': 'text/event-stream', 'Connection': 'keep-alive', 'Cache-Control': 'no-cache'}
        with self.session.get(url, stream=True, headers=headers) as stream_response:
            client = None
            try:
                client = SSEClient(stream_response)
                start_time = datetime.now()
                for event in client.events():
                    minutes_diff = (datetime.now() - start_time).total_seconds() / 60.0
                    if minutes_diff > REQ_MAX_AWAIT_MINUTES:
                        raise Exception(
                            f'Could not find completed request for [{identifier}] in [{REQ_MAX_AWAIT_MINUTES}] minutes')
                    event_data = json.loads(event.data)
                    event_data = event_data['generated']
                    if event_data['identifier'].split('.')[0] == identifier:
                        LOG.info(f'Received pushed notification for identifier [{identifier}]')
                        return DataLicenseFile(
                            identifier=identifier,
                            download_url=event_data['@id'],
                            content_type=event_data['contentType'],
                            output_dir='',
                            digest=event_data['digest']['digestValue'],
                            digest_algorithm=event_data['digest']['digestAlgorithm']
                        )
            finally:
                if client:
                    client.close()

    def get_status(self, catalog: str, identifier: str) -> Optional[DataLicenseFile]:
        url = urljoin(HOST, REQ_BWSLIB_RES.format(catalog))
        params = {'requestIdentifier': identifier}
        with self.session.get(url, params=params) as response:
            response.raise_for_status()
            response_content = response.json()
            for record in response_content['contains']:
                if record['metadata']['DL_REQUEST_ID'] == identifier:
                    LOG.info(f'Get status completed for identifier [{identifier}]')
                    digest = record['headers']['Digest'].split('=')
                    download_url = urljoin(HOST, REQ_BWSLIB_FIL.format(catalog, record['key']))
                    return DataLicenseFile(
                        identifier=identifier,
                        download_url=download_url,
                        content_type=record['headers']['Content-Type'],
                        output_dir='',
                        digest=digest[1],
                        digest_algorithm=digest[0].upper()
                    )
            return None
