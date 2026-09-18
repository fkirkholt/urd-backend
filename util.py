import time
import io
import inspect
import os
import re
from functools import wraps
from settings import Settings
from addict import Dict
import platform
import requests
import xml.etree.ElementTree as ET
import codecs
from charset_normalizer import from_bytes


cfg = Settings()
indent = 2


def time_func(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        if (end - start) > 0.1:
            print(f"{func.__module__}.{func.__name__} took {end - start:.6f} seconds")
        return result
    return wrapper


def time_stream_generator(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        async for item in func(*args, **kwargs):
            yield item
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.4f} seconds")
    return wrapper


def to_rec(row, crsr, lowercase=False):
    # Fixes additional characters at end of column names
    # This happens with special unicode characters in column name
    cols = [col[0] if '\x00' not in col[0]
            else col[0][:col[0].index('\x00')]
            for col in crsr.description]
    if lowercase:
        cols = [col.lower() for col in cols]
    return Dict(zip(cols, row))


def format_fkey(fkey, pkey):
    fkey = Dict(fkey)
    if (
        pkey.columns and
        set(pkey.columns) <= set(fkey.constrained_columns)
    ):
        fkey.relationship = '1:1'
    else:
        fkey.relationship = '1:M'

    ref_table_alias = fkey.name

    return fkey, ref_table_alias


def log_caller(func):
    """A decorator to log the name of the caller function."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        global indent
        # Get the call stack
        stack = inspect.stack()
        # The frame record at index 1 is the caller of the current wrapper function
        caller_frame_record = stack[1]
        # The fourth element of the frame record (index 3) is the function name
        caller_name = caller_frame_record[3]
        caller_file = caller_frame_record[1].split('/')[-1]
        caller_lnr = caller_frame_record[2]
        caller_ref = caller_file + ':' + str(caller_lnr)

        print(f"{'>' * indent} Function '{func.__name__}' was called by "
              f"'{caller_name}' in '{caller_ref}' ---")
        start = time.perf_counter()
        indent += 2
        result = func(*args, **kwargs)
        indent -= 2
        end = time.perf_counter()
        print(f"{'<' * indent} Function '{func.__name__}' finished executing "
              f"in {end - start:.6f} seconds ---")
        return result
    return wrapper


def get_installed_vector_path():
    system = platform.system().lower()
    ext = {"windows": ".dll", "darwin": ".dylib"}.get(system, ".so")
    ext_dir = os.path.expanduser(cfg.sqlite_ext_dir)
    ext_path = os.path.join(ext_dir, f"vector{ext}")
    if os.path.exists(ext_path):
        return os.path.join(ext_dir, 'vector')
    else:
        return None


def get_installed_ai_path():
    system = platform.system().lower()
    ext = {"windows": ".dll", "darwin": ".dylib"}.get(system, ".so")
    ext_dir = os.path.expanduser(cfg.sqlite_ext_dir)
    ext_path = os.path.join(ext_dir, f"ai{ext}")
    if os.path.exists(ext_path):
        return os.path.join(ext_dir, 'ai')
    else:
        return None


def chunk_text_with_positions(text: str) -> list[dict]:
    """
    Splits text by double newlines (paragraphs) and returns a list of
    objects containing start_pos, end_pos, and the chunked text.
    """
    if not text:
        return []

    chunks = []
    current_pos = 0

    # Split by double newlines, keeping empty elements temporarily
    # to calculate accurate positions in the original text
    raw_segments = text.split('\n\n')

    for i, segment in enumerate(raw_segments):
        if segment.strip():  # Skip empty segments (e.g., multiple consecutive newlines)
            # Find exactly where this segment starts in the original text
            start_pos = text.find(segment, current_pos)
            end_pos = start_pos + len(segment)

            chunks.append(Dict({
                "start_pos": start_pos,
                "end_pos": end_pos,
                "text": segment
            }))

            # Advance the search window past the current segment
            current_pos = end_pos
        else:
            # If the segment was empty noise, advance past its length
            current_pos += len(segment)

        # Account for the length of the delimiter '\n\n' (if not the last segment)
        if i < len(raw_segments) - 1:
            current_pos += 2

    return chunks


def get_xml_encoding(raw_bytes):
    head = raw_bytes[:100].decode("ascii", errors="ignore")
    # Look for `encoding="..."`
    match = re.search(r'encoding=["\']([^"\']+)["\']', head, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def is_siegfried_running(host="localhost", port=5138):
    url = f"http://{host}:{port}/identify"
    try:
        requests.get(url, timeout=0.5, proxies={"http": None, "https": None})
        return True
    except requests.exceptions.RequestException:
        return False


def get_sf_mime_type(blob_bytes):
    url = "http://localhost:5138/identify?format=json"
    files = {
        'file': ('doc.bin', io.BytesIO(blob_bytes), 'application/pdf')
    }
    proxies = {"http": None, "https": None}
    response = requests.post(url, files=files, proxies=proxies)

    if response.status_code == 200:
        result = response.json()
        fileinfo = result["files"][0]["matches"][0]
        return fileinfo['mime']
    else:
        print(f"Feil fra server: {response.status_code}")
        return False


def decode_file_to_utf8(file_bytes: bytes, file_extension: str = "") -> str:
    """
    Decode raw bytes to UTF-8 string
    """
    if not file_bytes:
        return ""

    # Check Unicode BOM
    for bom_name, bom_bytes in [
        ('utf-8-sig', codecs.BOM_UTF8),
        ('utf-16', codecs.BOM_UTF16),
        ('utf-32', codecs.BOM_UTF32)
    ]:
        if file_bytes.startswith(bom_bytes):
            return file_bytes.decode(bom_name)

    if file_extension.lower() in ['.xml', '.xhtml']:
        try:
            # ElementTree decodes the content according to specified encoding
            root = ET.fromstring(file_bytes)
            return ET.tostring(
                root,
                encoding='unicode',
                xml_declaration=True,
                short_empty_elements=True
            )
        except ET.ParseError:
            pass

    # Try decode to utf-8
    try:
        return file_bytes.decode('utf-8')
    except UnicodeDecodeError:
        pass

    # Analyze text to find encoding
    result = from_bytes(file_bytes).best()
    if result and result.encoding:
        try:
            return file_bytes.decode(result.encoding)
        except (UnicodeDecodeError, LookupError):
            pass

    # Last resort: decode and replace errors
    return file_bytes.decode('utf-8', errors='replace')

