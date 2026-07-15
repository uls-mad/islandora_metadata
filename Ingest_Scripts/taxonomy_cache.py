#!/usr/bin/env python3

"""Fetch, normalize, cache, and load Islandora taxonomy data."""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

import json
import logging
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utilities import (
    create_directory,
    df_to_csv
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TAXONOMY_EXPORT_URL = 'https://digital.library.pitt.edu/admin/taxonomy_json'
DEFAULT_PAGE_SIZE = 1000
DEFAULT_FETCH_DELAY = 1.5
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_PAGES = 1000

JSON_TO_CSV_COLUMNS = {
    'tid': 'Term ID',
    'name': 'Name',
    'code': 'Code',
    'combined_alt_names': 'Alternate Names',
    'authority_sources': 'Authority Sources',
    'authority_uri': 'Authority Sources URI',
    'description': 'Description',
    'vid': 'Vocabulary',
    'external_uri': 'External URI',
    'update_date': 'Updated date',
    'published': 'Published',
}
CSV_COLUMNS = list(JSON_TO_CSV_COLUMNS.values())
REQUIRED_JSON_FIELDS = set(JSON_TO_CSV_COLUMNS)


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

def create_session() -> requests.Session:
    """Create an HTTP session with retry handling."""
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({'GET'}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    session.headers.update({
        'Accept': 'application/json',
        'User-Agent': 'ULS-Islandora-Metadata-Toolkit/1.0',
    })
    return session


def fetch_taxonomy_page(
    session: requests.Session,
    page: int,
    base_url: str = TAXONOMY_EXPORT_URL,
    timeout: int = DEFAULT_TIMEOUT,
) -> list[dict[str, Any]]:
    """Fetch one page from the taxonomy REST export."""
    response = session.get(
        base_url,
        params={'page': page},
        timeout=timeout,
    )
    response.raise_for_status()

    content_type = response.headers.get('Content-Type', '').lower()

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError as error:
        preview = response.text[:300].replace('\n', ' ')
        raise ValueError(
            "Taxonomy endpoint did not return valid JSON. "
            f"Content-Type: {content_type or 'unknown'}. "
            f"Response preview: {preview}"
        ) from error

    if not isinstance(data, list):
        raise ValueError(
            "Expected the taxonomy REST export to return a JSON array, "
            f"but received {type(data).__name__}."
        )

    if any(not isinstance(record, dict) for record in data):
        raise ValueError(
            "Expected every taxonomy export record to be a JSON object."
        )

    return data


def fetch_all_taxonomy_records(
    base_url: str = TAXONOMY_EXPORT_URL,
    page_size: int = DEFAULT_PAGE_SIZE,
    delay: float = DEFAULT_FETCH_DELAY,
    timeout: int = DEFAULT_TIMEOUT,
    max_pages: int = DEFAULT_MAX_PAGES,
    logger: logging.Logger | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch every page from the taxonomy REST export."""
    if delay < 1 or delay > 2:
        raise ValueError("Fetch delay must be between 1 and 2 seconds.")

    session = create_session()
    records: list[dict[str, Any]] = []
    pages_fetched = 0

    try:
        for page in range(max_pages):
            page_records = fetch_taxonomy_page(
                session=session,
                page=page,
                base_url=base_url,
                timeout=timeout,
            )
            pages_fetched += 1

            if logger:
                logger.info(
                    "Fetched taxonomy page %d with %d record(s).",
                    page,
                    len(page_records),
                )
            else:
                print(
                    f"Fetched taxonomy page {page}: "
                    f"{len(page_records)} record(s)"
                )

            if not page_records:
                break

            records.extend(page_records)

            if len(page_records) < page_size:
                break

            time.sleep(delay)
        else:
            raise RuntimeError(
                "Taxonomy pagination reached the safety limit of "
                f"{max_pages} pages."
            )
    finally:
        session.close()

    return records, pages_fetched


def clean_taxonomy_value(value: Any) -> str:
    """Normalize a taxonomy export value to a clean string."""
    if value is None:
        return ''
    return ' '.join(unescape(str(value)).split())


def normalize_taxonomy_records(
    records: list[dict[str, Any]],
) -> pd.DataFrame:
    """Convert REST export records to the existing taxonomy CSV schema."""
    if not records:
        raise ValueError("The taxonomy REST export returned no records.")

    available_fields = set().union(*(record.keys() for record in records))
    missing_fields = REQUIRED_JSON_FIELDS - available_fields

    if missing_fields:
        raise ValueError(
            "Taxonomy export is missing required field(s): "
            f"{', '.join(sorted(missing_fields))}"
        )

    normalized_rows = [
        {
            csv_column: clean_taxonomy_value(record.get(json_field))
            for json_field, csv_column in JSON_TO_CSV_COLUMNS.items()
        }
        for record in records
    ]

    taxonomy_df = pd.DataFrame(normalized_rows, columns=CSV_COLUMNS)

    if taxonomy_df['Term ID'].eq('').any():
        raise ValueError(
            "Taxonomy export contains one or more blank term IDs."
        )

    duplicate_term_ids = taxonomy_df['Term ID'].duplicated(keep=False)
    if duplicate_term_ids.any():
        examples = (
            taxonomy_df.loc[duplicate_term_ids, 'Term ID']
            .drop_duplicates()
            .head(10)
            .tolist()
        )
        raise ValueError(
            "Taxonomy export contains duplicate term IDs. "
            f"Examples: {', '.join(examples)}"
        )

    return taxonomy_df


def read_taxonomy_cache(cache_path: str | Path) -> pd.DataFrame:
    """Read the local taxonomy cache."""
    cache_path = Path(cache_path)

    if not cache_path.exists():
        raise FileNotFoundError(
            f"Taxonomy cache not found: {cache_path}. "
            "Run refresh_taxonomies.py first."
        )

    taxonomy_df = pd.read_csv(
        cache_path,
        dtype=str,
        keep_default_na=False,
    )

    missing_columns = set(CSV_COLUMNS) - set(taxonomy_df.columns)
    if missing_columns:
        raise ValueError(
            "Taxonomy cache is missing required column(s): "
            f"{', '.join(sorted(missing_columns))}"
        )

    return taxonomy_df[CSV_COLUMNS].copy()


def replace_dataframe_contents(
    target_df: pd.DataFrame,
    source_df: pd.DataFrame,
) -> None:
    """Replace a DataFrame's contents without replacing the object itself."""
    target_df.drop(
        index=target_df.index,
        columns=target_df.columns,
        inplace=True,
    )

    for column in source_df.columns:
        target_df[column] = source_df[column].reset_index(drop=True)


def write_taxonomy_cache(
    taxonomy_df: pd.DataFrame,
    cache_path: str | Path,
    metadata_path: str | Path | None = None,
    pages_fetched: int | None = None,
    source_url: str = TAXONOMY_EXPORT_URL,
) -> Path:
    """Write taxonomy data atomically and save refresh metadata."""
    cache_path = Path(cache_path)
    create_directory(cache_path.parent)

    temporary_path = cache_path.with_suffix(f'{cache_path.suffix}.tmp')
    df_to_csv(taxonomy_df, temporary_path)
    temporary_path.replace(cache_path)

    if metadata_path:
        metadata_path = Path(metadata_path)
        create_directory(metadata_path.parent)
        metadata = {
            'last_refreshed': datetime.now().astimezone().isoformat(),
            'record_count': len(taxonomy_df),
            'pages_fetched': pages_fetched,
            # 'source_url': source_url,
        }
        temporary_metadata_path = metadata_path.with_suffix(
            f'{metadata_path.suffix}.tmp'
        )
        temporary_metadata_path.write_text(
            json.dumps(metadata, indent=2),
            encoding='utf-8',
        )
        temporary_metadata_path.replace(metadata_path)

    return cache_path


def refresh_taxonomy_cache(
    cache_path: str | Path,
    metadata_path: str | Path | None = None,
    base_url: str = TAXONOMY_EXPORT_URL,
    delay: float = DEFAULT_FETCH_DELAY,
    timeout: int = DEFAULT_TIMEOUT,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Refresh the local taxonomy cache from the REST export."""
    records, pages_fetched = fetch_all_taxonomy_records(
        base_url=base_url,
        delay=delay,
        timeout=timeout,
        logger=logger,
    )
    taxonomy_df = normalize_taxonomy_records(records)
    write_taxonomy_cache(
        taxonomy_df=taxonomy_df,
        cache_path=cache_path,
        metadata_path=metadata_path,
        pages_fetched=pages_fetched,
        source_url=base_url,
    )

    if logger:
        logger.info(
            "Taxonomy cache refreshed with %d record(s).",
            len(taxonomy_df),
        )

    return taxonomy_df
