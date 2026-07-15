#!/usr/bin/env python3

"""Manage loading, refreshing, and caching Islandora taxonomies."""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any

# Third-party imports
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Local imports
from definitions import (
    TAXONOMY_CACHE_PATH,
    TAXONOMY_CACHE_METADATA_PATH,
)
from utilities import (
    clear_google_sheet_ranges,
    create_directory,
    df_to_csv,
    get_google_sheet_titles_by_gid,
    update_google_sheet_ranges,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TAXONOMY_URL = os.getenv('TAXONOMY_API_URL')
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

TAXONOMY_COLUMNS = list(JSON_TO_CSV_COLUMNS.values())


# Replace each None value with the numeric GID for the corresponding worksheet.
TAXONOMY_SHEET_GIDS: dict[str, int] = {
    'Conference':                   396878965,
    'Contributing Institution':     1575477919,
    'Corporate Body':               1409963458,
    'Family':                       1497779932,
    'Genre':                        1648307337,
    'Genre (Japanese Prints)':      1434815758,
    'Language':                     73797721,
    'MARC Countries':               119108583,
    'Mode of Issuance':             421070294,
    'Person':                       794877117,
    'Physical Form':                675690206,
    'Resource Types':               422420822,
    'Rights Statement':             100516707,
    'Source Collection Identifier': 1068215276,
    'Source Collection Title':      53196990,
    'Source Repository':            1414771516,
    'Subject':                      1626707044,
    'Subject (Genre)':              1478114606,
    'Subject (Geographic)':         391063228,
    'Subject (Temporal)':           1134268931,
    'Subject (Title)':              953635158,
    'Type of Resource':             453469197,
}


# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------

@dataclass
class TaxonomyManager:
    """Manage taxonomy retrieval, normalization, caching, and loading.

    Basic Auth credentials may be passed directly with ``username`` and
    ``password`` or supplied through the ``ISLANDORA_USERNAME`` and
    ``ISLANDORA_PASSWORD`` environment variables.
    """

    cache_path: str | Path
    metadata_path: str | Path
    base_url: str = DEFAULT_TAXONOMY_URL
    page_size: int = DEFAULT_PAGE_SIZE
    delay: float = DEFAULT_FETCH_DELAY
    timeout: int = DEFAULT_TIMEOUT
    max_pages: int = DEFAULT_MAX_PAGES
    username: str | None = None
    password: str | None = None
    google_sheet_id: str | None = None
    google_credentials_file: str | Path | None = None
    sheet_gids: dict[str, int] | None = None
    logger: logging.Logger | None = None

    def __post_init__(self) -> None:
        """Normalize paths and validate configuration."""
        self.cache_path = Path(self.cache_path)
        self.metadata_path = Path(self.metadata_path)

        self.username = (
            self.username
            or os.getenv('ISLANDORA_USERNAME')
        )
        self.password = (
            self.password
            or os.getenv('ISLANDORA_PASSWORD')
        )

        self.google_sheet_id = (
            self.google_sheet_id
            or os.getenv('TAXONOMY_GOOGLE_SHEET_ID')
        )

        credentials_file = (
            self.google_credentials_file
            or os.getenv('GOOGLE_CREDENTIALS_FILE')
            or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        )
        self.google_credentials_file = (
            Path(credentials_file)
            if credentials_file
            else None
        )

        if not self.base_url:
            raise ValueError(
                "Taxonomy API URL is missing. Set TAXONOMY_API_URL "
                "in the project .env file."
            )

        if self.sheet_gids is None:
            self.sheet_gids = TAXONOMY_SHEET_GIDS.copy()

        if bool(self.username) != bool(self.password):
            raise ValueError(
                "Both taxonomy API username and password must be provided."
            )

        if not 1 <= self.delay <= 2:
            raise ValueError(
                "Taxonomy fetch delay must be between 1 and 2 seconds."
            )

        if self.page_size < 1:
            raise ValueError("Taxonomy page size must be positive.")

        if self.max_pages < 1:
            raise ValueError("Maximum page count must be positive.")

    def load(
        self,
        refresh: bool = False,
        sync_google_sheet: bool = False,
    ) -> pd.DataFrame:
        """Load taxonomy data, optionally refreshing the cache first.

        Args:
            refresh: Whether to refresh from the REST export before loading.
            sync_google_sheet: Whether to synchronize the configured Google Sheet
                after refreshing the local cache.

        Returns:
            Taxonomy DataFrame.
        """
        if sync_google_sheet and not refresh:
            raise ValueError(
                "Google Sheet synchronization requires refresh=True."
            )

        if refresh:
            return self.refresh(
                sync_google_sheet=sync_google_sheet,
            )

        self._log(
            logging.INFO,
            "Loading taxonomies from the local cache.",
        )
        taxonomy_df = self.read_cache()
        self._log(
            logging.INFO,
            "Loaded %d taxonomy record(s) from the local cache.",
            len(taxonomy_df),
        )

        return taxonomy_df

    def refresh(
        self,
        sync_google_sheet: bool = False,
    ) -> pd.DataFrame:
        """Refresh the taxonomy cache from the REST export.

        Args:
            sync_google_sheet: Whether to synchronize the configured Google Sheet
                after refreshing the local cache.

        Returns:
            Fresh taxonomy DataFrame.
        """
        self._log(
            logging.INFO,
            "Starting taxonomy refresh from the Drupal REST export.",
        )

        records, pages_fetched = self.fetch_all_records()

        self._log(
            logging.INFO,
            "Normalizing %d taxonomy record(s).",
            len(records),
        )
        taxonomy_df = self.normalize_records(records)

        self._log(
            logging.INFO,
            "Writing the refreshed taxonomy cache.",
        )
        self.write_cache(
            taxonomy_df,
            pages_fetched=pages_fetched,
        )

        self._log(
            logging.INFO,
            "Taxonomy cache refreshed with %d record(s).",
            len(taxonomy_df),
        )

        if sync_google_sheet:
            self._log(
                logging.INFO,
                "Starting Google Sheet taxonomy synchronization.",
            )
            self.sync_google_sheet(taxonomy_df)
            self._log(
                logging.INFO,
                "Google Sheet taxonomy synchronization complete.",
            )

        self._log(
            logging.INFO,
            "Taxonomy refresh complete.",
        )

        return taxonomy_df

    def refresh_shared_dataframe(
        self,
        taxonomy_df: pd.DataFrame,
        sync_google_sheet: bool = False,
    ) -> pd.DataFrame:
        """Refresh the cache and update a shared DataFrame in place.

        This preserves references created by imports such as
        ``from definitions import TAXONOMIES``.

        Args:
            taxonomy_df: Existing shared taxonomy DataFrame.
            sync_google_sheet: Whether to synchronize the configured Google Sheet
                after refreshing the local cache.

        Returns:
            The updated shared DataFrame.
        """
        fresh_df = self.refresh(
            sync_google_sheet=sync_google_sheet,
        )
        self.replace_dataframe_contents(taxonomy_df, fresh_df)

        return taxonomy_df

    def fetch_all_records(
        self,
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch all pages from the taxonomy REST export.

        Returns:
            Tuple containing all records and the number of pages fetched.
        """
        self._log(
            logging.INFO,
            "Connecting to the taxonomy REST export.",
        )

        session = self._create_session()
        records: list[dict[str, Any]] = []
        pages_fetched = 0

        try:
            for page in range(self.max_pages):
                page_records = self.fetch_page(
                    session=session,
                    page=page,
                )
                pages_fetched += 1

                self._log(
                    logging.INFO,
                    (
                        "Fetched taxonomy page %d with %d record(s); "
                        "%d record(s) retrieved so far."
                    ),
                    page,
                    len(page_records),
                    len(records) + len(page_records),
                )

                if not page_records:
                    break

                records.extend(page_records)

                if len(page_records) < self.page_size:
                    break

                time.sleep(self.delay)

            else:
                raise RuntimeError(
                    "Taxonomy pagination reached the safety limit of "
                    f"{self.max_pages} pages."
                )

        finally:
            session.close()

        self._log(
            logging.INFO,
            "Finished fetching %d page(s) and %d taxonomy record(s).",
            pages_fetched,
            len(records),
        )

        return records, pages_fetched

    def fetch_page(
        self,
        session: requests.Session,
        page: int,
    ) -> list[dict[str, Any]]:
        """Fetch one page from the taxonomy REST export.

        Args:
            session: Reusable HTTP session.
            page: Zero-based page number.

        Returns:
            Taxonomy records for the requested page.
        """
        response = session.get(
            self.base_url,
            params={'page': page},
            timeout=self.timeout,
        )

        if response.status_code in {401, 403}:
            if not self.username or not self.password:
                raise PermissionError(
                    "Taxonomy endpoint requires authentication, but no Basic "
                    "Auth credentials were provided. Set "
                    "ISLANDORA_USERNAME and ISLANDORA_PASSWORD, or pass "
                    "username and password to TaxonomyManager."
                )

            raise PermissionError(
                "Taxonomy endpoint rejected the supplied credentials or the "
                "service account lacks access to this REST View. Confirm that "
                "Basic Auth is enabled for the View and that its Access "
                "settings allow the service account's role. "
                f"HTTP status: {response.status_code}."
            )

        response.raise_for_status()

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as error:
            preview = response.text[:300].replace('\n', ' ')

            raise ValueError(
                "Taxonomy endpoint did not return valid JSON. "
                f"Response preview: {preview}"
            ) from error

        if not isinstance(data, list):
            raise ValueError(
                "Expected the taxonomy REST export to return a JSON array."
            )

        if any(not isinstance(record, dict) for record in data):
            raise ValueError(
                "Expected every taxonomy record to be a JSON object."
            )

        return data

    def normalize_records(
        self,
        records: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """Normalize JSON records to the existing taxonomy CSV schema.

        Args:
            records: Raw taxonomy JSON records.

        Returns:
            Normalized taxonomy DataFrame.
        """
        if not records:
            raise ValueError(
                "The taxonomy REST export returned no records."
            )

        available_fields = set().union(
            *(record.keys() for record in records)
        )
        missing_fields = (
            set(JSON_TO_CSV_COLUMNS) - available_fields
        )

        if missing_fields:
            raise ValueError(
                "Taxonomy export is missing required field(s): "
                f"{', '.join(sorted(missing_fields))}"
            )

        rows = [
            {
                csv_column: self.clean_value(
                    record.get(json_field)
                )
                for json_field, csv_column
                in JSON_TO_CSV_COLUMNS.items()
            }
            for record in records
        ]

        taxonomy_df = pd.DataFrame(
            rows,
            columns=TAXONOMY_COLUMNS,
        )

        if taxonomy_df['Term ID'].eq('').any():
            raise ValueError(
                "Taxonomy export contains blank term IDs."
            )

        duplicate_ids = taxonomy_df[
            'Term ID'
        ].duplicated(keep=False)

        if duplicate_ids.any():
            examples = (
                taxonomy_df.loc[duplicate_ids, 'Term ID']
                .drop_duplicates()
                .head(10)
                .tolist()
            )

            raise ValueError(
                "Taxonomy export contains duplicate term IDs. "
                f"Examples: {', '.join(examples)}"
            )

        return taxonomy_df

    def read_cache(self) -> pd.DataFrame:
        """Read taxonomy data from the local cache.

        Returns:
            Cached taxonomy DataFrame.
        """
        if not self.cache_path.exists():
            raise FileNotFoundError(
                f"Taxonomy cache not found: {self.cache_path}. "
                "Refresh the taxonomy cache before continuing."
            )

        taxonomy_df = pd.read_csv(
            self.cache_path,
            dtype=str,
            keep_default_na=False,
        )

        missing_columns = (
            set(TAXONOMY_COLUMNS) - set(taxonomy_df.columns)
        )

        if missing_columns:
            raise ValueError(
                "Taxonomy cache is missing required column(s): "
                f"{', '.join(sorted(missing_columns))}"
            )

        return taxonomy_df[TAXONOMY_COLUMNS].copy()

    def write_cache(
        self,
        taxonomy_df: pd.DataFrame,
        pages_fetched: int,
    ) -> None:
        """Write taxonomy data and cache metadata atomically.

        Args:
            taxonomy_df: Normalized taxonomy DataFrame.
            pages_fetched: Number of REST pages retrieved.
        """
        create_directory(self.cache_path.parent)
        create_directory(self.metadata_path.parent)

        temporary_cache = self.cache_path.with_suffix(
            f'{self.cache_path.suffix}.tmp'
        )

        df_to_csv(taxonomy_df, temporary_cache)
        temporary_cache.replace(self.cache_path)

        cache_metadata = {
            'last_refreshed': (
                datetime.now().astimezone().isoformat()
            ),
            'record_count': len(taxonomy_df),
            'pages_fetched': pages_fetched,
            # 'source_url': self.base_url,
        }

        temporary_metadata = self.metadata_path.with_suffix(
            f'{self.metadata_path.suffix}.tmp'
        )

        temporary_metadata.write_text(
            json.dumps(cache_metadata, indent=2),
            encoding='utf-8',
        )
        temporary_metadata.replace(self.metadata_path)

        self._log(
            logging.INFO,
            "Taxonomy cache saved to %s.",
            self.cache_path,
        )
        self._log(
            logging.INFO,
            "Taxonomy cache metadata saved to %s.",
            self.metadata_path,
        )

    def read_cache_metadata(self) -> dict[str, Any]:
        """Read information about the most recent cache refresh.

        Returns:
            Cache metadata dictionary, or an empty dictionary if unavailable.
        """
        if not self.metadata_path.exists():
            return {}

        return json.loads(
            self.metadata_path.read_text(encoding='utf-8')
        )

    @staticmethod
    def clean_value(value: Any) -> str:
        """Normalize a taxonomy value to a clean string."""
        if value is None:
            return ''

        return ' '.join(
            unescape(str(value)).split()
        )

    @staticmethod
    def replace_dataframe_contents(
        target_df: pd.DataFrame,
        source_df: pd.DataFrame,
    ) -> None:
        """Replace a DataFrame's contents without replacing the object."""
        target_df.drop(
            index=target_df.index,
            columns=target_df.columns,
            inplace=True,
        )

        for column in source_df.columns:
            target_df[column] = (
                source_df[column]
                .reset_index(drop=True)
            )

    def sync_google_sheet(
        self,
        taxonomy_df: pd.DataFrame,
        published_only: bool = True,
    ) -> dict[str, int]:
        """Replace taxonomy term lists in mapped Google Sheet tabs.

        Args:
            taxonomy_df: Normalized taxonomy DataFrame.
            published_only: Whether to include only published terms.

        Returns:
            Mapping of vocabulary names to term counts written.
        """
        self._validate_google_sheet_configuration()

        required_columns = {'Name', 'Vocabulary'}
        if published_only:
            required_columns.add('Published')

        missing_columns = required_columns - set(taxonomy_df.columns)
        if missing_columns:
            raise ValueError(
                "Taxonomy data is missing column(s) required for Google "
                "Sheets synchronization: "
                f"{', '.join(sorted(missing_columns))}"
            )

        self._log(
            logging.INFO,
            "Retrieving Google Sheet tab metadata.",
        )

        sheet_titles = get_google_sheet_titles_by_gid(
            sheet_id=self.google_sheet_id,
            credentials_file=self.google_credentials_file,
            logger=self.logger,
        )
        self._log(
            logging.INFO,
            "Preparing %d taxonomy tab(s) for synchronization.",
            len(self.sheet_gids),
        )

        clear_ranges: list[str] = []
        value_ranges: list[dict[str, Any]] = []
        term_counts: dict[str, int] = {}

        for vocabulary, gid in self.sheet_gids.items():
            sheet_title = sheet_titles.get(int(gid))
            if sheet_title is None:
                raise ValueError(
                    f"Could not find worksheet GID {gid} for taxonomy "
                    f"'{vocabulary}'."
                )

            terms = self._get_google_sheet_terms(
                taxonomy_df=taxonomy_df,
                vocabulary=vocabulary,
                published_only=published_only,
            )
            escaped_title = self._escape_sheet_title(sheet_title)
            clear_ranges.append(f"'{escaped_title}'!A:A")

            if terms:
                value_ranges.append({
                    'range': f"'{escaped_title}'!A1:A{len(terms)}",
                    'majorDimension': 'ROWS',
                    'values': [[term] for term in terms],
                })

            term_counts[vocabulary] = len(terms)

            self._log(
                logging.INFO,
                "Prepared '%s' with %d term(s).",
                vocabulary,
                len(terms),
            )

        self._log(
            logging.INFO,
            "Clearing existing values from %d taxonomy tab(s).",
            len(clear_ranges),
        )
        clear_google_sheet_ranges(
            sheet_id=self.google_sheet_id,
            ranges=clear_ranges,
            credentials_file=self.google_credentials_file,
            logger=self.logger,
        )

        self._log(
            logging.INFO,
            "Writing refreshed terms to the taxonomy Google Sheet.",
        )
        update_google_sheet_ranges(
            sheet_id=self.google_sheet_id,
            data=value_ranges,
            credentials_file=self.google_credentials_file,
            logger=self.logger,
        )

        for vocabulary, term_count in term_counts.items():
            self._log(
                logging.INFO,
                "Updated Google Sheet taxonomy '%s' with %d term(s).",
                vocabulary,
                term_count,
            )

        return term_counts

    def _get_google_sheet_terms(
        self,
        taxonomy_df: pd.DataFrame,
        vocabulary: str,
        published_only: bool,
    ) -> list[str]:
        """Return sorted unique terms for one vocabulary."""
        mask = taxonomy_df['Vocabulary'].eq(vocabulary)

        if published_only:
            published_values = (
                taxonomy_df['Published']
                .astype(str)
                .str.strip()
                .str.casefold()
            )
            mask &= published_values.eq('yes')

        terms = (
            taxonomy_df.loc[mask, 'Name']
            .astype(str)
            .str.strip()
        )
        terms = terms[terms.ne('')]
        unique_terms = {term.casefold(): term for term in terms}

        return sorted(unique_terms.values(), key=str.casefold)

    def _validate_google_sheet_configuration(self) -> None:
        """Validate Google Sheets synchronization configuration."""
        if not self.google_sheet_id:
            raise ValueError(
                "Taxonomy Google Sheet ID was not configured. Set "
                "TAXONOMY_GOOGLE_SHEET_ID."
            )

        if self.google_credentials_file is None:
            raise ValueError(
                "Google credentials file was not configured. Set "
                "GOOGLE_CREDENTIALS_FILE."
            )

        if not self.google_credentials_file.exists():
            raise FileNotFoundError(
                "Google credentials file not found: "
                f"{self.google_credentials_file}"
            )

        if not self.sheet_gids:
            raise ValueError("No taxonomy-to-GID mappings were configured.")

    @staticmethod
    def _escape_sheet_title(sheet_title: str) -> str:
        """Escape apostrophes for use in an A1 notation range."""
        return sheet_title.replace("'", "''")

    def _create_session(self) -> requests.Session:
        """Create an HTTP session with retry and optional Basic Auth."""
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
            'User-Agent': (
                'ULS-Islandora-Metadata-Toolkit/1.0'
            ),
        })

        if self.username and self.password:
            session.auth = (
                self.username,
                self.password,
            )

        return session

    def _log(
        self,
        level: int,
        message: str,
        *args: object,
        terminal: bool = True,
    ) -> None:
        """Write a message to the log file and optionally the terminal.

        Args:
            level: Standard logging level for the message.
            message: Logging-style message template.
            *args: Values interpolated into the message template.
            terminal: Whether to display the formatted message in the terminal.
        """
        if self.logger:
            self.logger.log(level, message, *args)

        if terminal:
            formatted_message = message % args if args else message
            print(formatted_message, flush=True)


# ---------------------------------------------------------------------------
# Taxonomy Loading
# ---------------------------------------------------------------------------

TAXONOMY_MANAGER = TaxonomyManager(
    cache_path=TAXONOMY_CACHE_PATH,
    metadata_path=TAXONOMY_CACHE_METADATA_PATH,
    username=os.getenv('ISLANDORA_USERNAME'),
    password=os.getenv('ISLANDORA_PASSWORD'),
    google_sheet_id=os.getenv('TAXONOMY_GOOGLE_SHEET_ID'),
    google_credentials_file=os.getenv('GOOGLE_CREDENTIALS_FILE'),
    sheet_gids=TAXONOMY_SHEET_GIDS,
)


TAXONOMIES = pd.DataFrame()


def load_taxonomies(
    refresh: bool = False,
    sync_google_sheet: bool = False,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Load or refresh the shared taxonomy DataFrame."""
    TAXONOMY_MANAGER.logger = logger

    if sync_google_sheet and not refresh:
        raise ValueError(
            "Google Sheet synchronization requires refresh=True."
        )

    if refresh:
        fresh_df = TAXONOMY_MANAGER.refresh(
            sync_google_sheet=sync_google_sheet,
        )
        TAXONOMY_MANAGER.replace_dataframe_contents(
            TAXONOMIES,
            fresh_df,
        )
    elif TAXONOMIES.empty:
        cached_df = TAXONOMY_MANAGER.read_cache()
        TAXONOMY_MANAGER.replace_dataframe_contents(
            TAXONOMIES,
            cached_df,
        )

    return TAXONOMIES
