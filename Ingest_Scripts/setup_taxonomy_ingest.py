#!/usr/bin/env python3

"""Generate taxonomy ingest batches from a completed remediation project.

This script converts a completed taxonomy remediation project into one or more
Workbench taxonomy ingest spreadsheets and the corresponding configuration
files. It maps source fields to Drupal vocabularies, prepares authority links,
and generates the commands required to create taxonomy terms in Islandora.

Usage:
    # Generate taxonomy ingest files
    python3 setup_taxonomy_ingest.py \
        --taxonomy_project \
        /workbench/batches/example/remediation/example_taxonomy_project.csv

    # Run interactively
    python3 setup_taxonomy_ingest.py
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import argparse
import os
import sys
from pathlib import Path
from urllib.parse import unquote

# Third-party imports
import pandas as pd

# Local imports
from definitions import UTILITY_FILES_DIR
from utilities import (
    ERROR_SYMBOL,
    SUCCESS_SYMBOL,
    WARNING_SYMBOL,
    create_df,
    create_directory,
    df_to_csv,
    prompt_for_input,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = (
    'field',
    'term_name',
    'uri',
)

DESCRIPTION_FIELDS = {
    'genre',
    'genre_japanese_prints',
    'source_collection',
}

VOCAB_MAPPING = {
    'format': 'physical_form',
    'genre': 'genre',
    'genre_japanese_prints': 'genre_japanese_prints',
    'source_collection': 'source_collection',
    'source_collection_id': 'source_collection_identifier',
    'source_repository': 'source_repository',
    'subject_genre': 'subject_genre',
    'subject_geographic': 'geo_location',
    'subject_name_conference': 'person',
    'subject_name_corporate': 'corporate_body',
    'subject_name_family': 'family',
    'subject_name_person': 'person',
    'subject_temporal': 'temporal',
    'subject_title': 'subject_title',
    'subject_topic': 'subject',
    'arranger': 'person',
    'composer': 'person',
    'conductor': 'person',
    'instrumentalist': 'person',
    'interviewee': 'person',
    'interviewer': 'person',
    'lyricist': 'person',
    'singer': 'person',
}

AGENT_SUFFIX_MAPPING = {
    '_person': 'person',
    '_corporate': 'corporate_body',
    '_family': 'family',
    '_conference': 'conference',
}


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

# --- Argument Parsing ---

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments for the taxonomy project.

    If the taxonomy project path is not provided with ``--taxonomy_project``,
    prompt the user to enter it interactively.

    Returns:
        Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Taxonomy Ingest Generator")
    parser.add_argument(
        '-t',
        '--taxonomy_project',
        type=str,
        help="Path to the taxonomy project tracking CSV file.",
    )
    args = parser.parse_args()

    if not args.taxonomy_project:
        args.taxonomy_project = prompt_for_input(
            "Enter path to completed taxonomy project (CSV): "
        )

    return args


# --- Mapping Helpers ---

def resolve_vocab_id(field: str) -> str | None:
    """Resolve a source field to a Drupal vocabulary ID.

    Args:
        field: Input field name from the taxonomy project CSV.

    Returns:
        Drupal vocabulary ID, or None if no match is found.
    """
    vocab_id = VOCAB_MAPPING.get(field)

    if vocab_id:
        return vocab_id

    for suffix, suffix_vocab_id in AGENT_SUFFIX_MAPPING.items():
        if field.endswith(suffix):
            return suffix_vocab_id

    return None


def get_authority_source(uri: str) -> str:
    """Determine the authority source code from a URI.

    Args:
        uri: Authority URI.

    Returns:
        Shortcode identifier, such as ``aat``, ``lcsh``, ``naf``, or ``viaf``.
        Returns an empty string if no authority source is identified.
    """
    if 'aat' in uri:
        return 'aat'

    if 'subjects' in uri:
        return 'lcsh'

    if 'names' in uri:
        return 'naf'

    if 'viaf' in uri:
        return 'viaf'

    return ''


def normalize_authority_uri(uri: str) -> str:
    """Normalize and decode an authority URI.

    Args:
        uri: Raw authority URI.

    Returns:
        Normalized authority URI.
    """
    if uri.startswith('aat/'):
        uri = uri.replace(
            'aat/',
            'http://vocab.getty.edu/page/aat/',
            1,
        )

    return unquote(uri)


# --- Taxonomy Processing ---

def process_vocab_group(
    vocab_id: str,
    group_df: pd.DataFrame,
) -> list[dict[str, str]]:
    """Transform rows for a vocabulary-specific taxonomy ingest sheet.

    Args:
        vocab_id: Drupal vocabulary machine name.
        group_df: Rows belonging to the vocabulary.

    Returns:
        Cleaned rows ready to write to a taxonomy ingest CSV.
    """
    rows_to_ingest = []

    for _, row in group_df.iterrows():
        field = row['field']
        uri = row['uri']
        term_entry = {
            'term_name': row['term_name'],
        }

        is_source = 'source_' in field

        if not is_source and field != 'format':
            if vocab_id == 'conference':
                auth_col = 'field_authority_sources_conf'
            elif vocab_id == 'subject_genre':
                auth_col = 'field_authority_sources_subj_gen'
            elif vocab_id == 'subject_title':
                auth_col = 'field_authority_sources_subj_ti'
            else:
                auth_col = 'field_authority_link'

            authority_source = get_authority_source(uri)
            clean_uri = normalize_authority_uri(uri)

            if clean_uri:
                term_entry[auth_col] = f'{authority_source}%%{clean_uri}'

        if (
            field in DESCRIPTION_FIELDS
            and 'description' in row
            and pd.notna(row['description'])
        ):
            term_entry['description'] = row['description']

        rows_to_ingest.append(term_entry)

    return rows_to_ingest


# --- File Writing Helpers ---

def write_workbench_config(
    project_dir: Path,
    ingest_path: Path,
    vocab_id: str,
    import_password: str,
) -> Path:
    """Generate a YAML configuration file for Drupal Workbench.

    Args:
        project_dir: Project directory.
        ingest_path: Generated taxonomy ingest CSV.
        vocab_id: Target vocabulary ID.
        import_password: Drupal import user password.

    Returns:
        Path to the created Workbench config file.
    """
    template_path = UTILITY_FILES_DIR / 'create_taxonomy_template.yml'
    config_content = template_path.read_text(encoding='utf-8')

    replacements = {
        '[PASSWORD]': import_password,
        '[INPUT_CSV]': ingest_path.as_posix(),
        '[VOCAB_ID]': vocab_id,
        '[INPUT_DIR]': project_dir.as_posix(),
    }

    for placeholder, value in replacements.items():
        config_content = config_content.replace(placeholder, str(value))

    config_path = project_dir / f'create_taxonomy_{vocab_id}_batch.yml'
    config_path.write_text(config_content, encoding='utf-8')

    return config_path


def write_taxonomy_ingest(
    project_dir: Path,
    vocab_id: str,
    rows_to_ingest: list[dict[str, str]],
) -> Path:
    """Write a vocabulary-specific taxonomy ingest CSV.

    Args:
        project_dir: Project directory.
        vocab_id: Target vocabulary ID.
        rows_to_ingest: Rows to write to the ingest CSV.

    Returns:
        Path to the written ingest CSV.
    """
    ingest_df = pd.DataFrame(rows_to_ingest)
    ingest_path = project_dir / f'taxonomy_{vocab_id}_batch.csv'

    df_to_csv(ingest_df, ingest_path)

    return ingest_path


def write_command_file(
    project_dir: Path,
    commands: list[str],
) -> Path:
    """Write generated Workbench commands to a text file.

    Args:
        project_dir: Project directory.
        commands: Workbench commands to write.

    Returns:
        Path to the command file.
    """
    command_file_path = project_dir / 'workbench_commands.txt'
    command_file_path.write_text(
        "\n".join(commands),
        encoding='utf-8',
    )

    return command_file_path


# --- Main Workflow ---

def validate_project_columns(df: pd.DataFrame) -> None:
    """Validate that the taxonomy project contains required columns.

    Args:
        df: Taxonomy project DataFrame.

    Raises:
        SystemExit: If any required columns are missing.
    """
    missing = [
        column for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing:
        print(f"Error: Missing columns {missing}")
        sys.exit(1)


def prepare_project_directory(project_dir: Path) -> None:
    """Create helper directories needed by Workbench operations.

    Args:
        project_dir: Taxonomy project directory.
    """
    for sub_dir in ('logs', 'tmp'):
        create_directory(project_dir / sub_dir)


def process_taxonomy_project(
    project_file: Path,
    import_password: str,
) -> None:
    """Process a completed taxonomy project CSV into ingest files.

    Args:
        project_file: Path to the completed taxonomy project CSV.
        import_password: Drupal import user password.
    """
    project_dir = project_file.parent
    prepare_project_directory(project_dir)

    df = create_df(project_file)
    validate_project_columns(df)

    df['vocab_id'] = df['field'].apply(resolve_vocab_id)

    if df['vocab_id'].isnull().any():
        unmapped = df[df['vocab_id'].isnull()]['field'].unique()
        print(f"\n{ERROR_SYMBOL} Unmapped fields found: {unmapped}")
        sys.exit(1)

    commands = []
    failed_vocabularies = []

    for vocab_id, group_df in df.groupby('vocab_id'):
        try:
            rows_to_ingest = process_vocab_group(vocab_id, group_df)

            # File Write 1: Ingest Sheet
            ingest_path = write_taxonomy_ingest(
                project_dir,
                vocab_id,
                rows_to_ingest,
            )
            print(
                f"{SUCCESS_SYMBOL} Ingest sheet for {vocab_id} saved: "
                f"{ingest_path}"
            )

            # File Write 2: Workbench Config
            config_path = write_workbench_config(
                project_dir,
                ingest_path,
                vocab_id,
                import_password,
            )
            print(
                f"{SUCCESS_SYMBOL} Config file for {vocab_id} saved: "
                f"{config_path}"
            )

            # Only queue the command if both file operations succeed
            commands.append(
                f'workbench --config remediation/{config_path.name} --check'
            )

        except (OSError, PermissionError) as io_err:
            print(
                f"{ERROR_SYMBOL} File system/permission error processing"
                f" vocabulary '{vocab_id}': {io_err}"
            )
            failed_vocabularies.append((vocab_id, f"I/O Error: {io_err}"))
        except Exception as exc:
            print(
                f"{ERROR_SYMBOL} Unexpected error processing vocabulary"
                f" '{vocab_id}': {exc}"
            )
            failed_vocabularies.append((vocab_id, f"Unexpected Error: {exc}"))

    # Write the commands file if any succeeded
    if commands:
        command_file_path = write_command_file(project_dir, commands)
        print(f"\nCommands saved: {command_file_path}")
    else:
        print(
            f"\n{ERROR_SYMBOL} No configuration commands were generated due to"
            " errors."
        )

    # Alert the user to failures at the end of execution
    if failed_vocabularies:
        print(
            f"\n{WARNING_SYMBOL} Warning: The following"
            f" {len(failed_vocabularies)} vocabularies failed to process"
            " completely:"
        )
        for vocab, reason in failed_vocabularies:
            print(f"  - {vocab}: {reason}")


def main() -> None:
    """Run the taxonomy ingest generation workflow."""
    args = parse_arguments()

    import_password = os.getenv('ISLANDORA_PASSWORD')

    if not import_password:
        print(f"{ERROR_SYMBOL} ISLANDORA_PASSWORD not found in .env")
        sys.exit(1)

    process_taxonomy_project(
        Path(args.taxonomy_project),
        import_password,
    )


if __name__ == '__main__':
    main()
