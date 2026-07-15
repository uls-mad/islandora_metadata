"""Extended EDTF Date Validation and Processing Utilities.

Provides functions for validating, normalizing, and deduplicating date values 
according to Extended Date/Time Format (EDTF) strings. 
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import re

# Third-party imports
from edtf import parse_edtf

# Local imports
from utilities import (
    DRUPAL_EXTENDED_EDTF_PATTERN,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATE_COMPONENT_PATTERN = r"(?:\d{4}|\d{1,3}X{1,3})(?:-\d{2}){0,2}"
EDTF_QUALIFIER_PATTERN = re.compile(r"[?~%]$")


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

def validate_edtf_date(value: str) -> bool:
    """Validate an EDTF date string.

    Uses edtf.parse_edtf for strict EDTF validation, then falls back to
    Islandora/Drupal-accepted EDTF-like interval forms.

    Args:
        value: Date string to validate.

    Returns:
        True if the value is valid EDTF or an accepted Drupal EDTF-like form;
        otherwise, False.
    """
    value = value.strip() if value else ""

    if not value:
        return False

    try:
        return bool(parse_edtf(value))
    except Exception:
        valid = bool(DRUPAL_EXTENDED_EDTF_PATTERN.search(value))
        return valid


def convert_marc_date_to_edtf(
    value: str,
    is_approximate: bool = False,
    is_inferred: bool = False,
    is_uncertain: bool = False,
) -> list[tuple[str, bool, bool]]:
    """Parse MARC date strings into standardized Extended Date Time Format.

    Args:
        value: Raw MARC date string requiring transformation.
        is_approximate: Whether the date is approximate.
        is_inferred: Whether the date is inferred.
        is_uncertain: Whether the date is uncertain.

    Returns:
        A list of tuples. Each tuple contains the generated EDTF string,
        a boolean indicating whether it represents a copyright date, and
        a boolean indicating whether fallback matching was used.
    """
    if not value:
        return []

    if ',' in value:
        parts = [part.strip() for part in value.split(',')]
        all_results = []

        for part in parts:
            all_results.extend(convert_marc_date_to_edtf(part))

        # De-duplicate while preserving order
        seen = set()
        unique_results = []

        for item in all_results:
            if item not in seen:
                unique_results.append(item)
                seen.add(item)

        return unique_results

    # Initial date normalization
    normalized_value = value.strip().rstrip('.,;:').lower()
    normalized_value = normalized_value.replace('u', 'X')

    # Detect Qualifiers
    if not is_approximate:
        is_approximate = bool(
            re.search(
                r"\b(ca\.?|circa|approximately|approx\.?)\b",
                normalized_value,
            )
        )

    normalized_value = re.sub(
        r"\b(ca\.?|circa|approximately|approx\.?)\b",
        '',
        normalized_value,
    ).strip()

    if not is_inferred:
        is_inferred = '[' in normalized_value or ']' in normalized_value

    normalized_value = (
        normalized_value
        .replace('[', '')
        .replace(']', '')
        .strip()
    )

    if not is_uncertain:
        is_uncertain = '?' in normalized_value

    normalized_value = normalized_value.replace('?', '')

    for _ in range(3):
        normalized_value = re.sub(
            r"\b([0-9X]{1,3})-(?!\d)",
            r"\1X",
            normalized_value,
        )

    # Detect Copyright
    is_copyright_date = bool(
        re.search(r"\b(copyright|c)\s*\d", normalized_value)
        or '©' in normalized_value
    )

    normalized_value = re.sub(
        r"\b(copyright|c)\s*|\u00A9\s*",
        '',
        normalized_value,
        flags=re.IGNORECASE,
    ).strip()

    # Standardize Internal Separators
    normalized_value = re.sub(r"(\d)/(\d)", r"\1-\2", normalized_value)

    def apply_edtf_qualifier(date_part: str) -> str:
        """Apply EDTF qualifier symbols based on date context."""
        if 'X' in date_part:
            return date_part

        if is_approximate and (is_inferred or is_uncertain):
            return f"{date_part}%"

        if is_approximate:
            return f"{date_part}~"

        if is_inferred or is_uncertain:
            return f"{date_part}?"

        return date_part

    # --- MATCHING LOGIC (Each returns a list of one tuple) ---

    # Open-ended range
    open_range_match = re.search(
        fr"^({DATE_COMPONENT_PATTERN})\s*[-/]\s*$",
        normalized_value,
    )

    if open_range_match:
        start = apply_edtf_qualifier(open_range_match.group(1))
        return [(f"{start}/..", is_copyright_date, False)]

    # Full Range
    range_match = re.search(
        fr"^({DATE_COMPONENT_PATTERN})\s*[-/]\s*"
        fr"({DATE_COMPONENT_PATTERN}|9999)$",
        normalized_value,
    )

    if range_match:
        start = apply_edtf_qualifier(range_match.group(1))
        end_value = range_match.group(2)
        end = '..' if end_value == '9999' else apply_edtf_qualifier(end_value)

        return [(f"{start}/{end}", is_copyright_date, False)]

    # Single Date
    single_match = re.search(fr"^{DATE_COMPONENT_PATTERN}$", normalized_value)

    if single_match:
        return [
            (
                apply_edtf_qualifier(single_match.group(0)),
                is_copyright_date,
                False,
            )
        ]

    # Fallback
    loose_match = re.search(DATE_COMPONENT_PATTERN, normalized_value)

    if loose_match:
        return [
            (
                apply_edtf_qualifier(loose_match.group(0)),
                is_copyright_date,
                True,
            )
        ]

    return []


def resolve_dates(
    date_list: list[str],
) -> list[str]:
    """Group dates by numeric base and prioritize EDTF-qualified versions.

    Identifies trailing EDTF operators (?, ~, %) to ensure that descriptive,
    qualified dates are preserved over clean, non-qualified duplicates sharing
    the same chronological root string.

    Args:
        date_list: Raw EDTF date strings needing evaluation and de-duplication.

    Returns:
        A prioritized list of unique date strings containing the most
        descriptive versions of each base date.
    """
    best_versions = {}

    for date in date_list:
        # Strip qualifier to find "base" date (e.g., '1967' from '1967?')
        base_date = EDTF_QUALIFIER_PATTERN.sub('', date)

        # Check for date qualifier
        has_qualifier = bool(EDTF_QUALIFIER_PATTERN.search(date))

        # Store date if base date hasn't been seen yet or date has a qualifier
        if base_date not in best_versions:
            best_versions[base_date] = date
        elif has_qualifier:
            best_versions[base_date] = date

    return list(best_versions.values())
