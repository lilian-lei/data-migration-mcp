"""
symliv_migration_mcp.py

MCP server that exposes the SymLiv CSV import pipeline as tools, so Claude
can drive a full community onboarding from a single conversation.

Run locally:
    uv add "mcp[cli]" httpx pydantic
    uv run symliv_migration_mcp.py

Or via stdio in Claude Desktop / Claude Code by adding to your MCP config:
    {
      "mcpServers": {
        "symliv-migration": {
          "command": "uv",
          "args": ["run", "/path/to/symliv_migration_mcp.py"]
        }
      }
    }
"""

# Enable PEP 604-style union syntax (X | Y) and postponed evaluation of
# annotations so type hints work on older Python versions (3.9+).
from __future__ import annotations

# Standard library imports
import csv          # CSV file parsing (reading headers and row data)
import io           # (unused but available for in-memory stream handling)
import logging      # Structured logging for debugging and audit trails
import os           # Access environment variables for secrets and config
import json         # Read/write profiles.json for runtime context switching
import re           # Regex for email/UUID/sentinel value validation
from datetime import datetime  # Date format parsing in validation checks
from pathlib import Path          # Cross-platform filesystem path operations
from typing import Annotated, Any, Literal, Optional  # Type-hint helpers

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("MCP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("symliv-mcp.log"),
        logging.StreamHandler(),            # also print to stderr
    ],
)
logger = logging.getLogger("symliv-migration")

# Third-party imports
import httpx                      # Async HTTP client used for GraphQL API calls
import jwt                        # PyJWT — sign HS256 JWTs to mint admin tokens
from mcp.server.fastmcp import FastMCP  # FastMCP framework for building MCP tool servers
from pydantic import Field               # Field metadata for tool parameter descriptions

# ---------------------------------------------------------------------------
# Configuration — loaded from src/profiles.json with env-var overrides.
# ---------------------------------------------------------------------------
# Resolution order for each setting (first non-empty wins):
#   1. The matching env var in .mcp.json (highest precedence — back-compat)
#   2. The active environment block in profiles.json
#   3. A built-in default (only for graphql_url + workspace_root)
#
# Callers can mutate the active context at runtime via `switch_context` —
# which rebinds these module-level globals AND rewrites profiles.json's
# "current" block, so the change survives MCP-server restarts.

PROFILES_PATH = Path(__file__).parent / "profiles.json"

def _load_profiles() -> dict[str, Any]:
    """Read profiles.json. Returns empty-shape if missing or invalid so the
    server still boots from env vars alone."""
    if not PROFILES_PATH.exists():
        return {"environments": {}, "current": {}}
    try:
        return json.loads(PROFILES_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {"environments": {}, "current": {}}

def _save_profiles(profiles: dict[str, Any]) -> None:
    """Persist profiles back to disk (called by switch_context)."""
    PROFILES_PATH.write_text(json.dumps(profiles, indent=2) + "\n")

def _active_env_block(profiles: dict[str, Any]) -> dict[str, str]:
    """Return the env-block (graphql_url, mongo_uri, jwt_secret, ...) for the
    currently-selected environment, or an empty dict if none is configured."""
    env_name = (profiles.get("current") or {}).get("environment") or ""
    return (profiles.get("environments") or {}).get(env_name) or {}

_PROFILES = _load_profiles()
_ENV_BLOCK = _active_env_block(_PROFILES)

# Each global is "env var wins, then profile, then built-in default."
SYMLIV_ENV_NAME = (_PROFILES.get("current") or {}).get("environment") or ""

SYMLIV_GRAPHQL_URL = (
    os.environ.get("SYMLIV_GRAPHQL_URL")
    or _ENV_BLOCK.get("graphql_url")
    or "https://api.symliv.com/graphql"
)
SYMLIV_ADMIN_TOKEN = (
    os.environ.get("SYMLIV_ADMIN_TOKEN")
    or _ENV_BLOCK.get("admin_token")
    or ""
)
SYMLIV_JWT_SECRET_KEY = (
    os.environ.get("SYMLIV_JWT_SECRET_KEY")
    or _ENV_BLOCK.get("jwt_secret")
    or ""
)
SYMLIV_COMMUNITY_ID = (
    os.environ.get("SYMLIV_COMMUNITY_ID")
    or (_PROFILES.get("current") or {}).get("community_id")
    or ""
)
SYMLIV_WORKSPACE_ROOT = (
    os.environ.get("SYMLIV_WORKSPACE_ROOT")
    or _ENV_BLOCK.get("workspace_root")
    or str(Path.home() / "Documents" / "symliv-data" / "communities")
)
# Mongo connection settings (consumed later in this module when the pymongo
# client is created). Promoted to globals so switch_context can swap them.
SYMLIV_MONGO_URI = (
    os.environ.get("SYMLIV_MONGO_URI")
    or _ENV_BLOCK.get("mongo_uri")
    or ""
)
SYMLIV_MONGO_DB = (
    os.environ.get("SYMLIV_MONGO_DB")
    or _ENV_BLOCK.get("mongo_db")
    or "main"
)

# Maps a human-friendly "import type" key (used by MCP tool callers) to the
# corresponding GraphQL mutation name on the SymLiv API.
# Each mutation expects a CSV string and a noMutation flag (for dry-run mode).
# Mirrors the table in CSV-Import-Guide.md.
IMPORT_MUTATIONS: dict[str, str] = {
    "community_addresses":  "parseCommunityAddressCsvFile",   # Gate/address setup
    "vendor_users":         "parseVendorUserCsvFile",          # Vendor company admin accounts
    "vendor_employees":     "parseVendorEmployeeCsvFile",      # Employees under a vendor
    "vendor_vehicles":      "parseVendorVehicleCsvFile",       # Vehicles linked to vendors
    "vendor_passes":        "parseVendorPassCsvFile",          # Passes assigned to vendors
    "resident_users":       "parseResidentUserCsvFile",        # Resident user accounts
    "resident_properties":  "parseResidentAddressCsvFile",     # Link residents → addresses
    "resident_passes":      "parseResidentPassesCsvFile",      # Passes assigned to residents
    "host_users":           "parseHostUserCsvFile",            # Short-term-rental host accounts
    "host_rental_units":    "parseHostRentalUnitCsvFile",      # Rental units under a host
    "host_guest_passes":    "parseHostGuestPassCsvFile",       # Guest passes for host guests
    "guest_users":          "parseGuestUserCsvFile",           # Standalone guest user accounts
}

# Full per-import-type schema, encoded from the docs in
# /Users/logan/Documents/symliv-data/data-docs/*.md and cross-referenced
# against the backend yup schemas in
# apps/admin-back/src/modules/DataImportService/schemas/. Used by both
# `validate_csv` (deep validation) and `identify_import_type` (file→type
# classification).
#
# Per-type entries:
#   required:        columns that MUST be present and non-empty for every row
#   required_or:     groups where AT LEAST ONE column must be present/non-empty
#   types:           column → datatype tag for value-level validation
#                    (email, uuid, date, int, positive_int, number, bool, string)
#   enums:           column → allowed values; case-sensitive unless noted
#   critical_string: columns that reject empty/"#N/A"/"#REF!"/whitespace-only
#                    (the docs spell these out for *.firstName / *.lastName)
#
# All status enums below come from CSV-Import-Guide.md and the per-type docs.

# Shared enum vocabularies — kept module-level so reuse is obvious
_ACCOUNT_STATUS = {"active", "inactive", "suspended"}
_APPLICATION_STATUS = {
    "incomplete", "pending-review", "needs-review", "approved",
    "rejected", "deleted", "suspended", "expired",
    "pending-renewal", "stalled",
}
_PASS_STATUS = {"incomplete", "inactive", "active", "expired", "suspended"}
_PAYMENT_STATUS = {"unpaid", "paid", "refunded", "partially-refunded"}
# Valid values for the passes.passType / pass.passType column. The backend
# rejects anything else with "must be one of the following values: …".
# Observed values that look plausible but FAIL: "cart" (golf carts must
# use passType=resident with a Long-Term-Lease-Barcode passInfoId instead).
_PASS_TYPE = {"guest", "single", "vendor", "invited-guest",
              "fast-pass", "wristband", "resident", "visitor"}
# Note: rental unit statuses are CAPITALIZED in the docs unlike the others
_RENTAL_UNIT_STATUS = {"Active", "Pending Review", "Rejected", "Expired", "Refunded"}

IMPORT_SCHEMAS: dict[str, dict[str, Any]] = {
    "community_addresses": {
        "required": ["address", "passesPerDay"],
        "types": {
            "address": "string",
            "passesPerDay": "positive_int",
            "communityAddressId": "uuid",
            "resortFeeAmt": "number",
            "resortFeeSymLivFeeAmt": "number",
            "delete": "bool",
        },
        "critical_string": ["address"],
    },
    "vendor_users": {
        "required": [
            "company.companyName", "user.firstName", "user.lastName", "user.email",
        ],
        "types": {
            "user.email": "email",
            "company.email": "email",
            "user.status": "string",
            "application.status": "string",
            "delete": "bool",
        },
        "enums": {
            "user.status": _ACCOUNT_STATUS,
            "application.status": _APPLICATION_STATUS,
        },
        "critical_string": [
            "company.companyName", "user.firstName", "user.lastName",
        ],
    },
    "vendor_employees": {
        "required": ["employee.firstName", "employee.lastName"],
        # The docs require ONE of company / userEmail / companyEmail
        "required_or": [["userEmail", "companyEmail", "company"]],
        "types": {
            "userEmail": "email",
            "companyEmail": "email",
            "employee.email": "email",
            "employee.driversLicenseExp": "date",
            "delete": "bool",
        },
        "critical_string": ["employee.firstName", "employee.lastName"],
    },
    "vendor_vehicles": {
        # No row-level required columns beyond the vendor identifier
        "required": [],
        "required_or": [["userEmail", "companyEmail", "company"]],
        "types": {
            "userEmail": "email",
            "companyEmail": "email",
            "company_vehicle.year": "int",
            "delete": "bool",
        },
    },
    "vendor_passes": {
        "required": ["pass.passInfoId", "pass.startDate", "pass.endDate"],
        "required_or": [["userEmail", "companyEmail", "company"]],
        "types": {
            "userEmail": "email",
            "companyEmail": "email",
            "employee.email": "email",
            "pass.passInfoId": "uuid",
            "pass.startDate": "date",
            "pass.endDate": "date",
            "pass.status": "string",
            "pass.paid": "string",
            "pass.passType": "string",
            "pass.facilityCode": "int",
            "pass.externalCredentialNumber": "string",
            "vehicle.destinationAddressId": "uuid",
            "vehicle.year": "int",
            "fcException": "bool",
            "delete": "bool",
        },
        "enums": {
            "pass.status": _PASS_STATUS,
            "pass.paid": _PAYMENT_STATUS,
            "pass.passType": _PASS_TYPE,
        },
    },
    "resident_users": {
        "required": ["user.firstName", "user.lastName", "user.email"],
        "types": {
            "user.email": "email",
            "resident_profile.email": "email",
            "resident_profile.emergencyEmail": "email",
            "user.status": "string",
            "application.status": "string",
            "registrations.renew": "bool",
            "registrations.complete": "bool",
            "registrations.stepNumber": "int",
            "delete": "bool",
        },
        "enums": {
            "user.status": _ACCOUNT_STATUS,
            "application.status": _APPLICATION_STATUS,
        },
        "critical_string": ["user.firstName", "user.lastName"],
    },
    "resident_properties": {
        "required": ["user.email", "user.address"],
        "types": {
            "user.email": "email",
            "resident_property.communityAddressId": "uuid",
            "resident_property.passesPerDay": "positive_int",
            "delete": "bool",
        },
    },
    "resident_passes": {
        "required": [
            "user.email", "passes.passInfoId",
            "passes.startDate", "passes.endDate",
        ],
        "types": {
            "user.email": "email",
            "passes.passInfoId": "uuid",
            "passes.startDate": "date",
            "passes.endDate": "date",
            "passes.status": "string",
            "passes.paid": "string",
            "passes.passType": "string",
            "passes.shared": "bool",
            "passes.facilityCode": "int",
            "passes.externalCredentialNumber": "string",
            "vehicle.destinationAddressId": "uuid",
            "vehicle.year": "int",
            "fcException": "bool",
            "delete": "bool",
        },
        "enums": {
            "passes.status": _PASS_STATUS,
            "passes.paid": _PAYMENT_STATUS,
            "passes.passType": _PASS_TYPE,
        },
    },
    "host_users": {
        "required": ["user.firstName", "user.lastName", "user.email"],
        "types": {
            "user.email": "email",
            "user.status": "string",
            "application.status": "string",
            "host_info.licenseStatus": "string",
            "registrations.renew": "bool",
            "registrations.complete": "bool",
            "registrations.stepNumber": "int",
            "delete": "bool",
        },
        "enums": {
            "user.status": _ACCOUNT_STATUS,
            "application.status": _APPLICATION_STATUS,
            "host_info.licenseStatus": _APPLICATION_STATUS,
        },
        "critical_string": ["user.firstName", "user.lastName"],
    },
    "host_rental_units": {
        "required": ["user.email", "rentalUnit.address"],
        "types": {
            "user.email": "email",
            "rentalUnit.email": "email",
            "rentalUnit.communityAddressId": "uuid",
            "rentalUnit.startDate": "date",
            "rentalUnit.endDate": "date",
            "rentalUnit.status": "string",
            "rentalUnit.complete": "bool",
            "rentalUnit.passesPerDay": "number",
            "delete": "bool",
        },
        "enums": {
            "rentalUnit.status": _RENTAL_UNIT_STATUS,
        },
    },
    "host_guest_passes": {
        "required": [
            "hostEmail", "guestEmail",
            "pass.passInfoId", "pass.startDate", "pass.endDate",
        ],
        # Docs: "destinationAddressId OR destination" is required to identify
        # the destination property.
        "required_or": [["destinationAddressId", "destination"]],
        "types": {
            "hostEmail": "email",
            "guestEmail": "email",
            "destinationAddressId": "uuid",
            "pass.passInfoId": "uuid",
            "pass.startDate": "date",
            "pass.endDate": "date",
            "pass.status": "string",
            "pass.paid": "string",
            "pass.passType": "string",
            "pass.facilityCode": "int",
            "pass.externalCredentialNumber": "string",
            "vehicle.year": "int",
            "rental.numberGuests": "int",
            "rental.numberPets": "int",
            "rental.guestCanEdit": "bool",
            "fcException": "bool",
            "delete": "bool",
        },
        "enums": {
            "pass.status": _PASS_STATUS,
            "pass.paid": _PAYMENT_STATUS,
            "pass.passType": _PASS_TYPE,
        },
    },
    "guest_users": {
        "required": ["user.firstName", "user.lastName", "user.email"],
        "types": {
            "user.email": "email",
            "user.status": "string",
            "registration.renew": "bool",
            "registration.complete": "bool",
            "registration.stepNumber": "int",
            "delete": "bool",
        },
        "enums": {
            "user.status": _ACCOUNT_STATUS,
        },
        "critical_string": ["user.firstName", "user.lastName"],
    },
}

# Backwards-compat shim for the existing list_import_types tool, which used
# REQUIRED_COLUMNS to report what each type needs. Derived from IMPORT_SCHEMAS.
REQUIRED_COLUMNS: dict[str, set[str]] = {
    k: set(v.get("required", [])) for k, v in IMPORT_SCHEMAS.items()
}

# The canonical import order from the SymLiv CSV Import Guide. Order matters
# because later imports depend on records created by earlier ones. For example:
#   1. community_addresses must exist before resident_properties can link to them
#   2. user accounts (vendor/resident/host/guest) must exist before their
#      sub-resources (employees, vehicles, passes, rental units) can reference them
# The run_pipeline tool automatically sorts files into this order.
RECOMMENDED_ORDER = [
    "community_addresses",                              # Step 1: addresses/gates
    "vendor_users", "resident_users", "host_users", "guest_users",  # Step 2: user accounts
    "resident_properties", "host_rental_units",         # Step 3: property linkages
    "vendor_employees", "vendor_vehicles",              # Step 4: vendor sub-resources
    "vendor_passes", "resident_passes", "host_guest_passes",  # Step 5: passes (last)
]

# Instantiate the MCP server with the name "symliv-migration".
# Tools decorated with @mcp.tool() are automatically registered and exposed
# to any MCP client (e.g., Claude Desktop, Claude Code) that connects.
mcp = FastMCP("symliv-migration")
logger.info("MCP server 'symliv-migration' initialized")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    """Single point of contact with the SymLiv GraphQL API.

    All outbound API calls flow through this function, which handles:
      - Auth: attaches the admin bearer token and community ID headers
      - Timeout: allows up to 120 seconds for large CSV imports
      - Error propagation: raises on HTTP-level errors (4xx/5xx)

    Returns the raw JSON response dict (may contain "data" and/or "errors" keys
    following the standard GraphQL response shape).
    """
    # Guard: refuse to call the API if no admin token is configured
    if not SYMLIV_ADMIN_TOKEN:
        raise RuntimeError(
            "SYMLIV_ADMIN_TOKEN env var is not set. The server requires a "
            "symlivAdmin token to call the import mutations."
        )

    # Guard: refuse to call the API without an explicit target community.
    # admin-back only honors the community-id header for symlivAdmin tokens;
    # if it's missing the backend silently falls back to whatever communityId
    # is baked into the JWT — which could write data to the wrong community.
    # Fail fast rather than risk a misdirected import.
    if not SYMLIV_COMMUNITY_ID:
        raise RuntimeError(
            "SYMLIV_COMMUNITY_ID env var is not set. Refusing to call the API "
            "without an explicit target community (the backend would otherwise "
            "fall back to the community embedded in the JWT)."
        )

    logger.info("GraphQL request → %s", query.strip().split("\n")[0].strip())
    logger.debug("GraphQL variables: %s", variables)

    # Build auth and routing headers required by the SymLiv API
    headers = {
        "Authorization": f"Bearer {SYMLIV_ADMIN_TOKEN}",  # Admin JWT
        # NOTE: admin-back reads the lowercase 'community-id' header
        # (apps/admin-back/src/main.ts) and only honors it for symlivAdmin
        # tokens. Using "X-Community-Id" here would be silently ignored and
        # the backend would fall back to the communityId baked into the JWT.
        "community-id": SYMLIV_COMMUNITY_ID,               # Target community
        "Content-Type": "application/json",
    }

    # Use an async HTTP client with a generous timeout — CSV imports with
    # thousands of rows can take a while on the server side.
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            SYMLIV_GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=headers,
        )
        resp.raise_for_status()  # Raise httpx.HTTPStatusError on 4xx/5xx
        result = resp.json()
        if "errors" in result:
            logger.warning("GraphQL errors: %s", result["errors"])
        else:
            logger.info("GraphQL response OK")
        return result


def _read_csv_headers(csv_path: str) -> list[str]:
    """Read and return just the first row (header row) of a CSV file.

    Uses utf-8-sig encoding to transparently strip the BOM (byte-order mark)
    that Excel often prepends to CSV files. Returns an empty list if the file
    is empty.
    """
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return next(reader, [])  # Return first row or [] if file is empty


def _read_csv_text(csv_path: str) -> str:
    """Read the entire CSV file as a UTF-8 string, stripping any BOM.

    The resulting text is sent as-is to the GraphQL mutation's `csv` argument.
    The utf-8-sig encoding is critical: Excel on Windows adds a BOM (\\xEF\\xBB\\xBF)
    that would corrupt the first column header if not stripped.
    """
    return Path(csv_path).read_text(encoding="utf-8-sig")


def _read_rows(file_path: str) -> list[dict[str, Any]]:
    """Read a .csv or .xlsx file and return a list of row dicts keyed by the
    raw header strings (dot-notation paths like 'user.email' are preserved).

    For xlsx, only the first worksheet is read and the first row is assumed to
    be the header row. Empty cells become "" so downstream comparisons can
    treat them as "no opinion" (matching yup's `.default('')` behavior).
    """
    ext = Path(file_path).suffix.lower()
    if ext == ".csv":
        with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
            return [dict(row) for row in csv.DictReader(f)]
    if ext in (".xlsx", ".xlsm"):
        # Import lazily so the dep is only required when actually reading xlsx
        import openpyxl  # type: ignore[import-untyped]
        wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
        ws = wb.active  # first sheet
        if ws is None:
            raise ValueError(f"xlsx file {file_path} has no active worksheet")
        rows_iter = ws.iter_rows(values_only=True)
        try:
            headers = [str(h) if h is not None else "" for h in next(rows_iter)]
        except StopIteration:
            return []
        out: list[dict[str, Any]] = []
        for raw in rows_iter:
            # Skip wholly-empty rows (openpyxl pads trailing empties)
            if not any(cell not in (None, "") for cell in raw):
                continue
            out.append({
                h: ("" if v is None else v)
                for h, v in zip(headers, raw)
                if h  # ignore unnamed columns
            })
        return out
    raise ValueError(f"Unsupported file extension '{ext}'; use .csv or .xlsx")


def _norm_lookup(val: Any, file_col: str) -> Any:
    """Normalize a value used for *Mongo lookup*. Strings get trimmed; only
    fields whose name contains 'email' also get lowercased (matching the
    yup schemas, which apply `.lowercase()` only to email columns). General
    string fields like `address` are stored mixed-case in Mongo, so blanket
    lowercasing here would cause false 'missing' reports."""
    if val is None:
        return None
    s = val if isinstance(val, str) else str(val)
    s = s.strip()
    if "email" in file_col.lower():
        s = s.lower()
    return s


def _values_equal(file_val: Any, server_val: Any) -> bool:
    """True if a CSV/xlsx cell and a Mongo value represent the same datum.
    Handles two common type-mismatch sources:
      - string from CSV ("5") vs int/float from Mongo (5)
      - mixed case / surrounding whitespace on strings
    """
    if file_val is None and server_val is None:
        return True
    if file_val is None or server_val is None:
        return False
    # Numeric coercion: if either side is a number, try to make both numeric
    if isinstance(server_val, bool) or isinstance(file_val, bool):
        # bool is a subclass of int in Python; treat it strictly
        return file_val == server_val
    if isinstance(server_val, (int, float)) or isinstance(file_val, (int, float)):
        try:
            return float(file_val) == float(server_val)
        except (ValueError, TypeError):
            pass
    # String compare: trim + lowercase
    return str(file_val).strip().lower() == str(server_val).strip().lower()


def _is_empty(v: Any) -> bool:
    """True for values the yup schemas treat as 'no opinion' (string default
    '' or None). Empty values are skipped in field-mismatch comparison so we
    don't flag a row just because an optional column wasn't filled in."""
    return v is None or (isinstance(v, str) and v.strip() == "")


# ---------------------------------------------------------------------------
# Community workspace — chronological archive of every import + diff
# ---------------------------------------------------------------------------
# Every dry-run and commit produces an archive folder. The folder name is
# `<UTC-timestamp>_<import_type>_<dry_run|commit>` so a directory listing
# already tells the chronological story. The structure under each community:
#
#   $SYMLIV_WORKSPACE_ROOT/<community_id>/
#     workbooks/                                — operator-provided onboarding xlsx
#     imports/<stamp>_<type>_<mode>/
#       manifest.json   — type, source path, row count, ok, summary
#       input.csv       — exact bytes uploaded (snapshot)
#       result.json     — backend response (successCount, errorCount, errors)
#     repairs/<stamp>_<type>.diff.json
#                       — added/removed/changed rows vs the previous archive
#                         of the same import_type (the "repair history")
#     timeline.jsonl    — single append-only log of every import event,
#                         chronological, one JSON object per line
#
# Primary keys for diffing are sourced from CSV-Import-Guide.md's
# "Master Quick Reference Card" so the diff identifies the same logical
# row across edits even when other fields change.
import json as _json     # local alias so JSON utility code is greppable
import shutil as _shutil # CSV copy
from datetime import timezone as _tz  # UTC timestamps

DIFF_KEYS: dict[str, list[str]] = {
    "community_addresses":  ["address"],
    "vendor_users":         ["user.email"],
    "vendor_employees":     ["userEmail", "employee.firstName", "employee.lastName"],
    "vendor_vehicles":      ["company_vehicle.licensePlate"],
    "vendor_passes":        ["pass.externalCredentialNumber"],
    "resident_users":       ["user.email"],
    "resident_properties":  ["user.email", "user.address"],
    "resident_passes":      ["passes.externalCredentialNumber"],
    "host_users":           ["user.email"],
    "host_rental_units":    ["user.email", "rentalUnit.address"],
    "host_guest_passes":    ["hostEmail", "guestEmail",
                             "pass.passInfoId", "pass.startDate"],
    "guest_users":          ["user.email"],
}


def _community_dir() -> Path:
    """Resolve (and mkdir-p) the community's workspace root and its
    standard subdirs. Raises if SYMLIV_COMMUNITY_ID is unset."""
    if not SYMLIV_COMMUNITY_ID:
        raise RuntimeError(
            "SYMLIV_COMMUNITY_ID env var is not set; cannot resolve a "
            "community workspace directory."
        )
    root = Path(SYMLIV_WORKSPACE_ROOT) / SYMLIV_COMMUNITY_ID
    for sub in ("imports", "repairs", "workbooks"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    return root


def _stamp() -> str:
    """UTC timestamp formatted for filesystem safety. Includes seconds so
    multiple imports of the same type in quick succession get distinct dirs."""
    return datetime.now(_tz.utc).strftime("%Y-%m-%dT%H-%M-%S")


def _row_key(row: dict[str, Any], import_type: str) -> str | None:
    """Build a stable, normalized key for a row using the import type's
    primary-key columns. Returns None if any PK column is empty — those rows
    fall out of the diff (since they can't be identified across edits)."""
    cols = DIFF_KEYS.get(import_type)
    if not cols:
        return None
    parts: list[str] = []
    for c in cols:
        v = _get_path(row, c)
        if _is_empty(v):
            return None
        parts.append(_norm_lookup(v, c) or "")
    return "|".join(parts)


def _compute_repair_diff(
    import_type: str,
    current_rows: list[dict[str, Any]],
    current_dir: Path,
    stamp: str,
) -> dict[str, Any]:
    """Diff the just-archived input.csv against the previous archive of the
    same import_type, identifying added, removed, and changed rows by
    primary key. Writes the result to `repairs/<stamp>_<type>.diff.json`
    so a future reviewer can replay the repair history.
    """
    imports = _community_dir() / "imports"
    candidates = sorted(
        (p for p in imports.iterdir()
         if p.is_dir() and p.name != current_dir.name
            and f"_{import_type}_" in p.name),
        key=lambda p: p.name, reverse=True,
    )
    if not candidates:
        return {"prior_archive": None,
                "note": f"first archived import of '{import_type}' — no diff"}
    prior_dir = candidates[0]
    try:
        prior_rows = _read_rows(str(prior_dir / "input.csv"))
    except Exception as e:
        return {"prior_archive": prior_dir.name,
                "error": f"failed to read prior input.csv: {e}"}

    def keymap(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            k = _row_key(r, import_type)
            if k is not None:
                out[k] = r
        return out

    cur_map = keymap(current_rows)
    prv_map = keymap(prior_rows)

    added_keys = sorted(set(cur_map) - set(prv_map))
    removed_keys = sorted(set(prv_map) - set(cur_map))
    common = set(cur_map) & set(prv_map)

    # Detect field-level changes for rows present in both
    changed: list[dict[str, Any]] = []
    for k in common:
        cr, pr = cur_map[k], prv_map[k]
        changes: list[dict[str, Any]] = []
        for col in sorted(set(cr) | set(pr)):
            new_v = str(cr.get(col, "")).strip()
            old_v = str(pr.get(col, "")).strip()
            if new_v != old_v:
                changes.append({"field": col, "old": old_v, "new": new_v})
        if changes:
            changed.append({"key": k, "changes": changes})

    diff = {
        "prior_archive": prior_dir.name,
        "current_archive": current_dir.name,
        "added_count": len(added_keys),
        "removed_count": len(removed_keys),
        "changed_count": len(changed),
        "added_keys_sample": added_keys[:50],
        "removed_keys_sample": removed_keys[:50],
        "changes_sample": changed[:50],
    }
    out_path = _community_dir() / "repairs" / f"{stamp}_{import_type}.diff.json"
    out_path.write_text(_json.dumps(diff, indent=2, default=str))
    diff["repair_file"] = str(out_path)
    return diff


def _archive_import(
    import_type: str,
    csv_path: str,
    mode: str,              # "dry_run" or "commit"
    result: dict[str, Any], # the _run_import return value
) -> dict[str, Any] | None:
    """Snapshot the CSV, write a manifest + the API result, append the
    event to timeline.jsonl, and compute a diff against the prior archive
    of the same import_type. Returns a small dict describing what was
    archived (path + diff summary), or None if archiving was skipped.

    Archive failures are logged but do NOT raise — an unwriteable workspace
    must never prevent an import from completing or returning its result.
    """
    try:
        if not SYMLIV_COMMUNITY_ID:
            logger.warning("Skipping archive: no SYMLIV_COMMUNITY_ID set")
            return None
        if not Path(csv_path).exists():
            logger.warning("Skipping archive: source file missing %s", csv_path)
            return None
        stamp = _stamp()
        folder = _community_dir() / "imports" / f"{stamp}_{import_type}_{mode}"
        folder.mkdir(parents=True, exist_ok=True)
        # Copy the exact bytes the API saw (preserves any quoting/encoding)
        _shutil.copy(csv_path, folder / "input.csv")
        # Read rows for row count + diff
        try:
            rows = _read_rows(csv_path)
        except Exception as e:
            logger.warning("Archive: row read failed (%s); using empty list", e)
            rows = []
        manifest = {
            "stamp": stamp,
            "community_id": SYMLIV_COMMUNITY_ID,
            "import_type": import_type,
            "mode": mode,
            "source_path": str(csv_path),
            "row_count": len(rows),
            "ok": result.get("ok"),
            "summary": result.get("summary"),
            "graphql_errors": result.get("graphql_errors"),
        }
        (folder / "manifest.json").write_text(
            _json.dumps(manifest, indent=2, default=str)
        )
        (folder / "result.json").write_text(
            _json.dumps(result, indent=2, default=str)
        )
        # Append to timeline (single source of chronological truth)
        timeline = _community_dir() / "timeline.jsonl"
        with open(timeline, "a") as f:
            f.write(_json.dumps(manifest, default=str) + "\n")
        # Compute diff vs the previous archive of the same type
        diff = _compute_repair_diff(import_type, rows, folder, stamp)
        return {"archive_dir": str(folder), "diff": diff}
    except Exception as e:
        # Logged, swallowed — never break the import path
        logger.exception("Archive failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Tools — discovery
# ---------------------------------------------------------------------------

# ---- Discovery Tool: list_import_types ----
# Provides a reference of all available import types so the MCP caller (e.g.,
# Claude) can determine which imports exist, what columns they need, and in
# what order they should be run.
@mcp.tool()
def list_import_types() -> dict[str, Any]:
    """List every supported import type, with its prerequisites and the
    recommended order. Call this first if you're unsure which tool to use."""
    return {
        "recommended_order": RECOMMENDED_ORDER,
        "types": [
            {
                "name": name,
                "mutation": IMPORT_MUTATIONS[name],
                "required_columns": sorted(REQUIRED_COLUMNS.get(name, [])),
            }
            for name in RECOMMENDED_ORDER
        ],
    }


# ---- Discovery Tool: get_pass_infos ----
# Before importing passes (vendor_passes, resident_passes, host_guest_passes),
# you need PassInfo UUIDs from the community's pass builder. This tool queries
# the API for those IDs so the caller can populate the passes.passInfoId column.
@mcp.tool()
async def get_pass_infos(
    portal: Annotated[
        Literal["resident", "vendor", "host", "any"],
        Field(description="Filter to one portal, or 'any' for all."),
    ] = "any",
) -> list[dict[str, Any]]:
    """Look up PassInfo UUIDs from the pass builder. Required before any
    pass import (resident_passes, vendor_passes, host_guest_passes)."""
    # Query all completed pass definitions from the community's pass builder.
    # The real schema field is getPassInfosByCommunity(complete, communityId),
    # which returns a wrapper { success, data[], error } rather than a raw array.
    query = """
        query GetPassInfos($complete: Boolean!, $communityId: String!) {
          getPassInfosByCommunity(complete: $complete, communityId: $communityId) {
            success
            error
            data { passInfoId name portal }
          }
        }
    """
    data = await _graphql(
        query, {"complete": True, "communityId": SYMLIV_COMMUNITY_ID}
    )
    payload = data.get("data", {}).get("getPassInfosByCommunity", {}) or {}
    infos = payload.get("data", []) or []
    # Optionally filter by portal type (resident, vendor, or host)
    if portal != "any":
        infos = [p for p in infos if p.get("portal") == portal]
    return infos


# ---- Discovery Tool: resolve_pass_info_id ----
# Look up a specific PassInfo UUID by its human-readable name. Pass-import
# CSVs often contain placeholder UUIDs that need to be replaced with the
# real value for the current community/environment — this tool wraps the
# lookup so callers can resolve "Resident RFID" or "Long Term Lease Barcode"
# without needing to know the UUID up front (and without needing to update
# CSVs when switching from staging to prod, where the UUIDs differ).
@mcp.tool()
async def resolve_pass_info_id(
    name: Annotated[
        str,
        Field(description="The human-readable PassInfo name from the pass "
                          "builder (e.g. 'Resident RFID', 'Long Term Lease "
                          "Barcode'). Matched case-insensitively."),
    ],
    portal: Annotated[
        Literal["resident", "vendor", "host", "guest", "visitor", "any"],
        Field(description="Restrict to a portal to disambiguate names that "
                          "appear in multiple portals."),
    ] = "any",
) -> dict[str, Any]:
    """Resolve a PassInfo UUID by name within the current community.

    Returns {"passInfoId": uuid, "portal": "resident", "name": "..."} on
    exact (case-insensitive) match. If no exact match, returns
    {"ok": False, "candidates": [...]} so the caller can disambiguate.
    """
    all_infos = await get_pass_infos(portal="any")
    needle = name.strip().lower()
    matches = [
        p for p in all_infos
        if (p.get("name") or "").strip().lower() == needle
        and (portal == "any" or p.get("portal") == portal)
    ]
    if len(matches) == 1:
        m = matches[0]
        return {
            "ok": True,
            "passInfoId": m.get("passInfoId"),
            "portal": m.get("portal"),
            "name": m.get("name"),
            "community_id": SYMLIV_COMMUNITY_ID,
        }
    if len(matches) > 1:
        return {
            "ok": False,
            "error": (f"Multiple PassInfos named {name!r} found "
                      f"(across portals). Filter by portal to disambiguate."),
            "candidates": matches,
        }
    # No exact match — return all candidates from the active portal filter
    # so the caller can see what's available and pick / fix the name.
    candidates = all_infos if portal == "any" else [
        p for p in all_infos if p.get("portal") == portal
    ]
    return {
        "ok": False,
        "error": f"No PassInfo named {name!r} in community {SYMLIV_COMMUNITY_ID!r}",
        "candidates": [
            {"passInfoId": c.get("passInfoId"),
             "name": c.get("name"),
             "portal": c.get("portal")}
            for c in candidates
        ],
    }


# ---- Discovery Tool: get_community_addresses ----
# Fetches all addresses already loaded into the community. Two main uses:
#   1. Post-import verification — confirm addresses imported correctly
#   2. CSV preparation — get communityAddressId UUIDs for resident_properties
#      CSV files that reference addresses by ID rather than string matching
@mcp.tool()
async def get_community_addresses() -> list[dict[str, Any]]:
    """List all existing community addresses with UUIDs. Useful for verifying
    address imports landed correctly, or for building resident_properties CSVs
    that use communityAddressId instead of string matching."""
    # Real schema field is getAllCommunityAddresses, returning a wrapper
    # { success, data[], error }. Note the address postal field is `zip`
    # (not `zipCode`).
    query = """
        query {
          getAllCommunityAddresses {
            success
            error
            data {
              communityAddressId address city state zip passesPerDay
            }
          }
        }
    """
    data = await _graphql(query, {})
    payload = data.get("data", {}).get("getAllCommunityAddresses", {}) or {}
    return payload.get("data", []) or []

# ---------------------------------------------------------------------------
# Tools — validation
# ---------------------------------------------------------------------------
# The validation tools answer two questions LOCALLY (no API round-trip):
#   1. validate_csv(import_type, file_path) — given a claimed type, is this
#      file actually importable? Returns counts + capped issue samples.
#   2. identify_import_type(file_path) — what is this file? Could it be
#      imported as anything we know about? Returns ranked candidates plus
#      an explicit "un-importable" verdict when nothing matches.
#
# Validation rules come from /Users/logan/Documents/symliv-data/data-docs/*.md
# (encoded into IMPORT_SCHEMAS above).

# ASCII-only email pattern — the playbook docs explicitly warn that
# non-UTF-8 / special-character emails will be rejected upstream, so we
# stay strict here. Matches "local@domain.tld" with standard local chars.
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# UUID v1–v5 (case insensitive). The yup schema uses `.uuid()` which accepts
# any 8-4-4-4-12 hex layout.
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# Spreadsheet error sentinels the docs call out by name as invalid for
# critical strings. Compared case-insensitive.
_FORBIDDEN_SENTINELS = {"#n/a", "#na", "#ref!", "#ref", "#name?", "#value!", "#null!"}


def _check_value(value: Any, dtype: str) -> str | None:
    """Validate a single cell against an IMPORT_SCHEMAS datatype tag.

    Returns None if the value is acceptable (including empty — emptiness is
    handled separately by the required-field check). Returns a short error
    code string ("invalid_email", "not_uuid", …) on failure.
    """
    if _is_empty(value):
        return None  # emptiness checked by required/required_or, not here
    s = str(value).strip()
    if dtype == "email":
        return None if _EMAIL_RE.match(s) else "invalid_email"
    if dtype == "uuid":
        return None if _UUID_RE.match(s) else "not_uuid"
    if dtype == "date":
        # Accept ISO 8601 (with or without time) and a few common variants
        # listed in CSV-Import-Guide.md. We don't try to be exhaustive — the
        # backend yup `date()` is permissive.
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ",
                    "%m/%d/%Y", "%m/%d/%y"):
            try:
                datetime.strptime(s, fmt); return None
            except ValueError:
                continue
        # Last-ditch: ISO 8601 with timezone via fromisoformat (Py3.11+)
        try:
            datetime.fromisoformat(s.replace("Z", "+00:00")); return None
        except ValueError:
            return "not_a_date"
    if dtype == "int":
        try:
            int(float(s)); return None
        except ValueError:
            return "not_an_integer"
    if dtype == "positive_int":
        try:
            n = int(float(s))
            return None if n > 0 else "not_positive"
        except ValueError:
            return "not_an_integer"
    if dtype == "number":
        try:
            float(s); return None
        except ValueError:
            return "not_a_number"
    if dtype == "bool":
        if s.lower() in {"true", "false", "1", "0", "yes", "no"}:
            return None
        return "not_a_boolean"
    return None  # unknown dtype tag → don't enforce


def _validate_rows(
    import_type: str,
    rows: list[dict[str, Any]],
    sample_size: int,
) -> dict[str, Any]:
    """Run all per-row checks defined in IMPORT_SCHEMAS[import_type].

    Returns a dict with totals and a capped sample of issues. Each issue is
    `{"row": N, "field": "...", "code": "...", "value": "..."}` so the
    caller can pinpoint exactly which cell to fix.
    """
    schema = IMPORT_SCHEMAS[import_type]
    required: list[str] = schema.get("required", [])
    required_or: list[list[str]] = schema.get("required_or", [])
    types: dict[str, str] = schema.get("types", {})
    enums: dict[str, set[str]] = schema.get("enums", {})
    critical: list[str] = schema.get("critical_string", [])

    issues: list[dict[str, Any]] = []
    rows_with_errors = 0
    total_issues = 0
    # Column-level blank tracking: numeric-typed columns whose cells are
    # blank cast to NaN at commit time (we hit this with vehicle.year and
    # passes.facilityCode). Track blank counts per typed column so we can
    # warn the caller to either drop the column or default the values.
    NUMERIC_DTYPES = {"int", "positive_int", "number"}
    blank_counts: dict[str, int] = {
        col: 0 for col, dt in types.items() if dt in NUMERIC_DTYPES
    }

    def add(row_n: int, field: str, code: str, value: Any) -> None:
        nonlocal total_issues
        total_issues += 1
        if len(issues) < sample_size:
            issues.append({"row": row_n, "field": field, "code": code,
                           "value": value})

    for i, row in enumerate(rows, start=2):  # row 2 = first data row
        row_errs = 0
        # Required columns: must be present AND non-empty in this row
        for col in required:
            v = _get_path(row, col)
            if _is_empty(v):
                add(i, col, "required_field_empty", v); row_errs += 1
        # required_or groups: at least one column must be non-empty
        for group in required_or:
            if not any(not _is_empty(_get_path(row, c)) for c in group):
                add(i, " OR ".join(group), "required_or_group_empty", None)
                row_errs += 1
        # Critical strings: reject spreadsheet error sentinels and whitespace
        for col in critical:
            v = _get_path(row, col)
            if _is_empty(v):
                continue  # already flagged above
            sv = str(v).strip()
            if sv.lower() in _FORBIDDEN_SENTINELS:
                add(i, col, "forbidden_sentinel", v); row_errs += 1
        # Datatype checks (skipped if empty — emptiness handled above)
        for col, dtype in types.items():
            v = _get_path(row, col)
            # Track blank cells in numeric columns (will NaN-cast at commit)
            if dtype in NUMERIC_DTYPES and _is_empty(v) and col in row:
                blank_counts[col] += 1
            code = _check_value(v, dtype)
            if code:
                add(i, col, code, v); row_errs += 1
        # Enum checks (case-sensitive; empty allowed)
        for col, allowed in enums.items():
            v = _get_path(row, col)
            if _is_empty(v):
                continue
            sv = str(v).strip()
            if sv not in allowed:
                add(i, col, "not_in_enum", v); row_errs += 1
        if row_errs:
            rows_with_errors += 1

    # Numeric columns with any blanks → commit-time NaN error. Emit as a
    # warning (not a hard error) since technically every row is well-formed.
    numeric_blank_warnings = [
        {"field": col, "blank_rows": n,
         "fix": f"Drop the {col} column from the CSV, or fill blanks with a default."}
        for col, n in blank_counts.items() if n > 0
    ]

    return {
        "row_count": len(rows),
        "rows_with_errors": rows_with_errors,
        "total_issues": total_issues,
        "issues_sample": issues,
        "warnings": {
            "numeric_columns_with_blanks": numeric_blank_warnings,
        },
    }


def _score_headers_against_schema(
    headers: set[str], import_type: str,
) -> dict[str, Any]:
    """Score how well a header set matches a given import_type's schema.
    Used by `identify_import_type` to rank candidates.

    Returns coverage of required columns, presence of at least one required_or
    column per group, and a verdict:
      - "importable":   all required + required_or columns are present
      - "near_miss":    >=50% of required present; treat as a candidate to fix
      - "no_match":     <50% required present
    """
    schema = IMPORT_SCHEMAS[import_type]
    required: list[str] = schema.get("required", [])
    required_or: list[list[str]] = schema.get("required_or", [])
    types: dict[str, str] = schema.get("types", {})
    enums: dict[str, set[str]] = schema.get("enums", {})

    missing_required = [c for c in required if c not in headers]
    missing_or_groups = [g for g in required_or
                         if not any(c in headers for c in g)]

    coverage = (
        (len(required) - len(missing_required)) / len(required)
        if required else 1.0
    )

    # Overlap floor — guards against false positives on schemas that have
    # very few required columns (e.g. vendor_vehicles only requires one of
    # an OR-group). If the headers don't share ANY known column with this
    # schema (required + types + enums + or-group members), it's not even
    # a near-miss; it's a totally unrelated file.
    known_cols: set[str] = (
        set(required) | set(types) | set(enums)
        | {c for g in required_or for c in g}
    )
    overlap = len(headers & known_cols)

    if not missing_required and not missing_or_groups:
        verdict = "importable"
    elif overlap == 0:
        verdict = "no_match"
    elif coverage >= 0.5:
        verdict = "near_miss"
    else:
        verdict = "no_match"

    return {
        "import_type": import_type,
        "verdict": verdict,
        "required_coverage": round(coverage, 2),
        "header_overlap": overlap,
        "missing_required": missing_required,
        "missing_or_groups": missing_or_groups,
    }


# ---- Validation Tool: validate_csv ----
@mcp.tool()
def validate_csv(
    import_type: Annotated[str, Field(description="One of: " + ", ".join(IMPORT_MUTATIONS))],
    csv_path:    Annotated[str, Field(description="Absolute path to a .csv or .xlsx file.")],
    sample_size: Annotated[
        int,
        Field(description="Cap on issues_sample length (totals are exact)."),
    ] = 25,
) -> dict[str, Any]:
    """Deep local validation for a CSV or XLSX file against a claimed
    `import_type`. Confirms headers, required columns, required-OR groups,
    per-cell types (email/UUID/date/int/enum), and rejects forbidden
    spreadsheet sentinels (#N/A, #REF!) in critical strings. NO API call.

    Returns ok=True only if the file has zero issues. ok=False means the
    file is not importable as-is for this type — see issues_sample for
    row-level details, missing_required_columns for header-level gaps.
    """
    logger.info("validate_csv: type=%s path=%s", import_type, csv_path)
    if import_type not in IMPORT_MUTATIONS:
        return {"ok": False, "error": f"Unknown import_type '{import_type}'"}
    if not Path(csv_path).exists():
        return {"ok": False, "error": f"File not found: {csv_path}"}

    # Read all rows (handles both .csv and .xlsx via _read_rows)
    try:
        rows = _read_rows(csv_path)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    headers = set(rows[0].keys()) if rows else set()
    schema = IMPORT_SCHEMAS.get(import_type, {})

    # Header-level: which required columns are entirely absent?
    missing_required_columns = sorted(
        c for c in schema.get("required", []) if c not in headers
    )
    missing_or_groups = [
        g for g in schema.get("required_or", [])
        if not any(c in headers for c in g)
    ]

    # If headers are wrong we don't bother with row-level checks — the file
    # is structurally un-importable as-is. Surface the gaps and stop.
    if missing_required_columns or missing_or_groups:
        return {
            "ok": False,
            "import_type": import_type,
            "headers": sorted(headers),
            "row_count": len(rows),
            "missing_required_columns": missing_required_columns,
            "missing_or_groups": missing_or_groups,
            "next_step": "Add the missing column(s) and re-validate.",
        }

    # Row-level deep validation
    result = _validate_rows(import_type, rows, sample_size)

    warnings = result.get("warnings", {}) or {}
    blank_warns = warnings.get("numeric_columns_with_blanks") or []
    next_step = (
        "Fix the cells listed in issues_sample, then re-validate."
        if result["total_issues"] > 0
        else (
            "WARNING: numeric columns have blank cells — commit will fail with "
            "NaN cast errors. See warnings.numeric_columns_with_blanks."
            if blank_warns
            else "Call dry_run_import to validate against the SymLiv API."
        )
    )
    return {
        "ok": result["total_issues"] == 0,
        "import_type": import_type,
        "headers": sorted(headers),
        "row_count": result["row_count"],
        "rows_with_errors": result["rows_with_errors"],
        "total_issues": result["total_issues"],
        "issues_sample": result["issues_sample"],
        "warnings": warnings,
        "next_step": next_step,
    }


# ---- Validation Tool: identify_import_type ----
@mcp.tool()
def identify_import_type(
    file_path: Annotated[str, Field(description="Absolute path to a .csv or .xlsx file.")],
) -> dict[str, Any]:
    """Given any file, decide whether it looks like a SymLiv import — and if
    so, which type. Returns ranked candidates. Use this when you don't yet
    know what `import_type` to pass to validate_csv/dry_run_import, or when
    you want to triage an arbitrary file from a customer.

    The top-level verdict is one of:
      - "importable":      at least one type matches all required columns.
                           See `best_match` for the type to use.
      - "near_miss":       file is plausibly one of the known types but is
                           missing some required columns. See `candidates`
                           for what's needed to fix.
      - "un-importable":   headers don't resemble any known import type;
                           this file is NOT a SymLiv import as-is.
    """
    logger.info("identify_import_type: path=%s", file_path)
    if not Path(file_path).exists():
        return {"ok": False, "error": f"File not found: {file_path}"}
    try:
        rows = _read_rows(file_path)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    headers = set(rows[0].keys()) if rows else set()
    scored = [_score_headers_against_schema(headers, t) for t in IMPORT_SCHEMAS]
    # Rank: importable > near_miss > no_match, then by coverage desc
    rank = {"importable": 0, "near_miss": 1, "no_match": 2}
    scored.sort(key=lambda s: (rank[s["verdict"]], -s["required_coverage"]))

    importable = [s for s in scored if s["verdict"] == "importable"]
    near_miss = [s for s in scored if s["verdict"] == "near_miss"]
    no_match = [s for s in scored if s["verdict"] == "no_match"]

    if importable:
        # If multiple types could match (rare; usually requires overlapping
        # column names), surface them all but pick the most-specific (the one
        # whose required set is the largest subset of the headers).
        best = max(importable, key=lambda s: len(IMPORT_SCHEMAS[s["import_type"]].get("required", [])))
        overall = "importable"
    elif near_miss:
        best = near_miss[0]
        overall = "near_miss"
    else:
        best = None
        overall = "un-importable"

    return {
        "ok": True,
        "verdict": overall,
        "best_match": best,
        "candidates": importable + near_miss,
        "rejected": [s["import_type"] for s in no_match],
        "row_count": len(rows),
        "header_count": len(headers),
    }

# ---------------------------------------------------------------------------
# Tools — execution
# ---------------------------------------------------------------------------

# Max rows per single GraphQL mutation, keyed by import_type. Large payloads
# routinely exceed the App Runner / proxy response timeout (~60s): the server
# keeps processing in the background and rows DO land, but the HTTP request
# returns empty / drops mid-flight, leaving the caller uncertain about state.
# Chunking keeps each call short enough to get a clean response back.
#
# Pass imports are slowest per row (the server creates pass + vehicle +
# registration documents each) — keep these small at 200. User and
# property/link imports are faster per row but a 1,800-row resident_users
# import still times out, so chunk those at 500.
CHUNK_SIZES: dict[str, int] = {
    "host_users":          500,
    "resident_users":      500,
    "host_rental_units":   500,
    "resident_properties": 500,
    "vendor_users":        500,
    "guest_users":         500,
    "resident_passes":     200,
    "vendor_passes":       200,
    "host_guest_passes":   200,
}


def _split_csv_into_chunks(csv_path: str, chunk_size: int) -> list[bytes]:
    """Split a CSV file into N chunks of ≤ chunk_size rows each (preserving
    the header on every chunk). Returns a list of UTF-8-encoded CSV bytes."""
    raw = Path(csv_path).read_text()
    lines = raw.splitlines(keepends=True)
    if not lines:
        return []
    header = lines[0]
    data_lines = lines[1:]
    chunks = []
    for i in range(0, len(data_lines), chunk_size):
        body = header + "".join(data_lines[i:i + chunk_size])
        chunks.append(body.encode("utf-8"))
    return chunks


async def _submit_one_batch(
    mutation_name: str,
    csv_bytes: bytes,
    filename: str,
    no_mutation: bool,
) -> dict[str, Any]:
    """Submit a single CSV payload to the SymLiv import endpoint and return
    the raw GraphQL response. Caller is responsible for parsing/aggregating.
    """
    import base64
    csv_b64 = base64.b64encode(csv_bytes).decode("ascii")
    mutation = f"""
        mutation Run($filePath: String!, $originalFileName: String!,
                     $noMutation: Boolean) {{
          {mutation_name}(
            filePath: $filePath,
            originalFileName: $originalFileName,
            noMutation: $noMutation
          ) {{ success error data }}
        }}
    """
    return await _graphql(mutation, {
        "filePath": csv_b64,
        "originalFileName": filename,
        "noMutation": no_mutation,
    })


# Tokens in the per-row `logs` column that indicate the row actually
# succeeded. Used by _parse_per_row_results to disambiguate real failures
# from "Could not match address ---"-style warnings (which appear in the
# `errors` column even on successful upserts).
_SUCCESS_LOG_TOKENS = (
    "data validation passed",
    "successfully",
    "skipping mutation",
)

def _parse_per_row_results(
    data_str: str | None,
    sample_size: int = 5,
) -> dict[str, Any]:
    """Parse the per-row CSV result that SymLiv import mutations return.

    The mutation's `data` field is a CSV with the original headers plus
    two appended columns: `logs` and `errors`. A row is considered OK if
    the `logs` column contains a success token ("Data validation passed",
    "successfully", or "Skipping mutation" for dry-runs). The `errors`
    column on a successful row may still contain warnings (e.g. "Could
    not match address ---") — those are NOT counted as failures.

    Returns:
        {
          "row_count":     int   # total rows after header
          "ok_count":      int   # rows with success token in logs
          "err_count":     int   # rows with no success token
          "warning_count": int   # rows that succeeded but had non-empty errors
          "sample_errors": list  # up to `sample_size` failing rows (verbatim)
          "format":        "csv" | "json" | "raw"
        }
    """
    out = {"row_count": 0, "ok_count": 0, "err_count": 0,
           "warning_count": 0, "sample_errors": [], "format": "raw"}
    if not data_str or not isinstance(data_str, str):
        return out

    s = data_str.strip()
    # Some endpoints (or future versions) may return a JSON summary instead.
    if s.startswith(("{", "[")):
        try:
            parsed = _json.loads(s)
            out["format"] = "json"
            if isinstance(parsed, dict):
                out["ok_count"] = int(parsed.get("successCount") or 0)
                out["err_count"] = int(parsed.get("errorCount") or 0)
                out["row_count"] = out["ok_count"] + out["err_count"]
                errs = parsed.get("errors") or []
                if isinstance(errs, list):
                    out["sample_errors"] = errs[:sample_size]
            return out
        except _json.JSONDecodeError:
            pass  # fall through to CSV parsing

    # CSV path
    import csv as _csv
    lines = s.split("\n")
    if len(lines) <= 1:
        return out
    out["format"] = "csv"
    try:
        header = next(_csv.reader([lines[0]]))
    except Exception:
        return out
    try:
        logs_idx = header.index("logs")
    except ValueError:
        logs_idx = -1
    try:
        err_idx = header.index("errors")
    except ValueError:
        err_idx = -1

    for ln in lines[1:]:
        if not ln.strip():
            continue
        out["row_count"] += 1
        try:
            cols = next(_csv.reader([ln]))
        except Exception:
            out["err_count"] += 1
            if len(out["sample_errors"]) < sample_size:
                out["sample_errors"].append(ln[:300])
            continue
        logs = (cols[logs_idx] if 0 <= logs_idx < len(cols) else "").lower()
        errs = (cols[err_idx]  if 0 <= err_idx  < len(cols) else "").strip()
        succeeded = any(tok in logs for tok in _SUCCESS_LOG_TOKENS)
        if succeeded:
            out["ok_count"] += 1
            if errs:
                out["warning_count"] += 1
        else:
            out["err_count"] += 1
            if len(out["sample_errors"]) < sample_size:
                out["sample_errors"].append(ln[:300])
    return out


async def _run_import(
    import_type: str,
    csv_path: str,
    no_mutation: bool,
) -> dict[str, Any]:
    """Core import execution shared by dry_run_import and commit_import.

    Pass imports (resident_passes, vendor_passes, host_guest_passes) are
    chunked at CHUNK_SIZES[import_type] rows per call to avoid the upstream
    HTTP timeout that silently truncates response bodies on big payloads
    (see project memory: symliv-pass-import-timeout). All other types run
    single-shot.

    Args:
        import_type: Key from IMPORT_MUTATIONS (e.g. "resident_users").
        csv_path:    Absolute filesystem path to the CSV file.
        no_mutation: If True, the API validates but does NOT write any records
                     (dry-run mode). If False, records are actually created.

    Returns a dict with "ok" (bool), import metadata, and a "summary" containing
    successCount, errorCount, and per-row error details from the API. When
    chunked, "summary.chunks" lists each batch's result and the top-level
    successCount/errorCount are aggregated.
    """
    mode = "DRY RUN" if no_mutation else "COMMIT"
    logger.info("_run_import [%s]: type=%s path=%s", mode, import_type, csv_path)

    # Basic input validation — same checks as validate_csv
    if import_type not in IMPORT_MUTATIONS:
        return {"ok": False, "error": f"Unknown import_type '{import_type}'"}
    if not Path(csv_path).exists():
        return {"ok": False, "error": f"File not found: {csv_path}"}

    mutation_name = IMPORT_MUTATIONS[import_type]
    original_filename = Path(csv_path).name
    chunk_size = CHUNK_SIZES.get(import_type, 0)

    # --- chunked path (pass imports) -----------------------------------
    if chunk_size > 0:
        chunks = _split_csv_into_chunks(csv_path, chunk_size)
        logger.info("_run_import [%s] chunking %s into %d batches of ≤%d rows",
                    mode, import_type, len(chunks), chunk_size)
        chunk_results: list[dict[str, Any]] = []
        total_success = 0
        total_errors = 0
        any_graphql_error = False
        for i, batch in enumerate(chunks, start=1):
            batch_name = f"{Path(original_filename).stem}.batch{i:03d}.csv"
            logger.info("  batch %d/%d (%d rows)", i, len(chunks),
                        batch.count(b"\n") - 1)
            data = await _submit_one_batch(
                mutation_name, batch, batch_name, no_mutation,
            )
            if "errors" in data:
                any_graphql_error = True
                chunk_results.append({"batch": i, "graphql_errors": data["errors"]})
                continue
            payload = data.get("data", {}).get(mutation_name, {}) or {}
            raw = payload.get("data")
            parsed = _parse_per_row_results(raw)
            total_success += parsed["ok_count"]
            total_errors  += parsed["err_count"]
            chunk_results.append({
                "batch": i, "rows": batch.count(b"\n") - 1,
                "server_success": bool(payload.get("success")),
                "server_error": payload.get("error"),
                "ok_count": parsed["ok_count"],
                "err_count": parsed["err_count"],
                "warning_count": parsed["warning_count"],
                "sample_errors": parsed["sample_errors"],
            })

        aggregate_ok = (not any_graphql_error
                        and all(c.get("server_success") for c in chunk_results)
                        and total_errors == 0)
        result = {
            "ok": aggregate_ok,
            "dry_run": no_mutation,
            "import_type": import_type,
            "mutation": mutation_name,
            "chunked": True,
            "chunk_size": chunk_size,
            "chunk_count": len(chunks),
            "summary": {
                "successCount": total_success,
                "errorCount": total_errors,
                "chunks": chunk_results,
            },
        }
        archived = _archive_import(
            import_type, csv_path,
            "dry_run" if no_mutation else "commit", result,
        )
        if archived:
            result["archive"] = archived
        return result

    # --- single-call path (everything else) ----------------------------
    csv_bytes = Path(csv_path).read_bytes()
    data = await _submit_one_batch(
        mutation_name, csv_bytes, original_filename, no_mutation,
    )

    # Check for top-level GraphQL errors (auth failures, schema errors, etc.)
    if "errors" in data:
        result = {"ok": False, "graphql_errors": data["errors"],
                  "dry_run": no_mutation, "import_type": import_type}
        # Archive even on auth/schema failure — the operator wants the full
        # chronological story, not just the happy path.
        archived = _archive_import(
            import_type, csv_path,
            "dry_run" if no_mutation else "commit", result,
        )
        if archived:
            result["archive"] = archived
        return result

    # Extract the mutation result payload. StringResponse returns
    # { success: bool, data: string?, error: string? }. The `data` field is
    # a per-row CSV with appended `logs`/`errors` columns (sometimes a JSON
    # summary instead). _parse_per_row_results handles both shapes — check
    # `logs` for success tokens first, then count remaining as real errors.
    payload = data.get("data", {}).get(mutation_name, {}) or {}
    server_success = bool(payload.get("success"))
    server_error = payload.get("error")
    raw_data = payload.get("data")
    parsed = _parse_per_row_results(raw_data)
    # Import is `ok` only if server reported success AND zero per-row errors.
    ok = server_success and parsed["err_count"] == 0
    logger.info(
        "_run_import [%s] result: server_success=%s ok=%s rows=%d ok_rows=%d "
        "err_rows=%d warn_rows=%d",
        mode, server_success, ok, parsed["row_count"],
        parsed["ok_count"], parsed["err_count"], parsed["warning_count"],
    )
    if not ok:
        logger.warning("Import not ok. server_error=%s sample_errors=%s",
                       server_error, parsed["sample_errors"][:3])
    result = {
        "ok": ok,
        "dry_run": no_mutation,
        "import_type": import_type,
        "mutation": mutation_name,
        "summary": {
            "row_count":     parsed["row_count"],
            "successCount":  parsed["ok_count"],
            "errorCount":    parsed["err_count"],
            "warningCount":  parsed["warning_count"],
            "sample_errors": parsed["sample_errors"],
            "format":        parsed["format"],
        },
        "server_success": server_success,
        "server_error": server_error,
    }
    # Snapshot every attempt — successful or not, dry-run or commit — into
    # the community workspace and compute a diff vs. the previous attempt
    # of this same import_type so repair history is recoverable later.
    archived = _archive_import(
        import_type, csv_path,
        "dry_run" if no_mutation else "commit", result,
    )
    if archived:
        result["archive"] = archived
    return result


# ---- Execution Tool: dry_run_import ----
# Sends the CSV to the SymLiv API with noMutation=True. The API parses and
# validates every row but does NOT create any records. Use this to catch
# data-level issues (invalid emails, duplicate entries, bad FK references)
# before committing. Always call this before commit_import.
@mcp.tool()
async def dry_run_import(
    import_type: Annotated[str, Field(description="One of the supported import types.")],
    csv_path:    Annotated[str, Field(description="Absolute path to the CSV.")],
) -> dict[str, Any]:
    """Validate a CSV against the live SymLiv API WITHOUT writing anything.
    This is the safe step — always run it before commit_import. The response
    includes per-row errors with the row number and field name."""
    return await _run_import(import_type, csv_path, no_mutation=True)


# ---- Execution Tool: commit_import ----
# The "real" import — sends the CSV with noMutation=False, causing the API to
# actually create/update records in the database. Includes a safety gate:
# the caller MUST set i_have_dry_run=True, which acts as a confirmation that
# a dry run was already performed and passed. This prevents accidental writes.
@mcp.tool()
async def commit_import(
    import_type:    Annotated[str, Field(description="One of the supported import types.")],
    csv_path:       Annotated[str, Field(description="Absolute path to the CSV.")],
    i_have_dry_run: Annotated[
        bool,
        Field(description="Set to True only after a successful dry_run_import."),
    ] = False,
) -> dict[str, Any]:
    """Actually write records to the SymLiv database. Refuses to run unless
    `i_have_dry_run` is True — this forces the caller to validate first."""
    # Safety gate: refuse to write if the caller hasn't confirmed a dry run
    if not i_have_dry_run:
        return {
            "ok": False,
            "error": (
                "Refusing to commit without a dry run. Call dry_run_import "
                "first, confirm errorCount is 0, then re-call with "
                "i_have_dry_run=True."
            ),
        }
    # Execute the actual import (noMutation=False means records ARE written)
    return await _run_import(import_type, csv_path, no_mutation=False)


# ---------------------------------------------------------------------------
# Tools — orchestration
# ---------------------------------------------------------------------------

# ---- Orchestration Tool: run_pipeline ----
# The high-level "do everything" tool. Accepts a batch of CSV files for
# multiple import types and runs them in the correct dependency order.
# Each step is always dry-run first; if commit=True and the dry run passes,
# the step is then committed before proceeding to the next.
# The pipeline aborts immediately on the first failure to avoid creating
# orphaned records (e.g., passes without their parent user accounts).
@mcp.tool()
async def run_pipeline(
    files: Annotated[
        dict[str, str],
        Field(description="Map of import_type -> absolute CSV path. "
                          "Order doesn't matter; the server sorts them."),
    ],
    commit: Annotated[
        bool,
        Field(description="If False, dry-run every step. If True, dry-run "
                          "then commit each step; abort the pipeline on the "
                          "first step with errors."),
    ] = False,
) -> dict[str, Any]:
    """Run a multi-step import in the SymLiv-recommended order. Each step is
    dry-run first; if `commit=True` and the dry run is clean, it's committed
    before moving on. The pipeline aborts at the first step with errors,
    preserving the foreign-key chain."""

    # Sort the provided files into RECOMMENDED_ORDER, ignoring any types
    # not present in the input dict. This ensures correct dependency order
    # regardless of the order the caller provides them.
    ordered = [(t, files[t]) for t in RECOMMENDED_ORDER if t in files]
    logger.info("run_pipeline: %d steps, commit=%s, order=%s",
                len(ordered), commit, [t for t, _ in ordered])
    results: list[dict[str, Any]] = []

    for import_type, path in ordered:
        # Always dry-run first to validate the CSV against the live API
        dry = await _run_import(import_type, path, no_mutation=True)
        step: dict[str, Any] = {"import_type": import_type, "dry_run": dry}

        # If the dry run fails, abort the entire pipeline — later steps
        # likely depend on records this step would have created.
        if not dry["ok"]:
            step["aborted"] = True
            results.append(step)
            break

        # If commit mode is on and dry run passed, write the records for real
        if commit:
            live = await _run_import(import_type, path, no_mutation=False)
            step["commit"] = live
            # Abort on commit failure too — downstream steps need these records
            if not live["ok"]:
                step["aborted"] = True
                results.append(step)
                break

        results.append(step)

    # Build a summary showing which steps ran, which were skipped (due to an
    # earlier abort), and the full per-step results.
    return {
        "ran": [r["import_type"] for r in results],
        "skipped": [t for t in RECOMMENDED_ORDER
                    if t in files and t not in [r["import_type"] for r in results]],
        "results": results,
    }


# ---------------------------------------------------------------------------
# MongoDB direct-access tools
# ---------------------------------------------------------------------------
# These tools query the SymLiv MongoDB database directly for data-quality
# checks that aren't exposed through the GraphQL API. They use READ-ONLY
# credentials and never modify data. Useful for pre-migration audits and
# post-migration verification.

from pymongo import MongoClient  # MongoDB driver for direct DB queries

# Connect to MongoDB. The URI + db come from the resolved config globals
# (env var → profiles.json → default). switch_context rebinds these at
# runtime, so we expose a helper to swap the client too.
def _connect_mongo(uri: str, db_name: str):
    """Create a MongoClient/db pair for the given URI/db. Used at init and
    re-used by switch_context when the active environment changes."""
    client = MongoClient(uri)
    return client, client[db_name]

if not SYMLIV_MONGO_URI:
    raise RuntimeError(
        "SYMLIV_MONGO_URI is not set in env, profiles.json, or built-in "
        "default. The MCP server cannot reach the database."
    )
_mongo, _db = _connect_mongo(SYMLIV_MONGO_URI, SYMLIV_MONGO_DB)


def _coll(name: str):
    """Return a collection handle, namespaced by community.

    SymLiv stores every community's data in a single database ("main") with
    collections prefixed by the community id, e.g. "pebblebeach.users" or
    "pebblebeach.resident_profiles" (see apps/admin-back resolvers and
    libs/backend/.../data-cleanup.ts). Callers pass the bare collection name
    (e.g. "resident_profiles") and this prepends the community prefix.
    """
    if not SYMLIV_COMMUNITY_ID:
        raise RuntimeError(
            "SYMLIV_COMMUNITY_ID env var is not set; cannot resolve the "
            "community-namespaced collection name."
        )
    return _db[f"{SYMLIV_COMMUNITY_ID}.{name}"]


# ---- MongoDB Tool: count_residents_missing_field ----
# Pre-migration data quality check. Counts how many resident profiles are
# missing a specific field (null, empty string, or not present at all).
# This helps estimate how much data cleanup is needed before importing.
# Only a whitelist of safe fields is allowed to prevent arbitrary queries.
@mcp.tool()
def count_residents_missing_field(field: str) -> dict[str, Any]:
    """Count resident profiles where `field` is empty or null. Use for
    pre-migration data quality checks (e.g. how many residents have no email).
    Read-only — does not modify any data."""
    logger.info("count_residents_missing_field: field=%s", field)
    # Whitelist of queryable fields — prevents arbitrary field access
    allowed = {"email", "phoneNumber", "mailingStreet", "emergencyPhoneNumber"}
    if field not in allowed:
        raise ValueError(f"field must be one of {allowed}")

    # Query the community's resident_profiles collection for documents where the
    # field is either missing entirely, set to an empty string, or explicitly null
    coll = _coll("resident_profiles")
    missing = coll.count_documents({
        "$or": [{field: {"$exists": False}}, {field: ""}, {field: None}]
    })
    return {"field": field, "missing_count": missing}


# ---- MongoDB Tool: find_orphan_addresses ----
# Post-migration cleanup helper. Uses a MongoDB aggregation pipeline with
# $lookup (left outer join) to find community addresses that have no
# associated resident properties. These orphans may indicate failed imports
# or addresses that were added but never linked to any residents.
@mcp.tool()
def find_orphan_addresses() -> list[dict[str, Any]]:
    """Find community addresses with no linked residents or rental units.
    Useful for cleaning up after a migration. Read-only."""
    logger.info("find_orphan_addresses called")
    pipeline = [
        # Left-join community_addresses → resident_properties on communityAddressId.
        # The $lookup target must also be community-namespaced.
        {"$lookup": {
            "from": f"{SYMLIV_COMMUNITY_ID}.resident_properties",
            "localField": "communityAddressId",
            "foreignField": "communityAddressId",
            "as": "residents",  # Array of matched resident properties
        }},
        # Keep only addresses with zero matches (orphans)
        {"$match": {"residents": {"$size": 0}}},
        # Return only the address string and its UUID (exclude _id and residents array)
        {"$project": {"_id": 0, "address": 1, "communityAddressId": 1}},
    ]
    return list(_coll("community_addresses").aggregate(pipeline))


# ---- MongoDB Tool: diff_against_db ----
# Compute net-new rows for a CSV vs the currently-active community in Mongo.
# Mirrors the ad-hoc diff scripts written for Champions Gate and Watersound
# pre-import: returns net-new / already-present / FK-unresolvable counts so
# you can know what an import would actually add (or skip) before committing.

def _norm_addr_for_diff(x: Any) -> str:
    """Normalize an address for set-comparison: lowercase, collapse spaces,
    strip punctuation, expand common abbreviations. Mirrors the canonicalizer
    used by build_clean_import.py so diffs are consistent."""
    if x is None: return ""
    a = str(x).strip().lower()
    if not a: return ""
    a = re.sub(r"\s+", " ", a)
    a = re.sub(r"[\.,]", "", a)
    a = re.sub(r"\s+#\s*", " # ", a)
    for pat, val in {r"\bdr\b":"drive", r"\brd\b":"road", r"\bln\b":"lane",
                     r"\bct\b":"court", r"\bpl\b":"place", r"\bave\b":"avenue",
                     r"\bblvd\b":"boulevard", r"\bst\b":"street",
                     r"\btrl\b":"trail", r"\bcir\b":"circle"}.items():
        a = re.sub(pat, val, a)
    return re.sub(r"\s+", " ", a).strip()


@mcp.tool()
def diff_against_db(
    import_type: Annotated[
        str,
        Field(description="One of: community_addresses, resident_users, "
                          "host_users, vendor_users, guest_users, "
                          "resident_properties, host_rental_units, "
                          "resident_passes, vendor_passes, host_guest_passes"),
    ],
    csv_path: Annotated[str, Field(description="Absolute path to a .csv or .xlsx file.")],
    sample_size: Annotated[
        int,
        Field(description="Cap on the sample lists returned per category."),
    ] = 15,
) -> dict[str, Any]:
    """Compare a CSV against the active community's Mongo to compute net-new
    counts before importing. No writes — read-only.

    Returns per-category counts:
      - in_file:        unique keys in the CSV
      - in_db:          keys already present in Mongo
      - both:           overlap
      - net_new_to_db:  keys in CSV not in Mongo (what an import would ADD)
      - in_db_only:     keys in Mongo missing from CSV (potentially orphaned)
      - fk_unresolvable (link tables only): rows whose foreign-key references
        don't exist in Mongo and would fail at commit
      - sample_net_new / sample_orphans: capped previews

    The "natural key" used depends on import_type:
      - *_users:           normalized email
      - community_addresses: normalized address
      - resident_properties/host_rental_units: (email, normalized address)
      - resident_passes/vendor_passes: externalCredentialNumber
    """
    if import_type not in IMPORT_MUTATIONS:
        return {"ok": False, "error": f"Unknown import_type {import_type!r}"}
    if not Path(csv_path).exists():
        return {"ok": False, "error": f"File not found: {csv_path}"}
    try:
        rows = _read_rows(csv_path)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    def ne(v: Any) -> str:  # normalize email
        return str(v).strip().lower() if v else ""

    # Build a uid → email index once if we'll need it (link tables, passes)
    def build_indexes() -> tuple[dict[str, str], dict[str, str]]:
        uid_to_email = {
            u["userId"]: ne(u.get("email", ""))
            for u in _coll("users").find({}, {"email": 1, "userId": 1, "_id": 0})
            if u.get("email")
        }
        caid_to_addr = {
            a["communityAddressId"]: a.get("address", "")
            for a in _coll("community_addresses").find(
                {}, {"communityAddressId": 1, "address": 1, "_id": 0}
            )
        }
        return uid_to_email, caid_to_addr

    # ---- by import_type ----------------------------------------------
    if import_type == "community_addresses":
        file_keys = {_norm_addr_for_diff(r.get("address")) for r in rows
                     if r.get("address")}
        db_keys = {_norm_addr_for_diff(a.get("address"))
                   for a in _coll("community_addresses").find(
                       {}, {"address": 1, "_id": 0}) if a.get("address")}
        return _diff_summary(import_type, "address", file_keys, db_keys, sample_size)

    if import_type in ("resident_users", "host_users", "vendor_users", "guest_users"):
        file_keys = {ne(r.get("user.email")) for r in rows
                     if r.get("user.email")}
        # Use role-specific membership if we can
        if import_type == "resident_users":
            uids = {p["userId"] for p in _coll("resident_profiles").find(
                {}, {"userId": 1, "_id": 0})}
            uid_to_email, _ = build_indexes()
            db_keys = {uid_to_email.get(u) for u in uids if uid_to_email.get(u)}
        elif import_type == "host_users":
            uids = {h["userId"] for h in _coll("host_infos").find(
                {}, {"userId": 1, "_id": 0})}
            uid_to_email, _ = build_indexes()
            db_keys = {uid_to_email.get(u) for u in uids if uid_to_email.get(u)}
        elif import_type == "vendor_users":
            db_keys = {ne(v.get("companyName", "")) for v in
                       _coll("company_infos").find({}, {"companyName": 1, "_id": 0})
                       if v.get("companyName")}
            file_keys = {ne(r.get("company.companyName"))
                         for r in rows if r.get("company.companyName")}
        else:  # guest_users
            uids = {g["userId"] for g in _coll("guest_infos").find(
                {}, {"userId": 1, "_id": 0})}
            uid_to_email, _ = build_indexes()
            db_keys = {uid_to_email.get(u) for u in uids if uid_to_email.get(u)}
        return _diff_summary(import_type,
                             "company" if import_type == "vendor_users" else "email",
                             file_keys, db_keys, sample_size)

    if import_type in ("resident_properties", "host_rental_units"):
        coll, fk_col = (
            ("resident_properties", "user.address")
            if import_type == "resident_properties"
            else ("rental_units", "rentalUnit.address")
        )
        uid_to_email, caid_to_addr = build_indexes()
        db_keys = set()
        for d in _coll(coll).find({}, {"userId": 1, "communityAddressId": 1, "_id": 0}):
            em = uid_to_email.get(d.get("userId"))
            a = caid_to_addr.get(d.get("communityAddressId"))
            if em and a:
                db_keys.add((em, _norm_addr_for_diff(a)))
        file_pairs = [(ne(r.get("user.email")), _norm_addr_for_diff(r.get(fk_col)))
                      for r in rows if r.get("user.email") and r.get(fk_col)]
        file_keys = set(file_pairs)
        # FK check: addresses on the CSV must exist in community_addresses
        norm_addrs = {_norm_addr_for_diff(a) for a in caid_to_addr.values()}
        fk_unresolvable = sum(1 for _, a in file_pairs if a and a not in norm_addrs)
        out = _diff_summary(import_type, "(email,address)", file_keys, db_keys, sample_size)
        out["fk_unresolvable_addresses"] = fk_unresolvable
        return out

    if import_type in ("resident_passes", "vendor_passes", "host_guest_passes"):
        # Pass external credential number — that's the natural dedupe key
        ecn_field = ("passes.externalCredentialNumber"
                     if import_type == "resident_passes"
                     else "pass.externalCredentialNumber")
        file_keys = {str(r.get(ecn_field, "")).strip()
                     for r in rows if r.get(ecn_field)}
        file_keys.discard("")
        db_keys = {str(p["externalCredentialNumber"]).strip()
                   for p in _coll("passes").find(
                       {"externalCredentialNumber": {"$exists": True, "$nin": [None, ""]}},
                       {"externalCredentialNumber": 1, "_id": 0})}
        return _diff_summary(import_type, "externalCredentialNumber",
                             file_keys, db_keys, sample_size)

    return {"ok": False, "error": f"Diff not implemented for import_type {import_type!r}"}


def _diff_summary(
    import_type: str, key_field: str,
    file_keys: set, db_keys: set, sample_size: int,
) -> dict[str, Any]:
    """Produce the standard diff response shape."""
    net_new = file_keys - db_keys
    orphans = db_keys - file_keys
    return {
        "ok": True,
        "community_id": SYMLIV_COMMUNITY_ID,
        "import_type": import_type,
        "key_field": key_field,
        "counts": {
            "in_file": len(file_keys),
            "in_db": len(db_keys),
            "both": len(file_keys & db_keys),
            "net_new_to_db": len(net_new),
            "in_db_only": len(orphans),
        },
        "sample_net_new": list(net_new)[:sample_size],
        "sample_orphans": list(orphans)[:sample_size],
    }


# ---------------------------------------------------------------------------
# Tools — upload verification
# ---------------------------------------------------------------------------
# Given a CSV or XLSX file that was (allegedly) uploaded via the import
# pipeline, confirm row-by-row that:
#   1. A corresponding record exists on the server (presence), and
#   2. Selected fields match between the file and the server (field match).
#
# Backend yup schemas live in
#   apps/admin-back/src/modules/DataImportService/schemas/
# and were used to derive each import_type's column layout. Server-side
# field names were sampled from sunnytown.* collections in staging.
#
# Field-match comparisons skip empty file values (yup .default(''))
# and lowercase/strip strings before comparing.


# Per-import-type verifier configuration. Each value is a dict the
# `_verify_simple` helper consumes. For relational imports (passes,
# properties, employees, etc.) we use dedicated functions instead, because
# the lookup requires a user-by-email join.
#
# `confidence`:
#   "tested"          – mapping was round-tripped against live staging data
#   "schema_derived"  – mapping derived from yup schema + sampled docs only
VERIFY_CONFIG: dict[str, dict[str, Any]] = {
    "community_addresses": {
        "collection": "community_addresses",
        # file column → mongo field used to LOOK UP the record
        "lookup": {"address": "address"},
        # file column → mongo field used to COMPARE values
        "compare": {
            "passesPerDay": "passesPerDay",
            "city": "city",
            "state": "state",
            "zipCode": "zip",   # field rename on community_addresses: zipCode → zip
        },
        "confidence": "tested",
    },
    "resident_users": {
        "collection": "users",
        "lookup": {"user.email": "email"},
        "compare": {
            "user.firstName": "firstName",
            "user.lastName": "lastName",
        },
        "confidence": "tested",
    },
    "vendor_users": {
        # Verifies the user side only; vendor company linkage in
        # company_infos is not checked here.
        "collection": "users",
        "lookup": {"user.email": "email"},
        "compare": {
            "user.firstName": "firstName",
            "user.lastName": "lastName",
        },
        "confidence": "schema_derived",
    },
    "host_users": {
        "collection": "users",
        "lookup": {"user.email": "email"},
        "compare": {
            "user.firstName": "firstName",
            "user.lastName": "lastName",
        },
        "confidence": "schema_derived",
    },
    "guest_users": {
        "collection": "users",
        "lookup": {"user.email": "email"},
        "compare": {
            "user.firstName": "firstName",
            "user.lastName": "lastName",
        },
        "confidence": "schema_derived",
    },
}


def _get_path(d: dict[str, Any], path: str) -> Any:
    """Read a value from a dict using dot-notation. CSV/xlsx readers return
    *flat* dicts with dotted keys (e.g. {'user.email': 'a@b'}), so we first
    try the literal key, then fall back to walking nested objects (in case
    a future caller hands us already-nested data)."""
    if path in d:
        return d[path]
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _build_user_email_index() -> dict[str, str]:
    """Return {email_lowercase: userId} for the configured community.

    Many relational imports key off user.email but store under userId. We
    build the index once per verify call and reuse it for every row, which
    is far cheaper than per-row queries on large CSVs.
    """
    idx: dict[str, str] = {}
    cur = _coll("users").find({}, {"email": 1, "userId": 1, "_id": 0})
    for u in cur:
        email = u.get("email")
        uid = u.get("userId")
        if email and uid:
            idx[email.strip().lower()] = uid
    return idx


def _verify_relational_pass(
    rows: list[dict[str, Any]],
    import_type: str,
    sample_size: int,
) -> dict[str, Any]:
    """Verify pass imports (resident_passes, vendor_passes, host_guest_passes).

    Lookup strategy: file → user.email → users.userId → passes documents
    matching {userId, passInfoId}. We treat a pass as present if any pass
    document with the same passInfoId exists for that user. Field-match
    compares only passInfoId presence here (start/end dates have timezone
    coercion that creates noise).
    """
    user_idx = _build_user_email_index()
    passes_coll = _coll("passes")

    # File field paths vary by import_type
    if import_type == "resident_passes":
        email_path = "user.email"
        info_path = "passes.passInfoId"
    elif import_type == "vendor_passes":
        # Vendor schema allows company / userEmail / companyEmail. We try
        # userEmail first, then companyEmail, then fall back to "missing".
        email_path = "userEmail"
        info_path = "pass.passInfoId"
    elif import_type == "host_guest_passes":
        # The pass is issued to the GUEST user (guestEmail), not the host.
        email_path = "guestEmail"
        info_path = "pass.passInfoId"
    else:
        raise ValueError(f"Unsupported relational pass type: {import_type}")

    matched = 0
    missing: list[dict[str, Any]] = []
    missing_count = 0
    for i, row in enumerate(rows, start=2):
        email_raw = _get_path(row, email_path)
        if not email_raw and import_type == "vendor_passes":
            # Fallback identifier for vendor passes
            email_raw = _get_path(row, "companyEmail")
        if not email_raw:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "no email/identifier in file"})
            continue
        uid = user_idx.get(str(email_raw).strip().lower())
        if not uid:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": f"no user with email {email_raw}"})
            continue
        info_id = _get_path(row, info_path)
        q = {"userId": uid, "passInfoId": info_id} if info_id else {"userId": uid}
        if passes_coll.find_one(q):
            matched += 1
        else:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "no pass matching userId+passInfoId", "query": q})

    return {
        "matched": matched,
        "missing_count": missing_count,
        "mismatched_count": 0,   # field compare is presence-only for passes
        "missing_sample": missing,
        "mismatched_sample": [],
    }


def _verify_resident_properties(rows: list[dict[str, Any]], sample_size: int) -> dict[str, Any]:
    """Resident properties: file links user.email → user.address. Two distinct
    on-server linkage patterns exist:
      (a) pre-import / legacy records denormalize the address as
          `street` (and `_street`) on the resident_property document
      (b) records inserted via parseResidentAddressCsvFile have NO `street`;
          they link to community_addresses solely via `communityAddressId`
    The verifier matches either pattern by resolving the raw user.address
    string to a communityAddressId once up front, and then accepting a
    resident_property as present if any of {street, _street, communityAddressId}
    points to that address."""
    user_idx = _build_user_email_index()
    # Build raw-address → communityAddressId index for the (b) pattern.
    # Uses _norm_lookup so the lookup key is trimmed (but case-preserved, since
    # addresses are stored mixed-case on the server).
    addr_to_caid: dict[str, str] = {}
    for a in _coll("community_addresses").find(
        {}, {"_id": 0, "address": 1, "communityAddressId": 1}
    ):
        if a.get("address") and a.get("communityAddressId"):
            key = _norm_lookup(a["address"], "address")
            if isinstance(key, str) and key:
                addr_to_caid[key] = a["communityAddressId"]
    props = _coll("resident_properties")
    matched = 0
    missing: list[dict[str, Any]] = []
    missing_count = 0
    for i, row in enumerate(rows, start=2):
        email = _get_path(row, "user.email")
        addr = _get_path(row, "user.address")
        if not email or not addr:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "missing email or address"})
            continue
        uid = user_idx.get(str(email).strip().lower())
        if not uid:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": f"no user with email {email}"})
            continue
        addr_key = _norm_lookup(addr, "address")
        target_caid = addr_to_caid.get(addr_key) if isinstance(addr_key, str) else None
        candidates = list(props.find(
            {"userId": uid},
            {"street": 1, "_street": 1, "communityAddressId": 1, "_id": 0},
        ))
        if any(
            _values_equal(addr, c.get("street"))
            or _values_equal(addr, c.get("_street"))
            or (target_caid and c.get("communityAddressId") == target_caid)
            for c in candidates
        ):
            matched += 1
        else:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "no resident_property matched by street or communityAddressId",
                                 "email": email, "address": addr,
                                 "target_caid": target_caid})
    return {"matched": matched, "missing_count": missing_count, "mismatched_count": 0,
            "missing_sample": missing, "mismatched_sample": []}


def _verify_host_rental_units(rows: list[dict[str, Any]], sample_size: int) -> dict[str, Any]:
    """Rental units: file user.email → users.userId → rental_units.userId,
    file rentalUnit.address matches rental_units.address."""
    user_idx = _build_user_email_index()
    units = _coll("rental_units")
    matched = 0
    missing: list[dict[str, Any]] = []
    missing_count = 0
    for i, row in enumerate(rows, start=2):
        email = _get_path(row, "user.email")
        addr = _get_path(row, "rentalUnit.address")
        if not email or not addr:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "missing email or address"})
            continue
        uid = user_idx.get(str(email).strip().lower())
        if not uid:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": f"no user with email {email}"})
            continue
        candidates = units.find({"userId": uid}, {"address": 1, "_address": 1, "_id": 0})
        if any(_values_equal(addr, c.get("address")) or _values_equal(addr, c.get("_address"))
               for c in candidates):
            matched += 1
        else:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "no rental_unit with matching address",
                                 "email": email, "address": addr})
    return {"matched": matched, "missing_count": missing_count, "mismatched_count": 0,
            "missing_sample": missing, "mismatched_sample": []}


def _verify_vendor_employees(rows: list[dict[str, Any]], sample_size: int) -> dict[str, Any]:
    """Vendor employees stored in persons collection (personType marks role).
    File identifier is one of company/userEmail/companyEmail; we use whichever
    is present. Match key: firstName + lastName for the resolved vendor user."""
    user_idx = _build_user_email_index()
    company_idx: dict[str, str] = {}
    for c in _coll("company_infos").find({}, {"userId": 1, "companyName": 1, "_id": 0}):
        if c.get("companyName") and c.get("userId"):
            company_idx[c["companyName"].strip().lower()] = c["userId"]
    persons = _coll("persons")
    matched = 0
    missing: list[dict[str, Any]] = []
    missing_count = 0
    for i, row in enumerate(rows, start=2):
        company = _get_path(row, "company")
        user_email = _get_path(row, "userEmail")
        company_email = _get_path(row, "companyEmail")
        fn = _get_path(row, "employee.firstName")
        ln = _get_path(row, "employee.lastName")
        # Resolve to a vendor userId via whichever identifier is available
        vendor_uid: str | None = None
        for ident in (user_email, company_email):
            if ident:
                vendor_uid = user_idx.get(str(ident).strip().lower())
                if vendor_uid:
                    break
        if not vendor_uid and company:
            vendor_uid = company_idx.get(str(company).strip().lower())
        if not vendor_uid or not fn or not ln:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "could not resolve vendor user or names"})
            continue
        if persons.find_one({"userId": vendor_uid,
                             "firstName": fn, "lastName": ln}):
            matched += 1
        else:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "no person with matching name under vendor",
                                 "firstName": fn, "lastName": ln})
    return {"matched": matched, "missing_count": missing_count, "mismatched_count": 0,
            "missing_sample": missing, "mismatched_sample": []}


def _verify_vendor_vehicles(rows: list[dict[str, Any]], sample_size: int) -> dict[str, Any]:
    """Vendor vehicles in company_vehicles. Identifier: vendor user + licensePlate."""
    user_idx = _build_user_email_index()
    company_idx: dict[str, str] = {}
    for c in _coll("company_infos").find({}, {"userId": 1, "companyName": 1, "_id": 0}):
        if c.get("companyName") and c.get("userId"):
            company_idx[c["companyName"].strip().lower()] = c["userId"]
    veh = _coll("company_vehicles")
    matched = 0
    missing: list[dict[str, Any]] = []
    missing_count = 0
    for i, row in enumerate(rows, start=2):
        plate = _get_path(row, "company_vehicle.licensePlate")
        user_email = _get_path(row, "userEmail")
        company_email = _get_path(row, "companyEmail")
        company = _get_path(row, "company")
        vendor_uid: str | None = None
        for ident in (user_email, company_email):
            if ident:
                vendor_uid = user_idx.get(str(ident).strip().lower())
                if vendor_uid:
                    break
        if not vendor_uid and company:
            vendor_uid = company_idx.get(str(company).strip().lower())
        if not vendor_uid or not plate:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "could not resolve vendor user or plate"})
            continue
        if veh.find_one({"userId": vendor_uid, "licensePlate": plate}):
            matched += 1
        else:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "no company_vehicle with matching plate",
                                 "plate": plate})
    return {"matched": matched, "missing_count": missing_count, "mismatched_count": 0,
            "missing_sample": missing, "mismatched_sample": []}


# Dispatch table mapping import_type → (verifier_callable, confidence)
_RELATIONAL_VERIFIERS = {
    "resident_passes":    (lambda r, s: _verify_relational_pass(r, "resident_passes", s),
                           "tested"),
    "vendor_passes":      (lambda r, s: _verify_relational_pass(r, "vendor_passes", s),
                           "schema_derived"),
    "host_guest_passes":  (lambda r, s: _verify_relational_pass(r, "host_guest_passes", s),
                           "schema_derived"),
    "resident_properties": (_verify_resident_properties, "schema_derived"),
    "host_rental_units":   (_verify_host_rental_units,   "schema_derived"),
    "vendor_employees":    (_verify_vendor_employees,    "schema_derived"),
    "vendor_vehicles":     (_verify_vendor_vehicles,     "schema_derived"),
}


# ---- Verifier Tool: verify_upload ----
@mcp.tool()
def verify_upload(
    import_type: Annotated[str, Field(description="One of the IMPORT_MUTATIONS keys.")],
    file_path:   Annotated[str, Field(description="Absolute path to a .csv or .xlsx file.")],
    sample_size: Annotated[
        int,
        Field(description="Max number of missing/mismatched rows to include "
                          "in the response (default 20). Totals are always exact."),
    ] = 20,
) -> dict[str, Any]:
    """Check, row-by-row, whether the records in `file_path` actually exist
    on the server for the configured community, and whether key fields agree.

    Accepts CSV and XLSX. Returns counts plus a capped sample of misses so
    you can investigate without being flooded.

    Each import_type has a `confidence` field in the response:
      - "tested":         mapping was round-tripped against live data
      - "schema_derived": mapping was extracted from yup schemas + sampled
                          docs but never validated end-to-end. Trust totals
                          less; spot-check at least one row before relying
                          on a clean result for these.
    """
    logger.info("verify_upload: type=%s path=%s sample_size=%s",
                import_type, file_path, sample_size)
    if not Path(file_path).exists():
        return {"ok": False, "error": f"File not found: {file_path}"}
    if import_type not in IMPORT_MUTATIONS:
        return {"ok": False, "error": f"Unknown import_type '{import_type}'"}

    rows = _read_rows(file_path)
    row_count = len(rows)

    # Run the appropriate verifier
    if import_type in VERIFY_CONFIG:
        cfg = VERIFY_CONFIG[import_type]
        coll = _coll(cfg["collection"])
        lookup: dict[str, str] = cfg["lookup"]
        compare: dict[str, str] = cfg["compare"]
        matched = 0
        missing: list[dict[str, Any]] = []
        mismatched: list[dict[str, Any]] = []
        missing_count = 0
        mismatched_count = 0
        for i, row in enumerate(rows, start=2):
            query = {db_field: _norm_lookup(_get_path(row, file_col), file_col)
                     for file_col, db_field in lookup.items()}
            doc = coll.find_one(query)
            if not doc:
                missing_count += 1
                if len(missing) < sample_size:
                    missing.append({"row": i, "query": query})
                continue
            row_mm: list[dict[str, Any]] = []
            for file_col, db_field in compare.items():
                f_val = _get_path(row, file_col)
                if _is_empty(f_val):
                    continue
                s_val = doc.get(db_field)
                if not _values_equal(f_val, s_val):
                    row_mm.append({"field": file_col, "file": f_val, "server": s_val})
            if row_mm:
                mismatched_count += 1
                if len(mismatched) < sample_size:
                    mismatched.append({"row": i, "mismatches": row_mm})
            else:
                matched += 1
        result = {
            "matched": matched,
            "missing_count": missing_count,
            "mismatched_count": mismatched_count,
            "missing_sample": missing,
            "mismatched_sample": mismatched,
        }
        confidence = cfg["confidence"]
    elif import_type in _RELATIONAL_VERIFIERS:
        verifier, confidence = _RELATIONAL_VERIFIERS[import_type]
        result = verifier(rows, sample_size)
    else:
        return {"ok": False, "error": f"No verifier implemented for '{import_type}'"}

    return {
        "ok": result["missing_count"] == 0 and result["mismatched_count"] == 0,
        "import_type": import_type,
        "confidence": confidence,
        "community_id": SYMLIV_COMMUNITY_ID,
        "row_count": row_count,
        **result,
    }


# ---------------------------------------------------------------------------
# Tools — token management
# ---------------------------------------------------------------------------

# ---- Token Tool: remint_admin_token ----
# Mints a fresh symlivAdmin JWT for the currently-configured community by:
#   1. Looking up any user with the `symlivAdmin` role in <community>.users
#   2. Signing { id, roles, communityId } with SYMLIV_JWT_SECRET_KEY (HS256),
#      mirroring the suite's apps/admin-back/src/scripts/test-import.sh
#   3. Rebinding the module-level SYMLIV_ADMIN_TOKEN so subsequent API calls
#      in this process use the new token (no MCP-client restart needed)
#
# SECURITY NOTE: this tool requires SYMLIV_JWT_SECRET_KEY, which is the same
# secret the backend uses to verify tokens. Anything that can call this tool
# can mint symlivAdmin tokens for any community whose users it can read. Only
# expose this MCP server in trusted environments.
@mcp.tool()
def remint_admin_token(
    expires_in_hours: Annotated[
        int,
        Field(description="Lifetime of the minted token in hours. Default 12."),
    ] = 12,
    update_in_process: Annotated[
        bool,
        Field(
            description="If True (default), also replace the in-process "
                        "SYMLIV_ADMIN_TOKEN so subsequent tool calls use the "
                        "new token immediately, without restarting the server."
        ),
    ] = True,
) -> dict[str, Any]:
    """Mint a fresh symlivAdmin JWT for SYMLIV_COMMUNITY_ID and (by default)
    swap it in for the current process. Returns the token and metadata so the
    caller can also paste it into .mcp.json if a permanent update is desired.

    Requires SYMLIV_JWT_SECRET_KEY to be set to the backend's JWT_SECRET_KEY."""
    logger.info(
        "remint_admin_token: community=%s expires_in_hours=%s update_in_process=%s",
        SYMLIV_COMMUNITY_ID, expires_in_hours, update_in_process,
    )

    # Guard rails — fail fast with clear messages rather than mysterious errors
    if not SYMLIV_JWT_SECRET_KEY:
        raise RuntimeError(
            "SYMLIV_JWT_SECRET_KEY env var is not set. Add it to the MCP "
            "server config (must match the backend's JWT_SECRET_KEY)."
        )
    if not SYMLIV_COMMUNITY_ID:
        raise RuntimeError(
            "SYMLIV_COMMUNITY_ID env var is not set; cannot pick a community "
            "to mint a token for."
        )
    if expires_in_hours <= 0 or expires_in_hours > 24 * 7:
        raise ValueError("expires_in_hours must be in (0, 168] (max 1 week).")

    # Find a symlivAdmin user in <community>.users. Any will do — the JWT only
    # needs a valid userId, role list, and communityId for the backend's
    # extractUserContext to accept it.
    user = _coll("users").find_one(
        {"roles": "symlivAdmin"},
        projection={"userId": 1, "roles": 1, "_id": 0},
    )
    if not user:
        raise RuntimeError(
            f"No symlivAdmin user found in {SYMLIV_COMMUNITY_ID}.users; "
            "cannot mint a token for this community."
        )

    # Sign HS256, mirroring the shape that extractUserContext (in
    # libs/backend/utils/src/lib/extractUserContext.ts) expects: { id, roles,
    # communityId }. The backend's getToken verifies with the same algorithm.
    import time
    now = int(time.time())
    exp = now + expires_in_hours * 3600
    payload = {
        "id": user["userId"],
        "roles": user["roles"],
        "communityId": SYMLIV_COMMUNITY_ID,
        "iat": now,
        "exp": exp,
    }
    token = jwt.encode(payload, SYMLIV_JWT_SECRET_KEY, algorithm="HS256")

    # Swap into the process so the very next GraphQL call uses the new token.
    if update_in_process:
        global SYMLIV_ADMIN_TOKEN
        SYMLIV_ADMIN_TOKEN = token
        logger.info("remint_admin_token: in-process SYMLIV_ADMIN_TOKEN updated")

    return {
        "token": token,
        "user_id": user["userId"],
        "roles": user["roles"],
        "community_id": SYMLIV_COMMUNITY_ID,
        "issued_at": now,
        "expires_at": exp,
        "in_process_updated": update_in_process,
        "note": (
            "In-process token replaced — subsequent tool calls in this MCP "
            "server lifetime will use it. To persist across restarts, paste "
            "`token` into SYMLIV_ADMIN_TOKEN in your .mcp.json."
            if update_in_process
            else "Token NOT applied to this process; paste it into "
                 "SYMLIV_ADMIN_TOKEN in your .mcp.json and restart."
        ),
    }


# ---------------------------------------------------------------------------
# Tools — context switching (community + environment)
# ---------------------------------------------------------------------------
# These tools let you swap the active community and/or environment WITHOUT
# editing .mcp.json or restarting Claude Code. They mutate the module-level
# config globals, swap the Mongo client, re-mint the admin JWT for the new
# context, and persist the selection back to profiles.json so it survives
# subsequent MCP-server restarts.

@mcp.tool()
def current_context() -> dict[str, Any]:
    """Inspect the active community + environment in this MCP process.
    Read-only — mirrors the runtime state after any switch_context calls."""
    return {
        "environment": SYMLIV_ENV_NAME,
        "community_id": SYMLIV_COMMUNITY_ID,
        "graphql_url": SYMLIV_GRAPHQL_URL,
        "mongo_db": SYMLIV_MONGO_DB,
        "workspace_root": SYMLIV_WORKSPACE_ROOT,
        "available_environments": sorted(
            (_PROFILES.get("environments") or {}).keys()
        ),
    }


@mcp.tool()
def switch_context(
    community_id: Annotated[
        str,
        Field(description="Target community (e.g. 'championsgate', 'solara')."),
    ],
    environment: Annotated[
        Optional[str],
        Field(
            description="Target environment from profiles.json "
                        "('staging', 'production', ...). Omit to keep the "
                        "current environment."
        ),
    ] = None,
    remint_token: Annotated[
        bool,
        Field(
            description="If True (default), also mint a fresh symlivAdmin JWT "
                        "for the new context. Set False if you've already "
                        "supplied SYMLIV_ADMIN_TOKEN by other means."
        ),
    ] = True,
) -> dict[str, Any]:
    """Swap the active community (and optionally environment) in-process.

    Updates module-level globals, swaps the MongoDB client to the new env's
    URI, mints a fresh admin JWT for the target community, and persists the
    new selection to src/profiles.json. No server restart needed — the very
    next tool call uses the new context.
    """
    global SYMLIV_COMMUNITY_ID, SYMLIV_ENV_NAME
    global SYMLIV_GRAPHQL_URL, SYMLIV_JWT_SECRET_KEY, SYMLIV_WORKSPACE_ROOT
    global SYMLIV_MONGO_URI, SYMLIV_MONGO_DB
    global SYMLIV_ADMIN_TOKEN
    global _mongo, _db, _PROFILES, _ENV_BLOCK

    # Reload profiles from disk so any out-of-band edits are picked up.
    _PROFILES = _load_profiles()

    target_env = environment or SYMLIV_ENV_NAME
    envs = _PROFILES.get("environments") or {}
    if target_env and target_env not in envs:
        raise ValueError(
            f"Environment {target_env!r} is not defined in {PROFILES_PATH}. "
            f"Known: {sorted(envs.keys())}"
        )
    if target_env:
        block = envs[target_env]
        # graphql_url + mongo_uri are required to switch at all (we'd error
        # out the moment any tool tried to read). jwt_secret is only needed
        # for remint_admin_token / commit_import, so a placeholder there
        # blocks token operations only — Mongo reads still work.
        for k in ("graphql_url", "mongo_uri"):
            v = block.get(k, "")
            if not v or v.startswith("FILL_IN_"):
                raise ValueError(
                    f"profiles.json environment {target_env!r} field {k!r} "
                    "is not configured."
                )
        jwt_placeholder = (block.get("jwt_secret") or "").startswith("FILL_IN_")
        SYMLIV_ENV_NAME = target_env
        SYMLIV_GRAPHQL_URL = block["graphql_url"]
        SYMLIV_JWT_SECRET_KEY = "" if jwt_placeholder else block.get("jwt_secret", "")
        SYMLIV_MONGO_URI = block["mongo_uri"]
        # Pull a cached admin_token if one was set for this env. (Will be
        # replaced moments later if remint_token=True and we can mint.)
        cached_token = block.get("admin_token", "")
        if cached_token:
            SYMLIV_ADMIN_TOKEN = cached_token
            os.environ["SYMLIV_ADMIN_TOKEN"] = cached_token
        SYMLIV_MONGO_DB = block.get("mongo_db", "main")
        SYMLIV_WORKSPACE_ROOT = block.get("workspace_root", SYMLIV_WORKSPACE_ROOT)
        # Reflect into os.environ so any subprocess or imported lib that
        # reads env vars at call time sees the new values.
        os.environ["SYMLIV_GRAPHQL_URL"] = SYMLIV_GRAPHQL_URL
        os.environ["SYMLIV_JWT_SECRET_KEY"] = SYMLIV_JWT_SECRET_KEY
        os.environ["SYMLIV_MONGO_URI"] = SYMLIV_MONGO_URI
        os.environ["SYMLIV_MONGO_DB"] = SYMLIV_MONGO_DB
        os.environ["SYMLIV_WORKSPACE_ROOT"] = SYMLIV_WORKSPACE_ROOT
        # Swap the Mongo client — the old one's connection pool gets GC'd.
        _mongo, _db = _connect_mongo(SYMLIV_MONGO_URI, SYMLIV_MONGO_DB)
        logger.info("switch_context: env → %s, mongo → %s",
                    SYMLIV_ENV_NAME, SYMLIV_MONGO_URI.split("@")[-1].split("/")[0])

    SYMLIV_COMMUNITY_ID = community_id
    os.environ["SYMLIV_COMMUNITY_ID"] = community_id

    # Persist the new selection so a Claude Code restart picks it up.
    _PROFILES.setdefault("current", {})
    _PROFILES["current"]["environment"] = SYMLIV_ENV_NAME
    _PROFILES["current"]["community_id"] = community_id
    _save_profiles(_PROFILES)
    _ENV_BLOCK = _active_env_block(_PROFILES)

    minted = None
    if remint_token and not SYMLIV_JWT_SECRET_KEY:
        # Can't mint without the secret — skip and tell the caller why.
        minted = {"skipped": "jwt_secret not configured for this environment"}
    elif remint_token:
        # Delegate to the existing remint logic — it reads from the globals
        # we just rebound, so it produces a token for the new community.
        try:
            minted = remint_admin_token(expires_in_hours=12, update_in_process=True)
        except Exception as e:
            logger.warning("switch_context: token remint failed: %s", e)
            minted = {"error": str(e)}

    return {
        "ok": True,
        "environment": SYMLIV_ENV_NAME,
        "community_id": SYMLIV_COMMUNITY_ID,
        "graphql_url": SYMLIV_GRAPHQL_URL,
        "mongo_db": SYMLIV_MONGO_DB,
        "workspace_root": SYMLIV_WORKSPACE_ROOT,
        "token_remint": (
            "ok" if minted and "token" in minted else
            ("skipped" if not remint_token else minted)
        ),
        "persisted_to": str(PROFILES_PATH),
    }


@mcp.tool()
def set_admin_token(
    token: Annotated[
        str,
        Field(description="A signed JWT (no 'Bearer ' prefix) with the "
                          "symlivAdmin role. Easiest source: paste from "
                          "DevTools → Network → graphql request → "
                          "Authorization header on a logged-in browser session."),
    ],
    persist: Annotated[
        bool,
        Field(
            description="If True (default), save the token to the current "
                        "environment's block in profiles.json so it survives "
                        "MCP-server restarts (file is gitignored). False = "
                        "in-process only."
        ),
    ] = True,
) -> dict[str, Any]:
    """Replace the in-process SYMLIV_ADMIN_TOKEN with a token you obtained
    out-of-band (typically a browser-issued JWT). Useful when you don't have
    the JWT_SECRET_KEY for the target environment and so can't call
    remint_admin_token. Decodes (without signature verification) to show
    you what's inside for sanity-checking."""
    global SYMLIV_ADMIN_TOKEN, _PROFILES, _ENV_BLOCK
    token = (token or "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if token.count(".") != 2:
        raise ValueError("Doesn't look like a JWT (needs three dot-separated "
                         "segments). Did you paste the full token?")

    # Decode without verifying the signature so we can show the user what
    # they just pasted. We don't have the prod secret to verify against
    # anyway — that's the whole point of this tool.
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
    except Exception as e:
        raise ValueError(f"Could not decode JWT payload: {e}")

    import time
    now = int(time.time())
    exp = claims.get("exp")
    roles = claims.get("roles") or []
    summary = {
        "user_id": claims.get("id"),
        "roles": roles,
        "community_id_in_jwt": claims.get("communityId"),
        "expires_at": exp,
        "expires_in_hours": round((exp - now) / 3600, 1) if exp else None,
        "has_symlivAdmin": "symlivAdmin" in roles,
    }
    if exp and exp <= now:
        raise ValueError(f"Token already expired at {exp} (now={now}).")
    if "symlivAdmin" not in roles:
        # Don't refuse — sometimes you want a non-admin token for testing —
        # but flag it loudly. Imports won't work without symlivAdmin.
        summary["warning"] = (
            "Token does NOT have symlivAdmin role. Imports and most admin "
            "queries will fail. Verify you grabbed the right user's token."
        )

    SYMLIV_ADMIN_TOKEN = token
    os.environ["SYMLIV_ADMIN_TOKEN"] = token

    if persist:
        # Write the token into profiles.json under the active environment.
        # profiles.json is gitignored — see project .gitignore.
        env_name = SYMLIV_ENV_NAME or "default"
        _PROFILES.setdefault("environments", {}).setdefault(env_name, {})
        _PROFILES["environments"][env_name]["admin_token"] = token
        _save_profiles(_PROFILES)
        _ENV_BLOCK = _active_env_block(_PROFILES)

    return {
        "ok": True,
        "in_process_updated": True,
        "persisted_to_env": SYMLIV_ENV_NAME if persist else None,
        "token_summary": summary,
        "note": (
            "Token applied. The next GraphQL call will use it. "
            "community-id header still comes from SYMLIV_COMMUNITY_ID, so "
            "you can target any community from a single token as long as "
            "the role is symlivAdmin."
        ),
    }


# ---------------------------------------------------------------------------
# Tools — workspace browsing
# ---------------------------------------------------------------------------
# The community workspace (see _community_dir docstring) holds every artifact
# we know about: archived CSVs, diffs, workbooks, timeline. These tools let a
# caller inspect that workspace without leaving the MCP context.


# ---- Workspace Tool: add_workbook ----
@mcp.tool()
def add_workbook(
    file_path: Annotated[
        str,
        Field(description="Absolute path to a .xlsx (or .csv) workbook produced "
                          "during onboarding, e.g. one of the 5 playbook workbooks."),
    ],
    label: Annotated[
        str,
        Field(description="Short identifier saved in the archived filename, e.g. "
                          "'community_addresses', 'host_verification'. Spaces "
                          "and slashes are sanitized."),
    ],
) -> dict[str, Any]:
    """Copy an onboarding workbook (or any reference artifact) into this
    community's `workbooks/` folder, with a timestamp prefix so multiple
    versions can coexist chronologically. Returns the archived path.
    """
    if not Path(file_path).exists():
        return {"ok": False, "error": f"File not found: {file_path}"}
    safe_label = re.sub(r"[^A-Za-z0-9_-]+", "_", label).strip("_") or "workbook"
    suffix = Path(file_path).suffix or ".xlsx"
    dest = _community_dir() / "workbooks" / f"{_stamp()}_{safe_label}{suffix}"
    _shutil.copy(file_path, dest)
    logger.info("add_workbook: %s -> %s", file_path, dest)
    # Also append a workbook event to the timeline so it shows up in
    # chronological reviews alongside imports
    event = {
        "stamp": _stamp(), "community_id": SYMLIV_COMMUNITY_ID,
        "event": "workbook_added", "label": safe_label,
        "source_path": str(file_path), "archived_path": str(dest),
    }
    with open(_community_dir() / "timeline.jsonl", "a") as f:
        f.write(_json.dumps(event, default=str) + "\n")
    return {"ok": True, "archived_path": str(dest)}


# ---- Workspace Tool: list_workspace ----
@mcp.tool()
def list_workspace() -> dict[str, Any]:
    """Return a structured listing of the current community's workspace —
    workbooks, every archived import folder (chronological), every repair
    diff. Use this to answer "what's been done for this community so far?"
    without opening a file browser."""
    cdir = _community_dir()
    def names_in(sub: str) -> list[str]:
        path = cdir / sub
        return sorted(p.name for p in path.iterdir()) if path.exists() else []
    return {
        "community_id": SYMLIV_COMMUNITY_ID,
        "root": str(cdir),
        "workbooks":     names_in("workbooks"),
        "imports":       names_in("imports"),
        "repairs":       names_in("repairs"),
        "timeline_path": str(cdir / "timeline.jsonl"),
        "timeline_exists": (cdir / "timeline.jsonl").exists(),
    }


# ---- Workspace Tool: get_timeline ----
@mcp.tool()
def get_timeline(
    limit: Annotated[
        int,
        Field(description="Max events to return, newest last. Default 50."),
    ] = 50,
) -> dict[str, Any]:
    """Read the community's append-only timeline.jsonl and return the last
    `limit` events in chronological order. Each event records one import
    attempt (dry-run or commit) or one workbook addition, with timestamp,
    type/label, mode, row count, and result summary."""
    cdir = _community_dir()
    path = cdir / "timeline.jsonl"
    if not path.exists():
        return {"community_id": SYMLIV_COMMUNITY_ID, "events": [],
                "note": "timeline.jsonl does not exist yet — no events recorded"}
    events: list[dict[str, Any]] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                events.append(_json.loads(line))
            except _json.JSONDecodeError:
                continue
    return {
        "community_id": SYMLIV_COMMUNITY_ID,
        "total_events": len(events),
        "events": events[-limit:],
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
# When run directly (e.g., `uv run symliv-data-migration.py`), starts the MCP
# server using stdio transport (the default). This allows Claude Desktop or
# Claude Code to connect via the process's stdin/stdout.
# For HTTP-based connections (e.g., remote clients), pass transport="streamable-http".
if __name__ == "__main__":
    mcp.run()