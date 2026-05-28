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
import re           # Regex for email/UUID/sentinel value validation
from datetime import datetime  # Date format parsing in validation checks
from pathlib import Path          # Cross-platform filesystem path operations
from typing import Annotated, Any, Literal  # Type-hint helpers

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
# Configuration — pull from env so credentials never live in code
# ---------------------------------------------------------------------------

# The SymLiv GraphQL API endpoint. Defaults to production; override via env
# var to point at staging or a local dev server.
SYMLIV_GRAPHQL_URL = os.environ.get(
    "SYMLIV_GRAPHQL_URL", "https://api.symliv.com/graphql"
)

# Admin bearer token (symlivAdmin role). Required for all import mutations.
# Must be set in the environment — the server will refuse to run imports without it.
# NOTE: declared as a module-level mutable so the `remint_admin_token` tool can
# replace it at runtime (its `global` statement rebinds this name).
SYMLIV_ADMIN_TOKEN = os.environ.get("SYMLIV_ADMIN_TOKEN", "")

# HS256 secret used to sign new admin JWTs in the `remint_admin_token` tool.
# Must match the backend's JWT_SECRET_KEY for the minted token to verify.
# Optional: only required if you intend to call `remint_admin_token`.
SYMLIV_JWT_SECRET_KEY = os.environ.get("SYMLIV_JWT_SECRET_KEY", "")

# The UUID of the community being migrated. Sent as the X-Community-Id header
# so the API knows which community's data to modify.
SYMLIV_COMMUNITY_ID = os.environ.get("SYMLIV_COMMUNITY_ID", "")

# Filesystem root for community workspaces — one subdirectory per community
# holding everything we know about it: archived CSVs from every import
# attempt, computed diffs (repairs), onboarding workbooks, and a single
# append-only timeline.jsonl of every event. See `_community_dir()` for
# the on-disk layout.
SYMLIV_WORKSPACE_ROOT = os.environ.get(
    "SYMLIV_WORKSPACE_ROOT",
    str(Path.home() / "Documents" / "symliv-data" / "communities"),
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
            "vehicle.destinationAddressId": "uuid",
            "vehicle.year": "int",
            "fcException": "bool",
            "delete": "bool",
        },
        "enums": {
            "pass.status": _PASS_STATUS,
            "pass.paid": _PAYMENT_STATUS,
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
            "passes.shared": "bool",
            "vehicle.destinationAddressId": "uuid",
            "vehicle.year": "int",
            "fcException": "bool",
            "delete": "bool",
        },
        "enums": {
            "passes.status": _PASS_STATUS,
            "passes.paid": _PAYMENT_STATUS,
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

    return {
        "row_count": len(rows),
        "rows_with_errors": rows_with_errors,
        "total_issues": total_issues,
        "issues_sample": issues,
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

    return {
        "ok": result["total_issues"] == 0,
        "import_type": import_type,
        "headers": sorted(headers),
        "row_count": result["row_count"],
        "rows_with_errors": result["rows_with_errors"],
        "total_issues": result["total_issues"],
        "issues_sample": result["issues_sample"],
        "next_step": (
            "Call dry_run_import to validate against the SymLiv API."
            if result["total_issues"] == 0
            else "Fix the cells listed in issues_sample, then re-validate."
        ),
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

async def _run_import(
    import_type: str,
    csv_path: str,
    no_mutation: bool,
) -> dict[str, Any]:
    """Core import execution shared by dry_run_import and commit_import.

    Args:
        import_type: Key from IMPORT_MUTATIONS (e.g. "resident_users").
        csv_path:    Absolute filesystem path to the CSV file.
        no_mutation: If True, the API validates but does NOT write any records
                     (dry-run mode). If False, records are actually created.

    Returns a dict with "ok" (bool), import metadata, and a "summary" containing
    successCount, errorCount, and per-row error details from the API.
    """
    mode = "DRY RUN" if no_mutation else "COMMIT"
    logger.info("_run_import [%s]: type=%s path=%s", mode, import_type, csv_path)

    # Basic input validation — same checks as validate_csv
    if import_type not in IMPORT_MUTATIONS:
        return {"ok": False, "error": f"Unknown import_type '{import_type}'"}
    if not Path(csv_path).exists():
        return {"ok": False, "error": f"File not found: {csv_path}"}

    # Look up the GraphQL mutation name and read the CSV content
    mutation_name = IMPORT_MUTATIONS[import_type]
    # Per schema introspection against staging admin-back, every parse*CsvFile
    # mutation accepts (filePath: String!, originalFileName: String!,
    # noMutation: Boolean), where `filePath` is misleadingly named but is
    # actually the **base64-encoded CSV bytes** (mirrors test-import.sh's
    # `FILE_B64=$(base64 -i $CSV_FILE)`). Return type is
    # StringResponse { success, data, error } — `data` is a JSON string
    # holding the row-by-row success/error counts.
    import base64
    csv_b64 = base64.b64encode(Path(csv_path).read_bytes()).decode("ascii")
    original_filename = Path(csv_path).name

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

    # Execute the mutation against the SymLiv GraphQL API
    data = await _graphql(mutation, {
        "filePath": csv_b64,
        "originalFileName": original_filename,
        "noMutation": no_mutation,
    })

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
    # { success: bool, data: string?, error: string? }. The `data` field
    # is typically a JSON string with per-row success/error counts; we try
    # to parse it but pass it through as-is on failure so the operator can
    # still see whatever the server said.
    payload = data.get("data", {}).get(mutation_name, {}) or {}
    server_success = bool(payload.get("success"))
    server_error = payload.get("error")
    raw_data = payload.get("data")
    parsed_data: Any = raw_data
    if isinstance(raw_data, str) and raw_data.strip().startswith(("{", "[")):
        try:
            parsed_data = _json.loads(raw_data)
        except _json.JSONDecodeError:
            parsed_data = raw_data
    # Treat the import as `ok` when:
    #   - server `success` is true, AND
    #   - if parsed_data exposes an `errorCount`/`errors`, that count is 0
    ok = server_success
    if isinstance(parsed_data, dict):
        ec = parsed_data.get("errorCount")
        if isinstance(ec, int) and ec > 0:
            ok = False
        if parsed_data.get("errors"):
            # Treat a non-empty errors list as a failure even if errorCount
            # wasn't surfaced.
            ok = False
    logger.info("_run_import [%s] result: server_success=%s ok=%s",
                mode, server_success, ok)
    if not ok:
        logger.warning("Import not ok. server error=%s data=%s",
                       server_error, parsed_data)
    result = {
        "ok": ok,
        "dry_run": no_mutation,
        "import_type": import_type,
        "mutation": mutation_name,
        # Keep `summary` as the structured payload (parsed JSON when possible),
        # plus the raw server fields for debugging.
        "summary": parsed_data,
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

# Connect to MongoDB using credentials from environment variables.
# IMPORTANT: these should be read-only credentials to prevent accidental writes.
MONGO_URI = os.environ["SYMLIV_MONGO_URI"]
_mongo = MongoClient(MONGO_URI)
_db = _mongo[os.environ["SYMLIV_MONGO_DB"]]  # Select the target database (e.g. "main")


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
    """Resident properties: file links user.email → user.address. Server stores
    the address as resident_properties.street linked by userId. We confirm a
    resident_property exists for that user with a matching street."""
    user_idx = _build_user_email_index()
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
        # Match either the canonical street or the raw _street, type-aware
        candidates = props.find({"userId": uid}, {"street": 1, "_street": 1, "_id": 0})
        if any(_values_equal(addr, c.get("street")) or _values_equal(addr, c.get("_street"))
               for c in candidates):
            matched += 1
        else:
            missing_count += 1
            if len(missing) < sample_size:
                missing.append({"row": i, "reason": "no resident_property with matching street",
                                 "email": email, "address": addr})
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