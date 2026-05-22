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
import os           # Access environment variables for secrets and config
from pathlib import Path          # Cross-platform filesystem path operations
from typing import Annotated, Any, Literal  # Type-hint helpers

# Third-party imports
import httpx                      # Async HTTP client used for GraphQL API calls
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
SYMLIV_ADMIN_TOKEN = os.environ.get("SYMLIV_ADMIN_TOKEN", "")

# The UUID of the community being migrated. Sent as the X-Community-Id header
# so the API knows which community's data to modify.
SYMLIV_COMMUNITY_ID = os.environ.get("SYMLIV_COMMUNITY_ID", "")

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

# Required CSV column headers per import type, sourced from the official SymLiv
# CSV Import Guides. The validate_csv tool checks these LOCALLY (no API call)
# so obvious mistakes (missing/mis-named columns) are caught immediately.
# Note: not every import type has required-column metadata listed here yet —
# types not present in this dict simply skip the column check.
REQUIRED_COLUMNS: dict[str, set[str]] = {
    "community_addresses":  {"address", "passesPerDay"},
    "vendor_users":         {"company.companyName", "user.firstName",
                             "user.lastName", "user.email"},
    "vendor_employees":     {"employee.firstName", "employee.lastName"},
    "resident_users":       {"user.firstName", "user.lastName", "user.email"},
    "resident_properties":  {"user.email", "user.address"},
    "resident_passes":      {"user.email", "passes.passInfoId",
                             "passes.startDate", "passes.endDate"},
    "host_users":           {"user.firstName", "user.lastName", "user.email"},
    "guest_users":          {"user.firstName", "user.lastName", "user.email"},
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

    # Build auth and routing headers required by the SymLiv API
    headers = {
        "Authorization": f"Bearer {SYMLIV_ADMIN_TOKEN}",  # Admin JWT
        "X-Community-Id": SYMLIV_COMMUNITY_ID,             # Target community
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
        return resp.json()


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
    # Query all completed pass definitions from the community's pass builder
    query = """
        query { getPassInfos(complete: true) { passInfoId name portal } }
    """
    data = await _graphql(query, {})
    infos = data.get("data", {}).get("getPassInfos", []) or []
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
    query = """
        query {
          getCommunityAddresses {
            communityAddressId address city state zipCode passesPerDay
          }
        }
    """
    data = await _graphql(query, {})
    return data.get("data", {}).get("getCommunityAddresses", []) or []

# ---------------------------------------------------------------------------
# Tools — validation
# ---------------------------------------------------------------------------

# ---- Validation Tool: validate_csv ----
# A fast, local-only sanity check. No network call is made — this just reads
# the CSV file from disk and compares its headers against REQUIRED_COLUMNS.
# Intended as the first step: catch typos and missing columns before spending
# time on an API round-trip with dry_run_import.
@mcp.tool()
def validate_csv(
    import_type: Annotated[str, Field(description="One of: " + ", ".join(IMPORT_MUTATIONS))],
    csv_path: Annotated[str, Field(description="Absolute path to the CSV file.")],
) -> dict[str, Any]:
    """Local pre-flight check: confirms headers exist, required columns are
    present, and counts rows. Does NOT hit the SymLiv API. Run this before
    dry_run_import to catch easy mistakes for free."""

    # Reject unknown import types early
    if import_type not in IMPORT_MUTATIONS:
        return {"ok": False, "error": f"Unknown import_type '{import_type}'"}

    # Make sure the file actually exists on disk
    if not Path(csv_path).exists():
        return {"ok": False, "error": f"File not found: {csv_path}"}

    # Read the header row and determine which required columns are absent
    headers = _read_csv_headers(csv_path)
    required = REQUIRED_COLUMNS.get(import_type, set())
    missing = sorted(required - set(headers))

    # Count data rows (total lines minus the header row)
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        row_count = max(sum(1 for _ in f) - 1, 0)

    # Return a structured result: ok=True means all required columns are present
    return {
        "ok": not missing,
        "import_type": import_type,
        "headers": headers,
        "missing_required_columns": missing,
        "row_count": row_count,
        "next_step": (
            "Call dry_run_import to validate against the SymLiv API."
            if not missing
            else "Add the missing columns and re-validate."
        ),
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
    # Basic input validation — same checks as validate_csv
    if import_type not in IMPORT_MUTATIONS:
        return {"ok": False, "error": f"Unknown import_type '{import_type}'"}
    if not Path(csv_path).exists():
        return {"ok": False, "error": f"File not found: {csv_path}"}

    # Look up the GraphQL mutation name and read the CSV content
    mutation_name = IMPORT_MUTATIONS[import_type]
    csv_text = _read_csv_text(csv_path)

    # Build the GraphQL mutation dynamically using the mutation name.
    # The `noMutation` flag controls dry-run vs. live mode.
    # NOTE: The payload shape below assumes the API accepts inline CSV text.
    # If the schema uses Base64 or Upload scalar instead, this will need updating.
    mutation = f"""
        mutation Run($csv: String!, $noMutation: Boolean!) {{
          {mutation_name}(csv: $csv, noMutation: $noMutation) {{
            successCount
            errorCount
            errors {{ row message field }}
          }}
        }}
    """

    # Execute the mutation against the SymLiv GraphQL API
    data = await _graphql(
        mutation, {"csv": csv_text, "noMutation": no_mutation}
    )

    # Check for top-level GraphQL errors (auth failures, schema errors, etc.)
    if "errors" in data:
        return {"ok": False, "graphql_errors": data["errors"]}

    # Extract the mutation result payload and determine success/failure
    payload = data.get("data", {}).get(mutation_name, {})
    return {
        "ok": payload.get("errorCount", 0) == 0,  # ok only if zero row errors
        "dry_run": no_mutation,
        "import_type": import_type,
        "mutation": mutation_name,
        "summary": payload,  # Contains successCount, errorCount, errors[]
    }


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
_db = _mongo[os.environ["SYMLIV_MONGO_DB"]]  # Select the target database


# ---- MongoDB Tool: count_residents_missing_field ----
# Pre-migration data quality check. Counts how many resident profiles are
# missing a specific field (null, empty string, or not present at all).
# This helps estimate how much data cleanup is needed before importing.
# Only a whitelist of safe fields is allowed to prevent arbitrary queries.
@mcp.tool()
def count_residents_missing_field(field: str) -> dict[str, int]:
    """Count resident profiles where `field` is empty or null. Use for
    pre-migration data quality checks (e.g. how many residents have no email).
    Read-only — does not modify any data."""
    # Whitelist of queryable fields — prevents arbitrary field access
    allowed = {"email", "phoneNumber", "mailingStreet", "emergencyPhoneNumber"}
    if field not in allowed:
        raise ValueError(f"field must be one of {allowed}")

    # Query the residentprofiles collection for documents where the field
    # is either missing entirely, set to an empty string, or explicitly null
    coll = _db["residentprofiles"]
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
    pipeline = [
        # Left-join communityaddresses → residentproperties on communityAddressId
        {"$lookup": {
            "from": "residentproperties",
            "localField": "communityAddressId",
            "foreignField": "communityAddressId",
            "as": "residents",  # Array of matched resident properties
        }},
        # Keep only addresses with zero matches (orphans)
        {"$match": {"residents": {"$size": 0}}},
        # Return only the address string and its UUID (exclude _id and residents array)
        {"$project": {"_id": 0, "address": 1, "communityAddressId": 1}},
    ]
    return list(_db["communityaddresses"].aggregate(pipeline))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
# When run directly (e.g., `uv run symliv-data-migration.py`), starts the MCP
# server using stdio transport (the default). This allows Claude Desktop or
# Claude Code to connect via the process's stdin/stdout.
# For HTTP-based connections (e.g., remote clients), pass transport="streamable-http".
if __name__ == "__main__":
    mcp.run()