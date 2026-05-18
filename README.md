# sqlalchemy-sql-on-fhir

SQLAlchemy dialect and Apache Superset engine spec for querying FHIR data via the
[SQL on FHIR](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/) `$sqlquery-run` operation.

## Installation

```bash
pip install sqlalchemy-sql-on-fhir
```

## Usage

### SQLAlchemy

```python
from sqlalchemy import create_engine, text

engine = create_engine("sqlonfhir://localhost:8080/fhir")
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM patients LIMIT 10"))
    for row in result:
        print(row)
```

### Apache Superset

After installing the package, restart Superset. The "SQL on FHIR" database type
will appear in the database connection dialog. Use a connection string like:

```
sqlonfhir://your-fhir-server:8080/fhir
```

Tables in Superset correspond to ViewDefinitions registered on the server.

## Authentication

If your server is unauthenticated, leave Secure extra empty (`{}`) — the driver
just makes plain requests.

For secured servers, the driver supports three modes, in order of precedence:
OAuth2 client-credentials (SMART), static bearer token, and HTTP basic auth.

### OAuth2 client-credentials (SMART)

SQL-on-FHIR servers commonly secure access via [SMART](https://hl7.org/fhir/smart-app-launch/)
client-credentials, issuing short-lived JWTs (often 5 minutes) — for example,
[Pathling's authorization](https://pathling.csiro.au/docs/server/authorization).
A static token is impractical in that setting, so the driver fetches and
refreshes tokens itself. Provide `client_id`, `client_secret`, `token_url`, and
optionally `scope`. Tokens are fetched on connect, refreshed proactively before
expiry, and retried once on a 401.

In **Superset → Edit database → Advanced → Security → Secure extra**:

```json
{"connect_args": {
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "token_url": "https://auth.example/realms/your-realm/protocol/openid-connect/token",
  "scope": "system/*.rs",
  "scheme": "https"
}}
```

Pair it with a minimal SQLAlchemy URI: `sqlonfhir://fhir.example.com:443/fhir`.

The same kwargs work via the SQLAlchemy URL query string (handy for tests, but
avoid in production — secrets land in logs):

```python
engine = create_engine(
    "sqlonfhir://fhir.example.com:443/fhir"
    "?client_id=cid&client_secret=csec"
    "&token_url=https://auth.example/realms/r/protocol/openid-connect/token"
    "&scope=system/*.rs"
)
```

### Static bearer token

For a long-lived token (e.g. a personal access token in dev), pass `token`:

```json
{"connect_args": {"token": "eyJhbGciOi...", "scheme": "https"}}
```

### Basic auth

Username and password go straight into the SQLAlchemy URI:

```
sqlonfhir://user:password@your-fhir-server:8080/fhir
```

### Standalone DBAPI

```python
from sqlonfhir.dbapi import connect

conn = connect(
    host="fhir.example.com",
    port=443,
    scheme="https",
    client_id="your-client-id",
    client_secret="your-client-secret",
    token_url="https://auth.example/realms/r/protocol/openid-connect/token",
    scope="system/*.rs",
)
cursor = conn.cursor()
cursor.execute("SELECT patient_id, gender FROM patients")
for row in cursor.fetchall():
    print(row)
```

## Testing

### Unit tests

```bash
uv sync --all-extras --dev
uv run pytest
```

The default run is offline and skips anything marked `integration`.

### Integration tests

End-to-end checks run the suite against a real Pathling FHIR server in
a Docker container. The fixture in `tests/integration/conftest.py` spins
up the container, waits for `/fhir/metadata` to return 200, and tears it
down at session end.

Requires Docker. Run with:

```bash
uv run pytest -m integration
```

By default the image tag is resolved at session start from the upstream
`aehrc/pathling` GitHub release feed — the newest non-prerelease
`server-v*` tag wins. Override knobs (in precedence order):

| Variable                    | Effect                                                      |
|-----------------------------|-------------------------------------------------------------|
| `PATHLING_IMAGE`            | Full image reference; skips upstream lookup entirely.       |
| `PATHLING_VERSION`          | Version only; combined with `PATHLING_REPOSITORY`.          |
| `PATHLING_REPOSITORY`       | Repository without tag (default `ghcr.io/aehrc/pathling`).  |
| `PATHLING_FALLBACK_VERSION` | Used only if the GitHub API call fails.                     |

```bash
# Pin a specific server release
PATHLING_VERSION=2.0.0 uv run pytest -m integration

# Test against a private mirror
PATHLING_IMAGE=my.registry/pathling:custom uv run pytest -m integration
```

### Continuous upstream-compatibility check

`.github/workflows/pathling-compat.yml` runs the integration suite daily
against the newest published `server-v*` release of `aehrc/pathling`. On
scheduled failures it opens (or comments on) a `pathling-compat` issue
so an upstream change that breaks the driver doesn't go unnoticed.
Trigger manually with `workflow_dispatch` and an optional `image` input
to test any specific reference.
