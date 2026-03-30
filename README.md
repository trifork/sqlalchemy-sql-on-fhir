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

### Standalone DBAPI

```python
from sqlonfhir.dbapi import connect

conn = connect(host="localhost", port=8080)
cursor = conn.cursor()
cursor.execute("SELECT patient_id, gender FROM patients")
for row in cursor.fetchall():
    print(row)
```
