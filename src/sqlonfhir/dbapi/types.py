"""FHIR type mappings for the Pathling DBAPI driver.

Maps FHIR primitive types to Python types and DBAPI 2.0 type constants.
"""

import datetime
import decimal


# DBAPI 2.0 type objects (PEP-249 section "Type Objects")
class _DBAPIType:
    def __init__(self, *values: str) -> None:
        self.values = frozenset(values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return other in self.values
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.values)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(sorted(self.values))})"


STRING = _DBAPIType("string", "code", "id", "uri", "url", "markdown", "base64Binary")
NUMBER = _DBAPIType("integer", "positiveInt", "unsignedInt", "decimal")
DATETIME = _DBAPIType("date", "dateTime", "instant", "time")
BINARY = _DBAPIType("base64Binary")
BOOLEAN = _DBAPIType("boolean")
ROWID = _DBAPIType("id")

# FHIR type string -> Python type
FHIR_TYPE_TO_PYTHON: dict[str, type] = {
    "string": str,
    "code": str,
    "id": str,
    "uri": str,
    "url": str,
    "markdown": str,
    "base64Binary": bytes,
    "boolean": bool,
    "integer": int,
    "positiveInt": int,
    "unsignedInt": int,
    "decimal": decimal.Decimal,
    "date": datetime.date,
    "dateTime": datetime.datetime,
    "instant": datetime.datetime,
    "time": datetime.time,
}

# FHIR type string -> DBAPI type object
FHIR_TYPE_TO_DBAPI: dict[str, _DBAPIType] = {
    "string": STRING,
    "code": STRING,
    "id": STRING,
    "uri": STRING,
    "url": STRING,
    "markdown": STRING,
    "base64Binary": BINARY,
    "boolean": BOOLEAN,
    "integer": NUMBER,
    "positiveInt": NUMBER,
    "unsignedInt": NUMBER,
    "decimal": NUMBER,
    "date": DATETIME,
    "dateTime": DATETIME,
    "instant": DATETIME,
    "time": DATETIME,
}


def infer_type_from_value(value: object) -> _DBAPIType:
    """Infer a DBAPI type object from a Python value."""
    if value is None:
        return STRING
    if isinstance(value, bool):
        return BOOLEAN
    if isinstance(value, int):
        return NUMBER
    if isinstance(value, float | decimal.Decimal):
        return NUMBER
    if isinstance(value, datetime.datetime):
        return DATETIME
    if isinstance(value, datetime.date):
        return DATETIME
    if isinstance(value, datetime.time):
        return DATETIME
    return STRING
