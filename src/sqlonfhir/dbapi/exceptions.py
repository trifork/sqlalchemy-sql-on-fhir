"""PEP-249 exception hierarchy for the Pathling DBAPI driver."""


class Error(Exception):
    """Base exception for the Pathling DBAPI driver."""


class InterfaceError(Error):
    """Exception for errors related to the database interface."""


class DatabaseError(Error):
    """Exception for errors related to the database."""


class DataError(DatabaseError):
    """Exception for errors due to problems with processed data."""


class OperationalError(DatabaseError):
    """Exception for errors related to the database's operation."""


class IntegrityError(DatabaseError):
    """Exception for referential integrity errors."""


class InternalError(DatabaseError):
    """Exception for internal database errors."""


class ProgrammingError(DatabaseError):
    """Exception for programming errors (e.g. SQL syntax, table not found)."""


class NotSupportedError(DatabaseError):
    """Exception for unsupported features."""
