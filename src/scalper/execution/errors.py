"""Broker failures. Kite's error_type names are the exception class names."""

from __future__ import annotations


class BrokerError(Exception):
    def __init__(
        self,
        message: str,
        *,
        error_type: str | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.status_code = status_code


class TokenException(BrokerError):
    """Kite rejected the session."""


class InputException(BrokerError):
    """Kite rejected the request as malformed."""


class NetworkException(BrokerError):
    """The call did not finish. This is not a broker reject."""


class OrderException(BrokerError):
    """Kite accepted the HTTP call and refused the order."""


class PermissionException(BrokerError):
    """Kite refused the call for this account."""


class DataException(BrokerError):
    """Kite's payload was missing or the wrong shape."""


class GeneralException(BrokerError):
    """Kite reported a general failure."""


class OrderMappingError(DataException):
    """An order or position row could not be mapped without inventing a state."""


class AmbiguousOrderOutcome(BrokerError):
    """A place call may have reached Kite, and the tag was not resolved.

    This is not a reject. The caller must engage the kill file and alert.
    The same order must not be posted again.
    """


KITE_ERROR_TYPES: dict[str, type[BrokerError]] = {
    "TokenException": TokenException,
    "InputException": InputException,
    "NetworkException": NetworkException,
    "OrderException": OrderException,
    "PermissionException": PermissionException,
    "DataException": DataException,
    "GeneralException": GeneralException,
}
