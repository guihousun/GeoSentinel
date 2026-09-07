"""Source-grounded global monitoring for the geopolitical research workspace."""

from ssl_compat import configure_outbound_ssl

configure_outbound_ssl()

from .service import event_monitor_service

__all__ = ["event_monitor_service"]
