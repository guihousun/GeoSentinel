"""Apply process-wide compatibility before CLI dependencies are imported."""

from ssl_compat import configure_outbound_ssl


SSL_MODE = configure_outbound_ssl()
