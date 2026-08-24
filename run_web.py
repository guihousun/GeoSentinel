"""Launch the formal geopolitical web service on a local-only port by default."""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv


load_dotenv(override=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the Geoenvironmental Intelligence public web service.")
    parser.add_argument("--host", default=os.getenv("NTL_WEB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("NTL_WEB_PORT", "8502")))
    parser.add_argument("--reload", action="store_true", help="Developer-only auto reload.")
    parser.add_argument("--dev-http", action="store_true", help="Allow non-Secure cookies for localhost-only development.")
    args = parser.parse_args()

    if args.dev_http:
        os.environ["NTL_WEB_FORCE_DEV_HTTP"] = "1"

    import uvicorn

    uvicorn.run(
        "web_api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        proxy_headers=True,
        forwarded_allow_ips="127.0.0.1",
        log_level="info",
    )


if __name__ == "__main__":
    main()
