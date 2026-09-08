# Security Policy

GeoSentinel is a controlled research pilot. The current DSH product runs bounded jobs in Docker and keeps multiuser ownership in its platform plugin. The legacy Python runtime has different isolation guarantees. Deploy only where you control users, credentials, files and network access; Docker is not an absolute guarantee against hostile code.

## Reporting a Vulnerability

Do not open a public issue for vulnerabilities, exposed credentials, authentication bypasses, unsafe path handling, or remote-code-execution risks. Contact the repository owner privately through the contact channel listed on the GitHub profile and include:

- affected version or commit;
- reproduction steps;
- expected and observed behavior;
- impact and affected data;
- a proposed mitigation, if available.

Do not include active API keys, tokens, database passwords, or private datasets in the report.

## Deployment Baseline

- Keep `.env` outside version control.
- Bind the product to `127.0.0.1` behind a maintained HTTPS reverse proxy; configure exact allowed hosts and secure cookies.
- Do not expose personal DSH control APIs, host terminals, Docker sockets or database ports.
- Use strong administrator passwords and revoke leaked invitations. No deployment credentials are distributed in this repository.
- Protect the complete DSH home, SQLite database, sessions and project artifacts. Legacy PostgreSQL deployments require their own backup and least-privilege policy.
- Restrict access to the server, `user_data`, Earth Engine credentials, and Earthdata tokens.
- Keep concurrency, subprocess timeout, and workspace quota limits enabled.
- Back up the database and user workspace separately.
- Review generated scripts and outputs before using them in operational decisions.
