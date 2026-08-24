# Security Policy

NTL-GPT is a research-preview application that executes local geospatial tools and may run generated Python scripts. Deploy it only in an environment whose users, credentials, files, and network access you control.

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
- Bind Streamlit to `127.0.0.1` and expose it through a maintained HTTPS reverse proxy.
- Use PostgreSQL credentials with least privilege.
- Restrict access to the server, `user_data`, Earth Engine credentials, and Earthdata tokens.
- Keep concurrency, subprocess timeout, and workspace quota limits enabled.
- Back up the database and user workspace separately.
- Review generated scripts and outputs before using them in operational decisions.
