# Security policy

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Report them privately via GitHub's [Security Advisories](https://github.com/VadimSurpin/postgres-paimon/security/advisories/new) feature, or send an email to the repository owner.

Include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- PostgreSQL version, OS, and extension version

You can expect an acknowledgement within 72 hours and a resolution timeline within 14 days for confirmed issues.

## Scope

This project is a PostgreSQL extension that runs inside the PostgreSQL server process with superuser privileges. The relevant threat model is:

- **In scope**: vulnerabilities reachable by a non-superuser database user that elevate privileges or cause data corruption/loss.
- **Out of scope**: attacks that already require superuser or OS-level access, denial-of-service via ring buffer exhaustion (this is a known design trade-off, documented in the README).
