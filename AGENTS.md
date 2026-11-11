# Repository guidance

## Secrets

- Never open, read, print, copy, or otherwise expose secret-bearing files or values, including `.env`, files under `secrets/`, credentials, API keys, tokens, and connection strings.
- Do not include secrets in source code, command output, logs, screenshots, commits, or messages.
- Use environment variable names and `.env.example` placeholders when documenting configuration. If a task requires using a secret, pass it to the relevant tool or process without displaying its value.
- Keep local secret files ignored by Git. Before committing, check that no secret-bearing files are staged; do not inspect their contents to do so.
