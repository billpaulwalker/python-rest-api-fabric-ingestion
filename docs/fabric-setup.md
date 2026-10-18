# Fabric Setup: GitHub Issues Notebook

## Shared modules

The notebook imports `src/api_ingestion` from its built-in resources as
`builtin.api_ingestion`. After changing the package, upload the updated
folder to the notebook's **Resources → Built-in** files. The notebook
requires `fabric_policy.py` alongside the existing modules.

## Runtime configuration and secrets

The committed notebook contains no secrets and no environment-specific
values. Two settings are resolved at runtime. In order of precedence,
each comes from:

1. A notebook parameter (pipeline notebook activity → base parameters).
2. A Spark property on the Fabric environment attached to the notebook.

| Parameter | Spark property | Example |
| --- | --- | --- |
| `key_vault_url` | `spark.api_ingestion.key_vault_url` | `https://<vault-name>.vault.azure.net/` |
| `github_token_secret_name` | `spark.api_ingestion.github_token_secret_name` | `github-api-token` |

If neither is set, the run fails at `bronze` with `Missing runtime setting`.

The committed pipeline (`pipelines/pl_github_issues_ingestion.json`)
supplies both values as base parameters on its notebook activity, so
pipeline runs need no environment properties. Interactive runs need the
Spark properties. These values identify an environment but are not
secrets. Use deployment pipeline rules to change them per environment.

The GitHub token itself is only ever read from Key Vault with
`notebookutils.credentials.getSecret`. The identity running the notebook
needs the Key Vault Secrets User role (or a Get secret access policy).

Other parameters (`owner`, `repository_name`, API limits, lookback and
overlap) have non-sensitive defaults in the parameters cell and can be
overridden per pipeline.

## Exporting the notebook to Git

Exported `.ipynb` files include cell outputs (identity, run IDs, data
previews) and DataFrame widget state. Clear outputs before exporting, or
use Fabric Git integration, which stores source without outputs.
`tests/test_notebook.py` fails if outputs, widget state, token patterns,
or Key Vault URLs are committed.

The notebook metadata keeps the default Lakehouse binding (workspace and
Lakehouse IDs). These are identifiers, not secrets. Use deployment
pipeline rules to rebind them per environment.
