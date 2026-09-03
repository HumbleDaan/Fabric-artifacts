# Security Policy

## Reporting a vulnerability

Do not open a public issue for suspected vulnerabilities, exposed credentials, authorization bypasses, or unsafe behavior in the workspace access utility.

Use GitHub private vulnerability reporting under **Security > Advisories > Report a vulnerability**. If that option is unavailable, contact the repository owner through an established private channel and ask for a secure reporting route without disclosing vulnerability details publicly.

Include the affected notebook or script, a minimal reproduction, the security impact, and any suggested mitigation. Do not include real customer data, tenant identifiers, tokens, secrets, or report definitions.

## Supported versions

Only the latest revision is maintained. Because Fabric APIs and `semantic-link-labs` can change independently, consumers must pin accepted package versions and repeat a controlled tenant pilot after runtime or dependency upgrades.

## Operational security

- Run the audit with a dedicated, least-privilege identity.
- Store credentials in Azure Key Vault; never place secret values in notebook source.
- Restrict the output Lakehouse because results can contain names, URLs, user identifiers, and visual endpoint declarations.
- Keep the grant utility in dry-run and probe-only modes unless a time-boxed campaign has explicit approval.
- Verify and document revocation after every temporary access campaign.