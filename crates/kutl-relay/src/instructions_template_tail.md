## Default-visibility assumption for OSS

**Signals here reach MCP-connected agents only.** This OSS-relay
deployment does not bundle a signal-rendering tool for humans.
If you want humans in editors / file-sync clients to see
something, write it into the doc body — the doc body is the
lowest-common-denominator reach mechanism. Pair signal emissions
with in-doc forms (callout for flags, marker for comments,
`## ?` / `## =` heading for decisions, `@did:...` token for
mentions) for cross-cutting reach.

If your deployment runs a custom human-facing client that surfaces
signals (uncommon but possible), tool descriptions on this relay
will tell you so. Trust the per-tool descriptions over this
general note when they conflict.
