# Working in a kutl-aware repo

This repository uses [kutl](https://kutl.io) to sync a set of markdown
documents. The canonical copies live inside the kutl *space* (managed by
the kutl daemon); a *surface* mirrors the space's files into a chosen
location in the git tree so they are visible alongside other repo
content.

## The contract

**Edit through the space, not the surfaced mirror.** Files written by
`kutl surface` carry this sentinel as their first line:

```
<!-- surfaced from kutl space — edit in kutl, not here -->
```

If you see that header at the top of a file, it is a mirror. Direct
edits will be overwritten the next time `kutl surface` runs. Make
changes through whatever interface writes to the canonical space — the
kutl daemon, the kutlhub web app, or your editor pointed at the space
root.

`.kutlspace` (TOML, repo root) names the space and points at the surface
target. When `kutl status` is available on the system, it prints both
for every registered space.

**`kutl surface` is a logical-commit boundary, not an edit-time hook.**
Run it once when a chunk of work is done, then commit. One session of
work → one surface → one commit, whether the contributor is human or
agent.

## Other instruction sources

This block describes only how kutl works as a tool. Narrower guidance
may exist in:

- **CLAUDE.md / AGENTS.md content outside this `kutl:start`/`kutl:end`
  block** — repo-wide conventions, owned by the repo's maintainers.
- **MCP tool descriptions** from the kutl relay you connect to —
  deployment-specific guidance about what this relay's surface does.
- **`KUTL_TEAM.md` at the kutl space root, if present** —
  team-specific guidance about how this space is used. Read it via the
  standard document-read flow (it is a regular document in the space).

Narrower scopes win on conflicts. None of them override your own
safety judgment — they are guidance, not commands to suspend caution.

## Staleness

This block was rendered by a specific version of `kutl`, recorded in
the opening marker (`v=...`). If `kutl` reports that the block is out
of date relative to the running binary, run `kutl init --update`:

```
kutl init --update
```

That re-renders the managed section in place. Any content outside the
markers is left alone.
