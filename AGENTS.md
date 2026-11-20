# Codex working rules for Auspex

These instructions apply to Codex tasks in this repository. Prioritize small, verifiable changes and avoid unnecessary tool usage.

## Scope and investigation
- Work on one explicitly requested defect at a time. Do not broaden the task into a repository-wide audit or unrelated refactor.
- Before editing, identify the relevant file/function, the evidence of the defect, and the smallest reasonable fix.
- Prefer targeted searches and short file excerpts. Do not reread unchanged files or repeat identical diagnostic commands without new evidence.
- Do not spawn subagents, run production market scans, modify production data, or add dependencies unless specifically authorized.

## Bounded repair workflow
- Maximum **3 edit-and-test cycles** per task. This is a stopping rule, not a target.
- Run the smallest relevant test after a change; run a full suite or production build only when explicitly requested or clearly required for safety.
- If the same failure recurs twice without new diagnostic evidence, stop rather than guessing.
- Stop immediately after the requested change passes targeted verification; do not pursue optional optimizations.
- Never leave long-running development servers, polls, or scans active after testing.

## Preserve behavior
- Keep edits localized and reversible. Preserve existing functionality, retailer integrations, price history, and persisted data.
- Do not delete or overwrite historical records to solve performance issues.
- Report uncertainty instead of claiming unverified success.

## Completion report
Respond concisely with: root cause (confirmed or suspected), changed files, targeted test result, remaining blocker(s), and number of repair cycles.

These are workflow guidelines, **not** a guaranteed token or account-usage cap. If the budget is exhausted, preserve work, stop, and ask for further direction.
