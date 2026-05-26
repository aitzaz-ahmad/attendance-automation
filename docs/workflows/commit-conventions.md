# Commit Conventions

## Commit Message Format

Commit format:

    ETLP-<issue_number>: <summary>

Examples:

    ETLP-8: adds GitHub Actions CI workflow
    ETLP-9: adds Ruff and MyPy validation
    ETLP-21: splits Raspberry Pi client module
    ETLP-29: implements canonical transformation module
    ETLP-33: adds messaging interface

---

## Rules

- use present tense verbs only
- start the summary with a verb
- preserve the exact `ETLP-<issue_number>` prefix
- keep summaries concise and subsystem-oriented
- avoid vague wording
- avoid trailing punctuation
- one commit should normally correspond to one GitHub issue

---

## Disallowed Examples

Avoid summaries such as:

- misc updates
- fixes stuff
- cleanup
- improvements
- changes

Avoid past tense verbs such as:

- added
- updated
- implemented
- fixed
