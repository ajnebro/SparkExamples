# Git and Commit Guidelines

This project follows the [Conventional Commits](https://www.conventionalcommits.org/) specification.

## Message format

```
<type>[(scope)][!]: <short imperative description>

[optional body]

[optional footer(s)]
```

- Subject line: imperative mood ("add", not "added"/"adds"), no trailing period, ≤ 72 characters.
- `scope` is optional and names the affected area, e.g. `rdd`, `dataframes`, `build`, `docs`.
- `!` after the type/scope marks a breaking change (see below).
- Identifiers and messages must be in **English**, per [CODING_GUIDELINES.md](CODING_GUIDELINES.md).

## Allowed types

| Type       | When to use                                                         |
| ---------- | ------------------------------------------------------------------- |
| `feat`     | A new feature, example, or public function                         |
| `fix`      | A bug fix                                                           |
| `perf`     | A change that improves performance without changing behavior        |
| `test`     | Adding or correcting tests (no production code)                     |
| `refactor` | Code change that is neither a fix nor a new feature                 |
| `style`    | Formatting only (whitespace, `black`/`ruff` fixes); no logic change |
| `docs`     | Documentation only (`README.md`, `AGENTS.md`, docstrings, etc.)     |
| `build`    | Packaging or dependency changes (`pyproject.toml`, `setup.cfg`)     |
| `ci`       | Changes to CI configuration (`.github/workflows/*.yml`)             |
| `chore`    | Tooling, `.gitignore`, and other changes that don't fit above       |
| `revert`   | Reverts a previous commit                                           |

## Breaking changes

Mark commits that change or remove a public API (e.g. the signature of a public function) with `!`:

```
feat(rdd)!: change the argument order of add_numbers
```

Explain the impact and migration in the body/footer:

```
BREAKING CHANGE: `add_numbers` now takes `numbers` before `spark_context`.
Update call sites accordingly.
```

## Body and footers

- Add a body when the *why* isn't obvious from the subject line alone (wrap at ~72 chars).
- Reference issues with a footer, e.g. `Closes #187`, `Refs #42`.
- Trailers such as `Co-Authored-By:` are allowed and go last.

## Atomic commits

Each commit must represent **one single logical change**. Guidelines:

- If the commit message needs "and" to describe what it does, split it into two commits.
- Before committing, run the checks relevant to the change:
  - `make test` (or `pytest tests/ -x`) — tests pass
  - `make lint` and `make format-check` — `ruff` is clean
  - `make check` runs all of them
- Never mix production code changes with test changes in the same commit.
- Never mix code changes with documentation changes in the same commit.

## Branches

Work happens on `main` as atomic commits. For larger changes, use a topic branch and
squash-merge the pull request, so `main` keeps one Conventional Commit per logical change.
If a merge commit is unavoidable, the default `Merge pull request #N from ...` message is
acceptable as an exception to the `<type>: ...` format.

## Examples

```bash
# Good
git commit -m "feat(rdd): add max_number example"
git commit -m "fix(rdd): stop the Spark context when the example fails"
git commit -m "test(rdd): add tests for max_number"
git commit -m "perf(rdd): avoid a shuffle in count_lines_containing_word"
git commit -m "docs: add a learning path to the README"
git commit -m "ci: run tests on Python 3.11 and 3.12"

# Bad — too broad, mixes concerns
git commit -m "add stuff and fix tests and update readme"

# Bad — no type, not English
git commit -m "cambios"
```
