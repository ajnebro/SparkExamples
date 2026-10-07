# Python Coding and Testing Guidelines (Python 3.11+)

All source code, identifiers, and comments **must be written in English**.

## 1. General Rules

- Target version: **Python 3.11+**
- One responsibility per function, one module per concept
- Use descriptive names for variables and functions

## 2. Typing

- Annotate the parameters and the return type of every function (use `|` for unions,
  e.g. `int | None`)
- Type hints are a style convention: the project doesn't run a static type checker

## 3. Function Rules

- One `return` per function (except parameter validation guards)
- Guard clauses only for invalid inputs
- Keep functions short (≤ 20 lines when possible) and simple: avoid nested `if` statements
  ("pyramid of ifs"). `ruff` checks that the complexity of a function stays low
- Prefer functions that receive their inputs as parameters and return a result, instead of
  using global variables

## 4. Error and Resource Handling

- Use context managers (`with`) for resources such as files
- Raise specific exceptions (`ValueError`, `TypeError`, etc.) for invalid input

## 5. Style and Documentation

- **ruff**: linting, formatting and import order (`make lint`, `make format`,
  `make format-check`)
- Use **Google-style docstrings** with Args / Returns / Raises
- Clarity over cleverness: no "smart" one-liners
- Every function has a docstring, and every example includes its tests

## 6. Unit Testing

- Framework: **pytest**
- Follow the **AAA pattern** (Arrange–Act–Assert)
- Name tests `test_should_<behavior>` (e.g. `test_should_raise_error_on_empty_list`) and put
  the detail in a one-line docstring
- Group related tests under `class Test<Subject>:`
- Prefer `@pytest.mark.parametrize` over near-duplicate tests for scenario variants
- Each test should focus on a single behavior
- Use fixtures for setup (e.g. the shared `spark_context`)
- Always include both success and failure paths
- Exception checks use `pytest.raises`
- Use plain `assert`

---

## Enforcement

- **ruff** (style, imports, complexity) — run `make lint` and `make format-check`
- **pytest** (tests) — run `make test`; `make check` runs everything
