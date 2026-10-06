# CLAUDE.md

Proyecto de aplicaciones con **Apache Spark 4.2.0** (PySpark) en **Python**.

## Entorno

- Usa siempre el entorno virtual Conda **`spark420`**. Actívalo con `conda activate spark420`
  o ejecuta los comandos con `conda run -n spark420 <comando>`.
- No instales paquetes en el entorno base ni en otros entornos. Si falta una dependencia,
  añádela a `spark420` y regístrala en el fichero de dependencias del proyecto.
- Python 3.11+ (ver [CODING_GUIDELINES.md](CODING_GUIDELINES.md)).

## Guías del proyecto

Sigue estrictamente estas dos guías; son de obligado cumplimiento:

- @CODING_GUIDELINES.md — código, tipado, estilo, tests.
- @GIT_GUIDELINES.md — commits y flujo de git.

### Resumen de código

- Todo el código, identificadores, comentarios y docstrings en **inglés**.
- Tipado en código nuevo o modificado; docstrings estilo Google (Args / Returns / Raises).
- Un único `return` por función (salvo guardas de validación), funciones cortas (≤ 20 líneas),
  complejidad cognitiva ≤ 10, sin condicionales anidados.
- Excepciones específicas para errores; `with` para recursos.
- Linting y formato con **ruff** (`make lint`, `make format`); tests con **pytest**
  (patrón AAA, nombres `test_should_<behavior>`, `parametrize` para variantes).

### Resumen de git

- Commits en formato **Conventional Commits** (`<type>[(scope)][!]: <imperative description>`),
  en inglés, asunto ≤ 72 caracteres y sin punto final.
- Commits atómicos: un cambio lógico por commit; nunca mezclar código de producción con tests
  ni código con documentación.
- Antes de commitear, ejecuta `make test` (o `pytest tests/ -x`) y `make lint`.
- No hagas commit ni push salvo que se pida expresamente.
- La sección "Branches and releases" de `GIT_GUIDELINES.md` describe el flujo de Evolver-Studio
  (`develop`/`main`, `CHANGELOG.md`, versiones `.dev0`); aplícala solo si este repositorio adopta
  ese flujo.

## Convenciones para Spark

- Crea la `SparkSession` en un único punto de entrada y pásala como parámetro; no la
  uses como variable global.
- Prefiere la API de DataFrame/SQL frente a RDD; evita UDF de Python cuando exista una
  función nativa de `pyspark.sql.functions`.
- Mantén las transformaciones en funciones puras (`DataFrame -> DataFrame`) para poder probarlas
  con una sesión local de Spark en los tests (`master("local[*]")`).
- Detén la sesión (`spark.stop()`) al terminar la aplicación.
