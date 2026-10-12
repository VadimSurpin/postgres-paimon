# Contributing

## Prerequisites

The C++ extension has non-trivial native dependencies. The easiest way to get a working build environment is Docker:

```bash
docker build -f docker/Dockerfile -t postgres-paimon-build .
docker run --rm -v "$PWD":/src -w /src/extension postgres-paimon-build make
```

For a native build, see [Requirements](README.md#requirements) in the README.

## Building

```bash
# C++ extension
cd extension
make

# Java tooling
mvn package
```

## Running the Java integration tests

The test JARs connect to a live PostgreSQL instance with the extension installed. Set up the instance first:

```ini
# postgresql.conf
shared_preload_libraries = 'paimon_heap'
paimon_heap.warehouse = '/tmp/paimon-warehouse'
```

```sql
CREATE EXTENSION paimon_heap;
CREATE DATABASE testdb;
```

Then run any test JAR:

```bash
java -jar java/target/paimon-feature-test.jar /tmp/paimon-warehouse \
  "jdbc:postgresql://localhost:5432/testdb?user=postgres&password="
```

## Code style

- C++: follow the surrounding code. `clang-format` with the LLVM style is a reasonable baseline.
- Java: standard Java conventions; the writer uses Spring Boot.
- No new dependencies without a discussion in an issue first — the native dependency surface is already large.

## Submitting changes

1. Open an issue describing the change before starting significant work.
2. Keep commits focused; one logical change per commit.
3. Update `README.md` if you add or change a GUC, supported DDL operation, or build step.
4. All source files must carry the `SPDX-License-Identifier: Apache-2.0` header.

## Java formatting

Both Maven modules use Spotless with the Eclipse Google Java Style profile in
`config/java-google-style.xml`. The project overrides declaration formatting:
each parameter and the closing `)` occupy separate lines for constructors,
methods, and records. From the repository root:

```bash
mvn spotless:apply   # Format Java sources and tests
mvn spotless:check   # Check formatting without modifying files
```

Formatting is checked during `validate`, so `mvn test`, `mvn package`, and CI also
check it. Run `mvn spotless:apply` before committing Java changes. `.editorconfig`
sets two-space Java indentation; Spotless is the shared formatter for line wrapping
and import ordering.
