<!-- igor:generated-report:v1 -->
# Experiment results

Status: **Ready**

## Family and generation

| Field | Value |
| --- | --- |
| Family | Scientific &#124; family &lt;baseline&gt; |
| Family ID | 00000000-0000-0000-0000-000000000001 |
| Project ID | 00000000-0000-0000-0000-000000000002 |
| Generation | 3 |
| Generation ID | 00000000-0000-0000-0000-000000000003 |
| Git revision | current-revision |
| Protocol digest | sha256:protocol |
| Generation status | comparable |

## Seed status

| Seed | Job | Job state | Latest attempt | Extraction | Document status |
| --- | --- | --- | --- | --- | --- |
| 2 | 00000000-0000-0000-0000-000000000020 | succeeded | 1 | succeeded | success |
| 10 | 00000000-0000-0000-0000-000000000010 | succeeded | 1 | succeeded | success |

## Primary metric series

Per-seed extractor observations; no cross-seed statistics are inferred.

| Metric | Seed | Attempt ID | Value |
| --- | --- | --- | --- |
| validation.score | 2 | 00000000-0000-0000-0000-000000000022 | 0.875 |
| validation.score | 10 | 00000000-0000-0000-0000-000000000011 | 0.625 |

## Current-generation attempts

### Seed 2

| Field | Value |
| --- | --- |
| Job | seed two |
| Job ID | 00000000-0000-0000-0000-000000000020 |

Latest-job logs: `igor logs 00000000-0000-0000-0000-000000000020`.

#### Attempt 1 (latest)

| Field | Value |
| --- | --- |
| Attempt ID | 00000000-0000-0000-0000-000000000022 |
| State | succeeded |
| Project configuration digest | sha256:project |
| Job configuration digest | sha256:job |
| Metric schema version | 1 |
| Extraction | succeeded |
| Extractor program | python3 |
| Extractor command digest | sha256:extractor |
| Git repository | https://example.invalid/research.git |
| Git root | /fixture/project |
| Git branch | main |
| Git revision | current-revision |
| Git dirty | false |
| Git dirty digest | none |

Configuration and input identities:

| Path | Role | Bytes | SHA-256 |
| --- | --- | --- | --- |
| config/model.toml | scientific&#95;configuration | 42 | sha256:config |

Extractor document (including selection and per-class metrics):

```json
{
  "classes": {
    "class-a": {
      "f1": 0.75
    }
  },
  "metrics": {
    "validation.score": 0.875
  },
  "schema_version": 1,
  "selection": {
    "epoch": 4,
    "metric": "score",
    "split": "validation",
    "value": 0.875
  },
  "status": "success"
}
```

Indexed selection metadata:

```json
{
  "epoch": 4,
  "metric": "score",
  "split": "validation",
  "value": 0.875
}
```

No registered artifacts.

### Seed 10

| Field | Value |
| --- | --- |
| Job | seed ten |
| Job ID | 00000000-0000-0000-0000-000000000010 |

Latest-job logs: `igor logs 00000000-0000-0000-0000-000000000010`.

#### Attempt 1 (latest)

| Field | Value |
| --- | --- |
| Attempt ID | 00000000-0000-0000-0000-000000000011 |
| State | succeeded |
| Project configuration digest | sha256:project |
| Job configuration digest | sha256:job |
| Metric schema version | 1 |
| Extraction | succeeded |
| Extractor program | python3 |
| Extractor command digest | sha256:extractor |
| Git repository | https://example.invalid/research.git |
| Git root | /fixture/project |
| Git branch | main |
| Git revision | current-revision |
| Git dirty | false |
| Git dirty digest | none |

Configuration and input identities:

| Path | Role | Bytes | SHA-256 |
| --- | --- | --- | --- |
| config/model.toml | scientific&#95;configuration | 42 | sha256:config |

Extractor document (including selection and per-class metrics):

```json
{
  "classes": {
    "class-a": {
      "f1": 0.75
    }
  },
  "metrics": {
    "validation.score": 0.625
  },
  "schema_version": 1,
  "selection": {
    "epoch": 4,
    "metric": "score",
    "split": "validation",
    "value": 0.625
  },
  "status": "success"
}
```

Indexed selection metadata:

```json
{
  "epoch": 4,
  "metric": "score",
  "split": "validation",
  "value": 0.625
}
```

Artifacts:

| Path | Role | Physical state | Lifecycle | Bytes | SHA-256 | Link |
| --- | --- | --- | --- | --- | --- | --- |
| checkpoint.bin | checkpoint | removed | removed | 8 | sha256:checkpoint | unavailable |
| model &#91;best&#93;.bin | model | published | retained | 12 | sha256:model | [Open artifact](<file:///fixture/artifacts/model%20%5Bbest%5D.bin>) |

## Historical generations

Excluded from primary metric series.

| Generation | ID | Git revision | Protocol digest | Status | Job counts |
| --- | --- | --- | --- | --- | --- |
| 1 | 00000000-0000-0000-0000-000000000004 | old-revision | sha256:old-protocol | complete (not comparable) | failed: 1; superseded: 1 |
| 2 | 00000000-0000-0000-0000-000000000005 | replaced-revision | sha256:protocol | complete (not comparable) | superseded: 2 |

## Repairs

### Recovery 00000000-0000-0000-0000-000000000030

| Field | Value |
| --- | --- |
| Generation ID | 00000000-0000-0000-0000-000000000004 |
| Scope | historical generation |
| Attempt ID | 00000000-0000-0000-0000-000000000031 |
| State | failed |

Diagnosis:

```json
{
  "old_score": 9999.0,
  "reason": "historical failure"
}
```

Patch metadata:

```json
{
  "sha256": "sha256:old-patch"
}
```

Validation:

```json
{
  "passed": false
}
```

Decision:

```json
{
  "outcome": "supersede"
}
```

## Tool provenance

| Tool | Version |
| --- | --- |
| Renderer | deterministic (format v1) |
| Igor context builder | 0.1.0 |
| Extractor | unknown (not recorded) |
| Prompt | unknown (not recorded) |
| Provider | unknown (not recorded) |
| Model | unknown (not recorded) |
