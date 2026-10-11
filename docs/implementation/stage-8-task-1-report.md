# Relatório — Etapa 8, Tarefa 1

**Status:** QA PostgreSQL do Root concluído e revisão scoped aprovada; gate da Tarefa 1 concluído. Nenhum commit foi criado; as tarefas 2 e 3 não foram iniciadas.

## Entrega

- `src/climate_logistics/business_records.py` valida JSON UTF-8 de até 16 KiB, contrato 21 campos, valores numéricos, tempos, intervalos e coerência com o motor de risco existente. Deriva identidade e SHA-256 determinísticos, alerta de 22 campos e valores JDBC sem mutar a entrada.
- `src/climate_logistics/business_sql.py` define a única instrução JDBC, com CTE de entrada e exatamente dois parâmetros (`jsonb`, `text`), UPSERT das métricas com `RETURNING` e UPSERT condicional do alerta na mesma instrução DML.
- `docker/postgres/migrations/001-business-sinks.sql` cria as três tabelas tipadas, chaves, FKs, verificações, índices e trigger que rejeita alteração de uma identidade existente. Um fingerprint em comentário valida colunas, nullability, constraints, índices, trigger/função, owners e ACLs antes de modificar as tabelas de negócio. Seed divergente, schema preexistente sem marcador ou drift abortam a transação.
- `docker/postgres/02-business-schema.sh` executa a migração administrativa com `ON_ERROR_STOP`; `compose.yaml` monta somente wrapper, SQL e CSV no serviço PostgreSQL. A mesma entrada roda no initdb e por chamada explícita em volume existente.
- `scripts/render_location_seed.py` gera `docker/postgres/locations.csv` diretamente de `LOCATIONS`, em UTF-8 e LF.
- `tests/unit/test_business_records.py`, `tests/unit/test_business_schema.py` e `tests/integration/test_postgres_business.py` cobrem o contrato puro, renderer, SQL parametrizada, replay, divergência, atomicidade e privilégios. A integração exige `POSTGRES_BUSINESS_TEST=1` e executa `psql` dentro do container, usando as credenciais já injetadas nele.
- `README.md` documenta geração do seed, initdb e reaplicação em volume existente.

## RED e GREEN

RED inicial no host do implementador (Python 3.14.6):

```text
python -m pytest tests/unit/test_business_records.py::test_valid_assessment_contract_has_exactly_21_fields_and_four_risk_classes -q --tb=short
1 failed: business_records ausente (falha esperada)

python -m pytest tests/unit/test_business_schema.py::test_location_seed_renderer_matches_catalog_and_versioned_utf8_artifact -q --tb=short
1 failed: renderer ausente (falha esperada)
```

O Root confirmou o RED inicial em Python 3.11.14 (`.venv/stage8-task1-red.log`). Depois da primeira implementação, o probe do Root revelou três lacunas. Os testes correspondentes foram escritos antes dos ajustes e produziram sete falhas esperadas; o `make_alert` já rejeitava surrogate UTF-8 durante a geração do hash.

GREEN focado mais recente no host do implementador:

```text
python -m pytest tests/unit/test_business_records.py tests/unit/test_business_schema.py -q --tb=short
91 passed in 0.79s

ruff check src/climate_logistics/business_records.py src/climate_logistics/business_sql.py scripts/render_location_seed.py tests/unit/test_business_records.py tests/unit/test_business_schema.py tests/integration/test_postgres_business.py
All checks passed

python -m pytest --collect-only tests/integration/test_postgres_business.py -q
3 tests collected
```

Os 91 testes acima foram executados com Python 3.14.6. O Root também reportou a suíte completa em Python 3.11.14: 573 aprovados e 25 ignorados em 7,15 s, com Ruff 0.14.0 limpo.

## QA PostgreSQL recebida

A primeira execução real da migração parou na criação da view temporária de fingerprint, antes do DDL de negócio: PostgreSQL 17 reportou ambiguidade no operador de concatenação entre `text` e `char` ao processar `pg_class.relkind`. Após os casts, uma segunda execução encontrou uma referência antiga ao alias `member`; ela também parou na view e foi revertida. A terceira execução chegou às constraints e foi revertida porque `jsonb_object_length(jsonb)` não existe nessa versão do PostgreSQL. A quarta execução foi revertida por precedência de operadores em `payload->'risk_components' - ARRAY[...]`. O Root então confirmou cold init e reaplicação para a versão anterior da migração. Na revisão de payload, o probe nativo confirmou aceitação indevida de `component:null`/`event_count:true`; a regressão produziu RED funcional (`.venv/stage8-task1-payload-red.log`: 1 failed, 2 deselected; transação revertida, nenhum fato gravado). O CHECK atual exige exatamente as 22 chaves, tipos JSON corretos, intervalos/ranges e os quatro scores derivados dos campos meteorológicos; o resultado externo do CHECK é `IS TRUE` para que SQL NULL também falhe. O fingerprint atual inclui flags e políticas RLS e ACLs por coluna, e o preflight também verifica owner do database/schema e atributo de replicação da role.

Para o schema DRAFT existente, foi preparado `.venv/stage8-task1-repair-draft.sql`, ignorado pelo Git. O Root revisou e aplicou esse reparo one-off como admin; a transação confirmou apenas alterações de metadata, sem alterar dados de negócio. O draft compara fingerprint recomputado da migração original com o marcador presente e recusa RLS/policies, ACL por coluna inesperada, owners divergentes, permissões não mínimas e tabelas de fatos não vazias.

O Root confirmou hot reapply da migração duas vezes no volume existente; os três testes PostgreSQL passaram (`3 passed`, 3,43 s). No cold strict, payloads inválidos com `component:null` e `event_count:true` foram rejeitados com SQLSTATE `23514`; drift de ACL por coluna e RLS/policies foi recusado sem autorreparo. O cluster temporário de teste foi removido pelo Root.

O arquivo `.gitattributes` fixa `docker/postgres/locations.csv` em LF (`/docker/postgres/locations.csv text eol=lf`) para preservar os bytes do seed versionado em checkouts Windows.

As tarefas 2 e 3 permanecem não iniciadas.

**Limite atual:** o implementador não executou Docker nem `psql`; o QA PostgreSQL descrito foi executado pelo Root. A igualdade integral entre as linhas de métricas e alertas é garantida pelo CTE único controlado de escrita; não há CHECK cruzando as tabelas para SQL manual fora desse fluxo de produto. Nenhum `.env` foi lido, nenhum dado foi apagado e os arquivos de Flink, imagens, checksums, dependências e credenciais não foram alterados.
