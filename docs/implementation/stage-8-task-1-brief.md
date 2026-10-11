# Etapa 8 — Business Sinks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persistir avaliações de janela e publicar alertas HIGH/CRITICAL, comprovando replay idempotente e recuperação dos sinks reais.

**Architecture:** Contrato puro compartilhado, uma SQL parametrizada com CTE/RETURNING para métricas e alertas no PostgreSQL, e KafkaSink nativo separado. Preservar o grafo stateful e acrescentar os destinos após o risco. Pequeno serializer Java transforma Row(key,payload) no ProducerRecord correto, sem implementar transporte Kafka próprio.

**Tech Stack:** Python3.11.14/PyFlink1.20.3, Java17, Kafka3.9.1, PostgreSQL17.11, JDBC connector3.3.0-1.20, pgJDBC42.7.14, Pytest8.4.2/Ruff0.14.0 e Compose existentes.

**Spec:** docs/superpowers/specs/2026-10-11-business-sinks-design.md — aprovada pelo usuário em2026-10-11.

## Global Constraints

- Principal orquestra, revisa e valida; implementadores GPT-6 Luna com esforço Max. Três tarefas sequenciais, cada uma com gate de revisão; não paralelizar tarefas dependentes.
- Mesmo checkout dedicado foundation conforme processo aprovado, commits locais sem push. Reavaliar isolamento com using-git-worktrees no preflight, sem criar infraestrutura concorrente.
- Persistir todas as classes; somente HIGH/CRITICAL (score>=50) geram alerta. Nenhum cooldown, consumer, serviço ou topic novo; etapa9 não iniciada.
- Avaliação21 preservada; alerta22 troca operation para weather_alert e acrescenta alert_id. Schema1/componentflink/rulesmvp-1. Identidade(loc,start_ms,end_ms,rules_version), hash SHA256 do JSON-list compacto UTF-8, ensure_ascii=False, sem sort de valores da lista.
- PostgreSQL UPSERT at-least-once, métrica/alerta da avaliação atômicos numa SQL; Kafka AT_LEAST_ONCE, keylocation_id. Não prometer exatamente uma mensagem nem atomicidade entre destinos.
- Preservar Row15, parser/qualidade, UIDs antigos, dedupTTL86400/cache0, janela30/5, watermark120s/idleness60s/lateness0, checkpoint60s/timeout120s/minpause10s/concorrência1/retenção2/restart3×10s e recursos atuais.
- Aplicação sem admin/DDL/DELETE; dimensão somente SELECT, métricas/alertas SELECT/INSERT/UPDATE. Secrets fora de URLs/CLI/logs/artefatos; senha em campo repr=False. JM/TM UID9999; somente credencial de aplicação no Flink.
- Principal controla Docker e fault tests. Nunca esperar silenciosamente por aprovação em subagente: enviar o comando/necessidade ao principal. Não apagar volumes/topics/checkpoints ou dados alheios.

## Review Focus

1. Replay com identidade igual: nenhuma linha adicional, nenhum alert_id diferente; avaliação divergente para a mesma identidade não deve deixar alerta stale ou ser ocultada.
2. Migração reaplicada sobre schema incompleto/incompatível: falhar e rollback, preservando dados e grants mínimos; não considerar IF NOT EXISTS uma validação de schema.
3. Native Kafka serializer: key precisa ser location_id e value JSON22, não Row serializada, JSON duplicado em ambos ou null key; comprovar no runtime Java real.
4. QA antiga/IDs desconhecidos/secrets: não escrever negócio em QA sem opt-in; desconhecidos continuam no diagnóstico; falha não pode imprimir senha/config sensível.
5. Falha após escrita externa mas antes de checkpoint: não confirmar entrega falsa; restauração do checkpoint anterior converge com UPSERT, mantendo dedup/offsets/acumuladores e slot coordenado.

## Task 1 — Contrato de negócio e persistência PostgreSQL

**Files:** criar src/climate_logistics/business_records.py, src/climate_logistics/business_sql.py, docker/postgres/migrations/001-business-sinks.sql, docker/postgres/02-business-schema.sh, docker/postgres/locations.csv, scripts/render_location_seed.py; modificar compose.yaml somente mounts PostgreSQL necessários; criar tests/unit/test_business_records.py, tests/unit/test_business_schema.py, tests/integration/test_postgres_business.py. README recebe comandos de migração/seed no gate desta tarefa.

**Interfaces:**
- BusinessRecordError(ValueError); AssessmentIdentity=tuple[str,int,int,str]. ASSESSMENT_FIELDS=frozenset das21 chaves; ALERT_FIELDS=ASSESSMENT_FIELDS|{'alert_id'}.
- parse_assessment(raw:str)->dict[str,object]: JSON<=16KiB/UTF-8, objeto/keys/envelope21 exatos, operação risk_assessment, schemaint1 não bool e versão mvp-1; tipos/metadados/finitude de todos os números, contagem positiva, fronteiras e extremos temporais, intervalos1..3600 ordenados. Reutilizar WINDOW_METRIC_FIELDS/inputs_from_metric/calculate_risk para validar scores/classe/componentes coerentes, sem manter outra tabela de risco.
- assessment_identity(assessment:Mapping[str,object])->AssessmentIdentity; alert_id(identity:AssessmentIdentity)->str; make_alert(assessment:Mapping[str,object])->dict[str,object]|None. Validar entrada e não mutá-la; None somente para LOW/MEDIUM válidos.
- jdbc_values(assessment:Mapping[str,object])->tuple[str,str|None]: JSON21 compacto determinístico (sort_keys=True/ensure_ascii=False/allow_nan=False) e alert_id para elegíveis, None para demais. A SQL usa exatamente dois placeholders, JSON e id, sem interpolar dados.
- business_sql.BUSINESS_UPSERT_SQL:str: CTE incoming(?::jsonb,?::text), UPSERT weather_metrics RETURNING, seguido de INSERT/UPSERT alertas HIGH/CRITICAL, payload22 construído a partir da mesma avaliação. Final da SQL é DML, sem result set SELECT retornado ao JDBC batch.
- render_location_seed: script stdlib que lê LOCATIONS e escreve CSV UTF-8 com cabeçalho location_id,city,latitude,longitude; artefato versionado é gerado, e teste exige igualdade com catálogo. Não copiar coordenadas manualmente em scripts independentes.
- 02-business-schema.sh: psql administrativo com ON_ERROR_STOP, nomes/credenciais via getenv existente, executa SQL montada em /opt/climate/postgres/migrations/001-business-sinks.sql e CSV /opt/climate/postgres/locations.csv. Mesma entrada para initdb novo e migração explícita no volume atual.

- [ ] Escrever RED: quatro classes e fronteira49/50; hash fixo independente, mudança de cada componente da identidade muda id, cópia idêntica mantém id/payload; entrada21 intacta, saída22 completa e LOW/MEDIUM=None. Recusar bool/null/nan/inf/tipos incorretos, campo extra/faltante, score/classe/componentes discrepantes, intervalos/timestamps incoerentes e reentrada weather_alert.
- [ ] Executar RED em Python3.11 e implementar o contrato mínimo; repetir focused GREEN/Ruff. Nenhum IO/tempo/SDK no módulo puro.
- [ ] Criar schema com colunas tipadas para os14 campos de dados temporais, schema_version e quatro campos de risco. PK(loc,start,end,rules_version), FKlocations, CHECKs coerentes, floats finitos e componentesJSONB com quatro keys/scores corretos. weather_alerts: alert_id PK, identidade UNIQUE/FKmetrics, score/level e payload22JSONB validado. Sem envelope constante duplicado na tabela de métricas; reconstrução em testes usa component/op constantes do contrato.
- [ ] Manter avaliação imutável por identidade: UPSERT repetido idêntico passa; trigger administrativo em weather_metrics rejeita alteração dos valores de negócio da mesma identidade com erro controlado. Isso evita downgrade stale de alerta sem inventar protocolo de revogação; nova regra usa nova versão/identidade. Testar divergência e rollback da SQL inteira, sem nova linha/alerta.
- [ ] Criar apenas um índice adicional por tabela de fatos (location_id,window_end_ms DESC); executar seed e grants mínimos. Validar catálogo/constraints/índices/grants ao reaplicar e abortar drift antes de commit. A transação da migração envolve DDL/seed/grants; não alterar bootstrap de roles da etapa1.
- [ ] Principal executa integração psql real em volume existente: migração2×, linha válida, replay2× sem aumento, avaliação divergente recusada, constraints negativas, FK e atomicidade métrica/alerta, roleapp semDDL/DELETE/admin. Capturar SQLSTATE/status sem senha e provar rollback.
- [ ] Principal verifica initdb num cluster PostgreSQL temporário separado do volume real (container ephemeral/tmpfs, sem porta pública, credenciais efêmeras não impressas); cleanup apenas desse container próprio, nunca volume existente. Confirmar mesmas estruturas/grants/seed no banco novo.
- [ ] Revisão independente contrato/SQL/segurança; principal confere source/RED/GREEN/integrados e corrige com LunaMax antes do commit local `feat: define idempotent business records and postgres schema`.


Plano aprovado em2026-10-11. Root executa Docker/QA real/commits; agente entrega alterações sem commit e sem subagentes. Relatório:docs/implementation/stage-8-task-1-report.md.
