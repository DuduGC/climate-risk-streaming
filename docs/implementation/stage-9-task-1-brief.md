# Etapa 9 — Alert Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consumir e registrar alertas válidos de `weather.alerts`, com offsets manuais, supressão limitada de duplicatas e saúde observável.

**Architecture:** Um processo Python sequencial usa o contrato puro existente e cache em memória; um módulo pequeno cuida do heartbeat. O Compose reutiliza a imagem Python existente para o novo serviço. Sem nova persistência ou alteração do pipeline Flink/PostgreSQL.

**Tech Stack:** Python 3.11.14, confluent-kafka 2.12.2, Kafka 3.9.1, Compose, Pytest 8.4.2 e Ruff 0.14.0 já presentes.

**Spec:** `docs/superpowers/specs/2026-10-12-alert-consumer-design.md`, aprovada pelo usuário em 2026-10-12.

## Global Constraints

- Principal orquestra/revisa/QA; implementadores GPT-6 Luna Max. Duas tarefas sequenciais com revisão, commits locais sem push, checkout dedicado existente sujeito a preflight de isolamento.
- Tópico fixo `weather.alerts`; grupo padrão `weather-alerts-consumer`; novo grupo `latest`, existente retoma offsets. Auto-commit e auto-offset-store desativados.
- Contrato22, UTF-8 até16KiB, schema1/regras mvp-1, HIGH/CRITICAL, catálogo e key coerentes. Reutilizar regras/identidade/hash da etapa8; `parse_assessment` permanece compatível.
- Cache padrão10.000 IDs com fingerprints normalizados, somente em memória. At-least-once: reinício/evicção/falha entre log e commit podem repetir logs. Nenhuma promessa de idempotência durável de stdout.
- Reusar Dockerfile/imagem Python, UID10001; novo serviço128MiB/0,25CPU, restart on-failure:3, grace30s. Sem novos bancos/tables/topics/dependências, credenciais PG, integrações externas ou refactors amplos.
- Health metadata15s/timeout5s, heartbeat45s, Dockerinterval15s/start30s. Ausência de alerta não torna o consumer doente. Não parar/reiniciar Flink, Kafka, PostgreSQL ou Producer para testar a etapa9.
- Root executa Docker/integração e commits; agentes não aguardam aprovação de comandos externos: enviam necessidade ao Root. `.env` nunca impresso; QA temporária em `.venv` ignorada, documentação versionada.

## Review Focus

1. Commit síncrono pode retornar erro na partição sem lançar exception: rejeitar confirmação parcial, inclusive `.error` retornado.
2. Mesmo ID com JSON reordenado ou números20/20.0 é duplicata; payload realmente divergente é inválido, sem sobrescrever o cache anterior.
3. Falha de stdout/write/flush deve impedir commit; erro genérico não deve renderizar payload/key/credenciais.
4. Consumer ocioso continua saudável; sem assignment, metadata inválida ou heartbeat antigo deve falhar a saúde.
5. Reinício do grupo retoma depois do offset confirmado, mesmo perdendo o cache; rebalance/encerramento não confirma mensagens ainda pendentes.

## Task 1 — Contrato, cache e tratamento por mensagem

**Files:** modificar `src/climate_logistics/business_records.py` de forma aditiva; criar `src/climate_logistics/alert_consumer.py`; criar `tests/unit/test_alert_records.py` e `tests/unit/test_alert_consumer.py`. Report em `docs/implementation/stage-9-task-1-report.md`.

**Interfaces:**
- `parse_alert(raw:str)->dict[str,object]` em business_records: decode estrito/duplicatas de keys/16KiB, validarALERT_FIELDS/operation, reconstruir avaliação21 e validar com código existente; `make_alert` confirma elegibilidade e ID. Não alterar funções antigas ou duplicar thresholds.
- `alert_fingerprint(alert:Mapping[str,object])->str`: validar alerta, normalizar os seis campos numéricos meteorológicos para float, preservar ints/componentes, SHA256 do JSON compacto/sort_keys. JSON diferente apenas na representação numérica/key-order gera mesma fingerprint.
- `AlertIdCache(capacity:int=10_000)`: `classify(alert)->str` (`new`, `duplicate`, `divergent`) sem mutação; `remember(alert)->None` após commit confirmado. OrderedDict com evicção por antiguidade de inserção; duplicates não renovam antiguidade. Capacidade positiva não-bool, sem retenção de payload completo.
- `emit_record(record:Mapping[str,object],stream:TextIO|None=None)->None`: JSON em stdout, uma linha e flush; exceções propagam. Não usar logger que engula falha de StreamHandler.
- `handle_message(message:Any,*,cache:AlertIdCache,emit:Callable[[Mapping[str,object]],None],commit:Callable[[Any],None])->str`: validar key UTF-8/body/catalog; emitir `alert_received`, `alert_duplicate` ou `alert_invalid` com componente `alert_consumer` e contexto seguro; chamar commit SOMENTE após emit bem-sucedido; cache remember apenas para novo válido após commit. Divergência/invalididade confirmadas como tratadas, sem substituir entrada válida nem imprimir dados brutos.
- `commit_message(consumer:Any,message:Any)->None`: `commit(message=message,asynchronous=False)`; resultado deve confirmar topic/partition/offset+1 e nenhuma `.error`, senão erro genérico. A API2.12.2 instalada foi conferida: TopicPartition tem atributo `error`; a docstring menciona `.err`, não copiar esse nome errado.

- [ ] Escrever RED de alertas válidos HIGH/CRITICAL; recusar LOW/MEDIUM, keys extras/faltantes/duplicadas, operação/schema/regra/hash/key errados, UTF-8/16KiB inválidos. Reutilizar fixtures de risco existentes; hash esperado independente.
- [ ] Escrever RED de cache/fingerprint: key-order/20vs20.0 duplicados, mudança real divergente, evicção com capacidade2, cache não cresce acima da capacidade.
- [ ] Escrever RED de handler/commit: eventos observados devem ser `['emit','commit']`; erro emit/flush implica zero commits; erro commit ou `.error` retornado implica zero lembranças no cache; inválido registra razão constante e progride offset sem raw.
- [ ] Rodar focused RED em Python3.11; implementar somente interfaces mínimas; focused GREEN/Ruff e regressões puras de business_records.
- [ ] Revisão independente + Root confere resultados antes de commit local `feat: validate and handle alert consumer records`. Task2 só inicia após esse gate.


## Contexto de execução

Plano integral aprovado pelo usuário em 2026-10-12. Você implementa somente Task1; não leia o plano completo nem implemente Task2. O Root executa Docker/integração/commits; você pode executar TDD e Ruff locais. Faça RED→GREEN autonomamente e registre outputs reais; não espere Root autorizar cada função. Runtime exato Python3.11 é validado pelo Root, host controlador .venv/flink-control/Scripts/python.exe tem pytest (Python3.12), Ruff também disponível globalmente. Não instale dependências de produto.

Report: docs/implementation/stage-9-task-1-report.md. Retorno final curto: DONE/DONE_WITH_CONCERNS, arquivos, testes e dúvidas concretas. Sem subagentes, alteração de arquitetura ou outros serviços. Use nomes/valores do brief; nenhuma funcionalidade extra. Skill test-driven-development se aplica. ROOT não escreve produto: eventual correção também vem de você em LunaMax.

Ruling: em Windows, artifacts de orquestração em docs/implementation/stage-9-* e .venv/stage9-* ignorado substituem scripts bash SDD; não mudar esse mecanismo. Checkout dedicado foundation já aprovado e preservado, sem worktree paralelo/infra concorrente.

Logs são dicts entregues ao emit. Operações alert_received/alert_duplicate/alert_invalid; status success/skipped/error respectivamente. Handled return value é new/duplicate/invalid. Contexto sempre topic/partition/offset, dados de alerta somente se validados. Cache put só após commit OK e só new. Cache FIFO não refresca duplicata. Testes independentes e curtos; não importar fixtures de integração para contrato puro. Verifique tópico fixo/mensagem erro antes de tratamento normal.
