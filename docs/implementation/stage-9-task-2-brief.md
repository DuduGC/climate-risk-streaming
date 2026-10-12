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

## Task 2 — Loop, saúde, Compose e integração

**Files:** completar `src/climate_logistics/alert_consumer.py`; criar `src/climate_logistics/consumer_health.py`, `tests/unit/test_consumer_health.py`, `tests/integration/test_alert_consumer.py`; modificar testes consumer Task1 quando necessário, `compose.yaml`, `.env.example` e `README.md`. Reports/aceite em `docs/implementation/stage-9-*` pelo agente/Root.

**Interfaces:**
- `ConsumerSettings.from_env(env:Mapping[str,str])->ConsumerSettings`: somente bootstrap existente e `ALERT_CONSUMER_GROUP_ID`; defaults Kafka9092/grupo operacional. Recusar brokerURL/credencial/controle/blank e grupo fora de `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`; erros sem ecoar valores.
- `run_consumer(settings:ConsumerSettings,*,stop:threading.Event,health_path:Path,consumer_factory:Callable|None=None,emit:Callable=emit_record)->int`: uma instância, subscribe topic fixo, poll1s, handlerTask1, metadata inicial e a cada15s/timeout5s, verificar topic/partitions/error. Config cliente inclui auto flags false, resetlatest, socket/session.timeout.ms10000, heartbeat.interval.ms3000 e max.poll.interval.ms60000. EOF não é registro; outros erros operacionais são genéricos e retornam1. `close()` em finally, sem commit global.
- `initialize_assignment(consumer:Any,partitions:Sequence[Any])->None`: callback de assignment consulta commits existentes; partições sem commit recebem high offset capturado como início intencional latest. Assign com esses offsets e confirmar explicitamente SOMENTE essas posições iniciais antes de processar mensagens/ready; verificar erros retornados. Isso evita perder a primeira mensagem após falha/restart quando o grupo ainda não tinha nenhum commit. Commit válido existente nunca é substituído por latest. Não usar posição avançada de uma mensagem já polled para essa inicialização.
- `write_health(path:Path,*,pid:int,ready:bool,updated_at:float,metadata_at:float)->None`: escrita atômica de JSON mínimo com relógio monotônico. `healthcheck(path:Path,*,now:float|None=None)->bool`: PID ativo, ready, timestamps finitos coerentes e ambos recentes<=45s; tratar falta/corrupção como unhealthy. CLI do módulo health lê `/tmp/alert-consumer-health.json`, retorna0/1, sem conexão Kafka.
- CLI consumer instala SIGTERM/SIGINT, usa stop Event, marca stopping/invalida heartbeat no finally. Saúde fica ready somente com metadata válida e assignment não vazio depois de poll; idle poll renova heartbeat, metadata expirada/falha não produz falso ready.
- Compose `alert-consumer`: mesma image/build de `producer`, command `python -m climate_logistics.alert_consumer`; env somente bootstrap/group; health `python -m climate_logistics.consumer_health`; defaults/recursos/restart/grace da spec. Dockerfile/requisitos existentes inalterados, sem novas portas/volumes.

- [ ] RED de config/cliente false flags e defaults; loop idle saudável, assignment vazio/metadataerror unhealthy; mensagem com erro não entra handler; Signal/exception fecha cliente e não usa commit global. Assignment preserva commits existentes e confirma baseline latest somente para partições novas antes de receber; falha na primeira mensagem não avança esse baseline, permitindo retomada.
- [ ] RED health arquivo faltante/corrompido/stale/PIDmorto/futuro e fluxo ready; GREEN com atomic replace e CLI local. Teste shutdown/process close e limites de espera pertinentes.
- [ ] Implementar loop/health/Compose/README; focused GREEN/Ruff. Documentar cache não durável, latest no primeiro uso, invalids confirmados, IDs históricos deQA reconhecíveis por timestamps.
- [ ] Root build somente imagem Python/newservice (`docker compose build alert-consumer`), sem recriar Producer ou nósFlink. Conferir cliente2.12.2/UID10001 e healthCLI sem cliente Kafka extra.
- [ ] Uma integração real opt-in `ALERT_CONSUMER_INTEGRATION=1`: container/grupo `weather-alerts-qa-<hex>` próprios; capturar ID antes de esperar, aguardar assignment/ready ANTESpublish. Publicar fixture HIGH com hash independente, repetição equivalente e inválido; verificar received1/duplicate1/invalid1 das fixtures por ID/partition/offset capturados no ACK (ignorar logs de outros alertas legítimos), ausência de raw, commits>=offset+1 e saúde idle. Recriar consumer do mesmo grupo e publicar outro alerta: antigos offsets confirmados não reaparecem, confirmando retomada. Deadline90s por fase; cleanup do próprio container em finally. Observer broker somente leitura, sem apagar grupos/topics/dados alheios.
- [ ] Root confirma suíte geral Python3.11/Ruff uma vez no código final e revisão independente; commit local `feat: run alert consumer with healthcheck and kafka integration` após gates. Corrigir apenas defeitos concretos porLunaMax; sem repetir QA cara etapa8.



## Contexto após Task1 aprovada

Contratos parse_alert/alert_fingerprint/AlertIdCache vivem em business_records (pure); alert_consumer importa/ex­põe AlertIdCache. Handler logs received/duplicate/divergent têm payload válido no campo alert, além timestampUTC e contexto topic/partition/offset. Invalidos sem payloadraw. Cache somente apóscommitOK. ReportTask1 disponível. Task2 deveráreutilizar essasinterfaces, sem mover/refatorar desnecessariamente. Doc da Kafka2.12.2 verificada: TopicPartition.error éatributo real. Task1 aprovada e commitada em0750ed9: Root128passedPython3.11/Ruff e revisorsemP1/P2. Task2 autorizada agora pelo plano integral aprovado.

Runtime Root controlaDocker/build/integração/commits; agente fazTDDlocalautônomo e report stage-9-task-2-report.md, sem esperarsinalporfunção/nemdespacharoutrosagentes. HostpytestPython3.12, Root valida3.11 exato. Ruffglobaldisponível. Flink operacional0b39a2acc47743560987333bea204347restored183/completed184+RUNNING: não parar/reiniciar/rebuildFlin/Producer; buildreusaDockerfilePython e upONLYalert-consumer. PlanoeuserpedemMVP simples. Sem extras.
