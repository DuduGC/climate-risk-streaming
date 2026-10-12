# Etapa 9 — Alert Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

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

- [x] Escrever RED de alertas válidos HIGH/CRITICAL; recusar LOW/MEDIUM, keys extras/faltantes/duplicadas, operação/schema/regra/hash/key errados, UTF-8/16KiB inválidos. Reutilizar fixtures de risco existentes; hash esperado independente.
- [x] Escrever RED de cache/fingerprint: key-order/20vs20.0 duplicados, mudança real divergente, evicção com capacidade2, cache não cresce acima da capacidade.
- [x] Escrever RED de handler/commit: eventos observados devem ser `['emit','commit']`; erro emit/flush implica zero commits; erro commit ou `.error` retornado implica zero lembranças no cache; inválido registra razão constante e progride offset sem raw.
- [x] Rodar focused RED em Python3.11; implementar somente interfaces mínimas; focused GREEN/Ruff e regressões puras de business_records.
- [x] Revisão independente + Root confere resultados antes de commit local `feat: validate and handle alert consumer records`. Task2 só inicia após esse gate.

## Task 2 — Loop, saúde, Compose e integração

**Files:** completar `src/climate_logistics/alert_consumer.py`; criar `src/climate_logistics/consumer_health.py`, `tests/unit/test_consumer_health.py`, `tests/integration/test_alert_consumer.py`; modificar testes consumer Task1 quando necessário, `compose.yaml`, `.env.example` e `README.md`. Reports/aceite em `docs/implementation/stage-9-*` pelo agente/Root.

**Interfaces:**
- `ConsumerSettings.from_env(env:Mapping[str,str])->ConsumerSettings`: somente bootstrap existente e `ALERT_CONSUMER_GROUP_ID`; defaults Kafka9092/grupo operacional. Recusar brokerURL/credencial/controle/blank e grupo fora de `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`; erros sem ecoar valores.
- `run_consumer(settings:ConsumerSettings,*,stop:threading.Event,health_path:Path,consumer_factory:Callable|None=None,emit:Callable=emit_record)->int`: uma instância, subscribe topic fixo, poll1s, handlerTask1, metadata inicial e a cada15s/timeout5s, verificar topic/partitions/error. Config cliente inclui auto flags false, resetlatest, socket/session.timeout.ms10000, heartbeat.interval.ms3000 e max.poll.interval.ms60000. EOF não é registro; outros erros operacionais são genéricos e retornam1. `close()` em finally, sem commit global.
- `initialize_assignment(consumer:Any,partitions:Sequence[Any])->None`: callback de assignment consulta commits existentes; partições sem commit recebem high offset capturado como início intencional latest. Assign com esses offsets e confirmar explicitamente SOMENTE essas posições iniciais antes de processar mensagens/ready; verificar erros retornados. Isso evita perder a primeira mensagem após falha/restart quando o grupo ainda não tinha nenhum commit. Commit válido existente nunca é substituído por latest. Não usar posição avançada de uma mensagem já polled para essa inicialização.
- `write_health(path:Path,*,pid:int,ready:bool,updated_at:float,metadata_at:float)->None`: escrita atômica de JSON mínimo com relógio monotônico. `healthcheck(path:Path,*,now:float|None=None)->bool`: PID ativo, ready, timestamps finitos coerentes e ambos recentes<=45s; tratar falta/corrupção como unhealthy. CLI do módulo health lê `/tmp/alert-consumer-health.json`, retorna0/1, sem conexão Kafka.
- CLI consumer instala SIGTERM/SIGINT, usa stop Event, marca stopping/invalida heartbeat no finally. Saúde fica ready somente com metadata válida e assignment não vazio depois de poll; idle poll renova heartbeat, metadata expirada/falha não produz falso ready.
- Compose `alert-consumer`: mesma image/build de `producer`, command `python -m climate_logistics.alert_consumer`; env somente bootstrap/group; health `python -m climate_logistics.consumer_health`; defaults/recursos/restart/grace da spec. Dockerfile/requisitos existentes inalterados, sem novas portas/volumes.

- [x] RED de config/cliente false flags e defaults; loop idle saudável, assignment vazio/metadataerror unhealthy; mensagem com erro não entra handler; Signal/exception fecha cliente e não usa commit global. Assignment preserva commits existentes e confirma baseline latest somente para partições novas antes de receber; falha na primeira mensagem não avança esse baseline, permitindo retomada.
- [x] RED health arquivo faltante/corrompido/stale/PIDmorto/futuro e fluxo ready; GREEN com atomic replace e CLI local. Teste shutdown/process close e limites de espera pertinentes.
- [x] Implementar loop/health/Compose/README; focused GREEN/Ruff. Documentar cache não durável, latest no primeiro uso, invalids confirmados, IDs históricos deQA reconhecíveis por timestamps.
- [x] Root build somente imagem Python/newservice (`docker compose build alert-consumer`), sem recriar Producer ou nósFlink. Conferir cliente2.12.2/UID10001 e healthCLI sem cliente Kafka extra.
- [x] Uma integração real opt-in `ALERT_CONSUMER_INTEGRATION=1`: container/grupo `weather-alerts-qa-<hex>` próprios; capturar ID antes de esperar, aguardar assignment/ready ANTESpublish. Publicar fixture HIGH com hash independente, repetição equivalente e inválido; verificar received1/duplicate1/invalid1 das fixtures por ID/partition/offset capturados no ACK (ignorar logs de outros alertas legítimos), ausência de raw, commits>=offset+1 e saúde idle. Recriar consumer do mesmo grupo e publicar outro alerta: antigos offsets confirmados não reaparecem, confirmando retomada. Deadline90s por fase; cleanup do próprio container em finally. Observer broker somente leitura, sem apagar grupos/topics/dados alheios.
- [x] Root confirma suíte geral Python3.11/Ruff uma vez no código final e revisão independente; commit local `feat: run alert consumer with healthcheck and kafka integration` após gates. Corrigir apenas defeitos concretos porLunaMax; sem repetir QA cara etapa8.

## Aceite final do principal

- [x] Revisão final do diff completo; conferir nenhum P1/P2 aberto, requisitos e limites da spec cobertos. Reusar evidências aprovadas sem reruns redundantes.
- [x] Subir somente `alert-consumer` operacional com grupo padrão; healthy/idle sem exigir alertas meteorológicos novos; registrar prova dos logs/offsets porQA e limites. Confirmar serviços existentes/jobFlink preservados.
- [x] Scan de secrets seguro, `.env` ignore, diff/indexcheck; atualizar spec/plano/progresso/aceite; commits locais e checkout limpo. Sem push, etapa10 pendente.

## Self-review e método

Spec contrato/cache/logging/offsets coberta pela Task1; execução/saúde/Compose/integração pela Task2. As duas compartilham apenas `alert_consumer.py` com interfaces explícitas e execução sequencial; não há ciclo entre tarefas. Cinco Review Focus têm testes definidos. Cache e stdout não são duráveis, versão do cliente não muda e os serviços já aceitos não são reiniciados. Nenhum código de produto escrito nesta fase.

Método já escolhido: subagent-driven com implementadores GPT-6 Luna Max, Root orquestrador/revisor/QA. Aguardar revisão deste plano antes de implementar; não perguntar novamente modelo ou método.

## Resultado em 2026-10-12

Duas tarefas concluídas. Integração renomeada para test_alert_consumer_integration.py para evitar colisão de import com o teste unitário. Implementação e correções por Luna Max; Root verificou Python 3.11, build, integração e operação; revisões independentes e aceite em docs/implementation/stage-9-acceptance.md. Checkout foundation reutilizado conforme decisão já autorizada; commits locais, sem push.
