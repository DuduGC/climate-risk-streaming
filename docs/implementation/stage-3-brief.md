# Etapa 3 — Python Producer + Kafka: plano

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Publicar eventos reais da Open-Meteo em weather.raw e comprovar entrega por consumo Kafka.
**Architecture:** Uma tarefa coesa para Luna Max implementa publisher, CLI e container. Principal executa os checks e um revisor independente examina a mudança inteira.
**Tech Stack:** Python 3.11.14 slim-bookworm, Requests 2.34.2, confluent-kafka 2.12.2, Kafka 3.9.1, Compose, Pytest 8.4.2 e Ruff 0.14.0.
**Spec:** docs/superpowers/specs/2026-10-08-producer-kafka-design.md (aprovada).

## Global Constraints

- Implementador gpt-6-luna, reasoning_effort=max; sem subagentes próprios.
- Reusar WeatherEvent/LOCATIONS/OpenMeteoClient e schema da Etapa 2.
- Topics weather.raw/weather.alerts: 3 partições/RF1; alerts vazio nesta etapa. Startup cria/confere em deadline total de 15 s; nunca excluir/recriar/alterar partições.
- Kafka key location_id UTF-8; valor JSON compacto UTF-8, allow_nan=False, máximo16384 bytes; CreateTime = ingestion_timestamp em milissegundos.
- Config: enable.idempotence=true, acks=all, gzip, linger.ms=20, batch.size=32768, delivery.timeout.ms=10000. Enqueue deadline de 5 s; flush ciclo/final de 12 s.
- Defaults: KAFKA_BOOTSTRAP_SERVERS=kafka:9092 e WEATHER_POLL_INTERVAL_SECONDS=300. Sem segredo PostgreSQL no Producer.
- Docker não-root, sem porta nova, restart on-failure:3, stop_grace_period de 75 s.
- Sem Flink job, sink, alert-consumer, formato/tecnologia extra ou container auxiliar.
- SDK fixado confluent-kafka==2.12.2, sem extras. Catálogo: https://pypi.org/project/confluent-kafka/2.12.2/. Validar wheel/runtime Python 3.11 durante execução.

## Review Focus

1. Nack detectado em poll: preservar erro até o fechamento do ciclo, sem sucesso falso.
2. Topic existente ou race TOPIC_ALREADY_EXISTS: conferir metadata/topologia sem mutação destrutiva.
3. Fila cheia: deadline monotônico global, sem loop infinito.
4. Flush0 sem confirmação/callback ou com pendentes: não aprovar pelo tamanho da fila apenas.
5. SIGTERM/falha parcial HTTP: parar novos trabalhos, fechar recursos, drenar com limite e saída coerente.

## Arquivos e interfaces

Implementador: src/climate_logistics/kafka_publisher.py, producer.py; docker/producer/Dockerfile; compose.yaml; .env.example; requirements.txt; tests/unit/test_kafka_publisher.py, test_producer.py; tests/integration/test_producer_kafka.py; README.md.
Principal: docs/progresso.md e docs/implementation/stage-3-* (brief, ledger, relatório, revisão). Sem alterar modelos/cliente HTTP anteriores salvo bug necessário.

- KafkaSetupError/KafkaPublishError(RuntimeError).
- ensure_topics(bootstrap_servers: str, *, admin=None, clock=time.monotonic) -> None.
- serialize_event(event: WeatherEvent) -> tuple[bytes, bytes, int]: key/value/CreateTime, sem campo novo no corpo.
- KafkaPublisher(bootstrap_servers: str, *, producer=None, clock=time.monotonic, logger=None); enqueue(event) -> None; finish_batch() -> int de entregas confirmadas; close() -> None com flush final.
- Rastrear cada mensagem aceita, inclusive duas com mesma identidade lógica; limpar estado por ciclo confirmado sem histórico ilimitado.
- ProducerSettings dataclass frozen(bootstrap_servers: str, poll_interval_seconds: int); from_env(env: Mapping[str,str]) -> ProducerSettings.
- run_cycle(client, publisher, locations=LOCATIONS, *, stop: threading.Event, logger: logging.Logger) -> bool: true apenas com todas coletadas e entregues.
- main(argv: Sequence[str] | None=None) -> int; CLI --once/modo contínuo; signals registrados só na execução, sem efeitos na importação.

## Preparação — principal

- [ ] Confirmar base 4acfd3d e branch foundation; manter checkout autorizado e preservar dados.
- [ ] Criar ledger stage-3-progress.md e brief desta tarefa; registrar aprovação do plano quando concedida.
- [ ] Confirmar Docker Linux/Python 3.11; verificar instalação SDK após aprovação. Delegar toda a implementação a um Luna Max, sem disputa de arquivos.

## Tarefa única — Producer verificável

- [ ] TDD: testes antes da lógica; observar RED e GREEN. Preservar os 291 testes existentes, sem duplicar validação da Etapa 2.
- [ ] Serialização: JSON/key/acentos/timestamps exatos; limite16 KiB inclusivo, excesso rejeitado antes de enqueue.
- [ ] Config: defaults, intervalo inteiro de 1 a 86400; rejeitar vazio/zero/negativo/fração/texto; bootstrap não vazio e sem URI/credencial/whitespace inválido, sem imprimir valores.
- [ ] Topics: 3 partições/RF1, existentes compatíveis, TOPIC_ALREADY_EXISTS com conferência, erro e timeout; deadline de 15 s compartilhado por chamadas/futures, não 15 s por topic.
- [ ] Publisher: callbacks sucesso/erro, nack servido por poll, flush pendente, flush0 sem callback, mensagens com mesma identidade lógica. Sucesso de enqueue não equivale a entregue.
- [ ] Queue BufferError: poll limitado pelo tempo restante, retry até 5 s usando monotonic; teste relógio que avança. Não re-enviar manualmente após timeout ambíguo de delivery.
- [ ] Ciclo: erro HTTP/contrato de uma localidade não impede restantes; publicar só válidos, flush confirmado; retornar false se parcial. Kafka fatal encerra. Sem retry HTTP adicional.
- [ ] StopEvent: parar novas coletas, Event.wait interrompe intervalo; fechar HTTP e flush final em finally sem mascarar erro. --once exit 0 só para ciclo completo; demais falhas exit 1. Parada intencional contínua exit 0 se mensagens aceitas drenadas.
- [ ] Logs JSON stdlib: timestamp UTC/component/operation/status e contexto útil. Testar JSON parseável, contagens por confirmação, sem secrets/body inteiro/log de enqueue como sucesso.
- [ ] Implementar interfaces mínimas, publisher/session reutilizados por execução; logs por ciclo/falhas, sem framework extra.
- [ ] Dockerfile FROM python:3.11.14-slim-bookworm, deps runtime, src, PYTHONPATH=/app/src, UID/GID10001 e CMD exec Python; sem .env/dev deps.
- [ ] Compose producer image climate-logistics-producer:local, build contexto raiz/Dockerfile definido, mem_limit=256m/cpus=0.5, depende do Kafka healthy. Environment só bootstrap/poll/PYTHONUNBUFFERED/PYTHONPATH; não env_file=.env. .env.example só acrescenta defaults públicos.
- [ ] Kafka real opt-in KAFKA_INTEGRATION=1: capture high watermarks antes de publicar fixture com location_id único; consume via assign/offsets sem commits/grupos operacionais; confira key/value/CreateTime/entrega. Sem topic extra/reset/remover dados.
- [ ] Caso SDK real contra endpoint local fechado: falha de entrega explícita e prazo limitado, sem parar o broker existente.
- [ ] Smoke com KAFKA_INTEGRATION=1 e OPEN_METEO_LIVE=1: producer contínuo parado, offsets capturados, CLI --once em subprocess timeout de 180 s, consumo dos 3 registros novos com ingestion_timestamp dentro do ciclo. Logs sozinhos não comprovam.
- [ ] Integrações skip explícito por padrão, markers integration/live; fixture/rede pública em funções separadas.
- [ ] README: build/up/--once/logs, bootstrap interno, inspeção temporária, schema/timestamps, duplicações esperadas entre polls/reinícios e limites. Idempotência de sessão não é exactly-once; HTTP sem deadline absoluto.
- [ ] Rodar suíte inteira Python 3.11 e Ruff; registrar RED/GREEN/contagens/status em stage-3-implementation-report.md.

## Revisão e aceite — principal

- [ ] Revisão independente inteira: deadlines/callbacks/signals/configs/Docker/secrets/testes. Corrigir achados com mesmo implementador.
- [ ] requirements runtime/dev; python -m pytest -q e python -m ruff check . em Python 3.11: exit 0 e skips opt-in claros.
- [ ] docker compose config --quiet; build producer; startup topics idempotente; metadata confirma3partições/RF1.
- [ ] Testes Kafka fixture/negativo no network climate-logistics_default, depois smoke API→CLI→Kafka com consumo validado.
- [ ] Subir producer contínuo, confirmar um ciclo, testar parada limpa e restaurar operação. Conferir não-root, sem secrets PostgreSQL e sem porta adicional.
- [ ] git diff --check, scan secrets sem exibir valores; atualizar progresso/ledger/spec/plano e commit local, sem publicação.
- [ ] Reportar ETAPA 3 CONCLUÍDA somente após critérios; Etapa 4 não iniciada.

## Revisão do plano

Spec coberta; entrega única evita disputa de interfaces. Os cinco riscos têm testes acima, schema preservado e nenhuma stack extra. Método já escolhido: Luna Max implementa e principal/revisor validam. Plano aprovado pelo usuário em 2026-10-08; execução iniciada.
