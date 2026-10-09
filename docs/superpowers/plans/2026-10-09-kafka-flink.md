# Etapa 4 — Kafka → Flink: plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Executar KafkaSource→parser Python→Row tipada→diagnóstico no Flink real.
**Architecture:** Uma entrega coesa para GPT-6 Luna Max cria parser puro, job DataStream e runtime comum JM/TM. Principal executa builds/integrações; um revisor independente examina a mudança.
**Tech Stack:** Flink/PyFlink 1.20.3, Java17, Python 3.11.14, conector Kafka3.3.0-1.20, Docker Compose, Pytest/Ruff existentes.
**Spec:** docs/superpowers/specs/2026-10-09-kafka-flink-design.md (aprovada).

## Global Constraints

- Implementador gpt-6-luna com reasoning_effort=max; sem subagentes próprios.
- Preservar schema/modelos/Producer/Kafka/PostgreSQL e todos os 329 testes anteriores.
- Topic weather.raw; bootstrap kafka:9092; group weather-flink-ingestion; earliest; auto-commit=false; paralelismo/slots 1.
- WatermarkStrategy.no_watermarks apenas para a API. Sem Event Time/janelas/estado/deduplicação, JDBC, sinks de negócio ou topic extra.
- Mensagem de 16 KiB; erros conhecidos descartados/contados/logados sem payload. Erro inesperado não pode ser silenciosamente convertido em dado válido.
- Imagem derivada oficial Flink1.20.3, Python 3.11.14 compilado no mesmo Ubuntu base, apache-flink==1.20.3 separado em requirements-flink.txt.
- Memória inicial atual mantida: JM processo768MiB/limite1GiB; TM processo1280MiB/limite1536MiB. Ajustar somente se falha/medição justificar.
- Nenhuma promessa de checkpoint/recuperação/exactly-once nesta etapa; submissão manual controlada, sem duplicar job operacional.

## Downloads e integridade fixados

- CPython: https://www.python.org/ftp/python/3.11.14/Python-3.11.14.tar.xz
- CPython SHA256: 8d3ed8ec5c88c1c95f5e558612a725450d2452813ddad5e58fdb1a53b1209b78
- JAR: https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
- JAR SHA256: 1086f3eee73d727e234860fcd03adafc0d76f2fc70a25d39c36427693fff749d
- Proveniência: digest CPython do manifesto oficial .sigstore; JAR consultado em memória por HTTPS, conferido contra .sha1 publicado e SHA256 calculado para pin forte no build. Não foi alegada verificação criptográfica da assinatura Sigstore.
- Downloads com timeout/retry finitos; sha256sum obrigatório antes de extrair/copiar. Nada é baixado ao iniciar o job.

## Review Focus

1. Python compartilhado/libs nativas: cliente e worker precisam funcionar no mesmo runtime, não apenas python --version.
2. Classe KafkaSource ausente/duplicada: instalar conector uma vez no classpath e provar source real.
3. Row e DTO com ordens/tipos diferentes: testar todos os 15 campos e executar serialização no worker.
4. JSON inválido seguido de válido: descartar/logar somente erro conhecido e demonstrar que o job termina/processa o válido.
5. Job marcado RUNNING mas Python não processa: exigir saída da fixture, estado final bounded e novos eventos unbounded; proteger jobs/dados existentes.

## Arquivos e interfaces

Implementador: src/climate_logistics/weather_parser.py, flink_job.py; requirements-flink.txt; docker/flink/Dockerfile, checksums.sha256; compose.yaml; tests/unit/test_weather_parser.py; tests/integration/test_kafka_flink.py; README.md.
Principal: docs/progresso.md e docs/implementation/stage-4-* (brief, ledger, report/review). Alterações do pacote anterior somente por bug concreto.

- WeatherParseError(ValueError).
- parse_weather_event(raw: str) -> WeatherEvent: parser puro, exige campos do schema1 e datetimes aware; reusa validação existente.
- WEATHER_FIELDS: tuple[str,...] com ordem: location_id,city,latitude,longitude,temperature,precipitation,wind_speed,wind_gust,visibility,weather_code,measurement_interval_seconds,event_timestamp,ingestion_timestamp,source,schema_version.
- WEATHER_KINDS mesma ordem: STRING,STRING,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,INT,INT,STRING,STRING,STRING,INT.
- event_values(event: WeatherEvent) -> tuple[object,...]: usa timestamps UTC/Z do to_dict e a ordem declarada.
- diagnostic_summary(values: Sequence[object]) -> str: JSON compacto component=flink/operation=parse/status=success mais location_id/event_timestamp/ingestion_timestamp/schema_version.
- Job CLI aceita --bounded, --job-name e --group-id para isolamento de testes; sem flags que apaguem dados. Defaults weather-kafka-ingestion/weather-flink-ingestion.
- build_pipeline(settings) constrói env/source/transformações e execute(job_name). Imports PyFlink restritos ao módulo/runtime do job; parser/tests puros não dependem da JVM.

## Preparação — principal

- [x] Registrar aprovação deste plano e base c6b07a5 e branch foundation; manter checkout autorizado.
- [x] Criar brief e ledger exclusivos da Etapa 4. Confirmar infraestrutura/jobs antes de executar, sem apagar volumes.
- [x] Delegar uma entrega a Luna Max; principal prepara builds/QA em paralelo sem disputar arquivos.

## Tarefa única — runtime e primeiro job

- [x] TDD parser: RED antes da lógica, GREEN com fixture de evento normalizado existente; não duplicar matriz inteira dos modelos.
- [x] Testar todos campos, ordem e tipos, UTC de timestamps com offset, JSON/schema/version inválidos, corpo acima16KiB, inteiro enorme/nesting e string não representável em UTF8. Erros devem ser WeatherParseError sem expor corpo.
- [x] Parser exige campos do wire schema incluindo source/version/interval; não preencher defaults silenciosos. Tratar somente exceções conhecidas de decode/contrato; preservar erros de programação.
- [x] event_values e diagnostic_summary são puros. Testar tupla esperada completa e JSON de diagnóstico com campos exatos; sem imprimir medições completas.
- [x] Runtime multi-stage FROM flink:1.20.3-scala_2.12-java17: builder instala deps de compilação, baixa/verifica CPython, configura prefix=/opt/python3.11, enable-shared/ensurepip, sem PGO/LTO, make paralelo limitado.
- [x] Runtime recebe prefix compilado e somente libs necessárias (SSL/zlib/bzip2/sqlite/ffi/lzma/readline/gdbm/uuid conforme build). Registrar libpython via ldconfig/rpath; não copiar binário Debian incompatível nem usar PPA.
- [x] requirements-flink.txt contém apache-flink==1.20.3, sem runtime Producer/dev deps. Não adicionar pipeline Apache Beam: suas dependências são somente o runtime oficial PyFlink.
- [x] Baixar/verificar JAR e instalar só em /opt/flink/lib. Usar classpath global desse único JAR, sem duplicar via add_jars. Não instalar JDBC.
- [x] Copiar src para /opt/climate/src, configurar PYTHONPATH/PYTHONUNBUFFERED e executáveis /opt/python3.11/bin/python3.11. Preservar entrypoint oficial que inicia processos flink como usuário flink.
- [x] Compose JM/TM usa image climate-logistics-flink:local e mesmo build; manter comandos jobmanager/taskmanager, healthchecks/portas/recursos. Acrescentar python.client.executable/python.executable no FLINK_PROPERTIES de ambos, sem secrets PostgreSQL.
- [x] Job KafkaSource: tópico explícito, SimpleStringSchema, group configurado, KafkaOffsetsInitializer.earliest(), enable.auto.commit=false. --bounded usa set_bounded(KafkaOffsetsInitializer.latest()) para fim dos offsets disponíveis ao iniciar.
- [x] StreamExecutionEnvironment parallelism1, from_source com no_watermarks, nomes/UIDs estáveis para source/parser/diagnóstico. Não ativar checkpoint/state temporal.
- [x] FlatMap wrapper chama parser, incrementa parsed_records/invalid_records por metrics group, produz Row(*event_values) com Types.ROW_NAMED(WEATHER_FIELDS, tipos explícitos). WeatherParseError: log JSON mínimo e não emitir Row; demais erros propagam.
- [x] Map diagnóstico extrai valores da Row e usa diagnostic_summary; Types.STRING e PrintSink identificado weather-ingestion. Saída permite comprovar execução do Python.
- [x] README: build/up, submissão CLI usando usuário flink, inspeção REST/list/cancel por ID, limite de slot, evitar segundo job com mesmo nome, replay earliest sem checkpoint e teste bounded opt-in.
- [x] Integração FLINK_INTEGRATION=1: orquestrador de testes no host com Pytest/stdlib e subprocess Docker/REST local. Usar Docker exec para publicar pelo SDK já instalado no Producer e submeter pelo CLI do JM; payload por stdin/argumentos estruturados, sem shell interpolado. Nenhum Docker SDK/socket adicional. O host controla a operação; parser e job executam obrigatoriamente em Python 3.11 no runtime Flink.
- [x] Antes do teste, listar jobs: se há job operacional identificado, coordenar cancelamento temporário e restaurar; não cancelar jobs desconhecidos. Publicar raw inválido e fixture válida com ID exclusivo usando SDKKafka, aguardar confirmação.
- [x] Submeter bounded com nome/group exclusivos, capturar jobID real da resposta/CLI; deadline240s para FINISHED, falhar em FAILED/CANCELED/timeout. Encontrar diagnóstico da fixture nos logs TM após timestamp de início e evidência de invalid_records/log controlado. Não aceitar somente numRecordsOut.
- [x] Teste bounded sempre limpa somente seu job ainda ativo via cancel porID, sem reset de offsets/remoção de dados. Integração skip explícito por padrão; nenhum shell destrutivo ou fixture descartando raw histórico.
- [x] Suíte completa leve e Ruff em Python 3.11; report RED/GREEN/contagens/concerns em stage-4-implementation-report.md. Build e integração real feitos pelo principal, sem declarar etapa concluída por unitários.

## Verificação final — principal

- [x] Review independente inteira de runtime/parser/graph/lifecycle/secrets; corrigir achados com implementador.
- [x] Builds JM/TM e compose config --quiet aprovados. Smoke de python3.11, ssl/ctypes/sqlite/bz2/lzma e versão PyFlink nos dois nós; Java17/conector presentes.
- [x] Confirmar processos não-root, TM registrado/slot, runtime sem credenciais PostgreSQL e Producer funcional após troca dos nós Flink.
- [x] Executar pytest inteiro e Ruff Python 3.11: exit0 e skip opt-in claro. SDK/jobs testados no runtime Flink real.
- [x] Integração bounded: inválido não causa crash; fixture aparece após parsing; FINISHED e slot livre.
- [x] Submeter job operacional unbounded, obter RUNNING e source/parser no REST; publicar novo evento válido identificado e encontrar diagnóstico. Medir RSS de worker/JVM; ajustar recursos somente se necessário e repetir checks afetados.
- [x] Deixar um único job weather-kafka-ingestion ativo ao final. Nenhum topic/registro/volume/grupo alheio removido.
- [x] git diff --check/scan secrets; atualizar progresso/spec/plano/ledger e commit local, sem publicar.
- [x] Reportar ETAPA 4 CONCLUÍDA após todos os critérios; Etapa 5 permanece pendente.

## Revisão do plano

Spec coberta por uma entrega coesa; os cinco riscos têm verificações reais. O schema preserva ordem/tipos explícitos e o parser reutiliza o modelo. Runtime separado e checksum fixado; sem nova stack ou etapa temporal antecipada. Método já escolhido: GPT-6 Luna Max implementa, principal/revisor validam. Plano aprovado em 2026-10-09; execução concluída/verificada; evidências em docs/progresso.md.
