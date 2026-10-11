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

## Preparação — principal antes das tarefas

- [ ] Confirmar checkout/base f2d7c7a mais documentação aprovada; registrar brief e ledger específicos, baseline482/SDK40/Ruff e saúde/JobID atual em execução, sem ler secrets em saída.
- [ ] Capturar latest checkpoint operacional7 COMPLETED, metadata legível nos dois nós, imagem/source hashes; preservar imagem com tag climate-logistics-flink:stage7-legacy. Cancelar apenas seu JobID conhecido e aguardar terminal antes de QA.
- [ ] Pausar Producer em try/finally; submeter grafo7 QA latest com nome/grupo exclusivos, auditoria e checkpoint15s. Publicar dois eventos conhecidos (RJ01HIGH por vento40; SP01LOW, demais fatores baixos) no mesmo instante histórico, com ingestion original coerente; aguardar aceitações e checkpoint após elas, mantendo janelas abertas.
- [ ] Salvar `.venv/stage8-legacy-checkpoint.json`: origin_stage7/origin_commit, has_business_sinks=False, job/name/group, imagem/hashes, metadata/id/trigger, perfis completos, catálogo e backup operacional. Confirmar checkpoint legível em ambos os nós após cancelamento; restaurar Producer/operação7 temporária quando necessário. Não usar checkpoint8 como prova de migração7→8.

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

## Task 2 — Sinks nativos, configuração e wiring

**Files:** criar src/climate_logistics/business_config.py, src/climate_logistics/flink_business.py, docker/flink/java/org/climatelogistics/AlertRecordSerializationSchema.java; modificar docker/flink/Dockerfile, docker/flink/checksums.sha256, compose.yaml, src/climate_logistics/flink_job.py e README; criar tests/unit/test_business_config.py e tests/unit/test_flink_business_sdk.py; ajustar somente testes existentes de execute/CLI afetados pelo carregamento operacional de configuração.

**Consumes:** contrato/SQL Task1. **Produces:**
- BusinessSinkSettings frozen dataclass: database,user,password(repr=False), host='postgres',port5432,kafka_bootstrap_servers, jdbc_connect_timeout_seconds10,jdbc_socket_timeout_seconds10; batch_size100/batch_interval_ms2000/max_retries3. from_environment(*,bootstrap_servers:str)->BusinessSinkSettings valida variáveis APP_DB_USER/APP_DB_PASSWORD/POSTGRES_DB, identidade app distinta de POSTGRES_USER quando presente, tipos/não vazio e identificadores seguros. Não consultar banco ou registrar config. jdbc_url()->str produz URL pública sem senha/user, parâmetros connectTimeout=10/socketTimeout=10/reWriteBatchedInserts=false. Porta tipada aceita1..65535 para opções de conexão/QA; from_environment usa5432, sem novo overrideCLI operacional.
- filter_business_assessments(stream:DataStream)->DataStream[STRING]: parse_assessment, validar membership em LOCATIONS, contador business_sinks.unknown_locations e log controlado; desconhecido não avança ramo negócio e continua nos ramos anteriores. Erro interno de contrato propagado, sem default/drop silencioso.
- attach_business_sinks(risk_stream:DataStream,settings:BusinessSinkSettings)->None: uma preparação JDBC Row(payload21,alert_id|null) com Types.ROW([STRING,STRING]); uma preparação Kafka somente elegíveis Row(location_id,payload22) com mesma RowType. JdbcSink.sink(BUSINESS_UPSERT_SQL,type_info,connection_options,execution_options); KafkaSink.sink_to AT_LEAST_ONCE/acksall.
- Java AlertRecordSerializationSchema(String topic) implements KafkaRecordSerializationSchema<Row>, serialize(Row,KafkaSinkContext,Long)->ProducerRecord<byte[],byte[]>: exatamente2 camposSTRING não nulos, key=field0UTF8 e value=field1UTF8, topicfixo; sem IO/parser/credenciais/retry próprio. Usar KafkaRecordSerializationSchema(j_java_schema), tópico weather.alerts, sem selector/wrapper Python de coluna.
- build_pipeline(settings:JobSettings,*,business_sinks:BusinessSinkSettings|None=None) mantém compatibilidade de chamadas SDK antigas; None constrói apenas grafo legado. execute operacional carrega BusinessSinkSettings e passa explicitamente. QA com job-name prefixo weather-flink-qa- porpadrão passaNone; --qa-business-sinks exige --qa-audit e nome/grupo exclusivo correspondente. Flag ausente não exige credenciais nem escreve negócios em QA legado. Sem flag genérica para desabilitar sinks operacionais.
- UIDs novos fixos: weather-business-records, weather-jdbc-records, weather-postgres, weather-alert-records, weather-alerts. Contadores só de preparação/rejeição, nunca chamar preparação de entrega persistida. UIDs antigos/state inalterados.

- [ ] SDK RED usando PyFlink1.20.3 real e bindPYTHONPATH atual: Row2/types/serializer keyvalue, SQL parametrizada2placeholders, options100/2000/3, deliveryATLEASTONCE, nomes/UIDs e ramos antigos. ConfigRED recusa ausência/blank/admin e não vaza password no repr/erro; QAflag sem identidade/audit recusa antes de carregar senha.
- [ ] Acrescentar somente JDBC connector3.3.0-1.20 e pgJDBC42.7.14, URLs Maven versionadas org/apache/flink/flink-connector-jdbc/3.3.0-1.20 e org/postgresql/postgresql/42.7.14. Obter hashes de publicação, verificar download e registrar SHA256 no arquivo existente; validar bytecode/API via build/SDK. Não mudar Kafka connector/Python/Flink ou requisitosPython.
- [ ] Compilar o Java pequeno no builder com javac --release17 e classpath dos jars existentes; instalar openjdk-17-jdk-headless somente no builder se javac ausente, em camada separada para preservar cache CPython. Copiar só jar da classe e dependências aprovadas ao runtime, que continua JRE/USERflink. Testar serialize no JVM SDK, inclusive UTF8 e forma errada recusada.
- [ ] Implementar opções Kafka enable.idempotence=true/acks=all/retries3/request.timeout.ms10000/delivery.timeout.ms30000/max.block.ms10000/max.in.flight.requests.per.connection5. Isso limita tentativas de transporte; garantia de recuperação continua AT_LEAST_ONCE. Testar config e falha propagada, não usar DeliveryGuarantee.NONE.
- [ ] Configurar também connectionCheckTimeoutSeconds5 no builder Java público de JdbcConnectionOptions e envolvê-lo em JdbcConnectionOptions(j_options), pois o wrapper Python instalado não expõe esse setter. Testar valor no SDK e medir deadline real de faultQA: não herdar default de validação de conexão60s por tentativa, que consumiria o orçamento de checkpoint. connectTimeout/socketTimeout10s e retries3 permanecem.
- [ ] Compose injeta apenas POSTGRES_DB/APP_DB_USER/APP_DB_PASSWORD nos nós necessários; dependência de postgres healthy; sem POSTGRES_USER/PASSWORD administrativo no Flink e sem credenciais no Producer. Mounts/healthchecks/checkpoints/recursos atuais preservados. Não imprimir compose config expandido.
- [ ] Wiring operacional com config explícita, QA opt-in validado, filtros e sinks após risco. Repetir SDK GREEN/grafo e baseline completo/Ruff; atualizar CLI/helper unit apenas quando mudança de assinatura exigir, sem remover teste ou aliviar assertion antiga.
- [ ] Principal build/recreate JM/TM somente após checkpoint7 preservado; confirmar jars/checksums/classes/imports/UID/config pública nos dois nós sem imprimir senhas. Revisão independente e correções com LunaMax antes de commit local `feat: attach native postgres and kafka business sinks`.

## Task 3 — Provas reais de entrega e recuperação

**Files:** criar tests/integration/test_flink_business.py; usar helpers existentes em test_flink_temporal/test_kafka_flink/test_flink_risk e SQL/contrato Task1; criar docs/implementation/stage-8-implementation-report.md; README documenta execução e limites. Principal mantém controladores/manifestos/provas temporárias em .venv, depois versiona stage-8-acceptance.md e status.

**Interfaces QA:** holders guardam JobID antes de RESTwait; helpers de submissão/cancelamento aguardam terminal e só atuam em jobs próprios. Consulta psql JSON por chaves exatas, sem SELECTraw/secrets; observer Kafka one-off usa offsets capturados e prazo, filter por identidade exata. Não adicionar dependênciaPythonDB ao produto.

- [ ] Escrever testes/fixtures antes de executar o novo fluxo: Producer pausado em try/finally, QAunboundedlatest/audit/business/checkpoint15s. Quatro perfis em três localidades reais: LOWRJ/MEDIUMSP/HIGHCT no mesmo instanteT histórico exclusivo; cópia extremaLOW mesma identidade antes do avanço deve serduplicate; CRITICALRJ emT+60min e sentinelCTLOW emT+100min fecham as24 janelas dos perfis sem fechar as janelas do sentinel. Ingestion original coerente, semoverride de relógio/validação. Oracle independente dos módulos de risco/hash sob teste.
- [ ] Comparar todas as24 avaliações persistidas por identidade/colunas completas; somente12 alertasHIGH/CRITICAL nas tabelas/topic, JSON22 e keycorretos. LOW/MEDIUM semalertas. Poll sob deadline paraJDBCflush2s/SDKbundle eKafkaACK; não usar sleeps como prova. Conferir desconhecido nos diagnósticos/contador, sem linha/alerta.
- [ ] Replay nativo de checkpoint próprioC após buffersflush, com mesmos grupo/UID e offsets: id/payload persistem, linhasnãoaumentam. Se mensagemKafka repetir, id/payload/key iguais. Persistir prova RESTlatest.restored e novoCOMPLETED, metadata emambosnodes.
- [ ] Migrar MANIFESTOLEGADO7→8, novoJobID/mesmonomegrupo/metadata7, não allowNonRestoredState. Reenviar revisões dos dois eventos capturados: duplicateaudit; sentinelavança watermark e avaliações abertas alimentam negócios. Comparar métricas/risco/SQL/topic; confirmar estado/offsets/restored7 e novoCP8COMPLETED.
- [ ] Fault tests reais separados: QA com checkpoint60s, disparar manualmente checkpointC bom com mesmosUIDs; publicar inputposteriorC, fechar suas janelas, observar escrita externa e cancelar o job bom antes de snapshot seguinte, conservandoC. QAentrypoint usa build_pipeline e dataclasses.replace somente nas opções tipadas do próprio job (JDBCpublicurl postgres:1 ou writerKafka kafka:1, source continuaKafka9092), restaurandoC. Aplicarvalidadoresdeidentidade antesdesubmissão; sem novasflags/opções inseguras deprodução. Comprovar falha/restart/exceção do destino e nenhumcheckpointfalsamenteCOMPLETED após entrega pendente. Deadline240s porcaso; cancelar somentejobfault próprio/aguardarterminal, restaurar configuração boa peloC e exigirconvergência dos destinos/UPSERT sobre a escrita externa anterior. Erros nãoexpõem credenciais. Não parar infra compartilhada para induzir falha.
- [ ] Principal coordena regressõesKafka3,temporais3,risk2 comslotúnico e QAsemwrites; caso legado skipped por cancelamento ainda emprogresso recebe execuçãoisolada. Logs duráveis e contagens de PASS, sem contabilizar skips como sucesso.
- [ ] Focused/suítes gerais/SDK/Ruff aprovados, revisão independente dos testes/oracles/cleanup, correções porLunaMax; principal confirma runtimes/build/resultados antes de commit local `test: verify business sinks and checkpoint recovery`.

## Aceite final — principal

- [ ] Revisão de diff imutável da etapa completa; revisor disponível mais capaz conforme skill, decisão registrada, implementadores sempreLunaMax. Resolver P1/P2 e diferenças entre relatos e execução real.
- [ ] Restaurar operação8 a partir do últimocheckpoint7 operacionalpreservado, no grupo padrão; únicojobRUNNING, checkpoint8COMPLETED60s, métricas deRJ/SP/CT noPostgreSQL eProducerfuncional. Janelasjáfechadas antesdossinks não precisam debackfill; alertas reais são condicionais ao risco, fixturescomprovamHIGH/CRITICAL semfingirdadosAPI.
- [ ] Verificar identidade/grants/nenhumsecretadmin, imagem/containersnão-root, recursos/backpressure/estado/latênciaflush sobcargapequena. Não aumentar limites semmedição. Documentar entrega por destino eausência deatomicidadePostgresKafka.
- [ ] Scan seguro de valores dascredenciais nos arquivostracked/index semimprimir, .envignore, diff/indexcheck; atualizar spec/plano/ledger/progresso/aceite, commitslocais echeckoutlimpo. Mantermanifesto7/imagem/checkpoints, sempush ou limpeza de dados alheios. Sóentão ETAPA8CONCLUÍDA; etapa9 pendente.

## Self-review e handoff

Spec coberta por três tarefas e preparação/aceite do principal. ReviewFocus1/2 emTask1,3/4 emTask2,5 emTask3; testes reais revalidam todos. Interface de SQL/Row tem exatamente2 parâmetros; alert22 difere da avaliação21 conforme spec. SerializerJava é adaptação de representação, mantém KafkaSink nativo e não cria aplicação/serviço Java. Versões/parametrização definidas após inspeção do SDK instalado e fontes primárias. Nenhum código de produto foi escrito nesta fase.

Método já selecionado pelo usuário: subagent-driven com agentes GPT-6 Luna Max, principal orquestrador/revisor/QA. Plano aprovado pelo usuário em2026-10-11; execução autorizada.

Fontes: [JDBC Flink1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/jdbc/), [Kafka Flink1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/), [artefatos JDBC versionados](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/), [pgJDBC oficial](https://jdbc.postgresql.org/download/). A interface Python específica foi inspecionada na imagem1.20.3 instalada, incluindo JdbcConnectionOptions.JdbcConnectionOptionsBuilder e KafkaRecordSerializationSchema(j_java_schema).
