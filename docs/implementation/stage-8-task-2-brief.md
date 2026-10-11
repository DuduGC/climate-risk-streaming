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


## Interfaces confirmadas da Task1

business_records: BusinessRecordError/AssessmentIdentity/ASSESSMENT_FIELDS/ALERT_FIELDS/parse_assessment/assessment_identity/alert_id/make_alert/jdbc_values conforme plano. SQLconstant BUSINESS_UPSERT_SQL com2? parâmetros(payload21str,alertidnullable). Schema19colunasfacts+alertpayload22, locaisLOCATIONS3; imutabilidade poridentidade impede alterarvaloresmétrica. SchemaaplicadoRoot, appmínimo, QApropsPSDlocal.

Root mantémop7temporária f50cfe22e4aaf194d1eea1cb55c0d84b; checkpointlegado7forcapturado pre-grafo8 em .venv/stage8-legacy-checkpoint.json (QAe0fc/CP2/hasbusinessFalse). Não ler .env nem manifestoPWD; essemanifestonãotemsecrets. Rootcancelknownop/build/deploy/capturaoperCP antesmudarruntime. Imagem7taglegacy preservada.

DoisartefatospublicadosMaven (RootmetadadosHTTPS): JDBC3.3.0-1.20 sha1=292553dba07f5e004065da539611a5c3d20ccd0f;PG42.7.14 sha1=0e7c94ce80cb1dd9875ff44ffe6a40a8aaa3eefd. .sha256 depublicação404; .sha1 e.ascdisponíveis. Root pode baixar/verificarhashpublicado porHTTPS ecalcularSHA256 para vocêregistrarchecksums. Nãoinventarhash,nãochamarnetwork/Dockerescalated noagente: solicitarRoot. NenhumjaradicionadoatéT2.

Read-onlySDKconfirma JdbcConnectionOptions.JdbcConnectionOptionsBuilder aninhado/Pythonnãoexpondo connectionCheckTimeoutSeconds; usarJavaBuilderpúblico5s+JdbcConnectionOptions(j_java_obj). KafkaRecordSerializationSchema(j_java_schema) customcomtopicselectorNone evitaSecondColumnwrappers; nãousarSimpleStringSchema emRow2comambosfieldssemshim.

Root controlaDocker/runtime/Gitcommits. AgenteLunaMax escreveprod/testes/README/checksums, semcommit e semsubagents. Não alterarDDL/schema/migration/seedTask1 salvofixsolicitadoRoot; sónovoFlintsinks/config/assets Java. Report:docs/implementation/stage-8-task-2-report.md. ObserveREDantesprodução; RootSDKREDnaimagem7bindsourceatual antesnewjar/build. ModeloMaxsempre; semdocs/Task3testesantesGateT2.
