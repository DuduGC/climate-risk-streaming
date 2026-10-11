# Relatório — Etapa 8, Tarefa 2

**Status:** implementação e validação SDK concluídas; revisão independente e gates de cluster/livesink do Root pendentes. Nenhum commit foi criado.

## Entrega

- `src/climate_logistics/business_config.py` adiciona `BusinessSinkSettings` imutável, senha fora do `repr`, validação de variáveis/identificadores/porta e URL JDBC sem usuário ou senha.
- `src/climate_logistics/flink_business.py` valida as avaliações e localidades no ramo de negócio, prepara os dois contratos `Row(STRING, STRING)` e conecta os sinks PostgreSQL/Kafka com contadores de preparação/rejeição.
- `src/climate_logistics/flink_job.py` mantém `build_pipeline(settings)` compatível com chamadas SDK legadas; operações carregam a configuração de negócio, jobs com prefixo `weather-flink-qa-` não a carregam por padrão e o opt-in `--qa-business-sinks` exige audit e identidade exclusiva antes de ler credenciais.
- `docker/flink/java/org/climatelogistics/AlertRecordSerializationSchema.java` mantém a chave `location_id` e o JSON UTF-8 no valor Kafka. `BusinessJdbcStatementBuilder.java` implementa `JdbcStatementBuilder<Row>`, valida as duas colunas e vincula o ID nulo como `VARCHAR`.
- `docker/flink/Dockerfile` baixa/verifica os conectores JDBC e pgJDBC, compila os helpers com `javac --release 17` em estágio builder e copia somente os jars necessários ao runtime. `checksums.sha256` registra os SHA256 obtidos após verificar os `.sha1` publicados pelo Maven.
- `compose.yaml` passa somente `POSTGRES_DB`, `APP_DB_USER` e `APP_DB_PASSWORD` ao JobManager e espera PostgreSQL saudável. O TaskManager recebe as opções serializadas no grafo e não precisa dessas variáveis. `.env.example` e `README.md` descrevem configuração, garantias e QA.
- `tests/unit/test_business_config.py` e `tests/unit/test_flink_business_sdk.py` cobrem configuração, filtro, preparação, APIs nativas, serializer, propriedades, UIDs e gate QA.

## RED e GREEN

O Root confirmou o RED inicial com PyFlink 1.20.3: 30 falhas em 1,56 s por ausência de `business_config` e `flink_business`. O teste do binder Java também falhou antes da implementação: 6 falhas e 11 casos desmarcados, por falta de `BusinessJdbcStatementBuilder`/adapter. A regressão QA sem audit falhou uma vez em 0,58 s ao tentar carregar credenciais para `weather-flink-qa-*`; a execução passou a manter os sinks desligados por padrão.

O teste SDK final no runtime Python 3.11/PyFlink 1.20.3 passou: **83 aprovados, 19 avisos, 22,84 s**. A suíte completa reportada pelo Root passou com **595 aprovados e 26 ignorados em 7,62 s**; Ruff passou. A suíte completa e Ruff foram executados antes da última correção de introspecção do fixture Java; depois dela, o Root repetiu a suíte SDK inteira e obteve 83 aprovados.

O Root compilou os helpers Java e fez o rebuild final do runtime com as camadas anteriores em cache; a cópia de `src` levou 2,85 s. ID da imagem resultante: `sha256:29ac52a48276e05d227e1f20b98f480f07e5653df6567b34b8dfa1277ca9e635`. O build inicial que incorporou os novos checksums refez as camadas do Python; os rebuilds seguintes usaram o cache.

## Decisões e limites

O wrapper Python `JdbcSink.sink` de PyFlink 1.20.3 tenta refletir uma classe JDBC removida. O sink usa o factory Java público `JdbcSink.sink(sql, JdbcStatementBuilder<Row>, executionOptions, connectionOptions)` e o wrapper público `JdbcSink(j_java_sink)`. O serializer custom também usa o builder Java público de `KafkaSink`, envolvido em `KafkaSink(j_java_sink)`; assim não depende da reflexão do wrapper Python sobre `topicSelector`. O serializer usa os tipos sombreados publicados pelo conector Kafka fixado.

As chaves de negócio e alertas são idempotentes no PostgreSQL por UPSERT. O sink Kafka continua `AT_LEAST_ONCE`; mensagens repetidas após recuperação precisam ser deduplicadas pelo `alert_id`. Não foi executada a prova de persistência real, replay de checkpoint, falha de broker/banco ou consistência entre os destinos nesta tarefa; essas provas de cluster pertencem aos gates seguintes do Root. A revisão independente também permanece pendente.

## Comandos e gates do Root

- Build realizado: `docker compose build flink-jobmanager`. O Root deve recriar JobManager e TaskManager somente após revisar o backup/checkpoint 7 preservado, confirmar jars/classes e os UIDs em ambos os nós, e restaurar/submeter a partir do checkpoint escolhido.
- Validação SDK realizada dentro do runtime 1.20.3 com os testes listados acima; a saída final está em `.venv/stage8-task2-sdk-accepted.log`. A suíte do checkout está em `.venv/stage8-task2-whole.log`.
- Ainda pendem a revisão independente, QA operacional PostgreSQL/Kafka, recuperação/fault QA e aprovação do Root antes de qualquer commit local.
