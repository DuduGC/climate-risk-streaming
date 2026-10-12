# Arquitetura do projeto

O sistema coleta previsões atuais da Open-Meteo, calcula métricas e risco em janelas e persiste resultados no PostgreSQL. Somente risco `HIGH` ou `CRITICAL` também vira mensagem em `weather.alerts`; o consumer valida essa mensagem e a registra em stdout como JSON.

```mermaid
flowchart LR
    API[Open-Meteo<br/>modelo meteorológico] --> P[Producer Python]
    P -->|weather.raw<br/>key: location_id| K[(Kafka)]
    K --> F[Flink<br/>validação · deduplicação<br/>janelas · risco]
    F -->|todas as avaliações| M[(PostgreSQL<br/>weather_metrics)]
    F -->|HIGH / CRITICAL| A[(PostgreSQL<br/>weather_alerts)]
    F -->|weather.alerts<br/>key: location_id| K
    K --> C[Consumer Python]
    C -->|logs JSON| O[stdout / logs do container]
```

## Serviços e tópicos

| Serviço | Responsabilidade |
| --- | --- |
| `producer` | Consulta RJ01, SP01 e CT01 em sequência, valida eventos e publica em `weather.raw`. |
| `kafka` | Broker KRaft local, com auto-criação de tópicos desabilitada. O Producer cria `weather.raw` e `weather.alerts` com três partições e replicação 1; `weather.alerts` recebe saída do Flink. |
| `flink-jobmanager` e `flink-taskmanager` | Executam um job standalone com um slot. O job lê `weather.raw` no grupo `weather-flink-ingestion`; a submissão e a retomada por checkpoint são explícitas. |
| `postgres` | Armazena o catálogo e os fatos do domínio no schema `climate`. |
| `alert-consumer` | Valida alertas de `weather.alerts`, registra logs JSON e confirma offsets após escrever e descarregar cada log. |

As duas chaves Kafka são o `location_id` codificado em UTF-8. `weather.raw` contém o evento v1; `weather.alerts` contém o alerta de 22 campos e só recebe avaliações elegíveis. Não há Schema Registry: as versões e validações pertencem ao código.

## Contratos de dados

O Producer serializa `WeatherEvent` como JSON compacto. Os 15 campos validados pelo parser são:

| Campo | Tipo ou unidade | Significado |
| --- | --- | --- |
| `location_id`, `city` | texto | Código e nome da localidade cadastrada. |
| `latitude`, `longitude` | graus decimais | Coordenadas da localidade. |
| `temperature` | °C | Temperatura atual modelada. |
| `precipitation` | mm | Precipitação acumulada no intervalo retornado pela fonte. |
| `wind_speed`, `wind_gust` | km/h | Velocidade média e rajada do vento. |
| `visibility` | m | Visibilidade. |
| `weather_code` | inteiro WMO | Código meteorológico da fonte. |
| `measurement_interval_seconds` | segundos, de 1 a 3600 | Intervalo associado à observação. |
| `event_timestamp`, `ingestion_timestamp` | ISO 8601 UTC, terminado em `Z` | Validade da observação e término da coleta, respectivamente. |
| `source` | `open-meteo` | Identificador fixo da fonte. |
| `schema_version` | inteiro `1` | Versão do contrato do evento. |

Os eventos da API representam dados de modelo, não medições de estação. Fixtures históricas são usadas em QA; mensagens e linhas de fixture retidas devem ser distinguidas pelos seus identificadores e timestamps, não apresentadas como alertas meteorológicos atuais.

## Tempo, janelas e risco

O Flink usa `event_timestamp` como Event Time e uma marca d'água com até 120 segundos de desordem; uma partição ociosa é excluída do avanço após 60 segundos. Janelas deslizantes de 30 minutos avançam a cada 5 minutos, alinhadas ao epoch UTC e no intervalo `[início, fim)`. Atraso permitido é zero: os 120 segundos afetam o avanço da marca d'água, mas não reabrem uma janela fechada.

A deduplicação de eventos usa a identidade `(location_id, event_timestamp normalizado em UTC)` em estado gerenciado pelo Flink, com TTL de 24 horas em processing time. Repetições não renovam o prazo. As métricas por localidade guardam contagem, média de temperatura, extremos de vento/rajada/visibilidade e intervalos e timestamps extremos. `precipitation_max` é um máximo; a projeção horária usada pelo risco é `precipitation × 3600 / measurement_interval_seconds`, não chuva acumulada na janela.

A versão atual de regras é `mvp-1`. Cada fator recebe score 0, 25, 50, 75 ou 100; o risco final é o maior score entre os fatores, sem soma. Limites de precipitação, vento e rajada são inclusivos; visibilidade usa limites estritos:

| Fator | Unidade e limites dos scores 25 / 50 / 75 / 100 |
| --- | --- |
| Projeção máxima de precipitação | mm/h: `>=2 / >=20 / >=30 / >=60` |
| Velocidade máxima do vento | km/h: `>=20 / >=40 / >=60 / >=100` |
| Rajada máxima | km/h: `>=30 / >=50 / >=80 / >=120` |
| Visibilidade mínima | m: `<5000 / <2000 / <1000 / <200` |

As faixas são `LOW` (0–24), `MEDIUM` (25–49), `HIGH` (50–74) e `CRITICAL` (75–100). A regra é uma classificação heurística ordinal do MVP; não representa probabilidade de prejuízo. Extremos de uma janela podem vir de observações diferentes.

## PostgreSQL e identidade

| Tabela | Conteúdo e identidade |
| --- | --- |
| `climate.locations` | Catálogo; chave primária `location_id`. |
| `climate.weather_metrics` | Uma avaliação por `(location_id, window_start_ms, window_end_ms, risk_rules_version)`. |
| `climate.weather_alerts` | Alertas elegíveis; chave primária `alert_id` e unicidade adicional pela identidade da avaliação. |

O `alert_id` é SHA-256 do JSON UTF-8 compacto da identidade `[location_id, window_start_ms, window_end_ms, risk_rules_version]`. O UPSERT de uma avaliação e seu alerta elegível ocorre em uma instrução SQL no PostgreSQL; o replay converge pela identidade e não altera a identidade imutável da métrica. A role de aplicação lê `locations` e tem `SELECT`, `INSERT` e `UPDATE` nas duas tabelas de fatos; não recebe DDL, exclusão ou privilégios administrativos.

Checkpoints Flink usam `EXACTLY_ONCE` para estado gerenciado e offsets do source. Essa garantia não se estende aos sinks: o UPSERT torna a persistência SQL repetível, enquanto o sink Kafka é `AT_LEAST_ONCE` e pode repetir alertas após recuperação. Não existe transação distribuída entre PostgreSQL e Kafka. O consumer usa cache FIFO volátil de 10.000 IDs; reinício, evicção ou falha entre o log e o commit podem repetir uma saída. Não há exatamente uma vez ponta a ponta.

## Operação, segurança e limites

Os healthchecks indicam prontidão dos serviços. O job precisa aparecer como `RUNNING` em `/jobs/overview`; reiniciar o JobManager standalone pode deixar a infraestrutura saudável sem job ativo. Não há alta disponibilidade nem submissão automática. Consulte o README para os comandos de inicialização, verificação e retomada com o `_metadata` de um checkpoint persistido.

Os limites de memória do Compose somam 4,375 GiB; a recomendação operacional é reservar pelo menos 6 GiB e cerca de 4 vCPUs para o Docker. Em uma execução pequena de entrega registrada na Etapa 8, JobManager e TaskManager foram observados em 551,3 MiB e 594,7 MiB; CPU registrada foi 1,70% e 2,10%, com backpressure `ok`. Essa amostra não é benchmark e não informa throughput sustentado.

Kafka é um nó local com PLAINTEXT, replicação 1 e sem TLS, SASL ou ACLs. O REST Flink é sem autenticação, mas fica vinculado ao loopback do host. Producer e consumer usam UID 10001; Flink executa como usuário `flink`. `.env` guarda credenciais locais fora do Git. A role PostgreSQL da aplicação tem privilégios restritos, e as credenciais administrativas não são fornecidas aos containers Python.

O consumer pode permanecer saudável sem mensagens novas: healthcheck mede processo, assignment, metadata e heartbeat. Ausência de `alert_received` pode significar que nenhuma janela alcançou HIGH ou CRITICAL; não indica falha por si só. QA usa fixtures, portanto alertas históricos dessa origem também não são evidência de condição atual.

Melhorias futuras fora do MVP: autenticação e criptografia no Kafka, alta disponibilidade e retomada automatizada do Flink, e deduplicação durável do consumer. O sistema atual opera com um broker e um slot Flink, sem benchmark de throughput ou garantia transacional de ponta a ponta.

## Fontes de verdade

Os contratos, limiares e identidades devem ser conferidos no código e no schema atual: [Compose](../compose.yaml), [modelo do evento](../src/climate_logistics/models.py), [parser e campos](../src/climate_logistics/weather_parser.py), [job Flink](../src/climate_logistics/flink_job.py), [janelas](../src/climate_logistics/flink_temporal.py), [agregações](../src/climate_logistics/temporal_metrics.py), [regras de risco](../src/climate_logistics/risk_engine.py), [registros e hash](../src/climate_logistics/business_records.py), [SQL de negócio](../src/climate_logistics/business_sql.py), [consumer](../src/climate_logistics/alert_consumer.py) e [migração PostgreSQL](../docker/postgres/migrations/001-business-sinks.sql).

## Arquivos versionados do projeto

O inventário abaixo cobre os arquivos versionados que permanecem no projeto. `.env`, `.venv` e `.git` são locais ou gerados e ficam fora deste inventário; `.env.example` é apenas o modelo versionado de configuração, sem credenciais locais.

### Raiz, configuração e documentação

| Arquivo | Função |
| --- | --- |
| `.dockerignore` | Exclui arquivos locais e desnecessários do contexto de build das imagens. |
| `.env.example` | Modelo de variáveis do Compose; nomes de banco e usuários ficam definidos, e os campos de senha devem ser preenchidos localmente. |
| `.gitattributes` | Padroniza atributos Git, incluindo finais de linha de scripts shell. |
| `.github/workflows/ci.yml` | Define os jobs de CI para qualidade Python, integração Kafka e runtime/SDK Flink. |
| `.gitignore` | Exclui ambientes virtuais, segredos, caches e provas locais do Git. |
| `README.md` | Orienta preparação, operação, verificação, QA e limites do MVP. |
| `compose.yaml` | Define serviços, redes, volumes, recursos, healthchecks e configuração local do sistema. |
| `pyproject.toml` | Configura projeto Python, pytest, Ruff e metadados das ferramentas. |
| `requirements-dev.txt` | Dependências para desenvolvimento e execução da suíte Python. |
| `requirements-flink.txt` | Dependências Python adicionais do runtime PyFlink. |
| `requirements.txt` | Dependências Python usadas pelo Producer e pelo consumer. |
| `docs/arquitetura.md` | Descreve fluxo, componentes, contratos, garantias, limites e inventário do código. |
| `docs/progresso.md` | Registra entregas, desafios, correções e validações das dez etapas concluídas. |
| `sql/.gitkeep` | Mantém versionado o diretório reservado a SQL fora das migrações do PostgreSQL. |

### Imagens e inicialização dos serviços

| Arquivo | Função |
| --- | --- |
| `docker/flink/Dockerfile` | Constrói o runtime compartilhado do Flink JobManager e TaskManager. |
| `docker/flink/checksums.sha256` | Fixa os hashes dos artefatos externos usados na imagem Flink. |
| `docker/flink/healthcheck.sh` | Verifica a prontidão da API REST e o registro do TaskManager. |
| `docker/flink/java/org/climatelogistics/AlertRecordSerializationSchema.java` | Serializa registros de alerta do sink Kafka para bytes. |
| `docker/flink/java/org/climatelogistics/BusinessJdbcStatementBuilder.java` | Mapeia linhas Flink aos parâmetros da escrita JDBC de negócio. |
| `docker/postgres/01-bootstrap.sh` | Inicializa banco, usuários, schema e privilégios do PostgreSQL. |
| `docker/postgres/02-business-schema.sh` | Aplica a migração do schema de negócio na inicialização do serviço. |
| `docker/postgres/locations.csv` | Fornece os dados iniciais do catálogo de localidades ao PostgreSQL. |
| `docker/postgres/migrations/001-business-sinks.sql` | Cria tabelas, chaves, restrições e permissões dos destinos de negócio. |
| `docker/producer/Dockerfile` | Constrói a imagem Python não-root compartilhada pelo Producer e pelo consumer. |
| `scripts/render_location_seed.py` | Gera `docker/postgres/locations.csv` a partir do catálogo Python de localidades. |

### Pacote Python `src/climate_logistics`

| Arquivo | Função |
| --- | --- |
| `src/climate_logistics/__init__.py` | Marca e identifica o pacote Python da aplicação. |
| `src/climate_logistics/alert_consumer.py` | Executa o consumer Kafka: valida alertas, emite logs JSON e confirma offsets após o flush. |
| `src/climate_logistics/business_config.py` | Lê e valida configurações de conexão e escrita dos sinks de negócio. |
| `src/climate_logistics/business_records.py` | Valida registros de métricas/alertas e calcula identidades e fingerprints determinísticos. |
| `src/climate_logistics/business_sql.py` | Define o UPSERT parametrizado e atômico das métricas e dos alertas elegíveis. |
| `src/climate_logistics/consumer_health.py` | Grava heartbeat atômico e implementa a verificação de saúde do consumer. |
| `src/climate_logistics/data_quality.py` | Valida coerência temporal e define identidade lógica de eventos meteorológicos. |
| `src/climate_logistics/flink_business.py` | Adapta avaliações do Flink para os sinks JDBC do PostgreSQL e Kafka. |
| `src/climate_logistics/flink_job.py` | Monta e submete o job de ingestão, validação, deduplicação e encaminhamento de eventos. |
| `src/climate_logistics/flink_risk.py` | Anexa o cálculo de risco às métricas de janela no grafo Flink. |
| `src/climate_logistics/flink_temporal.py` | Configura Event Time, watermarks, janelas, métricas tardias e checkpoints do Flink. |
| `src/climate_logistics/kafka_publisher.py` | Cria/verifica tópicos e publica eventos com serialização e confirmação de entrega. |
| `src/climate_logistics/locations.py` | Mantém e valida o catálogo de localidades monitoradas. |
| `src/climate_logistics/models.py` | Define os modelos imutáveis `Location` e `WeatherEvent` e valida seus campos. |
| `src/climate_logistics/open_meteo.py` | Normaliza respostas e implementa o cliente HTTP resiliente da Open-Meteo. |
| `src/climate_logistics/producer.py` | Executa ciclos de coleta por localidade e publicação no Kafka. |
| `src/climate_logistics/risk_engine.py` | Calcula scores, níveis e componentes de risco de forma pura e determinística. |
| `src/climate_logistics/temporal_metrics.py` | Agrega métricas escalares das janelas temporais por localidade. |
| `src/climate_logistics/weather_parser.py` | Valida JSON Kafka e converte eventos para o formato de linha do Flink. |

### Fixtures e testes de integração

| Arquivo | Função |
| --- | --- |
| `tests/fixtures/open_meteo_current.json` | Fixture de resposta atual da API para testes de contrato e normalização. |
| `tests/integration/.gitkeep` | Mantém o diretório de testes de integração no Git. |
| `tests/integration/business_qa_support.py` | Compara snapshots e diferenças semânticas de registros de negócio durante QA. |
| `tests/integration/test_alert_consumer_integration.py` | Exercita consumer, duplicatas, inválidos, offsets, healthcheck e retomada com Kafka real. |
| `tests/integration/test_flink_business.py` | Verifica sinks de negócio, replay, checkpoint e restauração em serviços reais. |
| `tests/integration/test_flink_business_faults.py` | Verifica recuperação do Flink após falha temporária de PostgreSQL ou Kafka. |
| `tests/integration/test_flink_risk.py` | Confere contratos de risco e restauração de checkpoint em Flink real. |
| `tests/integration/test_flink_temporal.py` | Confere janelas, atraso, deduplicação e recuperação temporal em Flink real. |
| `tests/integration/test_kafka_flink.py` | Verifica publicação Kafka e processamento bounded/unbounded pelo Flink. |
| `tests/integration/test_open_meteo_http.py` | Exercita HTTP em servidor local, incluindo retries, limites e respostas inválidas. |
| `tests/integration/test_open_meteo_live.py` | Smoke opt-in do contrato contra a API pública Open-Meteo. |
| `tests/integration/test_postgres_business.py` | Verifica migração, privilégios, replay, imutabilidade e atomicidade no PostgreSQL. |
| `tests/integration/test_producer_kafka.py` | Verifica entrega Kafka real, tratamento de falha e CLI do Producer. |

### Testes unitários e de runtime

| Arquivo | Função |
| --- | --- |
| `tests/unit/.gitkeep` | Mantém o diretório de testes unitários no Git. |
| `tests/unit/test_alert_consumer.py` | Testa validação, processamento, configuração, offsets e ciclo de vida do consumer. |
| `tests/unit/test_alert_records.py` | Testa contrato, identidade, fingerprint e cache de registros de alerta. |
| `tests/unit/test_business_config.py` | Testa validação das configurações dos sinks de negócio. |
| `tests/unit/test_business_records.py` | Testa parsing, validação e identidades de métricas e alertas. |
| `tests/unit/test_business_schema.py` | Testa definições e restrições do schema PostgreSQL de negócio. |
| `tests/unit/test_consumer_health.py` | Testa heartbeat e critérios de saúde do consumer. |
| `tests/unit/test_data_quality.py` | Testa política temporal e deduplicação lógica de eventos. |
| `tests/unit/test_flink_business_fixtures.py` | Testa fixtures e helpers que compõem registros para os sinks. |
| `tests/unit/test_flink_business_sdk.py` | Verifica configuração dos sinks contra o SDK PyFlink. |
| `tests/unit/test_flink_job.py` | Testa configuração e funções de construção/submissão do job Flink. |
| `tests/unit/test_flink_qa_helpers.py` | Testa validações de identidade e opções de execução usadas pelo QA Flink. |
| `tests/unit/test_flink_risk_sdk.py` | Verifica a integração do adaptador de risco com o SDK PyFlink. |
| `tests/unit/test_flink_temporal_sdk.py` | Verifica estratégias de watermark, janelas e checkpoints no SDK PyFlink. |
| `tests/unit/test_kafka_publisher.py` | Testa serialização, configuração de tópicos e publicação Kafka. |
| `tests/unit/test_locations.py` | Testa catálogo e invariantes das localidades. |
| `tests/unit/test_models.py` | Testa modelos meteorológicos e validação de seus campos. |
| `tests/unit/test_normalization.py` | Testa conversão dos campos da API Open-Meteo para `WeatherEvent`. |
| `tests/unit/test_open_meteo_client.py` | Testa transporte HTTP, timeout, retry e erros do cliente. |
| `tests/unit/test_producer.py` | Testa ciclos, configuração e encerramento do Producer. |
| `tests/unit/test_risk_engine.py` | Testa scores, níveis e composição do motor de risco. |
| `tests/unit/test_temporal_metrics.py` | Testa agregações e contrato das métricas temporais. |
| `tests/unit/test_weather_parser.py` | Testa parsing e validação do evento consumido pelo Flink. |