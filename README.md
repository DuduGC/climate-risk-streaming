# Climate Logistics Streaming Platform

Fundação local de uma plataforma de logística climática: o Producer coleta eventos da Open-Meteo e publica no Kafka; Flink processa as observações, persiste avaliações de risco no PostgreSQL e publica alertas de risco alto no tópico `weather.alerts`. A Etapa 3 inclui o contrato de eventos, o cliente HTTP e a publicação em `weather.raw`. A Etapa 4 adicionou o primeiro job Flink; as Etapas 5 e 6 validam coerência temporal, deduplicam eventos e calculam métricas por janela; a Etapa 7 acrescenta uma avaliação determinística de risco climático. O consumer de alertas continua reservado à etapa 9.

## Requisitos e recursos

Use Docker Engine com suporte a containers Linux e Docker Compose v2, ou Docker Desktop configurado para containers Linux. Reserve ao menos 6 GiB de memória para o Docker e cerca de 4 vCPUs: os limites dos cinco containers somam cerca de 4,25 GiB (Kafka 1 GiB, JobManager 1 GiB, TaskManager 1,5 GiB, PostgreSQL 512 MiB e Producer 256 MiB), além do sistema e do próprio Docker.

| Serviço | Imagem | Recurso máximo | Acesso e persistência |
| --- | --- | --- | --- |
| Kafka | `apache/kafka:3.9.1` | 1 GiB; heap 512 MiB | Rede Docker em `kafka:9092`; volume `kafka-data` |
| Producer | python:3.11.14-slim-bookworm | 256 MiB; 0,5 vCPU | Rede Docker em kafka:9092; sem porta própria |
| Flink JobManager | `climate-logistics-flink:local` | 1 GiB; processo 768 MiB | Console REST em `http://127.0.0.1:8081`; volume `flink-checkpoints` |
| Flink TaskManager | `climate-logistics-flink:local` | 1,5 GiB; processo 1280 MiB | 1 slot; paralelismo padrão 1; volume `flink-checkpoints` |
| PostgreSQL | `postgres:17.11-bookworm` | 512 MiB | Rede Docker em `postgres:5432`; volume `postgres-data` |

Kafka e PostgreSQL não publicam portas no host. Kafka usa um único nó KRaft combinado (broker/controller), replicação 1 e protocolo PLAINTEXT dentro da rede local; não há alta disponibilidade, TLS, SASL ou ACLs. O console REST do Flink fica limitado ao loopback do host e não tem autenticação.

No broker Kafka, a retenção está configurada para 24 horas e 1 GiB por partição, com segmentos de 256 MiB e verificação de retenção a cada 5 minutos. A remoção ocorre por segmentos fechados; por isso, 1 GiB não é um teto rígido e imediato para o armazenamento do broker.

## Preparar e iniciar

Copie o modelo de ambiente, preencha localmente as duas senhas e mantenha o arquivo `.env` fora do Git:

```powershell
Copy-Item .env.example .env
```

No Linux, use `cp .env.example .env`. O Compose exige os valores de `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `APP_DB_USER` e `APP_DB_PASSWORD`; o modelo já define os nomes do banco e das roles, mas deixa as senhas vazias.

```powershell
docker compose up -d
docker compose ps
```

Espere Kafka, os dois serviços Flink e PostgreSQL mostrarem `healthy` em `docker compose ps`. O Producer aparece como `running`; acompanhe suas entregas pelos logs. O healthcheck do Flink valida a API REST e o registro do TaskManager.

## Verificar os serviços

Confirme que o broker responde ao protocolo Kafka:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092
```

Consulte a visão geral REST do Flink e confira um TaskManager e um slot disponível. A mesma visão pode ser aberta em `http://127.0.0.1:8081`:

```powershell
Invoke-RestMethod http://127.0.0.1:8081/overview | ConvertTo-Json
```

Acesse PostgreSQL por TCP local dentro do container com a role da aplicação. `-h 127.0.0.1` testa a autenticação TCP por senha, e `-W` solicita a senha interativamente, sem colocá-la no comando:

```powershell
docker compose exec -it postgres psql -h 127.0.0.1 -U climate_app -d climate_logistics -W
```

No prompt `psql`, verifique o banco e o schema:

```sql
SELECT current_user, current_database();
SELECT has_schema_privilege(current_user, 'climate', 'USAGE') AS acesso_ao_schema;
```

Saia com `\q`. O bootstrap concede `CONNECT` ao banco e `USAGE` no schema `climate`; tabelas e privilégios de escrita entram em uma etapa posterior.

## Cliente Open-Meteo

O pacote Python fica em src e ainda não é instalado como wheel. Defina PYTHONPATH antes de importar o cliente:

~~~powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
~~~

~~~sh
export PYTHONPATH=src
~~~

O cliente síncrono consulta https://api.open-meteo.com/v1/forecast para uma localidade por chamada. Use as localidades configuradas e feche a sessão com o context manager:

~~~python
from climate_logistics.locations import LOCATIONS
from climate_logistics.open_meteo import OpenMeteoClient

with OpenMeteoClient() as client:
    event = client.fetch(LOCATIONS[0])

print(event.to_dict())
~~~

A resposta usa o bloco current e mantém as coordenadas da localidade configurada no evento. event_timestamp indica a validade do dado e ingestion_timestamp registra quando a resposta terminou de ser recebida. Os valores vêm de modelos meteorológicos, não de observações de estação. A precipitação é acumulada no intervalo informado pela fonte; measurement_interval_seconds registra esse intervalo.

| Campo Open-Meteo | Campo no evento | Unidade |
| --- | --- | --- |
| temperature_2m | temperature | °C |
| precipitation | precipitation | mm |
| wind_speed_10m | wind_speed | km/h |
| wind_gusts_10m | wind_gust | km/h |
| visibility | visibility | m |
| weather_code | weather_code | código WMO |

O cliente solicita UTC e timestamp Unix; o evento é serializado em ISO 8601 UTC terminado em Z. Intervalos aceitos vão de 1 a 3600 segundos. O contrato valida valores, campos e unidades, e rejeita respostas inválidas em vez de substituir dados ausentes por zero.

O transporte usa TLS verificado, não segue redirects, limita cada resposta a 128 KiB descomprimidos e faz até três tentativas para timeout/conexão transitória, HTTP 429 e HTTP 500/502/503/504. Em 429, um Retry-After numérico é limitado a cinco segundos; cabeçalhos ausentes, inválidos ou em formato de data usam o backoff local. Os timeouts de Requests são 3 segundos para conexão e 10 segundos de inatividade de leitura. Eles não representam um prazo total absoluto para a operação.

## Producer Python e Kafka

O container Producer consulta RJ01, SP01 e CT01 em sequência, valida os eventos com o contrato existente e publica somente respostas válidas em `weather.raw`. O bootstrap cria `weather.raw` e `weather.alerts` quando ausentes, cada um com três partições e replicação 1; tópicos existentes com topologia incompatível interrompem o startup sem alteração destrutiva. O Producer não publica em `weather.alerts`; esse tópico recebe alertas do sink de negócio do Flink.

O padrão é KAFKA_BOOTSTRAP_SERVERS=kafka:9092 e WEATHER_POLL_INTERVAL_SECONDS=300. O Producer usa somente configuração pública e não recebe credenciais PostgreSQL. Para compilar e iniciar o serviço contínuo:

~~~powershell
docker compose build producer
docker compose up -d producer
docker compose logs -f producer
~~~

Execute um único ciclo verificável em outra janela. O comando retorna zero apenas quando as três localidades foram coletadas e cada entrega recebeu confirmação do broker:

~~~powershell
docker compose run --rm --no-deps producer python -m climate_logistics.producer --once
~~~

Confira a topologia criada pelo serviço:

~~~powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic weather.raw
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic weather.alerts
~~~

Para uma inspeção temporária, comece a consumir a partir do offset mais recente em uma partição e execute --once em outra janela:

~~~powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic weather.raw --partition 0 --offset latest --timeout-ms 5000 --max-messages 1
~~~

A chave Kafka é location_id em UTF-8. O valor é o JSON compacto do WeatherEvent, sem campos adicionais, limitado a 16 KiB. event_timestamp conserva o horário do dado meteorológico; ingestion_timestamp registra a recepção HTTP e também define o CreateTime Kafka em milissegundos. O log JSON registra entregas confirmadas por callback, falhas e um resumo por ciclo sem incluir o corpo HTTP.

A idempotência do cliente reduz duplicações de retry na mesma sessão. Polls sucessivos e reinícios ainda podem publicar novamente o mesmo dado lógico; o job Flink mantém a primeira ocorrência válida por localidade e instante do evento durante o TTL descrito abaixo. Uma falha HTTP em uma localidade deixa as outras tentarem publicar, e o ciclo fica parcial. O Producer usa os timeouts de conexão/leitura HTTP já definidos pelo cliente; eles não garantem um prazo total absoluto. SIGTERM e SIGINT interrompem ciclos novos e iniciam flush limitado a 12 segundos.

## Job PyFlink: qualidade e diagnóstico

JobManager e TaskManager usam a imagem `climate-logistics-flink:local`, com o mesmo CPython 3.11.14, PyFlink 1.20.3 e o conector Kafka verificado. O job lê `weather.raw` em `kafka:9092`, valida o schema v1 e a coerência temporal, deduplica por localidade e envia um JSON compacto ao PrintSink do TaskManager. Mensagens inválidas conhecidas são descartadas e contadas sem registrar o corpo; falhas inesperadas interrompem o job.

A política temporal compara os timestamps em UTC e aceita os limites inclusivos: ambos devem ser posteriores ou iguais a `1970-01-01T00:00:00Z`; `event_timestamp` pode ficar até cinco minutos à frente de `ingestion_timestamp`; ingestão pode ficar até cinco minutos à frente do relógio observado; e a diferença entre ingestão e evento não pode passar de 24 horas. As razões estáveis de rejeição são `timestamp_before_epoch`, `event_after_ingestion`, `ingestion_in_future` e `excessive_ingestion_delay`, nessa ordem de precedência. Um relógio sem fuso ou que não possa ser convertido para UTC é erro de configuração e falha visivelmente.

Não há limite de idade baseado no horário em que o job começou: um replay antigo continua elegível se os timestamps originais respeitam a janela de 24 horas entre evento e ingestão. A identidade deduplicada é `location_id` mais `event_timestamp` normalizado em UTC; offsets diferentes para o mesmo instante e valores diferentes de ingestão continuam sendo a mesma identidade. A primeira ocorrência temporalmente válida vence, mesmo que uma publicação posterior traga payload revisado. Um evento rejeitado não cria marcador.

O operador usa `MapState` gerenciado por chave de localidade, com timestamp UTC como chave e marcador booleano como valor. O TTL operacional é de 86.400 segundos em processing time, com atualização somente na criação/escrita, leitura de entradas expiradas como ausentes e limpeza incremental do backend heap. O job desabilita o cache de estado Python (`python.state.cache-size=0`) para que uma leitura não devolva um marcador expirado. A consulta de uma duplicata não escreve nem renova o TTL. Esse prazo define a janela lógica de deduplicação: volume de identidades únicas afeta o tamanho do estado, e a remoção física é incremental e best effort, sem teto rígido de memória. Checkpoints preservam esses marcadores durante a recuperação enquanto não expirarem; o prazo continua baseado em processing time, então o tempo fora de execução também pode consumir o TTL. Depois do TTL, uma identidade pode voltar a ser aceita. O fluxo não promete deduplicação permanente.

As métricas ficam nos grupos `weather_ingestion` e `data_quality`: `parsed_records` conta schema válido antes da política temporal; `invalid_records` inclui erros de parsing e de temporalidade; `temporal_rejected_records` conta as rejeições temporais; `duplicate_records` conta identidades ainda vigentes; e `accepted_records` conta os registros que avançam após a deduplicação. O diagnóstico de ingestão continua no ramo atual, em paralelo às novas métricas temporais.

## Métricas temporais por localidade

Depois da validação e da deduplicação, o job usa `event_timestamp` UTC como Event Time. Atribui watermarks com até 120 segundos de desordem, emissão a cada 200 ms e idleness de 60 segundos. O relógio de parede não fecha janelas quando o fluxo inteiro fica parado; elas aguardam novos eventos ou, em fontes bounded, o watermark terminal. O atraso das métricas também depende da frequência de novas observações do Open-Meteo. Como o watermark é global ao fluxo, uma localidade com timestamps atrasados pode perder participação em janelas que já fecharam.

As janelas são deslizantes de 30 minutos, avançam em intervalos de 5 minutos e usam intervalos `[início, fim)` alinhados ao epoch UTC. Cada evento válido participa de seis janelas por `location_id`. As métricas aparecem no PrintSink `weather-metrics` como JSON com início/fim em milissegundos UTC, contagem, média aritmética de temperatura, extremos de vento e visibilidade, intervalos das observações e extremos dos timestamps. A média considera cada observação uma vez, sem ponderação pelo tempo.

`precipitation_max` é a maior precipitação observada na janela, não uma soma. `precipitation_rate_max_mm_per_hour` é a maior projeção `precipitação × 3600 / intervalo_da_observação`, não uma medição acumulada de uma hora. O agregador guarda apenas contagem, soma e extremos escalares; não mantém uma lista de eventos. O PrintSink de métricas temporais continua diagnóstico; o ramo de avaliação de risco alimenta os destinos de negócio descritos abaixo.

O `allowed_lateness` é zero. Os 120 segundos de desordem controlam o avanço do watermark e não reabrem janelas fechadas. Um evento parcialmente atrasado ainda contribui para as janelas abertas; `temporal_stream.late_records` conta uma vez somente quando todas as janelas do evento já fecharam. O payload atrasado é descartado após a contagem, sem tópico novo. Os checkpoints preservam o estado das janelas e da deduplicação para recuperação a partir do snapshot escolhido.

## Avaliação de risco por janela

Depois da agregação temporal, um ramo sem estado calcula uma avaliação para cada JSON `window_aggregate`. O PrintSink `weather-metrics` continua emitindo o contrato v1 original de 17 campos. O novo PrintSink `weather-risk` emite 21 campos: preserva os 17 valores, muda `operation` para `risk_assessment` e acrescenta `risk_score`, `risk_level`, `risk_components` e `risk_rules_version`. A versão de regras atual é `mvp-1`.

Cada fator recebe 0, 25, 50, 75 ou 100. Chuva e vento usam comparações inclusivas (`>=`); visibilidade usa limites estritos (`<`). A avaliação final escolhe o maior score entre os quatro fatores:

| Fator e unidade | Score 25 | Score 50 | Score 75 | Score 100 |
| --- | --- | --- | --- | --- |
| Intensidade máxima de chuva (mm/h) | >= 2 | >= 20 | >= 30 | >= 60 |
| Velocidade máxima do vento (km/h) | >= 20 | >= 40 | >= 60 | >= 100 |
| Rajada máxima (km/h) | >= 30 | >= 50 | >= 80 | >= 120 |
| Visibilidade mínima (m) | < 5000 | < 2000 | < 1000 | < 200 |

As faixas de `risk_level` são LOW (0–24), MEDIUM (25–49), HIGH (50–74) e CRITICAL (75–100). Por exemplo, vento a 40 km/h resulta em 50/HIGH; visibilidade de 199 m resulta em 100/CRITICAL. Dois fatores com score 50 continuam em 50: esta versão não soma fatores nem amplifica combinações moderadas, e uma condição extrema não é diluída pelas demais.

O score é ordinal e heurístico. A projeção de chuva em mm/h vem da precipitação e do intervalo informado em cada observação; não é chuva acumulada na janela nem probabilidade de prejuízo. Os extremos podem vir de observações diferentes, então a avaliação não afirma que os fatores ocorreram simultaneamente. Temperatura, código meteorológico e contagem de eventos não participam da regra.

O contador `risk_engine.assessed_windows` registra avaliações calculadas e serializadas pelo operador. Ele é uma métrica de processamento: pode reiniciar ou contar novamente após recuperação. O ramo de risco não cria estado próprio e o PrintSink não é transacional; checkpoints preservam as janelas existentes, mas a recuperação pode repetir linhas de diagnóstico. As escritas de negócio têm garantias separadas, descritas abaixo.

## Checkpoints e recuperação

O job operacional grava checkpoints a cada 60 segundos em `file:///opt/flink/checkpoints`, usando `EXACTLY_ONCE` para estado gerenciado e offsets do source. Cada job mantém até dois checkpoints concluídos; checkpoints externalizados ficam retidos quando o job é cancelado. O volume nomeado `flink-checkpoints` é compartilhado entre JobManager e TaskManager e persiste quando os containers são recriados.

O timeout é 120 segundos, a pausa mínima entre checkpoints é 10 segundos, há no máximo um checkpoint concorrente e nenhuma falha consecutiva de checkpoint é tolerada. Falhas do job podem reiniciar até três vezes, com 10 segundos entre tentativas; depois desse limite, o job termina com falha visível. Checkpoints de jobs cancelados podem continuar ocupando o volume mesmo com retenção de dois checkpoints por job. A limpeza é manual: confirme que o checkpoint exato e o JobID correspondente não são mais necessários antes de remover qualquer dado. O job não apaga diretórios de outros JobIDs.

Para restaurar um snapshot, use o caminho `_metadata` retornado pelo endpoint REST de checkpoints em `external_path` e preserve o mesmo nome/grupo e o grafo com seus UIDs. Por exemplo, para um job QA que usou intervalo de 10 segundos:

~~~powershell
docker compose exec -T -u flink flink-jobmanager /opt/flink/bin/flink run -d -s file:///opt/flink/checkpoints/JOB_ID/chk-CHECKPOINT_ID/_metadata -py /opt/climate/src/climate_logistics/flink_job.py --job-name weather-flink-qa-a1b2 --group-id weather-flink-qa-a1b2-group --qa-audit --qa-checkpoint-interval-seconds 10
~~~

Substitua o caminho pelo `external_path` real do snapshot escolhido. Um override de 5 a 60 segundos exige `--qa-audit` e um nome/grupo QA exclusivos e correspondentes; sem override, o intervalo operacional continua em 60 segundos. A restauração é explícita: reiniciar todo o cluster standalone não submete o job novamente, pois não há alta disponibilidade nem autossubmissão. O PrintSink não participa de transação: a recuperação de estado e offsets não impede repetição de linhas nos logs e não representa exactly-once ponta a ponta.

Construa a imagem compartilhada e inicie os serviços:

~~~powershell
docker compose build flink-jobmanager
docker compose up -d
docker compose ps
~~~

Espere os healthchecks do JobManager e do TaskManager. Confira a lista de jobs antes de submeter para não ocupar o único slot com uma segunda instância:

~~~powershell
Invoke-RestMethod http://127.0.0.1:8081/jobs/overview | ConvertTo-Json
~~~

Submeta o job operacional em modo detached pelo CLI do Flink, como usuário `flink` no JobManager. O comando retorna o JobID; use esse ID para acompanhar ou cancelar a execução:

~~~powershell
docker compose exec -T -u flink flink-jobmanager /opt/flink/bin/flink run -d -py /opt/climate/src/climate_logistics/flink_job.py --job-name weather-kafka-ingestion --group-id weather-flink-ingestion
~~~

Consulte um job específico e seus logs:

~~~powershell
Invoke-RestMethod http://127.0.0.1:8081/jobs/JOB_ID | ConvertTo-Json
docker compose logs --tail 100 flink-taskmanager
~~~

Para cancelar, passe somente o JobID que você identificou:

~~~powershell
docker compose exec -T -u flink flink-jobmanager /opt/flink/bin/flink cancel JOB_ID
~~~

O source começa em `earliest`, usa o grupo `weather-flink-ingestion`, não faz auto-commit e roda com paralelismo 1. Com checkpoints habilitados, Flink restaura explicitamente o estado e os offsets gravados no snapshot escolhido; uma submissão sem `-s` inicia de acordo com o initializer do source e pode reler registros retidos no tópico. O ramo de negócio persiste avaliações validadas no PostgreSQL e publica alertas elegíveis em `weather.alerts`; os logs de diagnóstico continuam temporários.

A integração bounded existente confirma o descarte de JSON inválido e a saída do evento válido. Um segundo cenário bounded publica JSON inválido, timestamps incoerentes, uma identidade válida, uma cópia revisada com ingestão e offset diferentes, a mesma hora em outra localidade e um instante novo na primeira localidade. Ele aguarda `FINISHED` e compara os JSON do PrintSink e a auditoria QA por identidade. Os testes temporais bounded comparam as seis participações de cada fixture a um oracle completo, incluindo desordem, deduplicação e a fronteira exclusiva da janela.

O cenário TTL sobe um job QA não bounded com TTL de 20 segundos, publica um evento histórico válido, descarta uma cópia perto do fim do prazo e aceita outra após o deadline original. A segunda aceitação ocorre antes do deadline que existiria se a consulta duplicada renovasse o estado; assim, a prova cobre processing time, replay e ausência de refresh. A execução usa bundle de tamanho 1 e tempo de 1000 ms somente no job QA. Os flags `--qa-audit`, `--qa-dedup-ttl-seconds` e `--qa-checkpoint-interval-seconds` exigem nome `weather-flink-qa-<id>` e grupo com o mesmo prefixo exclusivo e correspondente, por exemplo `weather-flink-qa-a1b2` e `weather-flink-qa-a1b2-group`. O override de TTL aceita apenas 2 a 60 segundos e o de checkpoint, 5 a 60 segundos; o job operacional mantém TTL de 86.400 segundos e checkpoint de 60 segundos sem override por ambiente.

Esses testes cancelam somente o próprio JobID. Antes de iniciar a integração, confira `/jobs/overview` e coordene a pausa/restauração de um job operacional conhecido se ele ocupar o único slot:

~~~powershell
$env:FLINK_INTEGRATION = "1"
python -m pytest tests/integration/test_kafka_flink.py tests/integration/test_flink_temporal.py tests/integration/test_flink_risk.py -q
Remove-Item Env:FLINK_INTEGRATION
~~~

No Linux, use `FLINK_INTEGRATION=1 python -m pytest tests/integration/test_kafka_flink.py tests/integration/test_flink_temporal.py tests/integration/test_flink_risk.py -q`. O cenário temporal unbounded e a restauração do checkpoint Stage 6 exigem que o Producer conhecido esteja pausado e que não haja outro job usando o único slot; os testes não cancelam nem alteram jobs alheios.

## Python e qualidade

A estrutura e os serviços Python futuros usam Python 3.11. Crie um ambiente virtual e instale as dependências fixadas:

~~~powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m ruff check .
python -m pytest
~~~

No Linux, crie/ative o ambiente com python3.11 -m venv .venv e source .venv/bin/activate. A suíte padrão é offline e o smoke live é pulado explicitamente. Os testes HTTP usam um servidor local no loopback.

Para executar a consulta real de RJ01, habilite o smoke apenas nessa execução:

~~~powershell
$env:OPEN_METEO_LIVE = "1"
python -m pytest tests/integration/test_open_meteo_live.py -q
Remove-Item Env:OPEN_METEO_LIVE
~~~

No Linux, use OPEN_METEO_LIVE=1 python -m pytest tests/integration/test_open_meteo_live.py -q.

## Persistência e próximos passos

Os dados de Kafka e PostgreSQL ficam nos volumes nomeados `kafka-data` e `postgres-data`. Para parar os containers e preservar os dados:

```powershell
docker compose stop
```

O script de bootstrap do PostgreSQL executa somente quando `postgres-data` está vazio. Alterar uma senha no `.env` não muda a senha de uma role que já existe; atualize-a no banco com uma sessão administrativa. O mesmo vale para as credenciais iniciais do banco.

### Schema de negócio do PostgreSQL

O catálogo CSV versionado é derivado de `src/climate_logistics/locations.py`. Depois de alterar as localidades, gere o arquivo novamente e revise o diff:

~~~powershell
python scripts/render_location_seed.py
~~~

Em um volume PostgreSQL novo, `02-business-schema.sh` roda após o bootstrap inicial. Para aplicar ou reaplicar a migração em um volume existente, execute:

~~~powershell
docker compose exec -T postgres bash /docker-entrypoint-initdb.d/02-business-schema.sh
~~~

A migração cria `climate.locations`, `climate.weather_metrics` e `climate.weather_alerts` em uma transação, carrega o seed e concede à role da aplicação leitura da dimensão e leitura/escrita das tabelas de fatos, sem DDL, `DELETE` ou criação de tabelas temporárias. Ela valida o schema versionado e interrompe a transação quando encontra drift.

### Sinks de negócio do Flink

O job operacional envia cada avaliação válida de RJ01, SP01 ou CT01 a `climate.weather_metrics`. Um único UPSERT SQL grava a avaliação e, quando o score é pelo menos 50 (HIGH ou CRITICAL), o alerta correspondente em `climate.weather_alerts`. O PostgreSQL mantém a identidade estável da avaliação; replay do mesmo checkpoint converge pelas chaves do UPSERT. O conector JDBC usa lotes de até 100 registros ou 2 segundos, três tentativas e timeouts de conexão/leitura de 10 segundos; a validação de conexão usa 5 segundos.

Somente avaliações HIGH ou CRITICAL seguem para `weather.alerts`. A chave Kafka é o `location_id` em UTF-8 e o valor é o JSON de alerta de 22 campos, incluindo `alert_id`. O sink usa `AT_LEAST_ONCE`, `acks=all` e tentativas limitadas. Kafka pode receber a mesma identidade mais de uma vez após uma recuperação; consumidores devem deduplicar por `alert_id`. Não há transação distribuída entre Kafka e PostgreSQL.

IDs de localidade que não existem no catálogo são contados em `business_sinks.unknown_locations` e descartados apenas do ramo de negócio; os ramos diagnósticos anteriores continuam. Os contadores `jdbc_records_prepared` e `alert_records_prepared` representam preparação de registros, não confirmação de gravação.

O Compose disponibiliza `POSTGRES_DB`, `APP_DB_USER` e `APP_DB_PASSWORD` apenas no JobManager, que carrega a configuração ao montar e submeter o grafo. Os conectores recebem as opções serializadas no job; o TaskManager não precisa das variáveis de ambiente do banco. O Flink usa a role da aplicação; `POSTGRES_USER` e `POSTGRES_PASSWORD` permanecem no serviço PostgreSQL. A senha não aparece na URL JDBC nem nos argumentos da CLI.

O modo QA legado não acessa os sinks de negócio. Para uma execução QA que precise exercitá-los, use `--qa-audit --qa-business-sinks` e nome/grupo exclusivos, por exemplo `weather-flink-qa-a1b2` e `weather-flink-qa-a1b2-group`. A configuração dos destinos só é carregada depois da validação de identidade QA.

A Etapa 3 criou os tópicos `weather.raw` e `weather.alerts` com três partições; a criação automática do broker segue desabilitada. A Etapa 4 adicionou o diagnóstico de ingestão; as Etapas 6 e 7 acrescentaram métricas temporais e avaliação de risco; a Etapa 8 criou o schema PostgreSQL e conectou os sinks nativos do job. O consumer de alertas continua reservado à etapa 9.
