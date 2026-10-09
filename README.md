# Climate Logistics Streaming Platform

Fundação local de uma plataforma de logística climática: o Producer coleta eventos da Open-Meteo e publica no Kafka; Flink fará o processamento e PostgreSQL armazenará resultados de negócio. A Etapa 3 inclui o contrato de eventos, o cliente HTTP e a publicação em weather.raw. A Etapa 4 adiciona o primeiro job Flink, que consome esse tópico e emite um diagnóstico técnico; tabelas de negócio e consumidor de alertas continuam para etapas posteriores.

## Requisitos e recursos

Use Docker Engine com suporte a containers Linux e Docker Compose v2, ou Docker Desktop configurado para containers Linux. Reserve ao menos 6 GiB de memória para o Docker e cerca de 4 vCPUs: os limites dos cinco containers somam cerca de 4,25 GiB (Kafka 1 GiB, JobManager 1 GiB, TaskManager 1,5 GiB, PostgreSQL 512 MiB e Producer 256 MiB), além do sistema e do próprio Docker.

| Serviço | Imagem | Recurso máximo | Acesso e persistência |
| --- | --- | --- | --- |
| Kafka | `apache/kafka:3.9.1` | 1 GiB; heap 512 MiB | Rede Docker em `kafka:9092`; volume `kafka-data` |
| Producer | python:3.11.14-slim-bookworm | 256 MiB; 0,5 vCPU | Rede Docker em kafka:9092; sem porta própria |
| Flink JobManager | `climate-logistics-flink:local` | 1 GiB; processo 768 MiB | Console REST em `http://127.0.0.1:8081` |
| Flink TaskManager | `climate-logistics-flink:local` | 1,5 GiB; processo 1280 MiB | 1 slot; paralelismo padrão 1 |
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

O container Producer consulta RJ01, SP01 e CT01 em sequência, valida os eventos com o contrato existente e publica somente respostas válidas em weather.raw. O bootstrap cria weather.raw e weather.alerts quando ausentes, cada um com três partições e replicação 1; tópicos existentes com topologia incompatível interrompem o startup sem alteração destrutiva. weather.alerts permanece vazio nesta etapa.

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

A idempotência do cliente reduz duplicações de retry na mesma sessão. Polls sucessivos e reinícios podem publicar novamente o mesmo dado lógico; não há deduplicação de negócio nesta etapa. Uma falha HTTP em uma localidade deixa as outras tentarem publicar, e o ciclo fica parcial. O Producer usa os timeouts de conexão/leitura HTTP já definidos pelo cliente; eles não garantem um prazo total absoluto. SIGTERM e SIGINT interrompem ciclos novos e iniciam flush limitado a 12 segundos.

## Job PyFlink: Kafka para diagnóstico

JobManager e TaskManager usam a imagem `climate-logistics-flink:local`, com o mesmo CPython 3.11.14, PyFlink 1.20.3 e o conector Kafka verificado. O job lê `weather.raw` em `kafka:9092`, valida o schema v1 e envia um JSON compacto ao PrintSink do TaskManager. Mensagens inválidas conhecidas são descartadas, contadas e registradas sem o corpo; falhas inesperadas interrompem o job.

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

O source começa em `earliest`, usa o grupo `weather-flink-ingestion`, não faz auto-commit e roda com paralelismo 1. Sem checkpoint nesta etapa, uma nova submissão pode reler os registros retidos no tópico; não há promessa de recuperação ou exactly-once. O diagnóstico é temporário e não grava no PostgreSQL nem em outro tópico.

A integração bounded publica uma mensagem inválida seguida da fixture válida, submete com nome e grupo exclusivos, aguarda FINISHED e confirma o JSON do PrintSink e o log `invalid_records`. Ela não cancela jobs desconhecidos; antes de executá-la, confira a lista e coordene a pausa/restauração de qualquer job operacional:

~~~powershell
$env:FLINK_INTEGRATION = "1"
python -m pytest tests/integration/test_kafka_flink.py -q
Remove-Item Env:FLINK_INTEGRATION
~~~

No Linux, use `FLINK_INTEGRATION=1 python -m pytest tests/integration/test_kafka_flink.py -q`.

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

A Etapa 3 criou os tópicos weather.raw e weather.alerts com três partições; a criação automática do broker segue desabilitada. A Etapa 4 executa o primeiro job Flink com diagnóstico no TaskManager; ainda não há persistência de negócio, JDBC ou checkpoint. Etapas posteriores tratarão os sinks no schema climate e o consumo de alertas.
