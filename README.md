# Climate Logistics Streaming Platform

Fundação local de uma plataforma de logística climática: Kafka transportará eventos, Flink fará o processamento e PostgreSQL armazenará resultados de negócio. Nesta etapa há infraestrutura, contrato de eventos e um cliente síncrono da Open-Meteo. Ainda não há produtor, tópicos, jobs Flink, tabelas de negócio ou consumidor de alertas.

## Requisitos e recursos

Use Docker Engine com suporte a containers Linux e Docker Compose v2, ou Docker Desktop configurado para containers Linux. Reserve ao menos 6 GiB de memória para o Docker e cerca de 4 vCPUs: os limites dos quatro containers somam 4 GiB (Kafka 1 GiB, JobManager 1 GiB, TaskManager 1,5 GiB e PostgreSQL 512 MiB), além do sistema e do próprio Docker.

| Serviço | Imagem | Recurso máximo | Acesso e persistência |
| --- | --- | --- | --- |
| Kafka | `apache/kafka:3.9.1` | 1 GiB; heap 512 MiB | Rede Docker em `kafka:9092`; volume `kafka-data` |
| Flink JobManager | `flink:1.20.3-scala_2.12-java17` | 1 GiB; processo 768 MiB | Console REST em `http://127.0.0.1:8081` |
| Flink TaskManager | `flink:1.20.3-scala_2.12-java17` | 1,5 GiB; processo 1280 MiB | 1 slot; paralelismo padrão 1 |
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

Espere os quatro serviços mostrarem `healthy` em `docker compose ps`. O healthcheck do Flink valida a API REST e o registro do TaskManager.

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

Conforme o planejamento, a Etapa 3 criará explicitamente os tópicos `weather.raw` e `weather.alerts`, cada um com três partições; a criação automática está desativada agora. A Etapa 4 adicionará PyFlink e os conectores Kafka/JDBC. Etapas posteriores implementarão ingestão, processamento, persistência de negócio no schema `climate` e consumo de alertas. Nenhum desses tópicos, componentes ou tabelas está presente nesta fundação.
