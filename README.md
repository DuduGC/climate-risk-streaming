# Climate Logistics: monitoramento climático em streaming

Este MVP acompanha previsões meteorológicas de três localidades e aplica regras para identificar risco climático em operações logísticas. O Producer consulta a Open-Meteo, o Flink valida e agrega os eventos, o PostgreSQL guarda as avaliações e o Kafka entrega alertas HIGH e CRITICAL a um consumer que escreve logs JSON.

Consulte também a [Arquitetura](docs/arquitetura.md) e o [Progresso](docs/progresso.md), com o fluxo, os contratos e o histórico das etapas.

## Status do projeto

MVP finalizado. Concluí as dez etapas entre 2026-10-07 e 2026-10-12. O fechamento registrado lista 695 testes aprovados, 31 testes opcionais ou dependentes do SDK ignorados, Ruff e builds locais das imagens aprovados. Também passaram os comandos locais equivalentes aos jobs de qualidade, integração Python/Kafka e SDK Flink. O GitHub Actions ainda não foi executado porque o repositório não tem remoto configurado.

## Funcionalidades

O MVP reúne estas funções:

- Coletar previsões de três localidades e publicar eventos validados em `weather.raw`.
- Validar horários, eliminar duplicatas por 24 horas e agregar métricas em janelas de Event Time.
- Classificar o risco de cada janela e gravar avaliações e alertas no PostgreSQL.
- Publicar alertas HIGH e CRITICAL em `weather.alerts` e registrá-los em logs JSON.

### Coleta e eventos

O Producer consulta RJ01, SP01 e CT01 em sequência, a cada 300 segundos, e publica eventos válidos em `weather.raw`. Ele usa dados do bloco `current` da Open-Meteo, que são previsões de modelo, não medições de estações. A requisição usa UTC e timestamp Unix; o evento serializa os horários em ISO 8601 UTC terminado em `Z`.

O contrato `WeatherEvent` v1 valida 15 campos: `location_id`, `city`, `latitude`, `longitude`, `temperature`, `precipitation`, `wind_speed`, `wind_gust`, `visibility`, `weather_code`, `measurement_interval_seconds`, `event_timestamp`, `ingestion_timestamp`, `source` e `schema_version`. Temperatura usa °C; precipitação, mm; vento, km/h; visibilidade, m; e o intervalo da observação, de 1 a 3600 segundos. A precipitação é a quantidade informada para o intervalo da fonte.

O cliente HTTP valida TLS, não segue redirecionamentos e limita cada resposta a 128 KiB descomprimidos. Ele tenta até três vezes em erros transitórios de conexão, HTTP 429 e HTTP 500/502/503/504. Em HTTP 429, um `Retry-After` numérico é limitado a cinco segundos; se o cabeçalho não for válido, o cliente usa o backoff local. Os timeouts são de 3 segundos para conexão e 10 segundos de inatividade de leitura; eles não limitam a duração total da operação. Um erro em uma localidade não impede a tentativa nas outras, mas deixa o ciclo parcial.

Os tópicos `weather.raw` e `weather.alerts` têm três partições e replicação 1. O bootstrap os cria se estiverem ausentes e interrompe a inicialização se encontrar uma topologia incompatível. O Producer publica apenas em `weather.raw`; `weather.alerts` recebe a saída do Flink. A chave dos dois tópicos é `location_id` em UTF-8. O evento em `weather.raw` é JSON compacto, limitado a 16 KiB.

### Validação, deduplicação e janelas

O Flink valida o schema e a coerência dos horários em UTC. Os timestamps precisam ser posteriores ou iguais ao início Unix; o evento pode ficar até cinco minutos à frente da ingestão, a ingestão até cinco minutos à frente do relógio observado, e a diferença entre ingestão e evento pode chegar a 24 horas. A deduplicação usa `location_id` e `event_timestamp` normalizado em UTC. A primeira ocorrência válida vence; uma repetição não renova o TTL de 24 horas em processing time. Depois desse prazo, o mesmo evento pode ser aceito novamente.

As métricas usam `event_timestamp` como Event Time. O watermark tolera 120 segundos de desordem e marca uma partição ociosa após 60 segundos. As janelas são de 30 minutos, avançam a cada 5 minutos e seguem intervalos `[início, fim)` alinhados ao epoch UTC. Cada evento pode participar de seis janelas por localidade. `allowed_lateness` é zero: um evento ainda pode entrar nas janelas abertas, mas não reabre as que já fecharam. O fluxo inteiro parado não fecha as janelas pelo relógio de parede; a emissão depende de novos eventos e do avanço do watermark.

Cada janela agrega contagem, média aritmética de temperatura e extremos de vento, rajada e visibilidade. `precipitation_max` é o maior valor observado, não a soma da chuva na janela. A projeção `precipitation × 3600 / measurement_interval_seconds`, em mm/h, também não é uma medição acumulada de uma hora. Os extremos podem vir de observações distintas.

### Avaliação de risco

A versão de regras é `mvp-1`. Cada fator recebe 0, 25, 50, 75 ou 100, e o maior score define o resultado; os fatores não são somados.

| Fator | Unidade | Score 25 | Score 50 | Score 75 | Score 100 |
| --- | --- | --- | --- | --- | --- |
| Projeção máxima de precipitação | mm/h | >= 2 | >= 20 | >= 30 | >= 60 |
| Velocidade máxima do vento | km/h | >= 20 | >= 40 | >= 60 | >= 100 |
| Rajada máxima | km/h | >= 30 | >= 50 | >= 80 | >= 120 |
| Visibilidade mínima | m | < 5000 | < 2000 | < 1000 | < 200 |

Os limites de chuva e vento são inclusivos; os de visibilidade são estritos. As faixas são LOW (0–24), MEDIUM (25–49), HIGH (50–74) e CRITICAL (75–100). Vento de 40 km/h resulta em 50/HIGH; visibilidade de 199 m resulta em 100/CRITICAL. O score é uma classificação ordinal e heurística, não uma probabilidade de prejuízo.

### Persistência e alertas

O Flink usa uma instrução SQL com UPSERT para gravar cada avaliação em `climate.weather_metrics`. Para scores a partir de 50, a mesma instrução também registra o alerta em `climate.weather_alerts`; essas tabelas ficam no schema `climate`. A identidade da avaliação combina localidade, início e fim da janela e versão das regras. O `alert_id` é um SHA-256 dessa identidade serializada em JSON compacto UTF-8.

Somente alertas HIGH e CRITICAL seguem para `weather.alerts`. A mensagem tem 22 campos e inclui `alert_id`. O sink Kafka usa `AT_LEAST_ONCE`, `acks=all` e tentativas limitadas; uma recuperação pode publicar novamente a mesma identidade. O consumer valida o contrato, a chave e o catálogo de localidades; depois escreve e descarrega o log JSON antes de confirmar o offset. Um grupo novo começa no high watermark atual (`latest`); grupos existentes retomam offsets confirmados. Logs de mensagens inválidas registram o motivo, sem incluir o payload bruto.

O consumer guarda até 10.000 IDs em um cache FIFO na memória. Esse cache não sobrevive a reinícios. PostgreSQL converge replays pelas chaves do UPSERT; Kafka e o consumer podem repetir alertas ou logs. Os checkpoints usam `EXACTLY_ONCE` para o estado Flink e os offsets do source, mas os sinks não participam de uma transação distribuída. Não há garantia exactly-once ponta a ponta.

### Limites operacionais

O ambiente usa um broker Kafka e um slot Flink, sem alta disponibilidade ou submissão automática do job após reinício do JobManager. Kafka usa PLAINTEXT e replicação 1, sem TLS, SASL ou ACLs. A API REST do Flink não tem autenticação e fica limitada ao loopback do host. Kafka e PostgreSQL não publicam portas no host. As métricas de PrintSink são diagnósticas e temporárias; não há uma API de negócio nem uma interface própria. O projeto também não tem benchmark de throughput.

O healthcheck confirma que um serviço está pronto. Ele não confirma que o job Flink foi submetido. O consumer pode estar saudável sem mensagens: um alerta só aparece quando uma janela fecha com risco HIGH ou CRITICAL. Essa saída também depende das observações recebidas e do watermark. Producer e consumer rodam como UID 10001; Flink roda como usuário `flink`. As credenciais PostgreSQL da aplicação chegam ao JobManager; os containers Python não recebem credenciais do banco.

## Tecnologias utilizadas

| Tecnologia | Versão | Uso |
| --- | --- | --- |
| Python | 3.11.14 | Producer, consumer e pacote do projeto |
| Apache Flink / PyFlink | 1.20.3 | Validação, janelas, checkpoints e sinks |
| Java | 17 | Runtime dos conectores Flink |
| Apache Kafka | 3.9.1 | Tópicos de eventos e alertas |
| PostgreSQL | 17.11 | Catálogo e tabelas de negócio |
| requests | 2.34.2 | Cliente HTTP da Open-Meteo |
| confluent-kafka | 2.12.2 | Cliente Kafka em Python |
| pytest | 8.4.2 | Testes |
| Ruff | 0.14.0 | Verificação de estilo e erros comuns |
| Docker Compose | v2 | Ambiente local com seis serviços |
| Open-Meteo | API pública | Previsões meteorológicas do bloco `current` |
| Git | sem versão fixa | Controle de versão do projeto |
| GitHub Actions | — | CI para qualidade, Kafka e SDK Flink |

As versões das dependências Python estão fixadas em `requirements.txt`. O projeto constrói duas imagens locais: Producer/consumer compartilham a imagem Python, e JobManager/TaskManager compartilham a imagem Flink.

## Pré-requisitos e instalação

### Pré-requisitos

Use Docker Engine com containers Linux e Docker Compose v2, ou Docker Desktop configurado para containers Linux. Reserve pelo menos 6 GiB de memória e cerca de 4 vCPUs para o Docker. Os limites dos seis containers somam 4,375 GiB, além dos recursos do sistema e do Docker.

| Serviço | Limite do container | Observação |
| --- | --- | --- |
| Kafka | 1 GiB; heap de 512 MiB | Broker e controller KRaft em um nó |
| Producer | 256 MiB; 0,5 vCPU | Sem porta própria |
| Flink JobManager | 1 GiB; processo de 768 MiB | REST em `127.0.0.1:8081` |
| Flink TaskManager | 1,5 GiB; processo de 1280 MiB | Um slot; paralelismo padrão 1 |
| PostgreSQL | 512 MiB | Acesso pela rede Docker |
| Consumer de alertas | 128 MiB; 0,25 vCPU | Logs JSON em stdout |

Python 3.11.14 é necessário para desenvolver e rodar comandos de qualidade localmente. Java 17 integra a imagem Flink e é necessário para o runtime PyFlink.

### Preparar as variáveis locais

Se ainda não existir um arquivo `.env`, crie-o a partir do modelo. O comando preserva um `.env` existente:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

No Linux:

```sh
test -f .env || cp .env.example .env
```

Preencha localmente `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `APP_DB_USER` e `APP_DB_PASSWORD`. O modelo já traz os nomes usados nos exemplos e deixa as senhas vazias. O Compose aplica as credenciais iniciais quando o volume `postgres-data` está vazio; alterar uma senha no `.env` depois disso não muda a senha de uma role já criada. Faça essa alteração dentro do banco com uma sessão administrativa. As configurações públicas opcionais usam estes defaults: `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` para Producer e consumer, `WEATHER_POLL_INTERVAL_SECONDS=300` para o intervalo de coleta e `ALERT_CONSUMER_GROUP_ID=weather-alerts-consumer` para o grupo do consumer.

### Instalar o subconjunto Python local

Para os comandos locais de desenvolvimento e qualidade, crie um ambiente virtual e instale o subconjunto fixado pelo arquivo de constraints:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -c requirements.txt requests confluent-kafka pytest ruff
python -m pytest -q
python -m ruff check src tests
```

No Linux, use:

```sh
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -c requirements.txt requests confluent-kafka pytest ruff
python -m pytest -q
python -m ruff check src tests
```

Esse é o conjunto padrão de qualidade, offline; o smoke da API pública é opt-in. `requirements.txt` também inclui PyFlink, que depende do runtime Java. Use a imagem do projeto para os testes do SDK Flink, em vez de instalar PyFlink por acidente nesse ambiente local.

As verificações que dependem de serviço ou API externa são opt-in: `OPEN_METEO_LIVE`, `FLINK_INTEGRATION`, `ALERT_CONSUMER_INTEGRATION`, `FLINK_BUSINESS_TEST` e `FLINK_BUSINESS_FAULT_TEST`. Antes dos cenários Flink, confira `/jobs/overview`, o slot único e o estado do Producer. Alguns cenários exigem pausar o Producer conhecido. Cada teste encerra apenas o JobID que criou; não cancele jobs de outra execução.

Os flags `--qa-audit`, `--qa-dedup-ttl-seconds` e `--qa-checkpoint-interval-seconds` são exclusivos de jobs QA: use nome e grupo próprios no formato `weather-flink-qa-<id>` e `weather-flink-qa-<id>-group`. Os overrides aceitam TTL de 2 a 60 segundos e checkpoint de 5 a 60 segundos. Testes de sink que usam `--qa-business-sinks` também precisam de `--qa-audit`. Essas opções não alteram os valores do job operacional: TTL de 86.400 segundos e checkpoint de 60 segundos.

### Construir as imagens e iniciar os serviços

O Compose inicia seis containers e constrói as duas imagens do projeto:

```powershell
docker compose build producer flink-jobmanager
docker compose up -d --wait --wait-timeout 180
docker compose ps
```

Espere Kafka, JobManager, TaskManager, PostgreSQL e o consumer ficarem `healthy`. O Producer fica `running`; as entregas aparecem nos logs. Kafka retém dados por 24 horas e até 1 GiB por partição, com segmentos de 256 MiB; como a remoção ocorre em segmentos fechados, 1 GiB não é um teto imediato.

## Como usar

### Conferir o cluster e o job Flink

O endpoint REST de monitoramento do Flink fica em `http://127.0.0.1:8081`. Use as rotas `/overview` e `/jobs/overview` para consultar o cluster e os jobs:

```powershell
Invoke-RestMethod http://127.0.0.1:8081/overview | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8081/jobs/overview | Select-Object -ExpandProperty jobs | Format-Table name,state,jid
```

Antes de submeter, consulte `/jobs/overview`. Se `weather-kafka-ingestion` já estiver `RUNNING`, acompanhe essa execução e não envie outro job para o único slot. Se não houver job ativo, confira se é a primeira submissão ou se precisa restaurar um checkpoint; `healthy` no JobManager e no TaskManager confirma a prontidão dos serviços, não a existência de um job ativo.

Na primeira execução, quando não há job ativo nem checkpoint a restaurar, submeta o job como usuário `flink`:

```powershell
docker compose exec -T -u flink flink-jobmanager /opt/flink/bin/flink run -d -py /opt/climate/src/climate_logistics/flink_job.py --job-name weather-kafka-ingestion --group-id weather-flink-ingestion
```

O source usa o grupo `weather-flink-ingestion`, não faz auto-commit e começa em `earliest`. Uma submissão nova sem checkpoint pode reler registros ainda retidos em `weather.raw`. Depois da submissão, confira novamente `/jobs/overview`.

Para consultar o job e os logs do TaskManager, use o JobID atual obtido na resposta REST:

```powershell
$jobId = "<jid-que-voce-identificou>"
Invoke-RestMethod "http://127.0.0.1:8081/jobs/$jobId" | ConvertTo-Json
docker compose logs --tail 100 flink-taskmanager
```

### Ver eventos e logs

Para acompanhar os ciclos do Producer e as mensagens processadas pelo consumer:

```powershell
docker compose logs -f producer
docker compose logs -f alert-consumer
```

Um ciclo único do Producer consulta as três localidades e retorna sucesso somente quando cada entrega foi confirmada pelo broker:

```powershell
docker compose run --rm --no-deps producer python -m climate_logistics.producer --once
```

Para conferir a topologia dos tópicos:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic weather.raw
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic weather.alerts
```

Para inspecionar uma mensagem nova de `weather.raw` na partição 0, inicie o consumer temporário e execute `--once` em outra janela:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic weather.raw --partition 0 --offset latest --timeout-ms 5000 --max-messages 1
```

### Consultar métricas e alertas no PostgreSQL

O PostgreSQL não publica uma porta no host. Abra uma sessão TCP dentro do container com a role de aplicação. O exemplo usa os nomes padrão do modelo; `-W` solicita a senha sem colocá-la no comando:

```powershell
docker compose exec -it postgres psql -h 127.0.0.1 -U climate_app -d climate_logistics -W
```

No prompt `psql`, confira a conexão e consulte as últimas avaliações e alertas:

```sql
SELECT current_user, current_database();
SELECT has_schema_privilege(current_user, 'climate', 'USAGE') AS acesso_ao_schema;

SELECT location_id, window_start_ms, window_end_ms, event_count, risk_score, risk_level
FROM climate.weather_metrics
ORDER BY window_end_ms DESC
LIMIT 10;

SELECT alert_id, location_id, window_start_ms, window_end_ms, risk_score, risk_level
FROM climate.weather_alerts
ORDER BY window_end_ms DESC
LIMIT 10;
```

Saia com `\q`. Se os nomes do banco ou da role forem diferentes no `.env`, ajuste `-d` e `-U` no comando de conexão. A role de aplicação pode ler o catálogo e consultar, inserir e atualizar fatos; ela não tem privilégios administrativos.

A ausência de mensagens recentes em `weather.alerts` não indica por si só falha do consumer. Alertas dependem de uma avaliação HIGH ou CRITICAL e da conclusão de uma janela pelo watermark.

### Retomar um job por checkpoint

O job salva checkpoints a cada 60 segundos no volume `flink-checkpoints`. Eles preservam o estado e os offsets do source, não tornam os sinks transacionais. Até dois checkpoints concluídos ficam retidos por job; snapshots externalizados podem continuar no volume após o cancelamento, então a limpeza é manual.

Confira `/jobs/overview` e só restaure se não houver job operacional ativo. Enquanto o job estiver ativo, consulte `/jobs/<jid>/checkpoints` e registre o `external_path` do último checkpoint concluído. Após reinício do JobManager, o job pode não aparecer na API; localize o checkpoint completo no volume `flink-checkpoints`. Confirme que o arquivo `_metadata` existe.

```powershell
$jobId = "<jid-atual>"
Invoke-RestMethod "http://127.0.0.1:8081/jobs/$jobId/checkpoints" | ConvertTo-Json -Depth 8
```

Depois de escolher um checkpoint completo, defina o caminho até `_metadata` e submeta o job restaurado. Após reinício do JobManager, use o caminho persistido no volume.

```powershell
$metadata = "file:///opt/flink/checkpoints/<diretorio-do-job>/chk-<checkpoint-concluido>/_metadata"
docker compose exec -T -u flink flink-jobmanager /opt/flink/bin/flink run -d -s $metadata -py /opt/climate/src/climate_logistics/flink_job.py --job-name weather-kafka-ingestion --group-id weather-flink-ingestion
```

Substitua o caminho de exemplo pelo `external_path` do checkpoint completo escolhido. Preserve o grupo, o nome e os UIDs do grafo; não use `--allowNonRestoredState`. Reiniciar os containers não submete o job automaticamente.

### Coletas de 12 e 13 de outubro de 2026

Este snapshot foi registrado em 13/10/2026. Os horários locais estão em BRT (UTC-3), e as datas abaixo seguem o horário dos eventos.

| Data dos eventos | Avaliações adicionadas ao banco | Por localidade | Alertas adicionados ao banco |
| --- | ---: | --- | ---: |
| 2026-10-12 | 21 | 7 por localidade | 7, todos de CT01 (CRITICAL, score 75) |
| 2026-10-13 | 6 | 2 por localidade | 0 |

Esse recorte mostra as linhas adicionadas durante a execução de 13/10, não o total diário. A retomada persistiu as 21 avaliações referentes a 12/10; por isso, os sete alertas pertencem a essa data. As seis avaliações de 13/10 não geraram alertas.

| Tabela PostgreSQL | Antes | Depois | Diferença |
| --- | ---: | ---: | ---: |
| `climate.weather_metrics` | 465 | 492 | +27 |
| `climate.weather_alerts` | 86 | 93 | +7 |

Os totais das tabelas incluem registros históricos e de testes, portanto não representam contagens diárias.

#### Ciclos do Producer em 13/10

Dois ciclos manuais `--once` confirmaram três entregas cada, uma para RJ01, SP01 e CT01.

| Horário dos eventos (BRT) | Entregas confirmadas |
| --- | ---: |
| 10:45 | 3 de 3 |
| 11:00 | 3 de 3 |

A amostra de métricas termina na janela 10:25–10:55, com evento às 10:45. O timestamp de 11:00 do segundo ciclo ainda não aparece nessas linhas.

#### Últimas janelas fechadas

Em 12/10, a última janela foi 19:15–19:45 BRT e contém uma observação, às 19:15:

| Cidade (ID) | Temperatura média (°C) | Precipitação máxima (mm) | Vento máximo (km/h) | Rajada máxima (km/h) | Visibilidade mínima (m) | Score | Nível |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Rio de Janeiro (RJ01) | 23,4 | 0 | 8,4 | 22,3 | 21.440 | 0 | LOW |
| São Paulo (SP01) | 19,6 | 0 | 6,3 | 14,4 | 11.240 | 0 | LOW |
| Curitiba (CT01) | 15,6 | 0 | 11,3 | 28,1 | 240 | 75 | CRITICAL |

Em 13/10, a última janela foi 10:25–10:55 BRT e contém uma observação, às 10:45:

| Cidade (ID) | Temperatura média (°C) | Precipitação máxima (mm) | Vento máximo (km/h) | Rajada máxima (km/h) | Visibilidade mínima (m) | Score | Nível |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Rio de Janeiro (RJ01) | 25,9 | 0 | 11,8 | 31,7 | 17.140 | 25 | MEDIUM |
| São Paulo (SP01) | 24,1 | 0 | 9,0 | 24,8 | 28.020 | 0 | LOW |
| Curitiba (CT01) | 22,9 | 0 | 9,9 | 25,6 | 31.400 | 0 | LOW |

Na consulta deste snapshot, os sete IDs novos apareceram nos logs de recebimento do consumer; o job estava `RUNNING`, no checkpoint 313, sem falhas de checkpoint registradas.

### Parar os serviços

Para parar os containers e preservar os volumes:

```powershell
docker compose stop
```

Os volumes `kafka-data`, `postgres-data` e `flink-checkpoints` mantêm os dados entre reinícios dos containers.
