# Etapa 2 — Open-Meteo

Status: proposta para revisão. Etapa 1 concluída; repositório limpo no início desta etapa.

## Objetivo e escopo

Obter dados atuais válidos da Open-Meteo, normalizar para contrato estável e comprovar comportamento com Pytest. Implementadores GPT-6 Luna Max; principal coordena, revisa e verifica. Sem producer Kafka, topics, job Flink, sinks ou containers de aplicação nesta etapa.

## Fonte confirmada

Endpoint HTTPS fixo: https://api.open-meteo.com/v1/forecast. Solicitar current=temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m,visibility,weather_code, unidades Celsius/mm/kmh, timezone=UTC e timeformat=unixtime.

Consulta pública real em 2026-10-07 para Rio de Janeiro retornou seis campos e unidades esperadas, timestamp Unix e interval=900 segundos. Dados são derivados de modelos meteorológicos, não observações de estação. Timestamp indica validade do dado. Precipitação é acumulada no intervalo da fonte; preservar esse período. Não combinar variáveis de horários diferentes.

## Abordagem

Cliente HTTP síncrono Requests, modelos dataclass da biblioteca padrão e normalização pura. Fixar versão HTTP no plano. Sem SDK, cache, biblioteca de schemas ou concorrência nesta etapa. urllib reduziria dependências mas aumentaria código de tratamento HTTP; async não se justifica para três localidades.

## Localidades e evento

Configuração versionada: RJ01 Rio de Janeiro (-22.90,-43.17), SP01 São Paulo (-23.55,-46.63), CT01 Curitiba (-25.43,-49.27). Validar IDs únicos, nomes não vazios, latitude [-90,90] e longitude [-180,180]. Coordenadas do evento vêm dessa configuração; a API pode devolver coordenadas da célula do modelo.

Evento: location_id, city, latitude, longitude, temperature, precipitation, wind_speed, wind_gust, visibility, weather_code, event_timestamp e ingestion_timestamp. Acrescentar source=open-meteo, schema_version=1 e measurement_interval_seconds para procedência/período da precipitação.

Serialização ISO 8601 UTC com Z. event_timestamp vem de current.time; ingestion_timestamp é capturado após recebimento. Relógio explícito na normalização permite testes reproduzíveis. Não derivar horário do evento do relógio local.

Rejeitar bools numéricos, null, NaN/infinito, campos ausentes, timestamps inválidos e unidades incompatíveis. Limites técnicos: temperatura [-100,100] Celsius, precipitação [0,1000] mm, vento/rajada [0,500] km/h, visibilidade [0,1000000] m, intervalo inteiro positivo até 3600 segundos. Weather code deve pertencer aos códigos WMO documentados pela Open-Meteo. Limites são validação de entrada, sem regras de risco antecipadas.

## HTTP e erros

- Timeouts conexão 3s/leitura 10s; três tentativas totais; backoff 0,5s e 1s.
- Retry só em timeout/conexão transitória, 429 e 500/502/503/504. Sem retry para 4xx permanentes, JSON ou schema inválido.
- Em 429, respeitar Retry-After numérico válido limitado a 5s; caso inválido/ausente, usar backoff padrão. Sem espera ilimitada.
- Ler resposta com teto de 128 KiB, sem confiar apenas no Content-Length; fechar conexões também em erro.
- Não seguir redirects; TLS verificado e endpoint sem controle por entrada externa.
- Erros explícitos com contexto de operação/localidade; sem despejar resposta inteira/secrets. Não ocultar falha ou substituir dado faltante por zero.

Um evento por localidade/chamada. Polling e tratamento entre localidades entram no Producer da Etapa 3. Identidade futura de deduplicação permanece location_id + event_timestamp.

## Testes e aceite

Pytest offline: mapeamento/unidades, UTC, intervalo, IDs/coordenadas, campos ausentes/null/bool/não finitos, ranges, códigos WMO e JSON inválido. HTTP contra servidor local controlado ou transporte injetado: timeout, tentativas/backoff, 429, erro permanente, resposta grande e fechamento de recursos. Mocks não serão única evidência HTTP.

Smoke live separado, opt-in, para localidade real com validação/serialização. Suíte offline não acessa rede pública. Indisponibilidade da fonte deve ser registrada como pendência de integração. Ruff e suíte inteira em Python 3.11 devem passar com casos reais e contagem registrada.

Aceite: dados reais normalizados, suíte offline e retry limitado comprovados, revisão sem achados impeditivos, README atualizado com campos/unidades/semântica e comando de verificação. Registrar progresso e então reportar ETAPA 2 CONCLUÍDA.

## Arquivos e delegação

- src/climate_logistics/models.py: modelos e validação do contrato.
- src/climate_logistics/locations.py: configuração das localidades.
- src/climate_logistics/open_meteo.py: normalização e cliente HTTP.
- requirements.txt: dependência HTTP fixada.
- tests/unit e tests/integration: offline e smoke live separados.
- README.md e docs/progresso.md: operação e evidências.

Delegação sequencial: Luna Max implementa modelos/localidades/normalização/testes; segundo Luna Max implementa HTTP/testes após contrato revisado. Principal revisa e verifica. Plano após aprovação define interfaces e ownership exatos.

## Fonte oficial

https://open-meteo.com/en/docs — current, unidades, timestamps e intervalo.
