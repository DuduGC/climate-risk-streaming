# Etapa 2 / Tarefa 1

## Global Constraints

- Implementadores gpt-6-luna, reasoning_effort=max.
- Localidades RJ01 (-22.90,-43.17), SP01 (-23.55,-46.63), CT01 (-25.43,-49.27).
- Endpoint HTTPS fixo https://api.open-meteo.com/v1/forecast, sem redirects, verificação TLS habilitada.
- Seis campos current: temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m,visibility,weather_code.
- Unidades Celsius/mm/kmh, timezone=UTC, timeformat=unixtime.
- Timeout conexão/leitura (3,10) segundos; três tentativas totais; backoff 0,5/1 segundo.
- Retry apenas timeout/conexão transitória, 429 e 500/502/503/504; Retry-After numérico limitado a 5 segundos.
- Resposta limitada a 131072 bytes descomprimidos; sem confiar só no Content-Length.
- Temperatura [-100,100], precipitação [0,1000], vento/rajada [0,500], visibilidade [0,1000000].
- Intervalo inteiro 1..3600; não aceitar bool/null/NaN/infinito nem converter strings numéricas silenciosamente.
- Sem componentes Kafka, Flink, bancos ou novas tecnologias na Etapa 2. Preservar infraestrutura.
- TDD para lógica; testes offline por padrão, live opt-in. Erros nunca substituem dados por zero.
- Requests 2.34.2 confirmado no PyPI, compatível com Python >=3.10: https://pypi.org/project/requests/2.34.2/.

## Review Focus

1. Unix timestamp bool/fora do domínio ou datetime sem timezone: falhar com erro de contrato, sem UTC implícito.
2. Coordenadas de célula retornadas pela API: identidade/coordenadas configuradas devem permanecer no evento.
3. Resposta sem Content-Length ou gzip grande: contar bytes recebidos/descomprimidos e fechar recurso ao ultrapassar limite.
4. HTTP não transitório, redirect ou TLS inválido: erro explícito sem retry/follow-up indevido.
5. Retry-After inválido/negativo/muito alto e erro durante leitura: espera/tentativas limitadas e resposta fechada antes de repetir.


## Tarefa 1

**Interfaces:**
- models.py: WeatherValidationError(ValueError); dataclass frozen Location(location_id: str, city: str, latitude: float, longitude: float).
- models.py: dataclass frozen WeatherEvent com campos da spec; event_timestamp/ingestion_timestamp são datetime aware; source e schema_version fixos; measurement_interval_seconds inteiro.
- WeatherEvent campos exatos: location_id/city str; latitude/longitude/temperature/precipitation/wind_speed/wind_gust/visibility float; weather_code/measurement_interval_seconds int; event_timestamp/ingestion_timestamp datetime; source str fixo open-meteo e schema_version int fixo 1. Validar também construção direta, não apenas payload.
- WeatherEvent.to_dict() -> dict[str, object]: ISO UTC/Z nos timestamps, escalares JSON nos demais campos.
- locations.py: LOCATIONS: tuple[Location, ...]; validate_locations(locations: Sequence[Location]) -> None, rejeitando duplicação e lista vazia.
- open_meteo.py: normalize_current(payload: Mapping[str, object], location: Location, *, ingestion_timestamp: datetime) -> WeatherEvent.
- Fixture manual mínima com current/current_units, sem respostas volumosas ou dados privados. epoch 1791379200 corresponde a 2026-10-07T13:20:00Z, confirmado via DateTimeOffset antes da revisão do plano.

- [ ] Escrever testes antes da implementação e executar para comprovar falha por comportamento ausente.
- [ ] Criar testes de Location: IDs/nome vazios, tipos errados, coordenadas bool/não finitas/fora do intervalo, duplicate IDs e configuração inicial exata.
- [ ] Criar fixture com time=1791379200, interval=900, temperatura27.4, precipitação18.2, vento42.1, rajada58.3, visibilidade7000, weather_code61 e unidades exatas.
- [ ] Testar normalização com ingestão datetime(2026,10,7,13,20,2,tzinfo=UTC); assert source=="open-meteo", schema_version==1, measurement_interval_seconds==900, event_timestamp != ingestion_timestamp e coord/ID da Location.
- [ ] Testar to_dict: timestamps terminam Z, json.dumps(..., allow_nan=False) funciona; offsets aware convertem UTC; datetime naive é rejeitado.
- [ ] Parametrizar ausentes/null/bool/string/NaN/infinito/unidades erradas e limites inclusivos para os seis campos. Testar interval 0/3601/True/900.5 e time True/null/NaN/overflow.
- [ ] Weather code válido é inteiro no conjunto {0,1,2,3,45,48,51,53,55,56,57,61,63,65,66,67,71,73,75,77,80,81,82,85,86,95,96,99}; rejeitar 4, bool, fração.
- [ ] Implementar mínimo para esses testes. Não depender de Requests nesta tarefa e não realizar rede.
- [ ] Rodar python -m pytest e python -m ruff check .; registrar número de casos/resultados e ciclo red/green no relatório.
- [ ] Principal revisa contrato, erros, UTC e schema; solicitar correções ao mesmo agente antes da Tarefa 2.
