# Etapa 2 — Open-Meteo: plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Obter e normalizar dados atuais da Open-Meteo com HTTP resiliente e testes reais.

**Architecture:** Modelos imutáveis e normalização pura formam o contrato; um cliente HTTP síncrono usa esse contrato. Duas tarefas sequenciais implementadas por Luna Max, revisadas pelo principal e por revisores independentes.

**Tech Stack:** Python 3.11, Requests 2.34.2, Pytest 8.4.2, Ruff 0.14.0, dataclasses padrão.

**Spec:** docs/superpowers/specs/2026-10-07-open-meteo-design.md (aprovada).

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

## Estrutura e ownership

| Tarefa | Arquivos |
| --- | --- |
| 1 | src/climate_logistics/models.py, locations.py, open_meteo.py (somente normalização); tests/unit/test_models.py, test_locations.py, test_normalization.py; tests/fixtures/open_meteo_current.json |
| 2 | open_meteo.py (adicionar cliente, preservar normalização), requirements.txt, tests/unit/test_open_meteo_client.py, tests/integration/test_open_meteo_http.py, test_open_meteo_live.py, README.md, pyproject.toml (somente marker live) |
| Principal | docs/progresso.md, briefs/relatórios/revisões, status deste plano/spec |

## Preparação — principal

- [x] Registrar aprovação da especificação e deste plano quando concedida; atualizar progresso sem antecipar aceite.
- [x] Confirmar branch foundation e estado limpo. Manter diretório/branch locais, conforme método já adotado, sem checkout redundante ou alterações Git globais.
- [x] Registrar ledger específico da Etapa 2 em docs/implementation/stage-2-progress.md; não sobrescrever relatórios da Etapa 1.
- [x] Extrair briefs com constraints/interfaces para cada agente; arquivos compartilhados apenas sequencialmente.
- [x] Preparar ambiente Python 3.11 (container já disponível), instalar requirements-dev; após Tarefa 2 instalar também requirements.txt.

## Tarefa 1 — Modelos, localidades e normalização

**Interfaces:**
- models.py: WeatherValidationError(ValueError); dataclass frozen Location(location_id: str, city: str, latitude: float, longitude: float).
- models.py: dataclass frozen WeatherEvent com campos da spec; event_timestamp/ingestion_timestamp são datetime aware; source e schema_version fixos; measurement_interval_seconds inteiro.
- WeatherEvent campos exatos: location_id/city str; latitude/longitude/temperature/precipitation/wind_speed/wind_gust/visibility float; weather_code/measurement_interval_seconds int; event_timestamp/ingestion_timestamp datetime; source str fixo open-meteo e schema_version int fixo 1. Validar também construção direta, não apenas payload.
- WeatherEvent.to_dict() -> dict[str, object]: ISO UTC/Z nos timestamps, escalares JSON nos demais campos.
- locations.py: LOCATIONS: tuple[Location, ...]; validate_locations(locations: Sequence[Location]) -> None, rejeitando duplicação e lista vazia.
- open_meteo.py: normalize_current(payload: Mapping[str, object], location: Location, *, ingestion_timestamp: datetime) -> WeatherEvent.
- Fixture manual mínima com current/current_units, sem respostas volumosas ou dados privados. epoch 1791379200 corresponde a 2026-10-07T13:20:00Z, confirmado via DateTimeOffset antes da revisão do plano.

- [x] Escrever testes antes da implementação e executar para comprovar falha por comportamento ausente.
- [x] Criar testes de Location: IDs/nome vazios, tipos errados, coordenadas bool/não finitas/fora do intervalo, duplicate IDs e configuração inicial exata.
- [x] Criar fixture com time=1791379200, interval=900, temperatura27.4, precipitação18.2, vento42.1, rajada58.3, visibilidade7000, weather_code61 e unidades exatas.
- [x] Testar normalização com ingestão datetime(2026,10,7,13,20,2,tzinfo=UTC); assert source=="open-meteo", schema_version==1, measurement_interval_seconds==900, event_timestamp != ingestion_timestamp e coord/ID da Location.
- [x] Testar to_dict: timestamps terminam Z, json.dumps(..., allow_nan=False) funciona; offsets aware convertem UTC; datetime naive é rejeitado.
- [x] Parametrizar ausentes/null/bool/string/NaN/infinito/unidades erradas e limites inclusivos para os seis campos. Testar interval 0/3601/True/900.5 e time True/null/NaN/overflow.
- [x] Weather code válido é inteiro no conjunto {0,1,2,3,45,48,51,53,55,56,57,61,63,65,66,67,71,73,75,77,80,81,82,85,86,95,96,99}; rejeitar 4, bool, fração.
- [x] Implementar mínimo para esses testes. Não depender de Requests nesta tarefa e não realizar rede.
- [x] Rodar python -m pytest e python -m ruff check .; registrar número de casos/resultados e ciclo red/green no relatório.
- [x] Principal revisa contrato, erros, UTC e schema; solicitar correções ao mesmo agente antes da Tarefa 2.

## Tarefa 2 — Cliente HTTP e integração

**Interfaces:**
- open_meteo.py: OpenMeteoError(RuntimeError); utc_now() -> datetime retorna datetime.now(UTC).
- OpenMeteoClient.__init__(*, session: requests.Session | None = None, sleeper: Callable[[float], None] = time.sleep, clock: Callable[[], datetime] = utc_now).
- OpenMeteoClient.fetch(location: Location) -> WeatherEvent; close() -> None; context manager.
- Cliente possui e fecha Session criada internamente; não fecha Session injetada pelo chamador. Respostas sempre fechadas.
- Endpoint de produção é constante de módulo OPEN_METEO_URL. Teste local substitui a constante via monkeypatch; não adicionar argumento URL público ou configuração por input externo.
- Sessão própria com trust_env=False para não anexar autenticação .netrc; TLS permanece padrão/verificado. Sem retry automático de adapter além do loop explícito.
- Erro de contrato continua WeatherValidationError; falha HTTP/JSON/tamanho torna-se OpenMeteoError sem conteúdo completo da resposta.

- [x] Escrever testes red antes do cliente; requirements.txt recebe somente requests==2.34.2 (dependência direta).
- [x] Unitários com transporte injetado: parâmetros exatos, timeout (3,10), stream=True, allow_redirects=False; captura clock após corpo; resposta/Session fechadas segundo ownership.
- [x] Retry: 503,503,200 resulta em três requests e sleeps [0.5,1.0]; falha contínua para na terceira sem sleep adicional. Timeout e conexão transitória seguem mesma regra.
- [x] 429 com Retry-After=2 espera2; valor100 limita5; ausente/não numérico/negativo usa backoff. Não interpretar HTTP-date nesta versão; documentar fallback.
- [x] Parametrizar 400/401/403/404, 301/302, SSLError, JSON inválido e schema inválido: uma tentativa, sem sleep; exceções com contexto limitado/localidade.
- [x] Ler iter_content em chunks pequenos, contando bytes descomprimidos. Limite exato aceito se JSON válido; byte seguinte falha. Empty body, UTF-8 inválido e JSON não objeto falham sem retry.
- [x] Capturar timeout/conexão também durante leitura; fechar response antes de backoff. Não tratar SSLError como falha transitória genérica.
- [x] Servidor local ThreadingHTTPServer no loopback/porta efêmera, teardown em finally: JSON válido, 503 seguido200, 429 seguido200, 400, redirect não seguido, corpo grande sem Content-Length e gzip grande. Validar eventos e contagem de requests observados no servidor; não somente contagem de mocks.
- [x] Simular resposta lenta no servidor com timeout curto apenas no transporte de teste; comprovar a classificação/finitude sem esperar 30s nem alterar defaults de produção.
- [x] Implementar cliente mínimo preservando contrato Tarefa1; não polling, paralelismo, Kafka ou regras de risco.
- [x] Criar smoke test marker live que só roda com OPEN_METEO_LIVE=1; teste no padrão usa skip explícito. Fazer consulta RJ01 real e validar contrato/serialização com intervalo informado.
- [x] README: instalação de requirements.txt e requirements-dev, uso do context manager em chamada única, comandos offline/live, campos/unidades/modelos/intervalo e limitações. Requests timeouts são conexão/inatividade de leitura, não garantia de prazo total absoluto.
- [x] Rodar suíte inteira offline e Ruff; principal faz review/testes/live e solicita correções antes do aceite.

## Verificação final e fechamento — principal

- [x] Revisão por tarefa e final com evidências, sem aceitar apenas declaração de agentes.
- [x] python -m ruff check . e python -m pytest em Python 3.11: exit0, contagem real, smoke skip esperado no offline.
- [x] OPEN_METEO_LIVE=1 python -m pytest tests/integration/test_open_meteo_live.py -m live: evento real normalizado, exit0. Se fonte indisponível, registrar pendência e não aprovar integração sem evidência.
- [x] Revisar limites, erro permanente, fechamento, ausência de secrets/logs excessivos, retries e compatibilidade futura do schema.
- [x] git diff --check; registrar progresso, relatórios e decisões. Commit local, sem remoto/push.
- [x] Reportar ETAPA 2 CONCLUÍDA no formato exigido somente após os critérios. Etapa 3 permanece fora deste plano.

## Revisão do plano

Spec coberta pelas duas tarefas; arquivo open_meteo.py compartilhado sequencialmente. Os cinco riscos têm testes específicos. Nenhum novo serviço da infraestrutura. Execução escolhida pelo usuário: agentes Luna Max; plano aprovado em 2026-10-07; execução iniciada.

## Resultado verificado

Execução concluída em2026-10-07. Principal:291passed/1skip offline, Ruff aprovado e1passed live emPython3.11.14. Revisões por tarefa/final aprovadas após as correções documentadas no progresso. Etapa3 não iniciada. Commit local inicial do contrato:bbaf1fd.
