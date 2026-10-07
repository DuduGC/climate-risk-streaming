# Etapa 2 / Tarefa 2

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


## Tarefa 2

**Interfaces:**
- open_meteo.py: OpenMeteoError(RuntimeError); utc_now() -> datetime retorna datetime.now(UTC).
- OpenMeteoClient.__init__(*, session: requests.Session | None = None, sleeper: Callable[[float], None] = time.sleep, clock: Callable[[], datetime] = utc_now).
- OpenMeteoClient.fetch(location: Location) -> WeatherEvent; close() -> None; context manager.
- Cliente possui e fecha Session criada internamente; não fecha Session injetada pelo chamador. Respostas sempre fechadas.
- Endpoint de produção é constante de módulo OPEN_METEO_URL. Teste local substitui a constante via monkeypatch; não adicionar argumento URL público ou configuração por input externo.
- Sessão própria com trust_env=False para não anexar autenticação .netrc; TLS permanece padrão/verificado. Sem retry automático de adapter além do loop explícito.
- Erro de contrato continua WeatherValidationError; falha HTTP/JSON/tamanho torna-se OpenMeteoError sem conteúdo completo da resposta.

- [ ] Escrever testes red antes do cliente; requirements.txt recebe somente requests==2.34.2 (dependência direta).
- [ ] Unitários com transporte injetado: parâmetros exatos, timeout (3,10), stream=True, allow_redirects=False; captura clock após corpo; resposta/Session fechadas segundo ownership.
- [ ] Retry: 503,503,200 resulta em três requests e sleeps [0.5,1.0]; falha contínua para na terceira sem sleep adicional. Timeout e conexão transitória seguem mesma regra.
- [ ] 429 com Retry-After=2 espera2; valor100 limita5; ausente/não numérico/negativo usa backoff. Não interpretar HTTP-date nesta versão; documentar fallback.
- [ ] Parametrizar 400/401/403/404, 301/302, SSLError, JSON inválido e schema inválido: uma tentativa, sem sleep; exceções com contexto limitado/localidade.
- [ ] Ler iter_content em chunks pequenos, contando bytes descomprimidos. Limite exato aceito se JSON válido; byte seguinte falha. Empty body, UTF-8 inválido e JSON não objeto falham sem retry.
- [ ] Capturar timeout/conexão também durante leitura; fechar response antes de backoff. Não tratar SSLError como falha transitória genérica.
- [ ] Servidor local ThreadingHTTPServer no loopback/porta efêmera, teardown em finally: JSON válido, 503 seguido200, 429 seguido200, 400, redirect não seguido, corpo grande sem Content-Length e gzip grande. Validar eventos e contagem de requests observados no servidor; não somente contagem de mocks.
- [ ] Simular resposta lenta no servidor com timeout curto apenas no transporte de teste; comprovar a classificação/finitude sem esperar 30s nem alterar defaults de produção.
- [ ] Implementar cliente mínimo preservando contrato Tarefa1; não polling, paralelismo, Kafka ou regras de risco.
- [ ] Criar smoke test marker live que só roda com OPEN_METEO_LIVE=1; teste no padrão usa skip explícito. Fazer consulta RJ01 real e validar contrato/serialização com intervalo informado.
- [ ] README: instalação de requirements.txt e requirements-dev, uso do context manager em chamada única, comandos offline/live, campos/unidades/modelos/intervalo e limitações. Requests timeouts são conexão/inatividade de leitura, não garantia de prazo total absoluto.
- [ ] Rodar suíte inteira offline e Ruff; principal faz review/testes/live e solicita correções antes do aceite.
