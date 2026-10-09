# Etapa 4 — relatório de implementação

Status: build, bounded real e job operacional validados pelo principal. Restam a revisão final e a confirmação de regressão do Producer/documentação antes do aceite da etapa.

## Entrega

- `src/climate_logistics/weather_parser.py`: parser puro do schema v1 que reutiliza `WeatherEvent`, limita o corpo a 16 KiB, valida timestamps aware/UTC, campos obrigatórios e strings UTF-8, e converte erros conhecidos sem expor o payload. Exporta os 15 campos/tipos da Row, `event_values` e o resumo compacto.
- `src/climate_logistics/flink_job.py`: KafkaSource earliest, modo bounded com parada em latest, no-watermarks, parsing em Row explícita, métricas `parsed_records`/`invalid_records`, log de erro mínimo, PrintSink identificado, UIDs estáveis e CLI para nome/grupo/bootstrap. O contexto de métricas usa o método Python `get_metrics_group()` confirmado no runtime PyFlink 1.20.3.
- `docker/flink/Dockerfile`, `docker/flink/checksums.sha256`, `requirements-flink.txt` e `compose.yaml`: runtime compartilhado de Python 3.11.14/PyFlink 1.20.3/conector Kafka; processo continua como `flink`, sem credenciais PostgreSQL e com os limites existentes.
- `tests/unit/test_weather_parser.py`, `tests/unit/test_flink_job.py` e `tests/integration/test_kafka_flink.py`: cobertura pura do parser, regressão do contexto de métricas com `pytest.importorskip`, e integração opt-in no host usando Docker Compose exec, REST e Flink CLI, com identificação e cancelamento limitados ao job criado pelo teste.
- `README.md`: build, submit/list/cancel por JobID, slot único, replay earliest sem checkpoint e smoke bounded opt-in.

## Evidência de testes e execução

- RED parser antes da implementação: 15 falhas porque `weather_parser` não existia; GREEN inicial: 15 testes passaram.
- RED adicional de surrogate escapado: 2 falhas (`DID NOT RAISE`) para `city` e `location_id` no CPython 3.11.14. Após validar strings JSON decodificadas, o parser passou os 17 testes.
- A primeira submissão bounded real chegou ao worker e terminou `FAILED`: a função consultava `get_metric_group()`, nome singular ausente no `StreamingRuntimeContext` PyFlink 1.20.3. O RED focado da regressão confirmou `AttributeError`; após trocar para `get_metrics_group()`, o teste no runtime passou.
- Suíte offline completa no Python 3.11.14: `346 passed, 6 skipped`; `ruff check .`: `All checks passed!`. Os skips incluem a integração opt-in quando `FLINK_INTEGRATION` não está definido e o teste de métricas quando PyFlink não está instalado no host.
- O teste `tests/unit/test_flink_job.py` também passou no runtime atualizado Python 3.11.14/PyFlink 1.20.3 (`1 passed`).
- Integração bounded real após a correção: principal confirmou `1 passed` em 18,06 s, estado final `FINISHED`, JobID `9103762ed2189a31f26be8d67dfc58ed`. Os logs do TaskManager mostram duas ocorrências controladas de `invalid_records` e o resumo PrintSink completo da fixture `QA-dd125bc64b10`.
- Job operacional: `weather-kafka-ingestion`, JobID `4de3a42bec3d9d68a659f5cce2064247`, em `RUNNING`. O principal publicou `OP-1d7e85dee11d44be` com ACK, encontrou seu JSON completo no PrintSink às 11:20:23 UTC e confirmou source/parser/sink, paralelismo 1 e slot ocupado.
- Memória reportada pelo principal: TaskManager 571 MiB e JobManager 286 MiB, dentro dos limites existentes. UID de execução `flink`, sem variáveis PostgreSQL no serviço Flink; Producer confirmou três entregas normais após a troca dos nós.

## Revisão restante

O principal está concluindo a revisão final, a regressão do SDK Producer e a documentação/progresso da etapa. O aceite final permanece com essa revisão; a implementação já passou pelo build e pelo fluxo Flink/Kafka bounded e operacional.