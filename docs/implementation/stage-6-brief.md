# Stage 6 brief — aprovado

Spec e plano aprovados em 2026-10-10. Principal reserva coordenação de serviços, commits e QA real. Implementadores não devem ler .env, expandir compose config, criar subagentes ou alterar docs/progresso.md/ledger. Alterações autorizadas nos arquivos de sua tarefa; documentar relatórios em stage-6-task-N-report.md. Não implementar próxima tarefa antes do gate do principal.

# Etapa 6 — Streaming temporal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gerar métricas temporais por localidade e comprovar recuperação de estado e offsets no Kafka/PyFlink local.

**Architecture:** DataStream existente mantém validação/dedup e seu diagnóstico. Novo ramo atribui Event Time/watermarks, usa SlidingEventTimeWindows e agregador incremental; checkpoints usam filesystem em volume compartilhado.

**Tech Stack:** Python 3.11.14, PyFlink 1.20.3, Kafka 3.9.1, Docker Compose, Pytest 8.4.2 e Ruff 0.14.0 existentes.

**Spec:** docs/superpowers/specs/2026-10-10-temporal-streaming-design.md — aprovada pelo usuário em2026-10-10.

## Global Constraints

- Janela1800000ms, slide300000ms, desordem120000ms, emissão watermark200ms, idleness60000ms, allowed_lateness=0, paralelismo1.
- Preservar schema raw v1, Row15, validação/dedup e UIDs existentes; TTL86400 processing time, python.state.cache-size=0.
- Checkpoints60000ms, EXACTLY_ONCE de estado/offsets, timeout120000ms, pausa10000ms, concorrência1, tolerância de falhas0; fixed delay3 tentativas/10000ms.
- Storage filesystem file:///opt/flink/checkpoints, volume flink-checkpoints nos dois nós; USER flink; retenção2 checkpoints concluídos, externalizados retidos no cancelamento.
- QA mantém janelas/desordem operacionais; override de checkpoint5–60s só com identidade QA validada. Sem override por ambiente.
- Sem novos pacotes/tópicos/serviços, risk engine, sinks de negócio ou mudanças de recursos sem causa medida. Não apagar volumes/dados/checkpoints alheios.
- Implementadores GPT-6 Luna Max; execução sequencial, principal faz revisão e QA; etapa7 não iniciada.

## Review Focus

1. Partições vazias e fluxo todo ocioso: atribuição depois de dedup não depende do mínimo por partição; nenhum fechamento artificial por relógio de parede.
2. Precisão extrema: microsegundos/UTC/ano9999 não passam por float, nem fronteiras de janela por datetime fora de faixa.
3. Chuva com intervalos diferentes: máximo/rate definidos por observação; não somar chuva nem alegar acumulado horário.
4. Atraso parcial: evento pode contribuir a algumas janelas; late_records conta somente evento totalmente atrasado, uma vez.
5. Recovery com offsets novos: restore deve consumir registro publicado depois do checkpoint mesmo usando QA latest, e preservar marcador e acumulador anteriores.

## Preparação — principal

- [ ] Confirmar base baa6474 e alterações apenas documentais; criar stage-6-brief.md e stage-6-progress.md com spec/plano/autorizações.
- [ ] Usar checkout dedicado existente como nas etapas anteriores; registrar ruling de isolamento. Ler skill using-git-worktrees na execução e não criar checkout redundante sem necessidade.
- [ ] Conferir APIs no SDK instalado antes de implementação: TimestampAssigner, aggregate(..., accumulator_type=..., output_type=...), ProcessWindowFunction, OutputTag, checkpoint/restart. API Java real é referência para regressões.
- [ ] Registrar jobs REST e estado do Producer; coordenar pausa apenas do operacional conhecido e do Producer conhecido durante QA. Revisor não repetirá Docker se depender de aprovação pendente; usar evidências do principal.

## Tarefa 1 — Agregados e janelas de Event Time

**Files:** criar src/climate_logistics/temporal_metrics.py e flink_temporal.py; modificar flink_job.py em build_pipeline; criar tests/unit/test_temporal_metrics.py, tests/unit/test_flink_temporal.py e tests/integration/test_flink_temporal.py; atualizar README.

**Interfaces:**
- Consome Sequence[object] na ordem WEATHER_FIELDS, já validada, via Row existente.
- temporal_metrics.timestamp_millis(timestamp: str) -> int: ISO aware para epoch UTC inteiro, truncando abaixo de1ms por timedelta.
- MetricAccumulator: dataclass frozen com11 escalares: event_count, temperature_sum, precipitation_max, precipitation_rate_max_mm_per_hour, wind_speed_max, wind_gust_max, visibility_min, measurement_interval_min_seconds, measurement_interval_max_seconds, first_event_timestamp_ms, last_event_timestamp_ms. Count/sum iniciam0; demais None no acumulador vazio.
- empty_accumulator() -> MetricAccumulator; add_observation(acc: MetricAccumulator, values: Sequence[object]) -> MetricAccumulator; merge_accumulators(left: MetricAccumulator, right: MetricAccumulator) -> MetricAccumulator; window_metric(acc: MetricAccumulator, *, location_id: str, start_ms: int, end_ms: int) -> dict[str, object]. Rejeitar finalização vazia; não repetir matriz de validação raw.
- window_metric produz exatamente campos/units do spec, soma/count para média, envelope component/operation/schema_version, sem temperatura_sum interna ou metadata arbitrária.
- flink_temporal.WeatherTimestampAssigner(TimestampAssigner), WeatherWindowAggregate(AggregateFunction), WeatherWindowResult(ProcessWindowFunction), CountLateWeather(MapFunction), que incrementa o contador e retorna True (BOOLEAN) para DiscardingSink; não serializa a Row descartada.
- attach_temporal_metrics(stream: DataStream) -> DataStream: liga atribuição/keyBy/window/side output e retorna JSON STRING de métricas; build_pipeline chama print('weather-metrics') no retorno.
- UIDs novos estáveis: weather-event-time, weather-temporal-windows, weather-late-counter, weather-late-discard, weather-metrics. Não renomear UIDs antigos.

- [ ] Escrever testes puros RED: duas observações temp20/30 dão média25/count2; chuva2mm/900s e1mm/300s dão max2/rate12mmh; máximos vento/rajada, mínima visibilidade, intervalos300/900, primeiro/último timestamp. Comparar floats com pytest.approx; não alterar entradas.
- [ ] Testar merge de acumuladores e identidade com vazio; estrutura tem11 escalares independentemente da contagem; empty finalization lança ValueError. Testar timestamp offset equivalente, .123999→123ms, epoch0 e datetime máximo→253402300799999ms.
- [ ] Executar pytest tests/unit/test_temporal_metrics.py -q, observar RED por contrato ausente; implementar módulo puro mínimo e repetir até GREEN.
- [ ] Escrever SDK RED: TimestampAssigner usa campo event_timestamp; SlidingEventTimeWindows real atribui seis janelas a timestamp0: starts[-1500000,-1200000,-900000,-600000,-300000,0], ends=starts+1800000; timestamp300000 pertence ao novo conjunto, não à janela cujo fim300000. Conferir dimensões1800000/300000, watermark120000, interval200 e idleness60000.
- [ ] Implementar adaptadores usando APIs SDK, aggregate com accumulator_type=Types.PICKLED_BYTE_ARRAY() e output_type=Types.STRING(); get_result retorna acumulador, ProcessWindowFunction só recebe o acumulador e fronteiras. Resultado JSON determinístico (allow_nan=False); sem lista de eventos/window apply que bufferize todos.
- [ ] Ligar WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(120)).with_timestamp_assigner(...).with_idleness(Duration.of_seconds(60)) depois de dedup; keyBy localização; SlidingEventTimeWindows.of(Time.minutes(30),Time.minutes(5)); allowed_lateness0.
- [ ] Definir OutputTag('weather-temporal-late', tipo Row existente), side_output_late_data e consumir ramo com counter temporal_stream.late_records e DiscardingSink, sem outro tópico. SDK testa contador e saída mínima/controlada; não interpretar parse accepted como contribuição a toda janela.
- [ ] Escrever integração bounded RED, preservando tests/integration/test_kafka_flink.py. Reutilizar helpers por import e pytest config existentes; novos helpers de CLI recebem args extras/restore, sem refactor grande. Jobs/grupos exclusivos, captura de JobID antes de espera e cleanup em finally só próprios.
- [ ] Fixture bounded usa base=floor(now_utc_ms/300000)*300000 (não ano2020 para esse caso, porque earliest lê backlog atual). Publicar A embase+90000ms, B embase+30000ms, cópiaA com ingestão revisada e C emoutra localidade. Timestamps/ingestões respeitam5min; primeiras janelas terminam embase+300000, portanto backlog anterior não deve fechá-las. Calcular oracle de seis participações por evento e comparar JSON completos por identidade; média tolerante. FINISHED/terminal watermark fecha todas; duplicata não infla count.
- [ ] Integração unbounded QA/latest usa fixture histórica, Producer pausado pelo principal. Confirmar source RUNNING e offsets das partições antes de publicar. Publicar A em12:10, B em12:09 (fora de ordem dentro120s); avanço em12:43 (watermark próximo de12:41) fecha todas as janelasA/B, cujo último fim é12:40. Aguardar métrica/watermark real antes de publicar C em12:15: parcialmente atrasado, ainda contribui a janelas abertas. Avançar para13:20, aguardar fechamento, então publicar D em12:09:30: totalmente atrasado. Exigir incremento late_records1, nenhuma nova/revisada saída das janelas fechadas, e resultados exatos das janelas ainda abertas deC. Sentinel emoutra localidade; preservar ing histórica coerente.
- [ ] Testar fluxo ocioso sem gerar métricas por passagem de wall clock: eventos de12:10 não fecham a última janela apenas por idle. Usar REST watermark e saídas como evidência; esperas com deadlines, não sleep de30min. Partições sem fixture não impedem o fechamento após sentinel.
- [ ] Executar suíte pura Python3.11.14, SDK na imagem final, Ruff e integração temporal. Principal rebuild/recria apenas quando source mudou; coordenar um slot. Registrar RED/GREEN, contagens e limitações no relatório tarefa1.
- [ ] README documenta campo/units, participação emseis janelas, atraso parcial/global, desordem≠allowed lateness, latência Open-Meteo e ociosidade. Principal/revisor aprovam tarefa antes de prosseguir; commit local somente arquivos desta tarefa+docs acordados.

## Tarefa 2 — Storage, checkpoints e recuperação

**Files:** modificar compose.yaml, docker/flink/Dockerfile, flink_job.py; acrescentar configure_checkpointing em flink_temporal.py; ampliar tests/unit/test_flink_temporal.py e tests/integration/test_flink_temporal.py; atualizar README.

**Interfaces:**
- configure_checkpointing(env: StreamExecutionEnvironment, *, interval_seconds: int = 60) -> None.
- JobSettings acrescenta checkpoint_interval_seconds: int = 60. CLI --qa-checkpoint-interval-seconds (5..60). Valores bool/não inteiros recusados; custom só com qa_audit=True e nome/grupo exclusivo na validação execute existente.
- Não adicionar argumento Python para restore: usar CLI Flink run -s <metadata> -py ... com os mesmos args/UIDs; não usar allowNonRestoredState.

- [ ] Escrever SDK RED para valores efetivos de checkpoints/restart/storage/retention e QA identity. Operacional60000ms, timeout120000, pausa10000, concurrency1, EXACTLY_ONCE, tolerable_failures0, RETAIN_ON_CANCELLATION; rejeitar custom em operacional/mismatch/valores fora5..60.
- [ ] Implementar configurações usando Configuration+env.configure para execution.checkpointing.storage=filesystem, execution.checkpointing.dir=file:///opt/flink/checkpoints, state.checkpoints.num-retained=2; API checkpoint para interval/mode/timeout/pause/concurrency/failures/external retention. RestartStrategies.fixed_delay_restart(3,Time.seconds(10)). Preservar state.cache-size0 e defaults do bundle operacional.
- [ ] Adicionar diretório /opt/flink/checkpoints na imagem com owner flink e volume flink-checkpoints em ambos os nós, sem serviço novo. Executar compose config --quiet sem imprimir configuração expandida e rebuild; confirmar writer/readers uid9999 conseguem criar/ler pequeno marcador no novo volume. Remover apenas esse marcador técnico próprio após teste, nunca diretório/volume inteiro.
- [ ] Executar SDK no runtime e suíte anterior para detectar opções ignoradas/signaturas erradas. Confirmar metadata/checkpoint storage efetivo, não só objeto mock.
- [ ] Escrever recuperação RED: QA/latest, checkpoint_interval10s e dedup TTL operacional86400; Producer pausado. Publicar A de2020 numa janela aberta, esperar aceiteaudit e checkpoint COMPLETED disparado depois do aceite; guardar external_path/_metadata, id, timestamps e offsets do source. Verificar arquivo do path capturado existe nos dois nós.
- [ ] Após capturar checkpointC, publicar B novo na mesma janela e confirmar ACK. Cancelar somente job próprio antes deC ser removido pela retenção2 (deadline menor que20s); restaurar explicitamente C com mesmo grafo/grupo e novo JobID, mesmo quando há checkpoint mais recente. Verificar path após cancel; se orçamento falhar, teste falha/reinicia com evidência, não escolhe um checkpoint que já contémB.
- [ ] Depois do restore, publicar cópiaA revisada e sentinel avançando Event Time40min emoutra localidade. Exigir duplicate audit deA, resultado da janela com A+B/count2, extremos/média esperados; isso prova tanto acumulador/marcador restaurado quanto leitura deB publicado depois deC, apesar de initializer QA/latest. Nenhum resultadocount1/3 para essa janela.
- [ ] Confirmar REST restored-checkpoint no novo job referenciaC e pelo menos um checkpoint novo COMPLETED. Separar logs por horário/JobID/localidade; repetição de PrintSink não será alegada exatamente uma vez. Capturar metadata e contagem de arquivos sem logs de secrets.
- [ ] Garantir cleanup finally cancela apenas ids QA capturados, preserva checkpoints para inspeção. Pausar/reiniciar TaskManager só se necessário para prova adicional, não simular falha do cluster inteiro para um teste que só precisa restore explícito.
- [ ] README documenta restore CLI semallowNonRestoredState, retenção2 não limita diretórios dejobs antigos, limpeza manual autorizada, restart limitado, ausência de HA/autossubmissão e PrintSink não transacional. Registrar tarefa2/QA; revisão antes decommit.

## Verificação final — principal

- [ ] Revisão de branch/diff completa por revisor independente; corrigir com implementador da tarefa, sem ampliar escopo.
- [ ] Suíte geral Python3.11, SDK1.20.3, Ruff, build e integrações antigas+temporais+checkpoint aprovados; nenhuma integração skipped contabilizada como aceite.
- [ ] Durante QA cancelar apenas operacional conhecido da etapa5; registrar a pausa do Producer e garantir restart em finally, inclusive se teste falhar. Testes unbounded devem recusar fontes concorrentes que invalidem a fixture histórica.
- [ ] Submeter job operacional etapa6, TTL86400/checkpoint60/janelas30/5/desordem2, grupo operacional; aguardar checkpoint COMPLETED e métricas por localidade. Confirmar único jobRUNNING e ciclos reais Producer.
- [ ] Verificar UID/permissões/ausência de credenciais PostgreSQL, recursos e backpressure emcarga pequena; nada de benchmark fictício ou aumento sem medição.
- [ ] Scan secrets (sem imprimir senhas), .env ignorado, git diff/index check, docs/spec/plano/ledger consistentes e commit local; sem push. Reportar ETAPA6 CONCLUÍDA só após todos os critérios. Etapa7 fica pendente.

## Revisão do plano e decisões de execução

Spec coberto nas tarefas1/2; contratos compartilhados definidos no módulo puro e adaptadores. APIs aggregate e configuração de checkpoint conferidas na documentação oficial1.20; confirmação SDK obrigatória na execução. Interface set_checkpoint_storage é deprecated desde1.19: usar Configuration conforme recomendado.

Ruling QA: pausar Producer conhecido para histórico unbounded/recovery e usar baseUTC atual no bounded earliest. Motivo: watermark global pode avançar com dados normais ou backlog; custo: pausa reversível durante QA, restaurada em finally. Isso não muda a política operacional nem adiciona filtros por locality.

Método já escolhido pelo usuário: agentes implementadores GPT-6 Luna Max, revisão/QA principal e revisor independente. Duas tarefas dependentes executadas sequencialmente; não iniciar múltiplos agentes escrevendo nos mesmos arquivos. Plano aprovado pelo usuário em 2026-10-10; execução iniciada.

Referências: [aggregate PyFlink1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/api/python/reference/pyflink.datastream/api/pyflink.datastream.data_stream.WindowedStream.aggregate.html), [checkpoint storage PyFlink1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/api/python/reference/pyflink.datastream/api/pyflink.datastream.checkpoint_config.CheckpointConfig.set_checkpoint_storage.html), fontes de windows/checkpoints do spec.
