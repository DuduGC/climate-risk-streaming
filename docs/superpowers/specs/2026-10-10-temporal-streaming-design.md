# Etapa 6 — Streaming temporal

Estado: desenho aprovado pelo usuário em 2026-10-10; plano preparado para revisão, implementação não iniciada.
Data: 2026-10-10. Base: baa6474, branch foundation.

## Objetivo e contexto

Gerar métricas temporais por localidade dentro do job Kafka/PyFlink existente, preservando validação e deduplicação da etapa 5. O pedido original determina Event Time, watermarks, keyBy, estado, janelas e agregações. Adotar os valores sugeridos: janela deslizante de30 minutos, slide5 minutos e desordem2 minutos. Acrescentar checkpoints para recuperação do estado/offsets, conforme continuidade técnica anunciada ao finalizar etapa5.

Implementadores GPT-6 Luna com esforço Max; principal revisa arquitetura e executa QA real. Não iniciar risk engine, tabelas de negócio, Kafka Sink ou alert-consumer, que pertencem às etapas7–9. Não adicionar serviços, bibliotecas ou infraestrutura cloud.

## Alternativas e escolha

1. DataStream com SlidingEventTimeWindows e AggregateFunction incremental: recomendado. Preserva o pipeline existente, estado gerenciado e testes SDK, sem manter listas de eventos.
2. ProcessFunction com timers e janelas manuais: exigiria implementar fechamento, sobreposição e limpeza que Flink já oferece; maior risco e código.
3. Table/SQL: viável, mas muda a representação/pipeline existente e aumenta a superfície de integração sem necessidade nesta etapa.

## Fluxo

KafkaSource → parsing/coerência temporal → deduplicação → atribuição de timestamps/watermarks → keyBy(location_id) → sliding window → agregado incremental → diagnóstico de métricas.

Manter o diagnóstico de ingestão existente em um ramo para não remover os testes das etapas4/5. Acrescentar PrintSink de métricas temporais. A saída fica provisoriamente em logs; persistência/alertas entram na etapa8.

## Event Time e watermarks

- Usar exclusivamente event_timestamp UTC do evento validado. Converter para milissegundos inteiros desde epoch por aritmética de datetime/timedelta, sem float; frações inferiores a1ms são truncadas na atribuição temporal, mas a identidade da deduplicação continua com a precisão original.
- Atribuir timestamps/watermarks depois de parsing e deduplicação, antes do novo keyBy. Eventos inválidos não podem avançar watermark.
- Bounded out-of-orderness120s, emissão periódica200ms e idleness60s. Paralelismo permanece1. Nesta topologia a atribuição ocorre após combinar as partições, portanto uma partição Kafka vazia não deve travar o watermark como aconteceria em atribuição no source por partição.
- Watermark é do fluxo, não individual por localidade. Uma localidade com relógio atrasado pode perder participação em janelas já fechadas; registrar esse limite.
- Idleness não faz o relógio avançar quando todo o fluxo está parado. Janelas finais aguardam novos eventos ou o watermark terminal de uma fonte bounded. Repetições descartadas não avançam o watermark; a frequência de novas medições Open-Meteo afeta a latência observada.

## Janelas e atrasados

- SlidingEventTimeWindows: tamanho1800000ms, avanço300000ms, alinhadas ao epoch UTC, intervalos[start,end). Cada evento participa de seis janelas.
- allowed_lateness=0: não reabrir janelas já finalizadas nem emitir revisões nessa etapa. Os120s de desordem do watermark não são uma configuração de allowed lateness.
- Um evento fora de ordem ainda participa das janelas abertas a que pertence. Se todas as suas seis janelas estiverem fechadas, encaminhar ao side output de atrasados, consumir esse ramo e incrementar temporal_stream.late_records, sem novo tópico nem log do payload completo.
- Métrica late_records conta eventos totalmente atrasados, não cada participação descartada; um evento parcialmente atrasado pode contribuir para as janelas ainda abertas. Documentar essa distinção.
- Estado de janela deve ser acumulador de tamanho constante por chave/janela, removido pelo lifecycle de Flink. Não aplicar TTL ao estado interno da janela de modo que expire antes do fechamento.

## Contrato das métricas

JSON com component=flink, operation=window_aggregate, schema_version=1 e os campos:

- location_id;
- window_start_ms e window_end_ms: inteiros de epoch UTC; fim exclusivo. Inteiros evitam overflow ao formatar uma fronteira além do ano9999 de um evento extremo aceito pelo schema;
- event_count: quantidade de identidades válidas deduplicadas que participaram;
- temperature_avg: média aritmética por observação, em°C, sem alegar média ponderada pelo tempo;
- precipitation_max: maior valor observado emmm; não representa precipitação acumulada da janela;
- precipitation_rate_max_mm_per_hour: máximo de precipitation×3600/measurement_interval_seconds, projeção da intensidade de cada observação, não acumulado medido de uma hora;
- wind_speed_max e wind_gust_max: km/h;
- visibility_min: metros;
- measurement_interval_min_seconds e measurement_interval_max_seconds: intervalo das observações para interpretar precipitação;
- first_event_timestamp_ms e last_event_timestamp_ms: extremos temporais das observações.

Chave lógica futura de métrica: location_id + window_start_ms + window_end_ms. Não incluir city/latitude/longitude escolhidos arbitrariamente pela ordem de chegada; a etapa8 poderá relacionar locations. Agregador guarda count, soma de temperatura, máximos/mínimos e extremos de tempo; não armazena eventos crus. Nenhum agregado vazio é emitido. Floats não devem ser arredondados durante acumulação; comparações em testes numéricos usam tolerância, extremos/contagem/fronteiras exigem igualdade.

## Checkpoints e recuperação local

- Intervalo60s, modo EXACTLY_ONCE para estado/offsets, timeout120s, pausa mínima10s, máximo1 checkpoint concorrente, tolerância0 a falhas persistentes de checkpoint.
- Restart strategy fixed delay: máximo3 tentativas com10s entre tentativas; após orçamento esgotado, falha visível. Sem retries infinitos.
- Backend heap existente; storage filesystem em file:///opt/flink/checkpoints. Named volume flink-checkpoints montado no mesmo caminho em JobManager e TaskManager.
- Criar diretório na imagem com owner flink antes da montagem, validar permissões efetivas no volume novo; manter USER flink. Não ampliar permissões do host, não montar Docker socket e não criar init-service permanente.
- Reter checkpoints externalizados no cancelamento para restauração explícita. Limitar checkpoints concluídos retidos a2; checkpoints de jobs antigos cancelados podem continuar ocupando disco, portanto documentar limpeza manual por JobID após confirmação do usuário. Nunca apagar automaticamente dados de outro job.
- Restaurar explicitamente com CLI flink run -s <checkpoint/_metadata>, preservando UIDs e contratos. Não usar allowNonRestoredState para mascarar incompatibilidade.
- Automatic task restart pode recuperar checkpoint; reinício completo do cluster standalone sem HA exige nova submissão/restauração manual. Não prometer autossubmissão após docker compose up.
- PrintSink não é transacional. Checkpoints não dão exactly-once ponta a ponta; logs podem repetir após recovery. Dedup TTL continua em processing time: tempo parado pode consumir seu horizonte. Teste recovery deve ocorrer dentro do TTL operacional.

## Arquivos e limites de mudança

Prever módulo puro temporal_metrics.py para acumulador/contrato, módulo flink_temporal.py para adaptadores SDK/timestamp/window e integração em flink_job.py. Ajustar compose.yaml e Dockerfile somente para storage/permissões. Testes puros, SDK e integração em arquivos próprios de etapa6; reutilizar helpers existentes sem remover seus casos. Atualizar README e relatórios. Sem novo pacote de terceiros, mudança de schema raw, recursos de CPU/memória ou paralelismo sem falha medida.

Flags QA existentes permanecem isoladas por job/group. Se um checkpoint interval reduzido for necessário nos testes, permitir5–60s apenas no modo QA correspondente; operacional continua60s. QA usa o mesmo tamanho/slide/watermark operacional, pois fixtures históricas conseguem avançar Event Time sem espera de30min. Não testar somente com janelas encolhidas.

## Aceite

1. Testes puros verificam média/extremos/contagem/intervalos, merge de acumuladores, timezone e precisão de timestamps, janela[start,end), entrada extrema, ausência de listas e imutabilidade dos eventos.
2. Testes SDK verificam assigner120s, sliding30/5, key correta, side output consumido, opções efetivas de checkpoint/restart e preservação de state.cache-size=0.
3. Suítes anteriores e Ruff aprovados em Python3.11; SDK executado na imagem PyFlink1.20.3 real. Skips não substituem integração.
4. Integração bounded com localidades exclusivas compara JSON completos das métricas, seis participações por evento e fronteiras, inclui desordem e cópia duplicada sem inflação. Fonte bounded fecha janelas via watermark terminal.
5. Integração unbounded comprova watermark real avançado, evento fora de ordem em janela ainda aberta, evento totalmente atrasado contado uma vez e ausência de reabertura. Publicações/sinais de avanço têm ACK e deadlines; não depender só de ordem publicada ou sleep fixo.
6. Checkpoint real COMPLETED confirmado pela REST e arquivos no volume. Recuperação explícita do mesmo grafo comprova marcador de dedup e janela parcialmente acumulada restaurados, offsets preservados e resultado esperado sem dupla contagem. Pode haver repetição de logs; distinguir isso do conteúdo do agregado.
7. Interromper somente job operacional conhecido durante QA; preservar JobIDs próprios, cleanup limitado a eles, restaurar único job operacional da etapa6. Confirmar métricas temporais reais, Producer funcionando, memória/estado/backpressure observados sob carga pequena e sem credenciais indevidas.
8. Revisão independente, scan de secrets, docs e commit local. Só então ETAPA6 CONCLUÍDA; etapa7 não iniciada.

## Fontes primárias

- Windows Flink1.20: https://nightlies.apache.org/flink/flink-docs-release-1.20/zh/docs/dev/datastream/operators/windows/
- Checkpointing Flink1.20: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/checkpointing/
- Configuration Flink1.20: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/config/
