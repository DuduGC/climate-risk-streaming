# Etapa 5 — Data Quality: plano

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Rejeitar eventos inválidos e emitir uma ocorrência por identidade válida enquanto o TTL estiver vigente.
**Architecture:** Uma entrega GPT-6 Luna Max acrescenta política temporal pura, keyBy de localidade e MapState TTL ao job existente. Principal executa QA real e um revisor independente verifica a mudança.
**Tech Stack:** Python 3.11.14, PyFlink 1.20.3, Kafka/Compose e ferramentas existentes; nenhuma dependência nova.
**Spec:** docs/superpowers/specs/2026-10-09-data-quality-design.md (aprovada).

## Global Constraints

- Implementador gpt-6-luna, reasoning_effort=max; sem subagentes próprios.
- Preservar schema 1/Row de 15 campos, Producer, parser/modelos e todos os 346 testes anteriores.
- Tempos >=1970-01-01 UTC; event <= ingestion+5min; ingestion <= observed_at+5min; ingestion-event <=24h; limites inclusivos.
- Não limitar idade por relógio de processamento para replay; não alterar timestamps ou preencher dados ausentes.
- Dedup: location_id + instante event_timestamp UTC; ingestion_timestamp fora da identidade; primeira válida vence inclusive payloads revisados.
- MapState gerenciado por localidade com chave timestamp STRING/valor BOOLEAN; TTL de 86400 s processing time, OnCreateAndWrite, NeverReturnExpired; duplicado não escreve/renova.
- Cleanup incremental(10,True), backend heap; sem dict/set global nem varredura integral por registro. Não prometer cap rígido de memória/remoção física imediata.
- Estado/keyBy aqui são para dedup; sem Event Time/watermarks/janelas/checkpoint/sinks/novo topic ou serviço.
- Sem checkpoint, restart perde marcadores; após TTL um evento pode voltar a ser aceito. Diagnóstico de sucesso compatível com Etapa4.

## Review Focus

1. Inválido ou relógio defeituoso: não gravar marcador nem esconder bug de relógio como dado ruim.
2. Offset ISO equivalente/ingestão diferente: mesma identidade; localidade ou instante diferentes independentes.
3. TTL renovado em duplicado/cache retornando expirado: leitura sem put; NeverReturnExpired e prova no backend real.
4. Lote histórico e QA: usar identidade exclusiva/clock explícito; não rejeitar replay por início tardio do job.
5. Prova fraca/cleanup: contar saída exata e cancelar somente jobs identificados, sem deixar slot ocupado por teste ou afetar grupos alheios.

## Arquivos e interfaces

Implementador: src/climate_logistics/data_quality.py, flink_job.py; tests/unit/test_data_quality.py, test_flink_job.py; tests/integration/test_kafka_flink.py; README.md.
Principal: docs/progresso.md e docs/implementation/stage-5-* para brief/ledger/report/review. Imagem/cache e recursos preservados inicialmente.

- DataQualityError(ValueError) com reason:str controlada.
- validate_event_time(event: WeatherEvent, *, observed_at: datetime) -> None; não muta event. Clock naive/inconvertível é ValueError de configuração, não DataQualityError.
- event_identity(event: WeatherEvent) -> tuple[str,str]: location_id e event.to_dict()['event_timestamp'] UTC/Z.
- Constantes EPOCH=datetime(1970,1,1,tzinfo=UTC), FUTURE_TOLERANCE=timedelta(minutes=5), MAX_INGESTION_DELAY=timedelta(hours=24), DEFAULT_DEDUP_TTL_SECONDS=86400.
- Razões: timestamp_before_epoch, event_after_ingestion, ingestion_in_future, excessive_ingestion_delay; ordem das verificações documentada.
- JobSettings acrescenta dedup_ttl_seconds=86400 e qa_audit=False; somente QA pode mudar TTL.
- Operador _DeduplicateWeather(KeyedProcessFunction) usa MapStateDescriptor weather_seen_event_times, Types.STRING()/BOOLEAN(), enable_time_to_live(config). process_element recebe Row existente e retorna somente primeira válida.
- group de métricas data_quality: temporal_rejected_records, duplicate_records, accepted_records. invalid_records existente também inclui temporal inválido; parsed_records permanece parse bem-sucedido antes da política.

## Preparação — principal

- [x] Confirmar base 0adc4f6 e branch foundation e aprovação do plano; criar brief/ledger exclusivos.
- [x] Conferir jobs existentes e API TTL no runtime real (Builder.cleanup_incrementally/set_update_type/set_state_visibility e descriptor.enable_time_to_live confirmados na preparação).
- [x] Delegar uma tarefa coesa a Luna Max; reservar execução/rebuild/integração/revisão ao principal.

## Tarefa única — qualidade e deduplicação

- [x] TDD antes da lógica; observar RED e GREEN. Preservar testes existentes e reutilizar fixtures/modelos, sem repetir matriz de ranges.
- [x] Testar epoch/future5min/delay24h inclusivos e ultrapassagem por1microssegundo; timezone aware/offsets e clock naive; reason correta e event imutável.
- [x] Testar replay antigo com diferença event/ingestion válida; nenhuma regra baseada em now-event. Testar inválido seguido de válido com mesma identidade.
- [x] Implementar política pura. Wrapper parsing chama validação temporal antes de emitir Row; catch apenas DataQualityError/WeatherParseError, reason/log mínimo sem corpo. Clock bug propaga.
- [x] Fluxo parsed_stream.key_by(lambda row:row[location_index], key_type=Types.STRING()).process(_DeduplicateWeather(...), output_type=_row_type()); nomes/UIDs estáveis, diagnóstico após process.
- [x] open usa runtime_context.get_metrics_group(), get_map_state e TTL por entrada; validar APIs no runtime instalado, sem transpor nomes Java.
- [x] StateTtlConfig: Time.seconds(86400), OnCreateAndWrite/NeverReturnExpired, cleanup_incrementally(10,True); nunca desabilitar limpeza por default.
- [x] No process, verificar marker por timestamp UTC canônico; se presente, incrementar duplicate e não put/yield. Se ausente/expirado, put True, incrementar accepted e yield Row. Não armazenar payload.
- [x] Testes SDK com contexto/map fake estritos: TTL configurado/descritor, key correta, put único, duas identidades/locais, duplicado não renova; importorskip PyFlink para suíte leve, execução também no runtime real.
- [x] Métricas/QA audit: rotina operacional não loga INFO por duplicado; qa_audit emite JSON mínimo duplicate/rejected/accepted para prova controlada, sem payload completo. Counters contam decisões reais.
- [x] CLI somente QA: --qa-dedup-ttl-seconds(2..60) e --qa-audit exigem job-name weather-flink-qa-<id> e group-id com mesmo prefixo exclusivo; recusar uso no nome/group operacional. Default86400 sempre sem override de ambiente.
- [x] Para teste TTL determinístico, QA usa bundle.size1/time curto via configuração restrita à própria execução; não alterar política/bundle operacional global.
- [x] Atualizar integração bounded existente e acrescentar qualidade: IDs exclusivos; publicar JSON inválido, temporal inválido, válidoA, dupeA com ing distinta/payload revisto, válidoB outra localidade/mesmo instante e válidoC mesmo local/novo instante.
- [x] Confirmar FINISHED, saída completa uma vez para A/B/C, zero para inválidos; QA audit/counters confirmam rejeição e duplicata. Comparar identidade/JSON após timestamp do teste, não substring ou métricas de source somente.
- [x] TTL real: job QA unbounded com TTL curto (usar20s para margem de scheduling) e audit/bundle isolados. Publicar identidade válida e observar aceitação; duplicata antesexpiry deve ser descartada; republish após 20 s deve ser aceita. Fixture com timestamps antigos mas válidos demonstra processing time.
- [x] Provar ausência de refresh: programar dupe perto do fim do TTL e republish depois do deadline original, antes do deadline que haveria se leitura renovasse. Usar evidência audit/monotonic real e margem, não contar desde submit. Se orçamento de scheduling impedir prova, falhar/registrar pendência, não aceitar silenciosamente.
- [x] Orquestração host reutiliza stdlib/Pytest/Docker exec/REST da Etapa4; sem SDK/socket extra, sem apagar dados/grupos. Toda espera com deadline; cleanup usa JobID preservado e fallback bounded por nome exclusivo.
- [x] Nunca skip interpretado como integração aprovada por slot ocupado: principal coordena parada temporária somente do job operacional conhecido; depois restaura único job.
- [x] README: razões/métricas/limites, primeira válida vence, replay, TTL processing time/best effort e perda de marcadores em restart sem checkpoint. Documentar que keyBy/state já aparecem pela dedup, restante temporal Etapa 6.
- [x] Report stage-5-implementation-report.md: RED/GREEN/contagens/concerns, sem declarar complete porunit. Ruff/suíte leve Python 3.11 e SDK tests no runtime.

## Aceite — principal

- [x] Review independente inteiro, correções com mesmo implementador, sem refactors/deps fora do escopo.
- [x] Suíte leve inteira/SDK regressões e Ruff aprovados; build atualizado/reuso cache, nenhuma alteração de recursos sem causa medida.
- [x] Integração bounded de qualidade e TTL real aprovadas; counters/audit/PrintSink e cardinalidade documentados, não só estado RUNNING.
- [x] Deixar um job operacional com TTL86400 ativo; publicar duas cópias da mesma identidade nova e comprovar uma saída; Producer segue funcional.
- [x] Medir RSS/atividade de estado sob carga pequena; invalids não geram marker, nenhum histórico Python cresce sem TTL.
- [x] Verificar não-root, sem secrets/logs de payload; git diff --check/scan secrets, atualizar docs/spec/plano/ledger e commit local.
- [x] Reportar ETAPA 5 CONCLUÍDA após todos os critérios; Etapa 6 não iniciada.

## Revisão do plano

Spec coberta por tarefa única. API TTL conferida no runtime 1.20.3, riscos com testes associados; checkpoint e política Event Time não antecipados. Método já escolhido: GPT-6 Luna Max + revisão/QA. Plano aprovado em 2026-10-09; execução iniciada.


Validação final: suíte361 passed/15 skipped; SDK8 passed; Ruff aprovado; integrações reais3 passed/74,65 s; operacional RUNNINGTTL86400 com uma saída e incremento duplicate_records41→42. Ruling adicional: python.state.cache-size=0 em todos os jobs, confirmado após falha real de TTL; registro no spec/relatório. Revisão independente e fechamento Git registrados no ledger.
