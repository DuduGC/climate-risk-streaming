# Etapa 7 — Risk Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Calcular risco determinístico e explicável por janela/localidade e comprovar integração e compatibilidade de checkpoint com a etapa6.

**Architecture:** Módulo puro com inputs/resultados imutáveis e tabelas versionadas. MapFunction stateless acrescenta risco ao agregado JSON; ramos anteriores e todo o estado/UIDs permanecem.

**Tech Stack:** Python3.11.14, PyFlink1.20.3, Kafka3.9.1, Pytest8.4.2, Ruff0.14.0 e Docker Compose existentes.

**Spec:** docs/superpowers/specs/2026-10-10-risk-engine-design.md — aprovada pelo usuário em2026-10-10.

## Global Constraints

- risk_score=max dos quatro componentes; valores produzidos0/25/50/75/100; classificação LOW0–24, MEDIUM25–49, HIGH50–74, CRITICAL75–100.
- Chuva mm/h: >=2/20/30/60; vento km/h: >=20/40/60/100; rajada km/h: >=30/50/80/120; visibilidade m: <5000/2000/1000/200. Scores25/50/75/100 respectivamente.
- risk_rules_version=mvp-1, constantes imutáveis em código. Sem override de política por ambiente/CLI, relógio, RNG, rede ou ML no cálculo.
- JSON novo21 campos: original17 preservados, só operation vira risk_assessment; acrescentar risk_score/risk_level/risk_components/risk_rules_version. Ramo window_aggregate v1 original17 intacto.
- Não diluir extremo nem amplificar combinação moderada nesta versão; projeção de intensidade não é chuva acumulada/probabilidade de prejuízo.
- Preservar schema raw/Row15, UIDs existentes, dedupTTL86400/cache0, janela30/5, watermark2/idleness60/assignerLAST/latenessint0 e checkpoint60/timeout120/minpause10/concurrency1/restart3/10/retention2JM+TM.
- Novas UIDs weather-risk-assessment e weather-risk; somente contador risk_engine.assessed_windows, sem novo estado/tópico/serviço/pacote/alteração de recursos.
- Implementador GPT-6 Luna Max; principal/revisor independente validam. Checkout dedicado foundation, commits locais sem push; etapa8 não iniciada.

## Review Focus

1. Comparações no limiar exato e vizinhos de float: chuva/vento >=, visibilidade <, sem arredondar a entrada.
2. Null/bool/NaN/inf ou campo ausente: recusar visivelmente, nunca preencher zero e produzir LOW artificial.
3. Mutação/alias: avaliar e serializar não alteram input nem expõem um dict mutável dentro do resultado imutável.
4. Contrato errado ou reentrada de risk_assessment: envelope/tipo de mensagem inesperado falha; métrica original17 e versão preservadas.
5. Migração real: checkpoint criado pelo grafo etapa6 sem ramo risk; preservar estado/UIDs e validar novo risco após restore/replay, sem usar um checkpoint recém-criado pela etapa7 como substituto.

## Preparação e checkpoint legado — principal

- [ ] Confirmar base7ae024e e mudanças apenas documentais; criar stage-7-brief.md/ledger exclusivo com autorizações e preflight de interfaces.
- [ ] Confirmar baseline suíte geral/SDK/Ruff nos runtimes existentes, jobs/serviços e imagem etapa6. Reutilizar checkout dedicado conforme aprovação anterior; preservar .env/volumes.
- [ ] ANTES de rebuild/deploy/sourcewiring etapa7, capturar o checkpoint operacional etapa6 atual e cancelar somente seu JobID conhecido, conservando metadata para restauração. Se já parado, subir somente infraestrutura e submeter grafo etapa6 conhecido.
- [ ] Pausar Producer em try/finally durante QA histórico. Submeter QA etapa6/latest com nome/grupo exclusivo e checkpoint15s permitido (intervaloQA5–60 existente). Usar parser/state/janelas reais do código legado, sem ramo risk.
- [ ] Publicar quatro perfis em2020-01-01T12:10Z com ingestão original coerente, IDs exclusivos: LOW(precip0/vento5/rajada10/vis10000), MEDIUM(precip0.5mm/900s=2mmh, outros baixos), HIGH(vento40, chuva0/rajada10/vis10000), CRITICAL(vis199, chuva0/vento5/rajada10). Temp20/interval900 para todos; campos raw restantes válidos.
- [ ] Observar aceite dos quatro; escolher checkpoint COMPLETED disparado após todos os aceites, com janelas abertas. Guardar URI/_metadata, id, jobid, nome/grupo, perfis/IDs/payloads e parâmetros em manifesto JSON .venv/stage7-legacy-checkpoint.json, sem secrets.
- [ ] Confirmar metadata legível nos dois nós e marcador de grafo antigo (HEAD/imagem/comando sem ramo risk) no manifesto. Cancelar somente QA próprio, aguardando terminal para liberar o slot; preservar checkpoint. Restaurar Producer; se necessário manter operação temporária com imagem/grafo etapa6 e checkpoint operacional guardado durante a implementação.
- [ ] Capturar evidência/hash do código legado antes de qualquer implementação no job. Imagem antiga fica disponível até prova de migração; não remover caches/volumes nem chamar allowNonRestoredState.

## Tarefa única — motor puro, ramo Flink e provas

**Files:** criar src/climate_logistics/risk_engine.py e flink_risk.py; modificar flink_job.py apenas para ligar o ramo após temporal_stream; criar tests/unit/test_risk_engine.py, tests/unit/test_flink_risk_sdk.py e tests/integration/test_flink_risk.py; atualizar README. Se necessário, acrescentar somente constante WINDOW_METRIC_FIELDS em temporal_metrics.py para compartilhar o contrato17, sem alterar seu output/algoritmo. Não tocar Compose/Dockerfile/requirements.

**Interfaces:**
- RiskInputError(ValueError): erro de domínio/contrato interno.
- RiskInputs: dataclass frozen com precipitation_rate_max_mm_per_hour, wind_speed_max, wind_gust_max e visibility_min. Aceitar int/float finite>=0 (bool proibido), normalizar para float; conversão com overflow tratado como RiskInputError. Sem duplicar limites físicos raw de etapa5.
- RiskLevel(str,Enum): LOW/MEDIUM/HIGH/CRITICAL. RULES_VERSION='mvp-1'.
- RiskAssessment: dataclass frozen com precipitation_rate_score, wind_speed_score, wind_gust_score e visibility_score; propriedades read-only risk_score=max e risk_level=classify_score(risk_score), rules_version=RULES_VERSION. Método components_dict() retorna dict novo com chaves precipitation_rate/wind_speed/wind_gust/visibility.
- calculate_risk(inputs: RiskInputs) -> RiskAssessment; classify_score(score: int) -> RiskLevel (bool/tipo inválido/fora0–100 recusados).
- inputs_from_metric(metric: Mapping[str,object]) -> RiskInputs; enrich_metric(metric: Mapping[str,object]) -> dict[str,object]. Validar envelope component=flink/operation=window_aggregate/schema_version int1 não bool e contrato17 via chave compartilhada; verificar também location_id não vazio, event_count inteiro positivo, fronteiras inteiras end>start e first/last inteiros dentro de[start,end), sem bool; preservar/copiar campos e acrescentar exatamente4 novos. Ausência/unknown schema/risk reentrada lança RiskInputError.
- flink_risk.AssessWindowRisk(MapFunction): open recebe runtime_context.get_metrics_group(), grupo risk_engine/counter assessed_windows. map(raw: str) -> str faz json.loads, enrich e dumps allow_nan=False; só então inc e return. Erros internos propagam; não capturar genericamente para emitir defaults.
- attach_risk_assessment(stream: DataStream) -> DataStream com output_type=Types.STRING(), name/uid weather-risk-assessment; build_pipeline mantém weather-metrics e liga print('weather-risk').name/uid weather-risk.

- [ ] Escrever RED dos unitários antes de código: todos16 limiares exatos +math.nextafter imediatamente abaixo/acima; usar inputs isolados para identificar cada score do componente e não esconder erro por max de outro fator.
- [ ] Cobrir baseline0LOW; chuva2→25MEDIUM; vento40→50HIGH; rajada80→75CRITICAL; vis199→100CRITICAL; vis200→75 e1000→50. Empates e dois fatores50→50HIGH; extremo isolado100 com demais0 mantém100.
- [ ] Parametrizar classificação0/24/25/49/50/74/75/99/100; invalid -1/101/True/25.0/'25' recusados. Inputs bool/None/string/NaN/inf/negativo e inteiro enorme que não cabe float recusados com erro controlado, sem saídaLOW.
- [ ] Testar monotonicidade com valores ordenados em cada fator (visibilidade inversa); determinismo de reavaliação, frozen dataclasses e alteração do dict serializado sem alterar RiskAssessment/input. Não acrescentar ferramenta property-testing externa.
- [ ] Observar RED em Python3.11, implementar tabelas/validação/funções puras mínimas e repetir GREEN. Não arredondar ou aplicar epsilon nos limiares.
- [ ] Helper contract tests: envelope/keys exatos, schema bool/2, JSONtipo lista e reentrada operation=risk_assessment recusados; event_count0/True e identidade/fronteiras inconsistentes recusados; input17 intacto, output21 exato e scorescomponentes/versão corretos. Compartilhar WINDOW_METRIC_FIELDS se necessário em vez de duplicar definições de schema independentes.
- [ ] Escrever SDK RED com contexto métrico estrito e callback real: assessed_windows incrementa uma vez por output válido, nenhuma vez em JSONmalformado/contratoinválido/fatorNaN. Validar erro por dado não usado que impeça dumps allow_nan=False também sem contador falso.
- [ ] Implementar adapter e wiring; não criar imports circulares. SDK graph testa três ramos/sinks e UIDs anteriores, nova saídaSTRING, cache0/assignerLAST/checkpoint settings preservados.
- [ ] Executar focused pure/SDK GREEN nos runtimes3.11/PyFlink1.20.3; testes SDK exigem imagem com bindPYTHONPATH atual para evitar source stale. Implementador envia comandos ao principal caso Docker/sandbox exija aprovação, sem espera silenciosa.
- [ ] Integração bounded Kafka comIDs exclusivos e baseUTCfloor5min atual para earliest/backlog. Quatro perfis produzem LOW/MEDIUM/HIGH/CRITICAL em seis janelas cada. Publicar cópia raw revisada extrema deLOW com mesmo instante/ingestão diferente; dedup mantémLOW/count1. Fonte bounded/terminal watermark fecha janelas; comparar multiconjuntoJSON completo21 campos e componentes/ruleversion; original17window_aggregate permanece.
- [ ] Novo arquivo de integração reutiliza helpers públicos de QA existentes por import local, sem mudar coleta pytest/global e sem duplicar scripts de publisher. SDK filename _sdk evita colisão basename. Publicador one-off do serviço existente; ACK obrigatório, IDs próprios/holder anteswait, finally cleanup apenas próprios e espera de terminal para liberar o slot.
- [ ] Restaurar MANIFESTOLEGADO etapa6, same job/group e novo JobID, imagem etapa7 após build atualizado; CLI -s <metadata>, semallowNonRestoredState. Snapshot C deve existir; manifest creation e ausência do risk no grafo origem precisam de evidência, não fixture gerada pela etapa7.
- [ ] Producer pausado em try/finally. Depois de restore, publicar cópias revisadas dos quatro eventos já marcados: exigir auditoria duplicate, não inflação nem troca da avaliação. Publicar sentinel2020T12:50Z emoutra localidade, esperar watermark max-delay-1 viaREST e fechamento das janelas restauradas. Comparar métricas17 e avaliações21 dos quatro perfis com oracle independente hardcoded/tabela aprovada, sem usar calculate_risk para construir o esperado.
- [ ] REST latest.restored id/URI aponta ao checkpoint6 legado; exigir pelo menosum checkpoint7 novoCOMPLETED e mesmo score/identidade sob replay. Campo assessed_windows é estatística e não contador exatamenteuma vez; verificar registro com deadline, nunca ausência=0.
- [ ] SDK/unitários/temporais/legados e casos novos aprovados, Ruff0.14.0 e compile/JSONserializer pertinentes. Não contabilizar skip por slot ocupado como PASS; coordenar execução e guardar log durável em .venv para retomada.
- [ ] README descreve tabela/unidades/comparações, score ordinal/coarse/max policy, ausência de amplificação/simultaneidade e versão, objeto21, preservação de métricas17, replay/PrintSink e contador. Escrever stage-7-implementation-report.md com RED/GREEN/contagens/problemas e provas do principal claramente atribuídas.
- [ ] Revisão de spec/código por revisor independente; corrigir com o implementador e scoped rereview. Principal confirma runtime/build/JSONcomparação/checkpointlegado antes de commit local.

## Aceite final — principal

- [ ] Whole-branch review em diff imutável, riscos concretos corrigidos; se modelo de revisão indisponível, usar mais capaz disponível com decisão registrada. Implementador continuaLunaMax.
- [ ] Baseline371/SDK31 daetapa6 preservado e suites ampliadas aprovadas; fonteJSON/controlada reproduzquatroclasses e migração6→7real. Skips não contam como integração.
- [ ] Deixar únicojob operacional etapa7, grupo padrão, mesmaspolíticas stateful, com métricas+risco reais de localidades echeckpoint60sCOMPLETED. Producerfuncional e IDs/metadata necessários registrados.
- [ ] Observar memória/backpressure/state sob carga pequena, UID9999/semPGcredentials noFlink, sem aumento de recurso sem medição. Score explicável e sem fonteinventada para contornarfalhaAPI.
- [ ] Scan de valores de credenciais sem imprimir, .env ignorado, diff/indexcheck, ledger/progresso/spec/plano atualizados e commitslocais; sempush. Evidências em stage-7-acceptance.md. Só então ETAPA7CONCLUÍDA; etapa8 pendente.

## Self-review e execução

Spec coberta pela tarefa única e preparação legada do principal. Cinco ReviewFocus possuem testes definidos. Todas funções/campos/tipos compartilhados definidos; plan não implementa algoritmo em texto duplicado. Política é a tabela aprovada, sem pesos ou interação adicionais. Ruling técnico proposto: checkpoint legado deve ser preparado com binário/grafo etapa6 antes de novo deploy; namespace e manifesto evitam alegação de migração baseada em snapshot da própria etapa7.

Método já escolhido: subagent implementador GPT-6 Luna Max, principal orquestrador/QA/revisor e revisão independente. Plano aprovado pelo usuário; execução iniciada.
