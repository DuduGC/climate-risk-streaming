# Etapa 7 — aceite técnico

Implementação por GPT-6 Luna, esforço Max; orquestração e QA pelo principal. Código revisado no commit local `5f9adaf`, branch `foundation`, base de revisão `51aef56`. Concluída em2026-10-10 após revisão independente e QA operacional do principal.

## Implementado

Motor puro e determinístico com entradas/resultados imutáveis. A política `mvp-1` atribui scores 0/25/50/75/100 a intensidade de precipitação, vento sustentado, rajadas e visibilidade; o score geral é o maior componente. Classes: LOW 0–24, MEDIUM 25–49, HIGH 50–74 e CRITICAL 75–100. Tabela e unidades documentadas no README e na spec aprovada.

Adaptador PyFlink stateless com validação explícita do contrato e serialização JSON sem NaN. O ramo `weather-risk-assessment`/`weather-risk` produz 21 campos e mantém os 17 campos anteriores, exceto `operation=risk_assessment`; acrescenta score, classe, componentes e versão. A saída original `window_aggregate` continua com 17 campos. `risk_engine.assessed_windows` incrementa somente após serialização bem-sucedida.

UIDs anteriores, offsets, deduplicação TTL86400/cache0, janelas30/5, watermarks, checkpoint60s e recursos permanecem. Nenhum sink PostgreSQL ou publicação de alertas foi antecipado.

## Testes e evidências

- Baseline etapa6: 371 passed/19 skipped; SDK31 passed. RED observado antes da implementação: 110 falhas por módulo puro ausente e 9 falhas por adaptador SDK ausente.
- Python3.11.14: 482 passed/22 skipped, 6,79s; Ruff0.14.0 aprovado. Skips opt-in/SDK ausente nessa imagem não contam como testes reais.
- SDK Python3.11.14/PyFlink1.20.3: 40 passed em16,55s; 17 warnings de dependências/APIs antigas, sem falhas.
- Integração nova Kafka/Flink: 2 passed em66,35s, sem skips. Comparação de JSONs completos17/21, quatro classes, seis janelas por perfil, revisão extrema deduplicada e contador.
- Regressão Kafka/Flink: 3 passed em93,27s, sem skips; parsing, qualidade/deduplicação e expiração TTL sem refresh.
- Regressão temporal: bounded/unbounded 2 passed em149,89s; recovery isolado 1 passed/2 deselected em107,27s. O skip durante cancelamento assíncrono legado não contou como PASS. Total: oito cenários reais aprovados em execuções coordenadas (seis legados e dois novos). Prova operacional aprovada conforme seção abaixo.

## Migração real da etapa 6

Antes de alterar o grafo/imagem, o principal capturou quatro perfis com janelas abertas no job `21a03a0b914c98a9d4dbc34357321745`, checkpoint2, metadata `/opt/flink/checkpoints/21a03a0b914c98a9d4dbc34357321745/chk-2/_metadata`. Origem git `7ae024e`; imagem6 `sha256:18cff02c4741bfad83e4efeb269b4363c9c6d9264201467685213e19f66ce3a2`, preservada com tag `climate-logistics-flink:stage6-legacy`. Manifesto privado ignorado `.venv/stage7-legacy-checkpoint.json` registra hashes, ausência do ramo de risco, payloads e backup operacional.

A integração restaurou esse checkpoint com grafo7, JobID novo, mesmos nome/grupo e sem `allowNonRestoredState`. Cópias revisadas dos quatro eventos foram deduplicadas; avanço de watermark fechou as janelas restauradas e produziu métricas17 e avaliações21 com os mesmos resultados determinísticos. REST confirmou checkpoint legado restaurado e checkpoint novo COMPLETED. Metadata preservada e legível nos dois nós.

Imagem7 construída e JM/TM saudáveis: `sha256:4c64deefa385bfbb69d11eabf93313077cb03782b8a5ff02cb5ecdacfc9d4a2a`.

## Operação final

Único job ativo `3caa432342067923b7edae698657e8d6` (weather-kafka-ingestion), grupo padrão weather-flink-ingestion, RUNNING. Restaurou o checkpoint37 mais recente preservado da operação temporária6: `/opt/flink/checkpoints/3f3e2e5ddf132324a9af5c6f4fae22a4/chk-37/_metadata`. Novo checkpoint38 COMPLETED, 298149 bytes de estado, intervalo60000ms, concorrência1, zero falhas na observação; metadata legível nos dois nós.

Prova verificou 12 métricas originais17 e 12 avaliações21 das localidades CT01/RJ01/SP01, mantendo identidade e todos os valores originais, com score=max(componentes) e rules=mvp-1. Exemplo SP01: intensidade0,4mm/h, vento9,7km/h, rajada23,4km/h, visibilidade9320m → score0/LOW.

Memória observada sob carga pequena: TaskManager633,1MiB/1,5GiB, JobManager335,5MiB/1GiB, Producer28,23MiB/256MiB. Backpressure=ok nos três vértices consultados. JM/TM UID/GID9999 e sem variáveis POSTGRES_/APP_DB_. Serviços de infraestrutura saudáveis; Producer ativo, último ciclo2026-10-11T00:38:56.612226Z (2026-10-10 local): collected3/accepted3/delivered3/failures0. Recursos não aumentados.

Provas e logs locais ignorados: `.venv/stage7-operational-proof.json`, `.venv/stage7-runtime-proof.json`, `.venv/stage7-risk-integration.log`, `.venv/stage7-kafka-regression.log`, `.venv/stage7-temporal-regression.log` e `.venv/stage7-temporal-restore-regression.log`. Documentação de aceite versionada contém os resultados necessários mesmo sem esses arquivos locais.
## Problemas, correções e decisões

Não houve defeito bloqueador encontrado nas revisões de tarefa e final. Uma tentativa de regressão temporal começou enquanto a regressão Kafka ocupava o único slot: três skips não foram contabilizados como aprovação; a execução coordenada foi iniciada novamente.

Revisão final independente por GPT-6.1 Sol High disponível, mantendo Luna Max no implementador; Astra apresentou limite de uso na etapa anterior. Pareceres em `stage-7-task-review.md` e `stage-7-final-review.md`, sem P1/P2 de código. Revisão estática e verificação de runtime possuem evidências separadas.

Checkout dedicado existente reutilizado conforme plano aprovado. Commits locais, sem push. `.env` ignorado e scan dos valores das credenciais nos arquivos versionados aprovado sem imprimir secrets. Nenhum volume, topic, checkpoint ou dado removido.

## Limites e próxima etapa

Score ordinal heurístico, sem calibração de probabilidade/prejuízo ou amplificação de fatores moderados; extremos de componentes podem vir de observações distintas. PrintSink não é transacional e pode repetir registros após recovery. Contador mede processamento, não avaliações exatamente uma vez. Checkpoints filesystem e broker local não oferecem HA. Observação de recursos sob carga pequena não equivale a benchmark.

Etapa8 — sinks de negócio/persistência e publicação de alertas — permanece pendente e não foi iniciada.
