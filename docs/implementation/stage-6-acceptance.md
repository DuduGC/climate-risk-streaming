# Etapa 6 — aceite técnico

Concluída em 2026-10-10. Implementadores: GPT-6 Luna, esforço Max. Código nos commits 5e81753 e 2e30975, branch foundation.

## Implementado

Event Time a partir de event_timestamp UTC, watermarks com desordem de 2 minutos e ociosidade de 60 segundos, keyBy por localidade e janelas deslizantes de 30 minutos com avanço de 5 minutos. O agregado incremental usa 11 escalares e produz um JSON de 17 campos. Inclui média de temperatura, máximos de precipitação/intensidade/vento/rajada, mínima de visibilidade, contagem, intervalos de medição e extremos temporais.

Eventos parcialmente atrasados participam das janelas ainda abertas; os totalmente atrasados seguem ao side output consumido e incrementam late_records uma vez. Não há reabertura de janela. Validação, deduplicação e diagnóstico anteriores permanecem.

Checkpoints de estado/offsets a cada 60 segundos, timeout 120 segundos, pausa mínima 10 segundos, concorrência 1 e tolerância zero a falhas. Storage filesystem em volume compartilhado, retenção efetiva de 2 checkpoints e preservação no cancelamento. Restart limitado a 3 tentativas com 10 segundos entre elas. Overrides de QA exigem nome/grupo exclusivos e auditoria, inclusive a flag explícita com valor 60.

## Testes e evidências

- Suíte geral final, Python 3.11.14: **371 passed, 19 skipped**, 6,93 s; Ruff aprovado. Skips são gates opt-in/SDK ausente nessa imagem, sem contabilização como integração aprovada.
- SDK PyFlink 1.20.3 final: **31 passed**, 16,80 s, 16 avisos de dependências/APIs deprecated; sem falha de runtime.
- Kafka/Flink legados com checkpoints: **3 passed**, 86,18 s.
- Temporal final após retomada: bounded e unbounded **2 passed**, 162,52 s. Recovery foi skipped nessa execução em conjunto e não recebeu aceite por isso.
- Recovery final isolado: **1 passed, 2 deselected**, 73,01 s. Todos os seis cenários reais têm evidência de PASS em execuções coordenadas.
- Prova de recovery anterior também aprovada em 77,26 s: original bc6d948e3b5710b1f96c61c37faffd45 tinha checkpoint 3 mais recente; job restaurado 804b3ee142e643f363bff67d47763741 confirmou checkpoint 2 pela REST e concluiu checkpoint 5. O evento B posterior a C foi relido; A permaneceu deduplicado; o agregado A+B confirmou recuperação do acumulador.
- Leitura/escrita do volume nos dois nós como UID 9999, sem elevar privilégios dos processos.
- Revisões independentes das tarefas e da etapa completa aprovadas, sem P1/P2 remanescente. A tentativa de revisão final com Astra falhou por limite de uso; revisão completa realizada por GPT-6.1 Sol High disponível, mantendo Luna Max nos implementadores.

## Operação deixada ativa

Único job RUNNING: **762137e915474bd6827555e68fc8e4c6**, weather-kafka-ingestion, grupo weather-flink-ingestion. Configuração operacional padrão: janela 30/5 minutos, desordem 2 minutos, TTL de deduplicação 86.400 segundos e checkpoint 60.000 ms.

A prova operacional observou 174 métricas de janela de CT01/RJ01/SP01 e checkpoint 1 com 228.818 bytes de estado. A conferência posterior observou **4 checkpoints concluídos, zero falhas**, checkpoint 4 com 232.628 bytes, duração 14 ms e metadata em file:/opt/flink/checkpoints/762137e915474bd6827555e68fc8e4c6/chk-4. A leitura compartilhada do metadata foi comprovada nos dois nós durante a prova operacional do checkpoint 1.

Memória observada sob carga pequena: TaskManager 781,6 MiB/1,5 GiB; JobManager 476,3 MiB/1 GiB. CPU naquele instante: 2,45% e 1,15%. Métricas de backpressure dos três vértices foram zero. São observações locais, não benchmark de capacidade.

Cinco serviços saudáveis, Flink como UID/GID 9999 e sem configuração PostgreSQL nos containers Flink. O Producer teve um ciclo parcial com OpenMeteoError em RJ01 às 23:44 UTC; o log registra a classe, sem causa específica. No ciclo seguinte, às 23:49 UTC, RJ01/SP01/CT01 tiveram ACK, collected/accepted/delivered=3 e failures=0. Não foram relaxadas validações para forçar sucesso.

## Problemas corrigidos

1. allowed_lateness recebeu Time; API Python exige inteiro em milissegundos. Corrigido para 0 e protegido por regressão SDK causal.
2. with_idleness criava outro wrapper e perdia o assigner Python configurado antes. O assigner agora é a última chamada, com regressão da estratégia efetiva e prova de fronteira exclusiva real.
3. Cleanup de submissão QA não preservava JobID antes do startup. Holder, fallback por namespace exclusivo e testes de falha CLI/startup corrigidos.
4. Métrica do consumidor de atrasados não estava registrada antes da primeira entrada. A prova aguarda registro com deadline e exige valor real 1; ausência nunca é tratada como zero.
5. Binding Python de retenção externalizada procurava classe Java incompatível. A opção pública de Configuration foi verificada pelo enum Java efetivo, sem alterar bibliotecas.
6. Retenção apenas no ambiente do job não controlava o coordenador. A chave canônica execution.checkpointing.num-retained=2 foi aplicada à aplicação e aos dois nós e confirmada por REST e preservação/restauração do checkpoint anterior.

## Decisões e custos registrados

- Checkout dedicado foundation mantido conforme plano: eventual mudança de isolamento exige mover alterações.
- Briefs e ledger versionados em docs/implementation: documentação adicional no repositório.
- Producer pausado para QA histórico e restaurado em finally; bounded usa anchor UTC atual: pausa reversível da coleta durante testes.
- SDK test renomeado para evitar colisão de basename: referências de arquivo atualizadas.
- Publisher QA em container descartável do serviço existente, com comando sobrescrito: startup curto por lote, sem novo serviço permanente.
- Retenção 2 aplicada ao coordenador: custo de dois snapshots por job, já previsto no desenho.
- Revisão final usou Sol High disponível após limite de Astra: modelo alternativo na revisão; implementadores preservados.
- Branch mantida local, sem push/merge, conforme plano aprovado: integração com remoto permanece uma decisão futura.

## Limites e próxima etapa

Watermark é global ao fluxo e não avança por relógio de parede quando tudo está ocioso. Precipitação máxima/intensidade projetada não equivale a acumulado de chuva na janela. TTL da deduplicação usa processing time; downtime pode consumir esse horizonte. Checkpoints recuperam estado/offsets; PrintSink não é transacional e logs podem repetir após recovery. Cluster standalone sem HA exige restauração/submissão manual após perda completa. Checkpoints de jobs cancelados foram preservados para inspeção e limpeza manual autorizada; nenhum volume foi apagado.

Etapa 7 — Risk Engine — não iniciada. Scan de credenciais, check do índice e fechamento Git registrados no ledger.
