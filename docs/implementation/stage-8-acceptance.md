# Etapa 8 — aceite

**ETAPA 8 CONCLUÍDA em 2026-10-11.** Implementação por agentes GPT-6 Luna Max; orquestração, revisão principal e execução dos testes reais pelo Root. Etapa 9 ainda não iniciada.

Todas as avaliações são persistidas no PostgreSQL. Somente HIGH/CRITICAL geram alertas na tabela e em `weather.alerts`, com `location_id` como key e JSON de 22 campos como value. A identidade por localidade, janela e versão de regras produz um `alert_id` determinístico. Uma CTE parametrizada mantém métrica e alerta atômicos dentro do PostgreSQL; Kafka e PostgreSQL têm entregas independentes, com recuperação at-least-once.

## Verificação

| Gate | Resultado |
|---|---|
| Suíte geral Python 3.11.14 | 619 passed, 30 skipped; 7,70 s. Skips não contados como aprovação dos testes externos. |
| Ruff | Aprovado no checkout final. |
| SDK real PyFlink 1.20.3 | 83 passed; mesmo código de produto da Task 2. |
| PostgreSQL | 3 integrações reais; migração reaplicada, cold init, replay/imutabilidade/atomicidade e privilégios/drift verificados. |
| Regressões anteriores | Kafka 3, temporal 3, risco 2: todas aprovadas; skips por cancelamento em andamento reexecutados isoladamente. |
| Entrega e replay | 24 métricas, 12 alertas SQL/Kafka completos, seis rejeições de desconhecidos e seis duplicatas após restore; C4 → C6; 103,15 s. |
| Migração real 7 → 8 | Checkpoint 7 capturado antes dos sinks, C2 → C5; 12 métricas/seis alertas alvo. Snapshot isolou 56 métricas/seis alertas introduzidos pelo job QA e preservou o baseline após cleanup; 59,84 s. |
| Falha JDBC e recuperação | Conexão `postgres:1` recusada, restarts esgotados, nenhum checkpoint novo no job ruim; C2 → recuperação C3; 12 métricas/seis alertas e IDs/payloads estáveis; 131,84 s. |
| Falha Kafka e recuperação | Apenas writer `kafka:1`, fonte em `kafka:9092`; nenhum checkpoint novo durante falha; C2 → recuperação C3; convergência e mensagens repetidas equivalentes; 188,00 s. |
| Revisões independentes | Três tarefas e revisão final aprovadas; nenhum P1/P2 pendente. |

O orçamento de 240 s é compartilhado pela falha do destino e sua recuperação; preparação do checkpoint bom e cleanup ficam fora desse orçamento. Os quatro cenários usaram jobs próprios, Producer pausado/retomado em `finally` e cleanup de identidades próprias após estado terminal. Nenhum volume, topic ou checkpoint preservado foi apagado. As tentativas intermediárias que falharam não foram contabilizadas como PASS.

## Operação retomada

Job `257be545438f67675ad4b7f5a235eb5a`, nome `weather-kafka-ingestion`, grupo padrão `weather-flink-ingestion`: **RUNNING**. Restaurado de `/opt/flink/checkpoints/f50cfe22e4aaf194d1eea1cb55c0d84b/chk-164/_metadata`, sem ignorar estado. Novo checkpoint 165 concluído; confirmação posterior no checkpoint 169, com zero falhas. Intervalo 60 s, timeout 120 s, pausa mínima 10 s e um checkpoint concorrente preservados.

O Producer confirmou três localidades entregues e zero falhas no ciclo observado. PostgreSQL contém métricas de 2026 para RJ01, SP01 e CT01, 45 por localidade no snapshot de aceite. O backlog Kafka também continha fixtures históricas de QA: o job operacional as reprocessou, e o snapshot distingue 186 métricas históricas e 66 alertas de demonstração. **Esses 66 alertas não são apresentados como alertas climáticos atuais da API; o snapshot de 2026 tem zero alertas.** As fixtures e dados da API são distinguíveis pelos timestamps. Alertas novos dependem das condições meteorológicas atingirem a regra HIGH/CRITICAL.

JobManager, TaskManager, Kafka e PostgreSQL saudáveis; Producer em execução. Ambos os nós Flink usam UID 9999. Observação sob carga pequena: JobManager 551,3 MiB de 1 GiB; TaskManager 594,7 MiB de 1,5 GiB; CPU 1,70%/2,10%; backpressure `ok` nos três vértices. Não houve aumento dos limites. Um episódio de MetaspaceOOM durante muitas submissões sucessivas de QA foi isolado com restart do TaskManager sem job ativo; a regressão correspondente passou em processo limpo. Isso não é um benchmark de carga sustentada.

## Segurança e limites

A role `climate_app` tem leitura da dimensão e escrita mínima dos fatos. Sem superuser, criação de role/database, replicação, DDL, TEMP, DELETE/TRUNCATE ou INSERT na dimensão. Flink recebe somente configuração de aplicação; credenciais administrativas ausentes. `.env` ignorado pelo Git e scan de valores reais de credenciais em arquivos versionados/index aprovado, sem imprimir valores.

Payload e campos SQL são validados; métrica é imutável por identidade e replay idêntico é permitido. A correlação completa entre métrica e payload de alerta é garantida pela CTE do fluxo controlado; acesso direto com credencial de aplicação permanece fora desse fluxo. Kafka pode repetir mensagens após recuperação, e o consumidor da etapa 9 deverá tratar `alert_id` de forma idempotente. Não há atomicidade distribuída entre PostgreSQL e Kafka.

Commits locais de implementação: `a88fb65`, `982f485`, `bf7ca05`. Sem push ou merge. Provas locais de checkpoints, runtimes e observações ficam em `.venv/stage8-*`; pareceres e relatório são versionados em `docs/implementation/`.

Próxima etapa: consumer de alertas, mantendo o escopo simples do MVP.
