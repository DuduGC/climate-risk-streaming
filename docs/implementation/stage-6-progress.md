# SDD ledger — plan: docs/superpowers/plans/2026-10-10-temporal-streaming.md

Spec/plano aprovados pelo usuário. Base baa6474, branch foundation; alterações documentais da etapa6 presentes antes de iniciar código.

Ruling: usar checkout dedicado existente foundation conforme plano aprovado — preserva infraestrutura local e preferência das etapas anteriores — custo se inadequado: migrar alterações para checkout isolado posteriormente.
Ruling: brief/ledger versionados em docs/implementation como nas etapas anteriores, sem workspace temporário Bash — trilha durável em Windows — custo: documentação adicional no repositório.
Ruling: pausar Producer somente durante QA temporal histórico e restaurar em finally; bounded usa anchorUTC atual — evita avanço do watermark por input concorrente/backlog — custo: pausa reversível da coleta durante QA.

| Tarefas | Contrato / preflight |
| --- | --- |
| 1 e 2 | Ambas usam flink_temporal/flink_job e testes: execução sequencial e UIDs estáveis; checkpoint liga após janela revisada |
| 1 | Acumulador11 escalares, output definido, seis janelas1800000/300000; tardio parcial e máximo datetime cobertos |
| 2 | Storage/config e restore explícito; prova offsetB depois checkpointC e dedupA, sem exactly-once PrintSink |

Baseline Python3.11.14: 361 passed,15 skipped; Ruff aprovado. Serviços inicialmente parados, restauração em andamento, volumes preservados.
Task 1: em execução — agente GPT-6 Luna Max.
Task 2: pendente; não despachar antes de revisão/QA tarefa1.
