# SDD ledger — plan: docs/superpowers/plans/2026-10-09-data-quality.md

Spec/plano aprovados. Base real0adc4f6, branch foundation.
Ruling: base de execução0adc4f6 inclui plano aprovado posterior a8348f49 — apenas docs — custo se errado: refazer comparação.

| Escopo | Preflight |
| --- | --- |
| Política / parser | Validação pura antes de state, sem mudar schema |
| KeyedMap / TTL | API verificada, padrão24h, QA isolado |
| Integração | IDs/Jobs isolados, sem remover dados, restabelecer operacional |

Task 1: pendente.

Task 1: stage5_impl (Luna Max) em execução. Infra restaurada, sem jobs ativos na consulta REST; runner host de integração disponível. Nenhum volume removido.

## Revisão e diagnóstico durante execução

- Suíte leve no Python 3.11.14: 361 passed, 14 skipped; skips são gates live/integration e SDK ausente. SDK executado separadamente: 7 passed.
- Ruff: aprovado após três E501 corrigidos.
- Configuração QA corrigida: bundle.time é Long/milissegundos, configure recebe somente Configuration. Regressão consulta a opção Java real.
- Primeira integração: 2 passed, 1 failed por janela de observação (duplicata aos 9,47 s versus limite 9 s).
- Ruling: ampliar QA TTL de 10 para 20 segundos, duplicata +10 s e repetição +23 s, preservando prova antes do deadline hipotético renovado. Custo: teste ~13 s maior.
- Repetição: 2 passed, 1 failed; logs mostram accepted 00:04:48.982, duplicate 00:05:00.978 e duplicate 00:05:13.599 (UTC), apesar de TTL20. Achado funcional mantido aberto.
- Protobuf instalado serializa ttl20000/OnCreateAndWrite/NeverReturnExpired/cleanup10true corretamente. SDK tenta desativar readcache via atributo _max_entries; Beam2.48 usa _max_weight. Investigação/correção pública de cache em andamento; etapa não concluída.

## Fechamento

- Regressão RED confirmou STATE_CACHE_SIZE1000; GREEN configura0 em operacional/QA. Teste real antes falhava após24,6s; depois passou com accepted/duplicate/accepted e margens verificadas.
- Final: 361 passed/15 skipped na suíte leve; SDK8 passed; Ruff aprovado; integrações reais3 passed/74,65s. Skips nunca considerados aceite de integração.
- Prova operacional: job27e5e15139ad82f8beb22c40025c95bf RUNNING, TTL86400, fixtureQA-OP-88606be90ad4 uma saída e duplicate_records41→42.
- Producer3/3 entregas sem falhas; TM710,1MiB/JM292,1MiB sob carga pequena; usuárioflink e sem credenciais PostgreSQL nos containers Flink.
- Revisão independente stage5_review: aprovada, nenhum P1/P2 aberto. Uma chamada Docker do revisor aguardava aprovação; foi abortada e o parecer final usa análise estática + evidências runtime do principal, sem duplicar testes.
- Scan de credenciais locais: zero ocorrências; .env ignorado. Dados/volumes preservados. Checklists encerrados, etapa6 não iniciada. Commit local criado após diff/check/scan; ID disponível no Git.
