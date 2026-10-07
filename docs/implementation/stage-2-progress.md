# SDD ledger — plan: docs/superpowers/plans/2026-10-07-open-meteo.md

Spec e plano aprovados pelo usuário em 2026-10-07.
Base: a352731 (foundation). Alterações iniciais apenas spec aprovada e plano novo.
Método: implementadores GPT-6 Luna Max, revisões independentes e validação pelo principal.

## Pré-flight

| Tarefas | Interface ou arquivos compartilhados | Verificação |
| --- | --- | --- |
| 1 / 2 | WeatherEvent, Location, normalize_current; open_meteo.py | Sequenciais, contrato congelado após revisão1 |
| 1 | Validação direta e de payload, UTC, ranges/WMO | Coerente, sem dependência HTTP |
| 2 | Cliente, ownership Session, timeout/retry e HTTP local/live | Coerente com spec |
| Principal | Progresso, docs e commit | Sem disputa de ownership |

Ruling: manter diretório e branch foundation adotados na etapa1 — plano aprovado preserva método — custo se inadequado: isolamento posterior.

## Estado

Task 1: pendente.
Task 2: pendente.
Validação final: pendente.

Task 1: entrega inicial240 testes aprovados e Ruff aprovado (principal Python3.11.14).
Task 1: fix round1 — P2 datetime aware cujo offset sai do domínio UTC; quatro casos red reproduzidos pelo implementador. Normalização/contrato restantes aprovados pela revisão.

Task 1: complete — 245 passed, Ruff aprovado no principal, revisão scoped aprovada, P2 UTC resolvido.
Task 2: implementador stage2_http despachado; contrato preservado.
