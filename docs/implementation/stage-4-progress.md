# SDD ledger — plan: docs/superpowers/plans/2026-10-09-kafka-flink.md

Spec/plano aprovados em2026-10-09. Base real c6b07a5, branch foundation.
Ruling: base de execução c6b07a5 inclui plano aprovado posterior a846ceb5 citado — só docs alterados — custo se errado: rever diff.

| Escopo | Preflight |
| --- | --- |
| Parser / Row / diagnóstico | Ordem/tipos/interfaces declarados |
| Runtime JM/TM | Mesma imagem, checksums fixados, sem segredo PG |
| Integração | Bounded isolado + operacional único, sem apagar dados |

Task 1: pendente; infraestrutura parada na consulta inicial, volumes preservados.

Task 1: complete — 346 passed/6 skips, Ruff0, runtime regression1passed, bounded1passed18.06s. Review aprovada, operação RUNNING+novo evento comprovado, UID/secrets/recursos/Producer verificados. Etapa4 completa; Etapa5 não iniciada.
