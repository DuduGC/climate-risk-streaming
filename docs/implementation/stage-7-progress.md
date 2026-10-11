# SDD ledger — plan: docs/superpowers/plans/2026-10-10-risk-engine.md

Spec e plano aprovados pelo usuário. Base7ae024e, branch foundation; mudanças prévias apenas docs.
Ruling: checkout dedicado foundation conforme plano aprovado — evitar checkout redundante/infra concorrente — custo: mover alterações depois se for necessário isolamento adicional.
Ruling: briefs/ledger em docs/implementation como etapas anteriores — trilha durável Windows — custo: docs adicionais.
Ruling: capturar checkpoint6 com grafo/imagem semrisk antesdeploy7 — comprovar migração real — custo: pausa temporária de operação/QA; checkpoints preservados.

| Escopo | Preflight |
| --- | --- |
| Pure engine + adapter | inputs4/modelimmutável, scoremax, JSON21 preserva17, sem newstate |
| Flink + migração | UIDs antigos mantidos, checkpoint6 antesalteração7, namespace/manifesto próprios |
| Contrato + testes | helpers legado21JSONoriginal17; limiares>=/vis< e key metadata válida; contador sóapósserialize |

Preparação checkpointlegado em andamento, nenhumprodcode alterado. Taskúnica: não despachada ainda. Etapa8 não iniciada.
