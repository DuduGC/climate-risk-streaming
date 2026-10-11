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

Baseline atualizado:371passed19skips/Ruff;SDK31passed14,96s. Kickoffdocs51aef56. Agente fresh stage7_risk_impl LunaMax despachado para testesRED/leituras, produçãoaguardaRootconfirmarLegacyCPcapture. Runtimeop6UID762 cancelada conhecida; preparaQA6SeedProfiles, semsourcealteradoprod.

LegacycapturePASS:job21a03a0b914c98a9d4dbc34357321745 CP2 metadata/opt/flink/checkpoints/21a.../chk-2/_metadata readablebothnodes apóscancel. Imagemtagclimate-logistics-flink:stage6-legacy SHA18cff02c4741bfad83e4efeb269b4363c9c6d9264201467685213e19f66ce3a2, hasriskFalse, sourcehashes reais pré-impl. Manifestoprivado .venv/stage7-legacy-checkpoint.json com4profilesorig2020/oldopbackup. Producerrestaurado. Imagematual/runtimeainda6; velhaop restaurotemporáriaconhecida permitido. Agente podeimplementar apósTDDRED; semfase8.

Op6temporária restaurada JobID3f3e2e5ddf132324a9af5c6f4fae22a4 (somente esteID conhecido deverásercancelado paraQA7/deploy7). NovoJobidmantémoldgrupo/UID echeckpointoperacional6. Baseline371/31 confirmado; TDDtestesnovosemescrita peloagente.

TDDRED confirmadoRootPython3.11:110purefailed1,69s porrisk_engineausente. SDKPyFlink1.20.3:9failed6,21s porflink_riskausente; importsSDKbase válidos. Tests3arquivoscoletados (Host110fail/SDKskip esperado). Agente liberadoprodução, semalterarimagem/deployantes QA. Stage6_opTemporary3f3e2e5ddf132324a9af5c6f4fae22a4RUNNING eRESTrestoredCP27prev762. Histórico2020mantéming2020+30seg legítimo (delay=ing-event<=24h, nãoobs-event); nenhumclockoverride/adaptvalidation.

Taskimplsource7ready:PureRoot110passed0,53s/Ruff; SDK40passed16,55s17warnings, build updatedpending. Host119pureincludingnewsharedcontracttest. RevisorTask stage7_risk_reviewdespachado, pacote .venv/stage7-task-review.txt. Sourceoperaçãotemporáriase6id3f...canceladaconhecidaparadeploy7. Nenhum checkpoint/dado apagado; tag6-legacy/manifest/rootbackup preservados.

WholelightRoot482passed22skips6,79s/Ruff. SDK40passed16,55s17warnings. Image7builtsha4c64deefa385bfbb69d11eabf93313077cb03782b8a5ff02cb5ecdacfc9d4a2a eJM/TMhealthy; Compose/Dockerfile/resources semalteração. Legacytag18cff/rootmanifestC2preservados. Reais7Bounded/Migração6→7emexecução comProducerpausado/finallyrestore, logdurável .venv/stage7-risk-integration.log. GateTaskúnicaaindanãoconcluído.

Gate da tarefa aprovado: revisão independente sem P1/P2 ou bloqueadores (stage-7-task-review.md). Integração real Python controlador: 2 passed em66,35s, sem skips; JSON17/21 dos quatro perfis e restauração do CP2 criado pelo grafo6 comprovados. Producer restaurado em finally. Regressões legadas em execução; etapa8 não iniciada.

Revisão final imutável51aef56..5f9adaf: semP1/P2, relatório stage-7-final-review.md. RegressãoKafka3passed93,27s. Temporaisbounded/unbounded2passed149,89s; restore skip por cancelamento assíncrono legado não contado, executado isoladamente:1passed2deselected107,27s. Total8cenários reais comPASS (2novos+6legados). Restauração operacional7 usando o checkpoint mais recente do job temporário6:3f3e2e5ddf132324a9af5c6f4fae22a4/chk-37/_metadata. Nenhumvolume/checkpoint apagado. Scancredenciais dos arquivos tracked aprovado semexporvalores; .envignore confirmado. Usuário reforçou principalorquestrador/revisor e implementadoresLunaMax; papelmantido.

Aceite operacionalPASS: únicoJob3caa432342067923b7edae698657e8d6RUNNING, grupo padrão, restoredCP37→novoCP38COMPLETED298149bytes/checkpoint60000. 12metric17+12risk21 CT/RJ/SP. Backpressureok; UID/GID9999semPG, TM633,1MiB JM335,5MiB. Producer3/3failures0; recursossemalteração. Etapa7concluída; etapa8nãoiniciada. Documentaçãofinal em stage-7-acceptance.md; commitslocaissempush.
