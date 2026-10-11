# SDD ledger — plan: docs/superpowers/plans/2026-10-11-business-sinks.md

Spec e plano aprovados pelo usuário em2026-10-11; BASE156fc0d, branch foundation. MétodoagentesLunaMax/principalorquestradorrevisorQA preservado.

Ruling: usar checkout dedicado foundation conforme plano/preferência anteriormente aprovados — não duplicar checkout/infra/volumes — custo: isolamento adicional exigiria transportar mudanças depois.
Ruling: briefs/ledger/report em docs/implementation, com controladores/provas em .venv ignorada — trilha Windows durável já adotada — custo: documentação extra; não usar scratchBash nem apagar trilha no fechamento.
Ruling: commits/Docker/integrações reais pelo principal, não pelos implementadores — .git/pipe Docker requerem execução autorizada neste ambiente — custo: coordenação root.

| Par/interfaces | Produz/consome e preflight |
| --- | --- |
| Task1→Task2 | jdbc_values(JSON21,idnullable), SQL2params; Row2STRING, FK/CTE/trigger; shapescoerentes |
| Task1→Task3 | schema19colunas de dados de avaliação, alertpayload22, SELECTapp e seed3; QAconsultaPSQL própriasidentidades |
| Task2→Task3 | build_pipeline(settings,business_sinks=None/typed), QAopt-in+nomenamespace, nativeCLI/UIDs; helperfalhasconfigsomentejobpróprio |
| Task1/Task2 | compose/README compartilhados, execuçãosequencial; mountsTask1/credenciaisTask2, semremoverconfiganterior |
| Task1 próprio | TDDpure/migrationreal/volumeexistente+temporário, sem raw ougrantsadmin; interfacesDDL/CSV/SQLcoerentes |
| Task2 próprio | tinyJavaRowserializer/nativeKafka, CP7antesdeploy8, versõeschecksums eSDKreal; semnovosserviços |
| Task3 próprio | conhecidos3loc/4profilesduasrodadas +legacy7doisprofiles; sentinellowsemjanelafechada, oracleindependent, faultrestoreC |

Captura do CP7 antes de alteração no grafo/imagem8; Task1 permite contrato/SQL/mounts após baseline, Flink source/imagem ficam7 até o principal liberarTask2. Task1 ainda não despachada; nenhumproduto8 implementado. Task2/Task3 pendentes. Etapa9nãoiniciada.

BaselineRootPython3.11.14:482passed22skips6,45s/Rufflimpo. Task1implementador stage8_records_impl LunaMax despachado paraREDantesprodução; semTask2/3. Ruling:score49nãoéavaliaçãogerada/coerentemvp1, teste49recusa emvezdeemitiralerta/None artificial;score50válidoHIGH — validação reutiliza políticaexistente — custo: futurasregras comscoresnão-discretos exigirão versão/contrato atualizados. BaselineSDK/captura7emexecução.

LegacycapturePASS: imagem7tagclimate-logistics-flink:stage7-legacy SHA4c64deefa385bfbb69d11eabf93313077cb03782b8a5ff02cb5ecdacfc9d4a2a; QA7Jobe0fc5a701089bc01b09f8de036b5f6b5/CP2metadata/opt/flink/checkpoints/e0fc5a701089bc01b09f8de036b5f6b5/chk-2/_metadata legívelambosnodesapóscancel; perfisRJHIGH/SPLOW2020T13:10janelasabertas. Manifestoprivado stage8-legacy-checkpoint.json origem7hasbusinessFalse/sourcehashes/backupopcf62 preservado, Producerrestaurado. SDKbaseline40passed17warnings17,69s. Tempop7 seráretomadapelobackupduranteTask1; nenhumproduto8noFlinkainda.

Tempoperacional7Jobf50cfe22e4aaf194d1eea1cb55c0d84b retomado dobackup conhecido; somenteesteID deve ser cancelado no gateTask2/deploy8. ManifestolegadodeQAe0fc/CP2não será sobrescrito.
