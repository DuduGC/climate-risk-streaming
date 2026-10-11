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

TDDREDRootTask1:82failed1,29s ausência business_records/renderer. ImplementadorliberadoproduçãoTask1. PureGREENRootPython3.11.14:81passed0,40s/Ruff0.14.0limpo. SQL/seed/migraçãoemimplementação, revisãoTask1nãoiniciada. ArtefatosMaven3.3.0-1.20e42.7.14 publicados: índicesHTTPSconfirmados, digests .sha1disponíveis(.sha256404); verificardownloadcontra publicado egerarSHA256buildnaTask2, semimaginarhash. JDBCsha1 292553dba07f5e004065da539611a5c3d20ccd0f; PGsha1 0e7c94ce80cb1dd9875ff44ffe6a40a8aaa3eefd. Nenhumjarinstaladoainda.

Revisão preliminarRootcontrato detectou/provou3aceitações indevidas:precipmaxnegativo, JSONescapeUnicodeSurrogateLOW, contagem2**63. ImplementadoradicionouregressõesRED(7falhas) ecorrigiu; Host90focusedGREEN. Rootvalidação3.11emexecução. Ruling: aplicação semDDL implica negarTEMP herdadoPUBLIC noDBdedicado, alémschemaCREATE/DELETE — nenhumtempénecessáriono processamento — custo:futurosusuariosque precisemTEMPdevem recebergrantadministrativoexplícito; adminsuper/ownerpreservado. SemrevokeCONNECTglobal oualteração de roles/secrets.

RootGREEN apóscorreções BusinessRecordsPython3.11.14:90passed0,46s/Ruff0.14limpo; logstage8-task1-pure-green-fixed.log. SQL/renderer/migration/integrações continuamTask1, report completo aindaaguardado; não despacharTask2 antecipadamente.

RuntimeQA Task1 atéagora: migrationfalhou3vezes comrollbackseguro: (1)text||catalog-char ambíguo; (2)aliasmemberrefdesatualizado; (3)jsonb_object_length inexistentePG17. Rootreportoucausas específicas; agente corrigiu1/2 e estáconsolidando3/keysets+rolreplication+DBownership. TestesPsql2falhasexecutadosantesdepMigrationPASS foram falhas depré-requisito(tabelas inexistentes/TEMPoriginal), nãoPASSnemdefeitodo grafo7. NenhumaDDL/dado businesscommitada; sourcepipeline/imagem7inalterados. PróximacomandaRootmigrationdevePASSantespsqltests, dependênciassequenciais.

Task1hotmigrationPASS2x após4correçõesSQL(charcasts,aliasmember,jsonb_object_lengthinexistente,precedênciaJSONsubtração). Psqlintegração2passed2,60s; WholeRootPython3.11 573passed24skips7,18s/Ruff. Snapshotindexedsourceem.venv/stage8-task1-review.diff Baseff1e5ef, códigoTask1aindasemcommit; reviewerfresh stage8_records_review SolHigh. Coldinit owncontainerclimate-postgres-stage8-qa-8b5d8a98666a PASS+reapply, containerremovido. Snapshotreadonlypreservado.venv/stage8-postgres-snapshot-8b5d8a98666a/.

Task1reviewrequiresfix:2P2(payloadNULL/typesschemaCheckincompleto; RLSnãofingerprinted),observaçãoattacldrift. Rootprobesconfirmamnullablecomponent/eventcountboolaceitos; RLSon→reapplyPASS→appUPSERTnega; columnGrantUPDATEcity→reapplyPASS,reparasilentemente(afterprivfalse), violatesabortdrift. Log.venv/stage8-task1-cold-probes.log. Fixwaveformal1paraimplementadorLunaMax: strengthenpayloadCHECK, includeRLS/policies/attacl fingerprint, regressionSQLtests. RootrealDBjátemDraftSchemaV1marker/oldconstraintsemfatos; agenteescreveráone-off.venv/stage8-task1-repair-draft.sql AdminmetadatacorrectiveTxn(sóCHECK/COMMENT), validaoldfingerprint/owner/nodrift/zeroFacts antes, nãoDROPtable/DELETEdata/registry4ªtabela. Task2bloqueadaporgateTask1nãoaprovado, nãoéblockedgoal. Nãofinalizaretapa8ainda. Op7f50...RUNNING/rootlegacyE0CP2preservados.

Fixwave1TDDREDRootPsql nova regressão payload:1failed2deselected1,38s P0001invalidpayloadfieldwasaccepted; rolledback. AgenteautorizadoaDDL/fingerprint+oneoffdraftrepairagora, arquivosnãoexecutarantesFrozenReady. PróximogateRoot:reviewrepairscope+oldfingerprint/nofacts→AdmincorrectiveTxn→hotmigration2x→Psql3tests→coldstrictprobes→scopedre-review→commitTask1. FonteRootop7f50/LegacyE0CP2mantidos. Métodoscriptcold atualizado --require-corrected exigeNULL/boolrejeitados, attacl/RLSreapplyrejeitamsemautorepair; validationcontainerown removidoemfinally.

Ruling: payloadCHECKcorrigido valida contrato22 completo/ranges/coerências internas eFKidentidade; snapshotcompleto igualàmetrics é garantido peloCTEúnico aprovado, nãoCHECKcrossTable — PostgresCHECKnãoconsultaoutraTabela e nãointroduzirtriggerdeferrable/colunasduplicadasforaescopo — custo:credencialappviaDirectSQLpodeescreverfatosválidos que nãosãoprodutos dofluxo, limite documentado. NativeNULL/booldefeitos continuamP2obrigatórios a corrigir. Reviewer/implementador informados. RootMainDraftread:locations3/facts0/alerts0/ownersclimate_admin,RLSfalse,marker stage8-business-schema-v1:a76f337af5bc94b63be7bcb2494c496d; oldfp deve ser recompuesto/compare nooneoffRepair (não trustliteral).

Fixwave1RootQA: coldstrict PASS(locs3/tables3; componentNULL/eventcountboolreject23514; columnACLdriftreapplyrejectsemreparar; RLSdriftreject+appUPsertnegado), owncontaineraf25501de523removido. Adminone-offRepairDOverificouoldfp/nofacts/owner/RLS/attacl, alterouapenasCHECKpayload+COMMENTfp, COMMIT. Hotreapplyfinal2xPASS; Psqlintegrações3passed3,43s semskip. Wholefinalemexecução. Scopedfixdiff.venv/stage8-task1-fix1.diff para revisororiginal semcrawlbroad. SemTask2sourceainda.

Task 1: complete — scopedreviewfix1stage8_records_reviewAPROVADOsemP1/P2, reportversionado; final573passed25skips7,15s/Ruff,Psql3passed3,43s,coldstrict/fingerprintrolesPASS, metadataonlyrepairrootaplicadocomsafeguardsnofacts. CSVLFvia.gitattributesregraespecífica mantémtesteartifactportável. RootcommitgateTask1agora; freshTask2LunaMaxdepois. Task2briefpreparadodocs/implementation/stage-8-task-2-brief.md, semproduçãoTask2ainda.
