# Etapa 7 — revisão final independente

Parecer sobre o código: **nenhum bloqueador; nenhum achado concreto P1/P2**. O commit implementa a avaliação determinística aprovada, mantém os contratos e o estado anteriores e inclui oráculos adequados para integração e migração. O aceite operacional final ainda deve ser fechado pelo principal com as evidências pendentes abaixo.

## Revisão vinculada ao commit

- Base: `51aef56b18d46f8769249027539e8c2a6a2d409f`.
- Commit revisado: `5f9adafaafa75dbfaae98fbbf7c01a59d955686b`.
- Intervalo: `git diff 51aef56..5f9adaf`, 11 arquivos, 1666 inserções e quatro remoções.
- Método: revisão estática em passes de contrato/política, integração/estado, testes/oráculos e documentação/escopo. Arquivos lidos por `git show`/`git diff`; os arquivos de implementação e testes no checkout também coincidem com esse commit.
- Processo: aplicada a skill `superpowers:requesting-code-review` e seu template de revisor. Este é o revisor independente de fallback disponível; o principal informou que Astra atingiu limite de uso na etapa anterior. A implementação permaneceu com Luna Max. Não foi solicitado outro revisor/subagente.

Nenhum teste repetido, Docker, rede, leitura de `.env`, alteração de produção ou mutação Git foi feito por este revisor. A única escrita desta rodada é este relatório. A revisão de tarefa anterior permanece em `stage-7-task-review.md`; este parecer examina o intervalo imutável completo e atualiza a situação das provas.

## Pontos fortes verificados no diff

O módulo puro mantém a política pequena e rastreável: tuplas imutáveis de limiares, versão fixa `mvp-1`, ausência de relógio/RNG/I/O e resultado derivado pelo máximo dos quatro fatores (`risk_engine.py:12–37,93–145`). Chuva/vento/rajada têm comparações inclusivas; visibilidade estrita tem ordenação correta para selecionar a maior severidade. Classes cobrem todo o domínio inteiro 0–100 (`risk_engine.py:148–158`).

`RiskInputs` normaliza somente int/float finitos não negativos, proíbe bool, trata overflow e falha com erro de domínio. Inputs e resultado são frozen; componentes são retornados em dict novo. O helper valida as 17 chaves compartilhadas, envelope/schema, localidade, contagem e metadados temporais antes de copiar e enriquecer (`risk_engine.py:44–71,83–116,161–219`). A saída tem 21 campos; preserva os valores de entrada e troca somente `operation`, além dos quatro novos campos.

O adaptador mantém as responsabilidades claras: parse, helper puro, serialização JSON com `allow_nan=False`, depois incremento do contador. Falhas não geram LOW artificial nem contam saída inválida (`flink_risk.py:14–34`). Há testes para NaN também num campo não usado pelo score, verificando a falha de serialização e contador zero.

## Grafo, UIDs, estado e regressões

O job mudou somente pelo import e pelas duas linhas do ramo de risco após o stream temporal. A transformação tem tipo STRING e name/UID `weather-risk-assessment`; o PrintSink tem name/UID `weather-risk`. O ramo `weather-metrics` permanece com a saída original de 17 campos (`flink_job.py:309–314`, `flink_risk.py:37–42`).

As UIDs anteriores foram conferidas no código: source, parser, deduplicação, diagnóstico, event-time, janelas, contagem/descarte de atrasados e sinks existentes. Não houve alteração do MapState descriptor, TTL 86400, cache zero, acumulador, janela 30/5, watermark de dois minutos/idleness 60, lateness zero, assigner, políticas de checkpoint/restart ou grupos operacionais. `temporal_metrics.py` apenas acrescenta o `frozenset` de campos; algoritmo e output não mudaram. Não há novo estado, tópico, serviço, biblioteca, recurso ou ciclo de import.

Os testes SDK constroem grafo real, examinam tipo STRING e presença dos três sinks. As descrições do plano testam presença de operadores, enquanto a atribuição das UIDs foi adicionalmente verificada no source. As verificações SDK legadas de checkpoint/cache/restart seguem exercitando o pipeline ampliado. O diff não toca Compose, Dockerfile, dependências, parser, data quality ou adaptador temporal legado.

## Cobertura contra a spec e o plano inteiro

| Critério de aceite | Verificação estática / situação das provas |
| --- | --- |
| 16 limiares e vizinhos, máximo sem diluição | Unitários isolam cada componente e usam `math.nextafter` abaixo/exato/acima. Incluem empate, fatores simultâneos, extremo isolado, baseline e exemplos aprovados. |
| Domínio, classes, monotonicidade, determinismo e imutabilidade | Classes incluem cortes 24/25/49/50/74/75/100; rejeições incluem bool/None/string/NaN/inf/negativo/overflow. Monotonicidade é testada nos quatro sentidos físicos; cópia/reavaliação e dataclasses frozen são verificadas. O código não usa fontes de variabilidade. |
| Contratos completos 17→21 e adaptador | Helpers verificam keys/envelope/metadata e cópia. SDK chama callback real com contexto métrico controlado e testa saída/erros/contador. |
| Suítes e Ruff em runtimes requeridos | Principal relata pure Python 3.11, SDK PyFlink 1.20.3, suíte geral e Ruff aprovados; números abaixo. Não reexecutados pelo revisor. |
| Kafka→Flink, quatro classes e duplicata extrema | Integração bounded compara 24 métricas e 24 avaliações completas, chaves/componentes/versão e auditorias. Oráculo hardcoded é independente do motor. Execução nova informada e log lido: dois testes reais passaram. |
| Checkpoint legítimo 6→7 | Manifesto/captura já revisados comprovam preparação documentada com grafo sem risco, imagem legada e CP2; teste submete `-s` no caminho desse manifesto, sem `allowNonRestoredState`, e exige estado/dedup restaurados, JSON completo e checkpoint novo concluído. Execução integra o resultado 2/2. |
| Operação única, métricas/risco, Producer, recursos/backpressure | Código mantém configurações e documenta limitações. Verificação operacional e regressões legadas estavam em andamento na abertura desta rodada; dependem do principal. |
| Revisão, scan de credenciais, documentação/commit e limites | Commit de implementação existe; revisão estática sem bloqueadores. Scan final e fechamento documental/operacional devem ser registrados pelo principal. Etapa 8 permanece pendente. |

O oráculo bounded não usa `calculate_risk` para construir o esperado. Compara multiconjuntos de JSONs completos e conjuntos exatos de campos, detectando metadados, contagem ou componentes incorretos, além de score/classe. A cópia extrema de LOW mantém mesma identidade e precisa ser duplicate; a avaliação deve continuar LOW/count 1 (`test_flink_risk.py:114–216,237–389`).

A migração usa as quatro janelas/perfis sem fechamento do grafo antigo. Depois da restauração, exige duplicate para revisões de todos os eventos e avança watermark com sentinel para fechar as janelas existentes. Isso exercita marcadores de dedup e acumuladores, além do ramo novo. Eventos de 2020 com ingestão original em 2020 e revisão ingestion=event+30 s respeitam a política existente; não há afrouxamento de validação ou override de relógio (`test_flink_risk.py:392–623`, helpers legados de submissão/restore).

## Fatos estáticos e evidências de runtime

Os resultados abaixo são execução do principal, não execução deste revisor:

| Evidência | Resultado | Fonte examinada |
| --- | --- | --- |
| Pure novo em Python 3.11 | 110 passed | Relato do principal e ledger no commit |
| SDK novo mais temporal legado, PyFlink 1.20.3 | 40 passed; 17 warnings; 16,55 s | Relato do principal e ledger/relatório |
| Suíte geral leve e Ruff | 482 passed, 22 skips, 6,79 s; Ruff aprovado | Ledger no commit; skips não tratados como integração real |
| Build etapa 7 | Imagem `sha256:4c64deefa385bfbb69d11eabf93313077cb03782b8a5ff02cb5ecdacfc9d4a2a`; JM/TM healthy informados | Ledger no commit |
| Integração nova bounded + restore legado | 2 passed, sem skips, 66,35 s | Relato do principal e `.venv/stage7-risk-integration.log` lido |
| Regressão temporal legada disponível nesta leitura | 3 skipped, 0,09 s, por job QA ativo `weather-flink-qa-acc1d03ec593` | `.venv/stage7-temporal-regression.log` lido; resultado pendente de execução coordenada, não PASS |

O manifesto legado externo ao commit registra origem 6/HEAD `7ae024e`, imagem `18cff02c…`, job `21a03a0b914c98a9d4dbc34357321745`, checkpoint 2 COMPLETED e metadata no volume compartilhado. A captura pré-7 e os payloads originais foram analisados na revisão de tarefa. Nesta rodada não houve inspeção de runtime da imagem ou metadata; a execução reportada dos testes fornece a prova operacional adicional.

O relatório de implementação no commit ainda registra integração pendente, enquanto o ledger já registra seu resultado posterior. Isso representa documento de fase anterior, sem alegação falsa de conclusão. No fechamento, o principal deve consolidar os resultados atuais no relatório/aceite e nos documentos de status, inclusive as regressões/observações operacionais ainda faltantes.

## Comportamentos considerados fora desta etapa

- Persistência PostgreSQL/JDBC, publicação em `weather.alerts` e consumidor de alertas: reservados expressamente à etapa 8; não foram iniciados pelo diff.
- Calibração empírica por veículo/rota/perdas e amplificação de fatores moderados: limites deliberados da política MVP aprovada, descritos no README; não são funcionalidades ausentes desta implementação.
- Garantia transacional ou exatamente uma linha por avaliação no PrintSink: diagnóstico com repetição após recovery é limite explícito do desenho; identidade e cálculo determinísticos são preservados.
- Ensaio de carga de grande escala: o aceite aprovado pede observação sob carga pequena; a prova operacional correspondente permanece com o principal.

## Achados e parecer

P1/P2: nenhum. Não foi identificada regressão concreta de contrato, política, UIDs/estado ou escopo no intervalo revisado. `git diff --check 51aef56..5f9adaf` também retornou sem erro.

**Código pronto para integração: sim, sem correções solicitadas. Etapa 7 declarada concluída por esta revisão: não.** O principal deve concluir as regressões legadas, a operação ativa/recursos/checkpoint, o scan final e a consolidação documental antes de fechar o aceite. Essas são verificações planejadas ainda em execução, não achados de defeito no commit.

## Fechamento posterior pelo principal

Após o parecer estático, o principal concluiu as verificações pendentes: Kafka3passed; temporais2passed e restoreisolado1passed; operacional7RUNNING com restauraçãoCP37/novoCP38, métricas17 e riscos21 deCT/RJ/SP; Producer3/3; backpressureok e recursos/identidades verificados. Scan final e commitsdocumentais registram o fechamento. O parecer independente permanece referente ao intervalo51aef56..5f9adaf. Evidências consolidadas em stage-7-acceptance.md.
