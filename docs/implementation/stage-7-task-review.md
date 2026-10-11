# Etapa 7 — revisão independente da tarefa

Parecer: **sem bloqueadores de código identificados**. A implementação e os oráculos examinados atendem ao desenho aprovado. Nenhum achado concreto P1/P2. Este parecer é estático e não declara a etapa concluída: build, suíte geral, integração real, restauração cruzada e aceite operacional pertencem à validação do principal.

## Escopo e método

Revisão contra `docs/superpowers/specs/2026-10-10-risk-engine-design.md` e `docs/superpowers/plans/2026-10-10-risk-engine.md`, pacote `.venv/stage7-task-review.txt` e arquivos atuais correspondentes. Base documental `51aef56`; implementação anterior `7ae024e`. Inspecionados motor puro, adaptador, wiring, constante compartilhada, README, três novos arquivos de teste, helpers legados de integração e SDK, manifesto/captura legada e relatório/ledger.

Nenhum teste, Docker, comando de rede, aprovação, cancelamento de job, alteração de source ou mutação Git foi executado por este revisor. A única escrita desta revisão é este relatório. As contagens informadas pelo principal (110 testes puros em Python 3.11, 40 SDK incluindo legado com 17 warnings, Ruff focado aprovado) são evidências atribuídas ao principal, sem reexecução independente.

## Política, domínio e contrato

- Os 12 limiares de chuva, vento e rajada usam `>=`; os quatro limites de visibilidade usam `<` em ordem que seleciona a faixa mais severa. Não há arredondamento ou epsilon. As tabelas são tuplas de tuplas e a versão é fixa `mvp-1` (`risk_engine.py:12–37,119–145`).
- O score deriva do máximo dos quatro componentes e a classificação cobre inteiros de 0 a 100, com cortes em 25/50/75. Bool, tipo incorreto e score fora do domínio são recusados. Empates não somam nem diluem extremos (`risk_engine.py:93–110,148–158`).
- `RiskInputs` recusa bool, None, strings, negativos, NaN/infinito e inteiro com overflow na conversão, usando `RiskInputError`; valores válidos são normalizados para float. Inputs/resultados são dataclasses frozen com slots; cada serialização dos componentes retorna dict novo (`risk_engine.py:44–71,83–116`).
- O helper verifica exatamente as 17 chaves por `WINDOW_METRIC_FIELDS`, envelope, versão inteira 1, identidade não vazia, contagem positiva e fronteiras/timestamps ordenados. Missing, campo adicional, reentrada de risco ou schema inesperado falham. A cópia preserva os outros 16 valores e acrescenta exatamente quatro campos, totalizando 21 (`risk_engine.py:161–220`).
- A constante é `frozenset` e corresponde à saída real `window_metric`; a mudança em `temporal_metrics.py` apenas acrescenta essa constante. O algoritmo, o acumulador e a serialização legados permanecem iguais.

## Integração e preservação de estado

`AssessWindowRisk` executa loads → enrich → dumps com `allow_nan=False` → incremento. Erros de parse, contrato, fator ou serialização propagam antes do contador; não existe fallback LOW. A transformação usa `Types.STRING()`, name/UID `weather-risk-assessment`, e o sink usa name/UID `weather-risk` (`flink_risk.py:14–42`, `flink_job.py:309–314`).

O diff do job contra `7ae024e` é somente o import e as duas linhas do novo ramo. As UIDs existentes, ramos de ingestão/métricas, deduplicação, cache zero, watermark, janelas, lateness, checkpoint/restart e configurações de execução não foram alterados. Não foi criado estado adicional. A cadeia de imports do módulo puro atravessa somente módulos puros locais; não há dependência de PyFlink/Kafka nem ciclo de import.

Os testes SDK chamam o callback do adaptador real com contexto métrico controlado, examinam o tipo STRING e constroem o pipeline com os três sinks. As descrições do plano isoladamente demonstram presença de operadores; as UIDs explícitas também foram conferidas diretamente no código. As verificações SDK legadas de checkpoint/cache/restart continuam aplicáveis ao pipeline ampliado.

## Testes e oráculos examinados

Os unitários cobrem os 16 limites e seus vizinhos imediatos por `math.nextafter`, com entradas isoladas e verificação do componente correspondente. Cobrem classes, max/empates/extremo, monotonicidade, domínio inválido, overflow, imutabilidade, cópia, metadados e envelope. O SDK verifica que JSON inválido, reentrada, fator inválido e NaN de campo não usado não incrementam o contador.

O cenário bounded gera quatro classes em localidades exclusivas, seis janelas por evento, e uma revisão extrema de LOW com a mesma identidade. Espera LOW/count 1 e auditoria duplicate. O esperado é construído com campos completos e scores hardcoded independentes do motor; compara multiconjuntos de JSON completos e conjuntos exatos de 17/21 chaves, incluindo componentes e versão. Assim, uma avaliação correta com metadados incorretos ou duplicação indevida falha no oráculo (`test_flink_risk.py:114–216,237–389`).

## Proveniência e migração 6 → 7

O manifesto registra origem etapa 6, HEAD `7ae024e`, `has_risk_branch=false`, imagem legada `sha256:18cff02c4741bfad83e4efeb269b4363c9c6d9264201467685213e19f66ce3a2` e hashes dos três sources legados. O checkpoint 2 é do job próprio `21a03a0b914c98a9d4dbc34357321745`, status COMPLETED, com metadata no volume compartilhado. O script/log de captura e ledger registram a preparação antes da implementação/deploy 7, aceites dos quatro perfis, seleção de checkpoint disparado após esses aceites e legibilidade nos dois nós após cancelamento. Trata-se de evidência documental examinada, sem inspeção independente da imagem/metadata em runtime.

O teste restaura o caminho do manifesto pelo helper CLI `-s`, mantém nome/grupo, exige novo JobID e REST latest.restored com o id legado. Não usa `allowNonRestoredState`. Publica revisões dos quatro eventos, exige duplicate, avança watermark com sentinel e compara os JSONs completos das janelas existentes; portanto também exercita restauração dos marcadores de dedup e do acumulador, além do novo ramo stateless. Depois exige um checkpoint novo COMPLETED e que o checkpoint restaurado continue sendo o legado (`test_flink_risk.py:392–626`; helper legado `test_flink_temporal.py:319–499`).

Eventos históricos em 2020 com ingestão original em 2020 são válidos pela política real: ela limita ingestion−event a 24 h e ingestão futura relativa ao relógio, sem exigir evento recente. A revisão usa ingestion=event+30 s; não há override de relógio nem afrouxamento de validação. O sentinel é temporalmente coerente e fecha as seis janelas históricas.

## Evidência vinculada ao código revisado

SHA-256 dos arquivos lidos nesta revisão:

| Arquivo | SHA-256 |
| --- | --- |
| `risk_engine.py` | `fe89e5b958c83808aaa9428b5fe127329af4dbd43bf30435fb1dca17312de130` |
| `flink_risk.py` | `6ac100e52f495681a7097acc42194a019e877aa74a80942bdf0fe05ea102c5b0` |
| `flink_job.py` | `e4a44d79e4679c18d94ec915bc99bcf8912dc6c9a0cb907fc070ecd199e2b43f` |
| `temporal_metrics.py` | `47214a8772f9b4afd2305837de3b771deab175674e71d6d96ee806d862323d8e` |
| `test_risk_engine.py` | `49bff3a8f78350fe21d234b757a6499633642dee3992b3ad235ce2c08a37d3b7` |
| `test_flink_risk_sdk.py` | `b6769186f824ec691548a124845bfeefb63d3f02904d2f283124634e61ef770a` |
| `test_flink_risk.py` | `0fd97ccff258611ec196ccb32804f2b92df33ff752e5062020ba56849c32f0db` |
| `.venv/stage7-task-review.txt` | `ec6bf2dbbb8c0ec1605413ea33a98eda2cb2d5006e4f127d18aae46987c80fc4` |

Não há correção solicitada por esta revisão. O aceite final deve incorporar as evidências reais ainda reservadas ao principal e manter etapa 8 pendente.
