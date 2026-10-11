# Revisão independente — Etapa 8, Task 1

**Veredito: requer correções antes do commit da Task 1.** Dois achados P2 demonstráveis por análise estática. Nenhum P1 identificado.

## Escopo e evidência

Brief e relatório lidos nessa ordem. Revisão do snapshot `.venv/stage8-task1-review.diff`, relativo ao BASE `ff1e5ef`, sem commit da Task 1. O primeiro retorno truncou o miolo do diff; somente os trechos omitidos foram recuperados do próprio snapshot. Foram consultados `risk_engine.py` e `01-bootstrap.sh` para conferir os contratos e privilégios antecedentes. Não foram executados testes, Docker, rede, leitura de `.env` nem alterações de implementação. Este documento é a única escrita do revisor.

O Root relatou Python 3.11: **573 passed, 24 skipped, 7,18 s**, Ruff aprovado, migração no volume existente reaplicada duas vezes e integração PostgreSQL **2 passed, 2,60 s**. Estes resultados são evidência relatada pelo Root, não execuções deste revisor. O initdb e os probes de drift isolados ainda estavam em execução no momento da revisão. As quatro falhas SQL anteriores foram corrigidas e revertidas, conforme relatório; `NOTICE` benigno de objeto já existente não é falha funcional.

## Pontos fortes e aderência

- O contrato Python tem 21 campos exatos, verifica bool separadamente de int, UTF-8, números finitos, BIGINT, intervalos e extremos temporais, e reutiliza o motor de risco. Identidade SHA-256 e serialização determinística possuem testes com hashes fixos ASCII/Unicode e preservação da entrada.
- A SQL usa exatamente dois parâmetros e DML final, insere os 19 campos tipados das métricas e deriva o alerta de 22 campos da mesma entrada. O `RETURNING` liga as CTEs, e a FK junto com a instrução única conserva a atomicidade entre métrica e alerta.
- O trigger recusa qualquer mudança nos valores de uma métrica existente; o replay idêntico conserva a identidade. Seed derivado de `LOCATIONS`, transação administrativa, owners e atributos de role incluindo replication estão presentes. Compose altera somente os mounts solicitados.
- Os testes existentes exercitam replay, divergência e rollback após falha no alerta. Os achados abaixo envolvem entradas negativas e drift que esses dois testes de integração não cobrem.

## Achados

### [P2] CHECK do alerta permite payload fora do contrato de 22 campos

**Referência:** `docker/postgres/migrations/001-business-sinks.sql:562`, especialmente linhas 584–594.

As comparações `payload->>'component' = 'flink'`, operation/schema/id/identidade/score/level/rules não são protegidas contra SQL NULL e não verificam o tipo JSON. A chave presente com valor JSON `null` satisfaz o teste de presença e produz SQL NULL em `->>`; se os demais termos forem verdadeiros, o CHECK resulta UNKNOWN, que PostgreSQL aceita. Além disso, os campos temporais e meteorológicos listados no teste de chaves não recebem validação de tipo, faixa, ordem ou correspondência à métrica. Por exemplo, `event_count: true` é aceito.

**Consequência:** a role da aplicação, que tem UPDATE em alertas, pode persistir um alerta inválido sem alterar a métrica válida. A garantia solicitada de payload22 JSONB validado fica incompleta, ainda que o caminho Python normal produza payload correto.

**Probe mínimo para o Root, após criar um alerta válido e em transação com ROLLBACK:**

```sql
UPDATE climate.weather_alerts
SET payload = jsonb_set(payload, '{component}', 'null'::jsonb)
WHERE alert_id = <id válido>;
-- Deve falhar com 23514; a constraint atual permite a atualização.

UPDATE climate.weather_alerts
SET payload = jsonb_set(payload, '{event_count}', 'true'::jsonb)
WHERE alert_id = <id válido>;
-- Deve falhar; atualmente o campo é verificado apenas pela presença da chave.
```

**Correção sugerida:** rejeitar explicitamente NULL/tipos errados e validar todos os campos do payload conforme o contrato; adicionar casos negativos na integração. Fazer o CHECK aceitar apenas TRUE, sem deixar UNKNOWN passar, resolve o primeiro exemplo mas não o segundo.

### [P2] Fingerprint não detecta RLS que impede o sink de escrever

**Referência:** `docker/postgres/migrations/001-business-sinks.sql:62`–64.

O registro de relação inclui kind/owner/persistence/reloptions, mas omite `pg_class.relrowsecurity` e `relforcerowsecurity`; políticas em `pg_policy` também não entram no fingerprint. Ativar RLS sem policies em `weather_metrics` preserva os demais itens do fingerprint. A migração reaplicada aceita o marcador, executa seed como administrador e conserva os grants, podendo concluir COMMIT normalmente. Entretanto, a próxima escrita da aplicação encontra a política default deny de RLS e falha.

**Consequência:** drift incompatível é aceito silenciosamente apesar do requisito de abortar antes do commit; um volume que aparenta ter migrado com sucesso continua inutilizável para o sink.

**Probe mínimo para o Root em cluster isolado:** migrar; como administrador executar `ALTER TABLE climate.weather_metrics ENABLE ROW LEVEL SECURITY`; reaplicar a migração; como aplicação executar a instrução de negócio válida. A reaplicação deve recusar drift com rollback. Pela definição atual, o hash não muda e a falha aparece somente na escrita da aplicação.

**Correção sugerida:** incluir os flags RLS e políticas no fingerprint, ou exigir explicitamente RLS desativado nas três tabelas. Cobrir a reaplicação com RLS ativado em teste/probe negativo.

## Observação a confirmar, não contada como achado

O fingerprint das colunas (`:68`–75) não inclui `pg_attribute.attacl`. Um grant explícito `UPDATE(city)` em `locations` não é registrado no hash. É necessário confirmar no PostgreSQL 17 se o `REVOKE ALL ON TABLE ... FROM app` posterior remove esse grant de coluna. O revisor não afirma que a capacidade UPDATE persiste sem esse probe. Mesmo se o REVOKE reparar o grant, a omissão significa que esse drift não é detectado pelo fingerprint; conferir o resultado e a política de abortar antes de incorporá-lo como achado.

## Comportamentos considerados fora deste gate

- Runtime JDBC/PyFlink, serializer Java e key Kafka: são entregas da Task 2.
- Recuperação de checkpoint, fault tests e atomicidade entre destinos: são entregas da Task 3; nenhuma garantia de exactly-once foi inferida aqui.
- Consumer, cooldown e serviços adicionais: etapa 9 ou explicitamente fora do brief.
- Auditoria de permissões em outros schemas/objetos alheios: fora do schema e dos privilégios de negócio da Task 1.

## Gate recomendado

Corrigir os dois P2 e obter evidência negativa no PostgreSQL real, além de concluir initdb/drift do Root. O caminho normal, replay e organização dos módulos atendem ao brief; as lacunas restantes são garantias persistidas pelo banco e rejeição de schema incompatível.

## Re-review restrita — fix wave 1

**Veredito atualizado: correções aprovadas neste escopo; nenhum P1/P2 novo identificado.** Esta seção substitui o gate pendente acima quanto aos dois achados e à observação de ACL de coluna. Não constitui uma nova revisão completa da etapa 8.

Foram lidos apenas `.venv/stage8-task1-fix1.diff` e a seção de correção/QA do relatório da Task 1, tomando os achados anteriores como referência. O retorno inicial do diff truncou um pequeno trecho do CHECK; somente esse trecho foi recuperado do mesmo snapshot. Não foram executados testes, Docker, rede, leitura de `.env`, commits ou alterações de source.

- **P2 do payload: resolvido.** O CHECK externo termina com `IS TRUE`, rejeitando UNKNOWN causado por JSON `null`. O novo corpo verifica tipos numéricos, contagem positiva dentro de BIGINT, intervalos inteiros ordenados, extremos temporais inteiros dentro da janela e BIGINT, faixas meteorológicas e componentes de risco derivados dos próprios valores do payload. Os testes adicionais exercitam NULL, bool, número excessivo, precipitação negativa, intervalo zero, extremo fora da janela, componente incoerente e chave extra; preservam o payload válido e usam ROLLBACK.
- **P2 de RLS: resolvido.** O fingerprint inclui `relrowsecurity`, `relforcerowsecurity` e definições de `pg_policy`. O preflight recusa explicitamente flags ou políticas nas três tabelas, antes das operações de seed/grants. Assim o drift apontado não pode terminar em COMMIT de reaplicação aparentemente bem-sucedida.
- **Observação de ACL por coluna: resolvida.** O fingerprint passa a incluir `pg_attribute.attacl`; uma mudança de grant por coluna é detectada antes do REVOKE de reparação. O Root confirmou que a versão anterior removia o grant após reaplicar, logo o defeito anterior era aceitar/reparar drift silenciosamente, e não conservar privilégio indevido. A versão corrigida aborta preservando o grant para revisão administrativa.

**Evidência nativa relatada pelo Root, não executada pelo revisor:** cluster isolado confirmou rejeição `23514` para `component:null`/`event_count:true`; reaplicação recusou ACL de coluna e manteve o estado anterior; RLS recusou a reaplicação e a escrita da aplicação continuou negada. Cold init e reaplicação passaram. No banco principal, o Root relatou reparo administrativo pontual somente de metadata, com preflight do fingerprint anterior, zero fatos, RLS desligado, owners e ACLs seguros; em seguida duas reaplicações passaram. Integração final: **3 passed, 3,43 s**; suíte Python 3.11: **573 passed, 25 skipped, 7,15 s**; Ruff aprovado. Logs informados: `.venv/stage8-task1-cold-fixed.log` e `.venv/stage8-task1-postgres-integration-final.log`. O reparo pontual não integra o diff desta re-review e sua implementação não foi inspecionada aqui.

**Fronteira de contrato confirmada pelo Root:** os campos do payload são validados internamente pelo CHECK; a correlação integral do snapshot com a métrica é garantida pela única instrução CTE do fluxo de produto e pela FK de identidade. SQL manual fora desse fluxo pode escrever fatos individualmente válidos; não foi exigido CHECK cruzando tabelas, trigger adicional ou novas colunas nesta Task 1. Essa fronteira não enfraquece a exigência de rejeitar payloads inválidos, que motivou e foi atendida pela correção.
