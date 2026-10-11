# Revisão final independente — Etapa 8

**Parecer: aprovado no escopo de revisão de código; nenhum achado P1/P2 confirmado.** O aceite operacional final permanece com o Root e não é declarado concluído por este parecer.

Base `a26109e` → HEAD `bf7ca05101907c8ea3f052bb8e10c9abc258c6bd`. Snapshot `.venv/stage8-final-review.diff`, SHA256 `BB350A7DB5AA69B31EAD0BAEAD838A88240C3D7BB050A182FB82F59E6B5F2A31`. A comparação textual, normalizando somente finais de linha, confirmou igualdade entre o snapshot e o diff desses commits.

Foram consultados a spec e o plano aprovados, os pareceres das três tarefas com suas correções e o relatório de implementação. A análise estática cobriu os módulos de produto, DDL/migração/seed, configuração/build/Compose, wiring e helpers Java; os testes foram consultados para avaliar contratos, oracles, replay, falhas e cleanup. Não foram executados testes, Docker, rede ou leitura de `.env`. Este documento é a única alteração do revisor.

## Conclusões

- A identidade usa os quatro componentes acordados e SHA256 de JSON compacto UTF-8. O contrato preserva avaliação21 e alerta22; somente HIGH/CRITICAL gera alerta. IDs desconhecidos são rejeitados apenas no ramo de negócio.
- A instrução JDBC contém dois parâmetros e termina em DML. A CTE/RETURNING relaciona a gravação da métrica e do alerta na mesma instrução; o trigger recusa avaliação divergente para a mesma identidade. Replay idêntico mantém chaves e conteúdo. A correlação integral do payload com a métrica pertence a esse fluxo controlado, conforme ruling aprovado, sem exigir CHECK entre tabelas ou trigger adicional.
- A migração é transacional e verifica estrutura, catálogo e privilégios antes de reaplicar. As correções anteriores de payload inválido/UNKNOWN, RLS/policies e ACL de coluna estão presentes. A aplicação recebe os grants de negócio acordados; a identidade administrativa permanece fora do Flink.
- Os helpers Java adaptam Row de dois campos aos conectores nativos. JDBC usa parâmetros e ID nullable; Kafka escreve key e value distintos em UTF-8, com `AT_LEAST_ONCE`. URL/repr/CLI não incorporam senha. O Compose fornece a configuração da aplicação ao JobManager e o job serializa as opções necessárias ao TaskManager.
- O grafo acrescenta somente o ramo de negócio após o risco e conserva UIDs e estado anteriores. QA legado fica sem escritas por padrão; o opt-in pela CLI exige auditoria e nome/grupo exclusivos antes de carregar credenciais.
- Os testes reais comparam as colunas SQL e payload/key/hash completos com oracles independentes. Faults conservam a fonte Kafka, restauram C anterior à escrita externa, exigem causa do destino, ausência de checkpoint novo durante a falha e convergência após recuperação. As correções de sigilo dos erros, deadline compartilhado e checkpoint correlacionado por request-ID estão presentes. Cleanup espera estado terminal e limita as exclusões às identidades próprias, sob preflight de slot livre e Producer pausado.

## Evidência recebida e limite do parecer

Os resultados abaixo foram fornecidos pelo Root e pelos registros das tarefas; não são execuções deste revisor:

- PostgreSQL: três integrações reais aprovadas, cold init, duas reaplicações e probes de drift/ACL/RLS; SDK real: 83 aprovados; build e jars verificados.
- Gate final local: 619 aprovados, 30 skips, Ruff aprovado; 26 testes focados aprovados. As oito regressões reais anteriores passaram separadamente. Skips não contam como PASS.
- Delivery/replay: 24 métricas e 12 alertas completos; restauração C4 → C6. Legado7 → 8: C2 → C5, 12 métricas/seis alertas alvo e cleanup do delta de 56 métricas/seis alertas com baseline preservada.
- Fault JDBC: C2 → FAILED sem checkpoint novo → recuperação C3, em 131,84 s. Fault Kafka: mesma sequência, em 188,00 s. Ambos convergiram para 12 métricas/seis alertas e mensagens repetidas equivalentes.

As garantias permanecem por destino: PostgreSQL idempotente por identidade; Kafka pode repetir; não existe atomicidade PostgreSQL/Kafka nem backfill automático das janelas já fechadas. Na emissão deste parecer, o Root ainda conduz a restauração operacional do checkpoint7 preservado e os gates finais de saúde/checkpoint, dados reais, recursos, credenciais e documentação. Esses gates não são substituídos pela aprovação estática.
