# Revisão independente — Etapa 8, Tarefa 2

**Parecer:** nenhum achado P1/P2 confirmado no snapshot revisado. A implementação dos sinks nativos, configuração e wiring está coerente com os requisitos da Task 2. Este parecer não comprova entrega externa ou recuperação em runtime.

## Escopo e evidência

A revisão usou `docs/implementation/stage-8-task-2-brief.md`, `docs/implementation/stage-8-task-2-report.md` e o snapshot estável `.venv/stage8-task2-review.diff`, baseado em `a88fb65`. Foram analisados configuração, preparação de registros, helpers Java, factories JDBC/Kafka, wiring operacional/QA, Dockerfile, checksums, Compose, documentação e testes afetados. A SQL da Task 1 foi consultada apenas para verificar sua compatibilidade com o batch JDBC; não foi alterada.

Os resultados fornecidos pelo Root foram considerados como evidência de validação: suíte completa Python 3.11 com 595 aprovados e 26 ignorados; Ruff aprovado; suíte SDK real PyFlink 1.20.3 com 83 aprovados; build nativo aprovado na imagem `sha256:29ac52a48276e05d227e1f20b98f480f07e5653df6567b34b8dfa1277ca9e635`; JobManager e TaskManager saudáveis com UID 9999, Python 3.11.14 e verificações dos jars/classes. Esta revisão não executou testes, Docker ou acessos de rede.

## Conclusões

- O serializer Java valida `Row` de dois campos não nulos `String`, escreve `location_id` como key e JSON como value em UTF-8, com tópico fixo. Não implementa transporte próprio.
- O binder JDBC valida payload e ID nullable, usa `setString` e `setNull(2, Types.VARCHAR)`. A SQL parametrizada termina em `INSERT`; o `RETURNING` está dentro da CTE e não devolve um result set final ao batch.
- As factories Java públicas e os wrappers PyFlink mantêm os conectores nativos, contornando incompatibilidades dos builders Python da versão instalada. O Kafka usa `AT_LEAST_ONCE` e as propriedades limitadas especificadas; o JDBC preserva os defaults 100/2000/3, connect/socket timeout de 10 segundos e validação de conexão de 5 segundos.
- A configuração é imutável, omite a senha do `repr` e da URL JDBC e produz erros de validação sem interpolar valores sensíveis. O Compose fornece somente a configuração de aplicação ao JobManager; as opções são serializadas para o TaskManager.
- O wiring acrescenta os sinks após a avaliação de risco, preserva os operadores e UIDs anteriores e usa os novos UIDs fixos. Localidades desconhecidas são rejeitadas apenas no ramo de negócio. Os contadores indicam preparação/rejeição, sem afirmar entrega.
- A CLI valida audit e identidade QA antes de carregar credenciais no opt-in. Execuções com prefixo QA mantêm os sinks desabilitados por padrão; a passagem explícita de `business_sinks` pela API representa opt-in deliberado.

## Limites e gates da Task 3

A entrega real de avaliações LOW/HIGH ao PostgreSQL e de alertas ao Kafka, a convergência após replay e a recuperação a partir de checkpoint ainda precisam ser comprovadas na Task 3. Também permanecem pendentes as medições de deadline sob falha do banco/broker e a verificação da propagação de falhas sem confirmação falsa de entrega.

O fixture Java de `getParameters` verifica o contrato e a forma dos valores, mas não chama `accept` sobre um `PreparedStatement` real. A implementação de `accept` está correta na leitura estática; uma avaliação LOW persistida no runtime deve comprovar o bind nulo. O serializer foi verificado no JVM SDK, mas a mensagem consumida do broker ainda deve confirmar key/value no fluxo completo.

Os resultados SDK/build aprovados sustentam a conclusão da Task 2. Não equivalem a um gate de runtime PostgreSQL/Kafka, replay ou fault QA aprovado.
