# Progresso do projeto

As dez etapas do MVP foram concluídas entre 2026-10-07 e 2026-10-12. Este registro reúne o objetivo, a entrega, os principais desafios e as validações de cada etapa. As contagens de testes abaixo vêm das execuções documentadas no fechamento correspondente; testes opt-in pulados não são apresentados como aprovados.

## Etapa 1 — fundação da infraestrutura

**Feito:** Compose com Kafka, PostgreSQL, Flink JobManager e TaskManager; bootstrap, healthchecks, volumes, limites de recursos, estrutura Python e documentação inicial.

**Desafios e correções:** o primeiro limite de memória não permitiu a prontidão do TaskManager; o processo foi ajustado para 1280 MiB e o container para 1536 MiB. A revisão do README também corrigiu o comando `psql` para testar autenticação TCP por senha e esclareceu a política de retenção do Kafka.

**Validação:** os quatro serviços ficaram saudáveis, o broker respondeu, o Flink registrou um TaskManager e um slot, e verificações operacionais confirmaram autenticação PostgreSQL, privilégios mínimos e persistência após reinício. Ruff passou. Pytest ainda não tinha casos nessa etapa; isso foi registrado como ausência de suíte, não como aprovação de testes.

## Etapa 2 — cliente Open-Meteo

**Feito:** modelos imutáveis `Location` e `WeatherEvent`, catálogo de três localidades, normalização do bloco `current` e cliente HTTP com TLS verificado, limites de resposta, timeout e retries controlados.

**Desafios e correções:** corrigida a conversão de timestamps com offsets que saíam do domínio UTC aceito. JSON excessivamente profundo ou com inteiros extremos passou a virar erro de domínio, sem retry indevido. O README ganhou instruções de `PYTHONPATH` para o pacote em `src`.

**Validação:** suíte Python: 291 aprovados e 1 cenário live pulado; Ruff passou. O smoke opt-in contra a API pública passou com 1 caso. Os dados continuam sendo previsões de modelo, e os timeouts HTTP não formam um prazo total absoluto.

## Etapa 3 — Producer e Kafka

**Feito:** publicação dos eventos validados em `weather.raw`, criação e conferência dos tópicos, CLI contínua e `--once`, logs JSON e imagem Python não-root. O Producer não recebe credenciais PostgreSQL.

**Desafios e correções:** foram reforçadas a validação da topologia de tópicos existentes, a contagem de confirmações em lotes com falha parcial e a aplicação dos prazos de retry. Um erro transitório `NOT_LEADER_FOR_PARTITION` levou o observador de testes a atualizar metadata e repetir somente dentro do prazo configurado.

**Validação:** 329 testes aprovados e 4 opt-in pulados; Ruff passou. Três integrações reais com Kafka/API passaram. O build, a entrega de três localidades, a identidade não-root e o encerramento por SIGTERM foram verificados. Idempotência de sessão não elimina repetição lógica entre ciclos ou reinícios.

## Etapa 4 — Kafka para Flink

**Feito:** runtime PyFlink comum aos dois nós, leitura de `weather.raw`, parser tipado, métricas de ingestão, diagnóstico JSON e modos bounded para QA e unbounded para operação.

**Desafios e correções:** a primeira execução real falhou porque o runtime PyFlink usa `get_metrics_group()`, e não `get_metric_group()`. A chamada foi corrigida e ganhou regressão. Também foram rejeitadas strings JSON com surrogates inválidos antes da serialização para o formato de linha Flink.

**Validação:** 346 testes aprovados e 6 opt-in/SDK pulados; Ruff passou. O SDK PyFlink e uma integração Kafka→Flink bounded passaram. A execução operacional recebeu confirmação REST de estado `RUNNING` e produziu diagnóstico da fixture. Checkpoints e recuperação ainda não faziam parte desta etapa.

## Etapa 5 — qualidade temporal e deduplicação

**Feito:** validação de coerência entre horário do evento, ingestão e relógio observado; identidade por localidade e instante UTC; deduplicação em `MapState` com TTL de 24 horas e métricas de decisões.

**Desafios e correções:** o QA exigia a duração do bundle em milissegundos numéricos. Uma prova real mostrou que o cache de estado Python do SDK podia manter visível um marcador expirado; o pipeline passou a configurar `python.state.cache-size=0`. A janela do teste TTL foi ampliada para dar margem ao overhead do Docker e continuar distinguindo expiração de renovação.

**Validação:** 361 testes aprovados e 15 cenários opt-in/SDK pulados; SDK Flink: 8 aprovados; Ruff passou. Três integrações reais passaram. A prova TTL confirmou aceitação, rejeição de duplicata e nova aceitação após expiração; uma prova operacional confirmou uma saída para duas cópias.

## Etapa 6 — Event Time, janelas e checkpoints

**Feito:** watermarks com tolerância a desordem, janelas deslizantes de 30 minutos com avanço de 5 minutos, agregações incrementais e checkpoints filesystem que guardam estado e offsets.

**Desafios e correções:** `allowed_lateness` foi ajustado para o tipo de milissegundos exigido pelo binding Python. A ordem de configuração do assigner foi corrigida para sobreviver ao wrapper de idleness. Também foram ajustados o registro da métrica de eventos tardios, a captura do JobID em falhas de QA e a configuração de retenção no coordenador Flink.

**Validação:** 371 testes aprovados e 19 cenários opt-in/SDK pulados; SDK Flink: 31 aprovados; Ruff passou. Seis cenários reais foram aprovados em execuções coordenadas, incluindo restauração de checkpoint e recuperação de estado. Quatro checkpoints operacionais concluíram sem falhas durante a observação registrada.

## Etapa 7 — motor de risco

**Feito:** regras determinísticas `mvp-1` para chuva projetada, vento, rajada e visibilidade; o maior score define a classe. O grafo passou a emitir avaliações de risco mantendo o contrato original das métricas.

**Desafios e correções:** a primeira tentativa de regressão temporal disputou o único slot Flink com outra integração; os skips dessa tentativa não foram contados como aprovação e os cenários foram reexecutados de forma coordenada. A migração comprovou que o novo grafo ainda restaura checkpoint da etapa anterior.

**Validação:** 482 testes aprovados e 22 cenários opt-in/SDK pulados; SDK Flink: 40 aprovados; Ruff passou. Oito cenários reais passaram, incluindo as regressões anteriores e dois cenários novos de risco. A saída foi conferida para os contratos de 17 e 21 campos. Os scores são heurísticos e não representam probabilidade de prejuízo.

## Etapa 8 — persistência e alertas

**Feito:** avaliações persistidas em `climate.weather_metrics`; apenas riscos `HIGH` e `CRITICAL` seguem para `climate.weather_alerts` e `weather.alerts`. Identidades determinísticas e UPSERT permitem replay convergente no PostgreSQL.

**Desafios e correções:** os wrappers Python dos sinks JDBC/Kafka eram incompatíveis com as interfaces dos conectores nativos na combinação Flink/Java 17; adaptadores Java pequenos e linhas `Row` de dois campos resolveram a integração sem trocar os transportes. Um episódio de Metaspace durante várias submissões de QA foi isolado em processo limpo e coberto por regressão; testes de falha dos destinos usaram checkpoints próprios para verificar replay.

**Validação:** 619 testes aprovados e 30 cenários opt-in/SDK pulados; SDK Flink: 83 aprovados; Ruff passou. Três integrações PostgreSQL reais verificaram migração, privilégios, replay e atomicidade. Cenários reais também cobriram entrega, restauração e falhas dos destinos. PostgreSQL e Kafka mantêm entregas independentes at-least-once; não há transação distribuída.

## Etapa 9 — consumer de alertas

**Feito:** consumer de `weather.alerts` com validação estrita do contrato, supressão volátil de duplicatas, logs JSON, healthcheck e confirmação de offset após escrever e descarregar o log.

**Desafios e correções:** aninhamento JSON excessivo passou a ser tratado como erro de contrato. Foram corrigidos nomes e tipos de opções da API Kafka nativa, o nome de um módulo de teste que colidia na coleta e a ausência de logs de ciclo de vida esperados. Um reinício externo do JobManager standalone deixou os serviços saudáveis, mas sem job ativo; a operação retomou o job por checkpoint existente, sem alterar o código Flink nem adicionar alta disponibilidade ou ressubmissão automática.

**Validação:** 692 testes aprovados e 31 cenários opt-in pulados; Ruff passou. Build e integração real passaram; foram verificados alerta válido, duplicata equivalente, mensagem inválida, commits de offsets, estado saudável sem tráfego e retomada com o mesmo grupo. A deduplicação do consumer fica em memória, então reinícios podem repetir logs.

## Etapa 10 — fechamento do MVP e CI

**Feito:** workflow CI com jobs de qualidade Python, integração Kafka e SDK Flink; README e documentação de arquitetura revisados. As Actions estão fixadas por SHA e o workflow não publica nem faz deploy.

**Desafios e correções:** a imagem SDK combinava instalação de dependências de desenvolvimento com rede desabilitada; a configuração foi corrigida. A captura UTF-8 da saída de QA no Windows também foi ajustada, e o bloco SDK foi executado novamente para confirmar a saída completa. Também na etapa 10, um reinício externo do JobManager deixou os serviços saudáveis, mas sem job ativo; a operação foi retomada por checkpoint existente, sem adicionar alta disponibilidade ou ressubmissão automática.

**Validação:** 695 testes aprovados e 31 cenários opt-in/SDK pulados; Ruff e builds das imagens Python e Flink passaram. Os comandos locais equivalentes ao CI passaram: uma integração Kafka real e 61 testes SDK sem skips. O workflow foi revisado estaticamente, mas não foi executado no GitHub Actions; não há remoto configurado. Revisões finais não deixaram achados P1/P2 abertos.

## Limites conhecidos do MVP

O ambiente é local, com um broker Kafka e um slot Flink; não há alta disponibilidade, TLS/SASL/ACL no Kafka ou retomada automática do job após perda do JobManager. Checkpoints protegem estado e offsets do source, mas não tornam os sinks transacionais. O consumer mantém deduplicação apenas em memória. As evidências de recursos são observações de execuções pequenas, não benchmarks de throughput. Dados da API vêm de modelos meteorológicos, e fixtures de QA não representam condições atuais.
