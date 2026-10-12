# Progresso do projeto

Concluí as dez etapas do MVP entre 2026-10-07 e 2026-10-12. Este documento registra o que entreguei, os desafios de cada etapa e como validei o trabalho. As contagens de testes correspondem às execuções registradas no fechamento de cada etapa. Testes opt-in pulados não entram como aprovados.

## Etapa 1: fundação da infraestrutura

**Feito:** Montei o Compose com Kafka, PostgreSQL, Flink JobManager e TaskManager. Também configurei bootstrap, healthchecks, volumes e limites de recursos, e criei a estrutura Python e a documentação inicial.

**Desafios e correções:** O primeiro limite de memória impediu o TaskManager de ficar pronto. Ajustei o processo para 1280 MiB e o container para 1536 MiB. Na revisão do README, corrigi o comando `psql` para testar autenticação TCP por senha e expliquei a política de retenção do Kafka.

**Validação:** Os quatro serviços ficaram saudáveis. O broker respondeu, e o Flink registrou um TaskManager e um slot. Também verifiquei autenticação PostgreSQL, privilégios mínimos e persistência após reinício. Ruff passou. Pytest ainda não tinha casos nessa etapa. Registrei a ausência da suíte sem contar isso como aprovação de testes.

## Etapa 2: cliente Open-Meteo

**Feito:** Criei os modelos imutáveis `Location` e `WeatherEvent`, cadastrei três localidades e normalizei o bloco `current`. Também fiz um cliente HTTP com TLS verificado, limite de resposta, timeout e retries controlados.

**Desafios e correções:** Corrigi a conversão de timestamps com offsets que ficavam fora do domínio UTC aceito. JSON muito profundo ou com inteiros extremos agora gera erro de domínio, sem retry indevido. No README, incluí instruções de `PYTHONPATH` para usar o pacote em `src`.

**Validação:** A suíte Python teve 291 aprovações e 1 cenário live pulado; Ruff passou. O smoke opt-in contra a API pública passou em 1 caso. Os dados da API são previsões de modelo. Os timeouts HTTP não definem um prazo total absoluto.

## Etapa 3: Producer e Kafka

**Feito:** Publiquei eventos validados em `weather.raw` e criei e conferi os tópicos. O Producer tem modos contínuo e `--once`, gera logs JSON e usa uma imagem Python não-root. Ele não recebe credenciais PostgreSQL.

**Desafios e correções:** Reforcei a validação dos tópicos existentes, a contagem de confirmações em lotes com falha parcial e o respeito aos prazos de retry. Um erro transitório `NOT_LEADER_FOR_PARTITION` fez o observador de testes atualizar metadata e repetir a operação dentro do prazo configurado.

**Validação:** 329 testes aprovados e 4 opt-in pulados; Ruff passou. Passaram três integrações reais com Kafka/API. Verifiquei o build, a entrega de três localidades, a identidade não-root e o encerramento por SIGTERM. A idempotência de sessão não elimina repetições lógicas entre ciclos ou reinícios.

## Etapa 4: Kafka para Flink

**Feito:** Configurei o runtime PyFlink nos dois nós. O job lê `weather.raw`, usa um parser tipado, registra métricas de ingestão e gera diagnósticos JSON. Também criei os modos bounded para QA e unbounded para operação.

**Desafios e correções:** A primeira execução real falhou porque o runtime PyFlink oferece `get_metrics_group()`, mas o código chamava `get_metric_group()`. Corrigi a chamada e incluí uma regressão. Também passei a rejeitar strings JSON com surrogates inválidos antes de serializá-las para o formato de linha Flink.

**Validação:** 346 testes aprovados e 6 opt-in/SDK pulados; Ruff passou. O SDK PyFlink e uma integração bounded de Kafka para Flink passaram. A execução operacional recebeu confirmação REST do estado `RUNNING` e produziu o diagnóstico da fixture. Checkpoints e recuperação ainda não faziam parte desta etapa.

## Etapa 5: qualidade temporal e deduplicação

**Feito:** Validei a coerência entre o horário do evento, a ingestão e o relógio observado. Defini a identidade do evento por localidade e instante UTC, e implementei deduplicação em `MapState` com TTL de 24 horas e métricas das decisões.

**Desafios e correções:** O QA exigia a duração do bundle em milissegundos numéricos. Uma prova real mostrou que o cache de estado Python do SDK podia manter visível um marcador expirado. Por isso, configurei `python.state.cache-size=0` no pipeline. Também ampliei a janela do teste TTL para acomodar o overhead do Docker e continuar distinguindo expiração de renovação.

**Validação:** 361 testes aprovados e 15 cenários opt-in/SDK pulados; 8 testes do SDK Flink aprovados; Ruff passou. Três integrações reais passaram. A prova TTL confirmou a aceitação do evento, a rejeição da duplicata e uma nova aceitação após a expiração. Uma prova operacional confirmou uma saída para duas cópias.

## Etapa 6: Event Time, janelas e checkpoints

**Feito:** Configurei watermarks que toleram eventos fora de ordem e janelas deslizantes de 30 minutos, com avanço a cada 5 minutos. Também implementei agregações incrementais e checkpoints filesystem para guardar estado e offsets.

**Desafios e correções:** Ajustei `allowed_lateness` para o tipo de milissegundos exigido pelo binding Python. Corrigi a ordem de configuração do assigner para funcionar com o wrapper de idleness. Também ajustei o registro da métrica de eventos tardios, a captura do JobID em falhas de QA e a configuração de retenção no coordenador Flink.

**Validação:** 371 testes aprovados e 19 cenários opt-in/SDK pulados; 31 testes do SDK Flink aprovados; Ruff passou. Em execuções coordenadas, passaram seis cenários reais, incluindo restauração de checkpoint e recuperação de estado. Durante a observação registrada, quatro checkpoints operacionais terminaram sem falhas.

## Etapa 7: motor de risco

**Feito:** Implementei as regras determinísticas `mvp-1` para chuva projetada, vento, rajada e visibilidade. O maior score define a classe de risco. O grafo passou a emitir avaliações de risco e manteve o contrato original das métricas.

**Desafios e correções:** A primeira tentativa de regressão temporal disputou o único slot Flink com outra integração. Não contei os skips dessa tentativa como aprovações e reexecutei os cenários de forma coordenada. A migração confirmou que o novo grafo ainda restaura o checkpoint da etapa anterior.

**Validação:** 482 testes aprovados e 22 cenários opt-in/SDK pulados; 40 testes do SDK Flink aprovados; Ruff passou. Passaram oito cenários reais, incluindo as regressões anteriores e dois cenários novos de risco. Conferi a saída para os contratos de 17 e 21 campos. Os scores são heurísticos e não representam a probabilidade de prejuízo.

## Etapa 8: persistência e alertas

**Feito:** Gravei as avaliações em `climate.weather_metrics`. Só os riscos `HIGH` e `CRITICAL` seguem para `climate.weather_alerts` e `weather.alerts`. Com identidades determinísticas, o UPSERT faz com que o replay convirja para os mesmos registros da avaliação no PostgreSQL.

**Desafios e correções:** Os wrappers Python dos sinks JDBC/Kafka não funcionavam com as interfaces dos conectores nativos na combinação Flink/Java 17. Criei adaptadores Java pequenos e usei linhas `Row` de dois campos para integrar os conectores sem trocar os transportes. Um problema de Metaspace durante várias submissões de QA foi isolado em um processo limpo e coberto por regressão. Nos testes de falha dos destinos, usei checkpoints próprios para verificar o replay.

**Validação:** 619 testes aprovados e 30 cenários opt-in/SDK pulados; 83 testes do SDK Flink aprovados; Ruff passou. Três integrações reais com PostgreSQL verificaram migração, privilégios, replay e atomicidade. Outros cenários reais cobriram entrega, restauração e falhas dos destinos. PostgreSQL e Kafka mantêm entregas independentes at-least-once, sem transação distribuída.

## Etapa 9: consumer de alertas

**Feito:** Montei um consumer de `weather.alerts` que valida o contrato, mantém em memória um cache de duplicatas, gera logs JSON e usa healthcheck. Ele confirma o offset depois de escrever e descarregar cada log.

**Desafios e correções:** Passei a tratar aninhamento JSON excessivo como erro de contrato. Corrigi nomes e tipos das opções da API Kafka nativa, o nome de um módulo de teste que colidia durante a coleta e a falta de logs de ciclo de vida esperados. Um reinício externo do JobManager standalone deixou os serviços saudáveis, mas sem job ativo. Retomei o job usando um checkpoint existente, sem alterar o código Flink nem adicionar alta disponibilidade ou ressubmissão automática.

**Validação:** 692 testes aprovados e 31 cenários opt-in pulados; Ruff passou. O build e a integração real passaram. Verifiquei alerta válido, duplicata equivalente, mensagem inválida, commits de offsets, estado saudável sem tráfego e retomada com o mesmo grupo. Como a deduplicação do consumer fica em memória, reinícios podem repetir logs.

## Etapa 10: fechamento do MVP e CI

**Feito:** Criei o workflow CI com jobs de qualidade Python, integração Kafka e SDK Flink. Também revisei o README e a documentação de arquitetura. As Actions estão fixadas por SHA, e o workflow não publica nem faz deploy.

**Desafios e correções:** A imagem SDK tentava instalar dependências de desenvolvimento com a rede desabilitada. Corrigi essa configuração. Também ajustei a captura UTF-8 da saída de QA no Windows e executei o bloco SDK de novo para confirmar a saída completa. Nesta etapa, um reinício externo do JobManager também deixou os serviços saudáveis, mas sem job ativo. Retomei a operação por um checkpoint existente, sem adicionar alta disponibilidade ou ressubmissão automática.

**Validação:** 695 testes aprovados e 31 cenários opt-in/SDK pulados; Ruff e os builds das imagens Python e Flink passaram. Também passaram os comandos locais equivalentes ao CI: uma integração real com Kafka e 61 testes SDK sem skips. Revisei o workflow estaticamente, mas não o executei no GitHub Actions porque não há remoto configurado. As revisões finais não deixaram achados P1/P2 abertos.

## Limites conhecidos do MVP

O ambiente roda localmente com um broker Kafka e um slot Flink. Não há alta disponibilidade, TLS/SASL/ACL no Kafka nem retomada automática do job após a perda do JobManager. Checkpoints protegem o estado e os offsets do source, mas não tornam os sinks transacionais. O consumer mantém a deduplicação apenas em memória. As observações de recursos vieram de execuções pequenas e não são benchmarks de throughput. A API fornece dados de modelos meteorológicos, e as fixtures de QA não representam as condições atuais.
