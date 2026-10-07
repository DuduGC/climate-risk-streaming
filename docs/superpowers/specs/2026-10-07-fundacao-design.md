# Etapa 1 — Fundação e ambiente

Status: especificação aprovada pelo usuário e Etapa 1 implementada/verificada em 2026-10-07. Evidências em `docs/progresso.md`.

## Objetivo e limites

Criar a fundação local da Climate Logistics Streaming Platform conforme os requisitos fornecidos pelo usuário. Kafka transportará eventos; Flink será o motor de processamento; PostgreSQL armazenará resultados de negócio. A entrega desta etapa é infraestrutura funcional, sem integração Open-Meteo ou processamento de eventos.

O desenvolvimento permanece dividido nas dez etapas fornecidas. Não iniciar a Etapa 2 antes do aceite real da Etapa 1. Agentes implementadores devem usar `gpt-6-luna` com esforço `max`; o agente principal coordena, revisa e verifica.

## Estado observado

- Diretório inicialmente vazio, sem repositório Git e sem AGENTS.md encontrado no diretório ou nos ancestrais consultados.
- Docker CLI e Docker Compose presentes.
- Consulta ao daemon negada pelo sandbox; disponibilidade do engine e suporte a containers Linux ainda não verificados.
- Nenhum container, teste ou build executado. Este documento não comprova o critério de aceite.

## Abordagem

Recomendação: Docker Compose local com quatro serviços persistentes nesta etapa: `kafka`, `flink-jobmanager`, `flink-taskmanager` e `postgres`. Usar uma rede Compose compartilhada, DNS pelos nomes dos serviços, volumes nomeados para Kafka/PostgreSQL e política de reinício dos serviços.

Alternativa: instalar serviços diretamente no Windows, com maior esforço de reprodução e divergência do requisito Docker. Alternativa de múltiplos brokers ou TaskManagers aumenta consumo e configuração sem necessidade demonstrada. Adotar o Compose mínimo.

Os containers `producer` e `alert-consumer` serão acrescentados nas etapas 3 e 9, respectivamente. Não criar processos fictícios para simular esses componentes. Não adicionar serviços ou tecnologias fora da stack obrigatória.

## Serviços e contratos

### Kafka

Um broker KRaft com papéis broker/controller combinados, fator de replicação 1 e volumes persistentes. Listener interno anunciado como `kafka:9092`. Sem publicação de porta no host inicialmente; ferramentas podem operar por `docker compose exec`. Sem criação automática de topics para evitar nomes acidentais. Topics serão criados explicitamente na Etapa 3: `weather.raw` e `weather.alerts`, começando com três partições cada.

Healthcheck deve consultar o protocolo Kafka usando as ferramentas disponíveis na imagem, com timeout limitado. Retenção e limites locais documentados; nenhum dimensionamento de produção implícito.

### Flink

Cluster standalone de sessão com um JobManager, um TaskManager e paralelismo inicial 1. Console e REST publicados apenas em `127.0.0.1:8081`. Configurar endereço RPC do JobManager, slots e limites de memória explícitos.

Healthchecks devem verificar REST do JobManager e registro efetivo do TaskManager, com ferramenta comprovadamente disponível na imagem ou imagem derivada mínima. Não considerar um container em execução como prova de prontidão. Nesta etapa não existe job; instalação de PyFlink e conectores Kafka/JDBC será validada na Etapa 4 antes do primeiro job.

### PostgreSQL

Sem porta publicada no host. Volume nomeado. Credenciais locais obrigatórias fornecidas por `.env`, sem senha padrão funcional no arquivo de exemplo.

Bootstrap usa uma identidade administrativa somente para inicialização e cria usuário exclusivo da aplicação sem SUPERUSER, CREATEDB ou CREATEROLE. O usuário da aplicação terá apenas CONNECT e acesso necessário ao schema da aplicação; tabelas e privilégios DML específicos entram na Etapa 8. Não conceder criação de objetos à identidade usada pelo processamento.

Healthcheck via `pg_isready`; teste separado comprova login real da aplicação e ausência de privilégios administrativos. SQL não deve interpolar senhas em texto sem escaping seguro. Bootstrap em volume novo deve ser documentado, pois scripts de inicialização não reaplicam em volume existente.

## Versões e dependências

Baseline proposta: Kafka `apache/kafka:3.9.1`, Flink `flink:1.20.3-scala_2.12-java17`, PostgreSQL `postgres:17.11-bookworm` e Python 3.11 para os componentes futuros. São versões explícitas para compatibilidade, não uma alegação de versões mais recentes. A documentação de PyFlink 1.20 inclui Python 3.11. O catálogo oficial de imagens lista a tag PostgreSQL indicada.

As imagens e os executáveis dos healthchecks devem ser verificados por pull/inspeção e execução antes do aceite. Não prometer compatibilidade de conectores ainda não testados.

Criar `pyproject.toml` e dependências de desenvolvimento Pytest/Ruff com versões explícitas. Separar dependências futuras de producer/consumer das de PyFlink para não carregar o runtime Flink em todos os containers. Nenhuma instalação de bibliotecas HTTP/Kafka/banco é necessária para simular funcionalidade inexistente.

## Arquivos previstos

- `.gitignore`, `.dockerignore` e `.env.example`.
- `compose.yaml` com infraestrutura.
- `pyproject.toml` e arquivos de dependências de desenvolvimento.
- Diretórios `src/`, `tests/unit/`, `tests/integration/`, `docker/`, `sql/`, `docs/` preservados no Git conforme necessário.
- Bootstrap PostgreSQL limitado ao banco/usuário/schema, sem tabelas de negócio antecipadas.
- `README.md` com preparação de `.env`, comandos, requisitos, verificação e diagnóstico.
- Documento de progresso registrando evidências e pendências por etapa.

Inicializar Git sem alterar configuração global e sem criar remoto ou publicar conteúdo.

## Segurança e operação

Ignorar `.env`, ambientes virtuais, caches, logs e artefatos locais. Não registrar credenciais em logs ou saídas de validação. Usar imagens oficiais e usuário não-root quando suportado. Não montar socket Docker nos serviços. Healthchecks com intervalos e retries limitados. Restart policy não substitui retry limitado da lógica de aplicação futura.

Kafka PLAINTEXT será restrito à rede Docker local; documentar TLS, SASL e ACLs como melhorias futuras. Flink REST local sem autenticação é limitação explícita do MVP. Não executar limpeza de volumes sem solicitação, pois remove dados persistentes.

## Delegação e revisão

Após aprovação da especificação, preparar plano de implementação. Um agente Luna Max implementará Compose/bootstrap; outro poderá cuidar da estrutura/dependências/documentação se os arquivos atribuídos forem independentes. Evitar sobreposição de arquivos. O agente principal revisará arquitetura, segurança, healthchecks, persistência e consumo de recursos, solicitará correções e executará a validação.

## Critérios de aceite

1. Configuração Compose resolve sem erros e sem revelar segredos.
2. Após preparar `.env`, `docker compose up -d` sobe os quatro serviços e estes ficam saudáveis dentro de um prazo de validação limitado.
3. Comando administrativo Kafka confirma broker acessível.
4. REST Flink confirma um TaskManager registrado e slots disponíveis.
5. Consulta autenticada PostgreSQL com a identidade da aplicação confirma acesso ao banco e privilégios limitados.
6. Reiniciar serviços preserva dados/identidade de cluster nos volumes e recupera prontidão.
7. Ruff e verificações pertinentes passam. Não apresentar uma suíte vazia de Pytest como prova de funcionamento; cenários funcionais surgem nas etapas seguintes.
8. Registrar comandos, resultados, limitações e correções. Somente então reportar `ETAPA 1 CONCLUÍDA`.

Se acesso ao daemon, download de imagens ou execução Linux não estiver disponível, preservar arquivos e reportar a verificação pendente. Não aprovar a etapa nem avançar com base apenas em inspeção estática.

## Fontes oficiais consultadas

- Kafka Docker: https://kafka.apache.org/39/getting-started/docker/
- Kafka 3.9.1 quickstart: https://kafka.apache.org/39/getting-started/quickstart/
- Tags Flink: https://hub.docker.com/_/flink/tags?name=1.20.3&page=1
- PyFlink 1.20: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/installation/
- Flink Docker standalone: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/
- Catálogo PostgreSQL oficial: https://github.com/docker-library/official-images/blob/master/library/postgres
