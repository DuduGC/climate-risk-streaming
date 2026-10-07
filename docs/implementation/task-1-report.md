# Tarefa 1 — Infraestrutura: entrega para revisão

Status: arquivos materializados; validação de containers, autenticação e persistência fica pendente para o agente principal.

## Arquivos

- `.env.example` contém os nomes locais do banco e das roles, com senhas vazias para exigir configuração local.
- `compose.yaml` define Kafka KRaft de nó único, JobManager/TaskManager Flink e PostgreSQL, com volumes nomeados, recursos, reinício e healthchecks limitados.
- `docker/postgres/01-bootstrap.sh` cria a role de aplicação e o schema `climate` em uma transação, revoga `CREATE` em `public` e concede somente `CONNECT`/`USAGE` necessários. Os identificadores e a senha são escapados pelo psql (`%I`, `%L` e interpolação de variável tipada).
- `docker/flink/healthcheck.sh` verifica o REST do JobManager e, no TaskManager, consulta `/taskmanagers` e exige que o caminho RPC registrado contenha o hostname configurado do container.

## Checks e expectativas

Executar com segredos locais gerados pelo principal, sem imprimir a configuração expandida:

1. `docker compose --env-file .env.example config --quiet` deve falhar com a mensagem de senha obrigatória vazia.
2. Com `.env` preenchido, `docker compose config --quiet` deve retornar código 0.
3. `docker compose up -d` deve chegar a estado saudável para os quatro serviços. Kafka deve responder a `kafka-broker-api-versions.sh`; PostgreSQL deve responder a `pg_isready`; o JobManager deve servir `/overview`; o TaskManager deve aparecer em `/taskmanagers` pelo hostname `flink-taskmanager`.
4. Depois de `docker compose down` e novo `docker compose up -d`, Kafka e PostgreSQL devem recuperar a identidade/estado persistido pelos volumes nomeados.
5. Em volume de PostgreSQL separado e credencial temporária, confirmar que login da aplicação funciona, `rolsuper`/`rolcreatedb`/`rolcreaterole` são falsos, a role não possui `CREATE` em `public` nem em `climate`, e que a senha contendo aspas e barras invertidas funciona. Não reutilizar o volume principal nesse teste.

## Resultados desta entrega

- `docker compose --env-file .env.example config --quiet`: falhou como esperado, pois as duas senhas estão vazias.
- `docker compose --env-file .env.example config --quiet` com placeholders temporários apenas no ambiente do processo: passou (exit 0), sem imprimir a configuração expandida e sem criar `.env`.
- O Docker CLI emitiu aviso de acesso negado ao arquivo de configuração do usuário, mas ambas as validações de Compose foram concluídas com os resultados acima.
- Não executei pulls, build, subida de containers, checks reais ou teste de senha especial; o principal assumiu a verificação runtime e confirmou que as imagens Kafka e PostgreSQL já foram baixadas.
- A verificação sintática Bash via WSL não pôde iniciar por `E_ACCESSDENIED`; não foi possível fazer o check local dos scripts.
- Não foi criado Dockerfile Flink: a inspeção fornecida pelo principal confirmou que a imagem contém `curl`, e o healthcheck usa apenas `sh`, `hostname`, `curl` e `grep`.
- O principal reproduziu falha de inicialização do TaskManager com `taskmanager.memory.process.size: 768m`: a memória reservada para framework, managed e network superava a memória Flink disponível. Ajustei o processo do TaskManager para `1280m` e o limite do container para `1536m`; o JobManager permanece em `768m` com limite `1g`. O reinício e a prontidão após a correção ainda precisam de validação runtime pelo principal.
