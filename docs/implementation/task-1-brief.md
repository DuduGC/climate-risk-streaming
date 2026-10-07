# Tarefa 1 — Infraestrutura


**Interfaces:** Compose recebe `POSTGRES_DB=climate_logistics`, `POSTGRES_USER=climate_admin`, `POSTGRES_PASSWORD`, `APP_DB_USER=climate_app` e `APP_DB_PASSWORD`. Senhas obrigatórias e vazias no exemplo. Produz DNS `kafka:9092`, `postgres:5432`, `flink-jobmanager` e console local 8081. Schema PostgreSQL: `climate`; nome do banco e roles escapados como identificadores, senha como literal usando mecanismos seguros de psql.

- [ ] Criar primeiro os checks de prontidão e bootstrap; escrever down-to-up expectations no relatório do agente. Configuração de infraestrutura será provada por checks reais, sem testes que apenas espelhem YAML.
- [ ] Criar `.env.example` e `compose.yaml`. Senhas exigidas por interpolação `${VAR:?mensagem}`; sem defaults funcionais. KRaft single node com ID de cluster estável e diretório de dados persistido; limites de retenção explícitos e heap inicial pequeno.
- [ ] Criar bootstrap PostgreSQL para banco e schema previstos: restringir CREATE em public, criar role LOGIN sem privilégios administrativos, conceder CONNECT/USAGE necessários. Não usar senha interpolada diretamente em SQL e não registrar o valor. Transação de bootstrap e falha explícita em erro SQL.
- [ ] Implementar healthcheck Kafka por ferramenta de protocolo e PostgreSQL por `pg_isready`. Flink verifica REST e TaskManager registrado, usando a identidade do container para distinguir registro efetivo. Se ferramenta faltar, imagem derivada mínima da tag aprovada; não instalar PyFlink nesta etapa.
- [ ] Fixar recursos Flink adequados à execução local, slots e paralelismo 1; configurar restart e healthcheck com timeout/start period/retries finitos. Não montar Docker socket ou publicar portas Kafka/PostgreSQL.
- [ ] Verificar `docker compose --env-file .env.example config --quiet`: deve falhar por senhas vazias. Preparar `.env` local com senhas aleatórias, sem imprimir os valores; executar `docker compose config --quiet`: exit 0. Não imprimir configuração expandida com secrets.
- [ ] Fazer pull/build e verificar executáveis utilizados nos checks. `docker compose up -d`; aguardar saúde com prazo global de 180 segundos (se pull/build tiver terminado). Capturar somente estado e logs redigidos necessários.
- [ ] Testar senha especial em um projeto Compose de validação com volume separado e credencial temporária ignorada pelo Git, sem alterar volumes da execução principal. Se ambiente impedir o teste, registrar pendência concreta.
- [ ] Reportar arquivos criados, comandos, resultados e limitações ao principal. Principal revisa arquitetura, segurança, saúde, recursos e pede correções ao mesmo agente antes de aprovar a tarefa.
