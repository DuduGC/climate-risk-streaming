# Tarefa 2 — Estrutura e operação


**Interfaces:** Consome os nomes de serviços/variáveis e schema da Tarefa 1. Produz pacote `climate_logistics`, comando `python -m ruff check .` e configuração `python -m pytest` com marcadores `unit`/`integration`. Não adicionar casos fictícios só para evitar exit 5 de suíte vazia.

- [ ] Criar exclusões de `.env`/variantes (mantendo `.env.example`), venv, caches, logs, credenciais temporárias e contextos Docker desnecessários. Confirmar com `git check-ignore .env` que secrets não entram no Git.
- [ ] Criar estrutura do pacote e diretórios da tabela. Fixar versões disponíveis de Pytest e Ruff; configurar Python 3.11, testpaths e marcadores. Não instalar bibliotecas de serviços futuros ou criar CI antes da Etapa 10.
- [ ] Criar README em português: preparar `.env`, preencher senhas localmente, instalar dev deps, `docker compose up -d`, listar saúde, verificar Flink, acessar PostgreSQL por exec e parar serviços sem apagar volumes.
- [ ] Documentar requisitos Docker Linux, memória planejada, portas, volumes, versões, KRaft single node sem HA, PLAINTEXT local e console sem autenticação. Registrar que bootstrap roda apenas em volume novo e que mudar senha no `.env` não altera roles existentes.
- [ ] Documentar etapas futuras e topics previstos sem afirmar que já existem; PyFlink e conectores entram na Etapa 4. Comandos PostgreSQL não devem conter senha explícita na linha de comando.
- [ ] Instalar dependências dev em ambiente local e rodar Ruff. Executar Pytest e registrar a ausência de casos nesta etapa, se exit 5; isso não representa testes passando. Revisar documentação contra o Compose final.
- [ ] Reportar arquivos e verificações; principal revisa escopo, exclusões e operação antes da validação final.
