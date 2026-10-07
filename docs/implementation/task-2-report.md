# Relatório — Tarefa 2: estrutura e operação

## Entrega

- Criados `.gitignore` e `.dockerignore` com exclusões de arquivos de ambiente, ambientes virtuais, caches, logs, credenciais locais e conteúdo dispensável de contextos de build. `.env.example` continua fora das exclusões.
- Adicionado `.gitattributes` para normalizar arquivos de texto e manter scripts shell com LF, evitando conversão para CRLF em checkouts futuros no Windows.
- Criado `src/climate_logistics/__init__.py`, sem lógica de aplicação futura, e preservados `tests/unit/`, `tests/integration/` e `sql/` com `.gitkeep`.
- Configurados Ruff e Pytest em `pyproject.toml`, com alvo Python 3.11, `testpaths = ["tests"]`, caminho de importação `src` e marcadores `unit` e `integration`.
- Fixadas as ferramentas em `requirements-dev.txt`: Pytest 8.4.2 e Ruff 0.14.0. As páginas oficiais do PyPI confirmam as distribuições: [Pytest 8.4.2](https://pypi.org/project/pytest/8.4.2/) e [Ruff 0.14.0](https://pypi.org/project/ruff/0.14.0/).
- Criado README em português com preparação local do `.env`, início, prontidão, verificações Kafka/Flink/PostgreSQL, preservação de volumes, limites, portas, versões, segurança local e roadmap previsto. O comando psql usa `-h 127.0.0.1` para testar autenticação TCP por senha e `-W` para solicitá-la sem exibi-la na linha de comando.
- Documentada a política Kafka de retenção (24 horas, 1 GiB por partição, segmentos de 256 MiB e verificação a cada 5 minutos), esclarecendo que a aplicação depende da remoção de segmentos fechados e não configura um teto rígido instantâneo.

## Verificações desta implementação

- `git check-ignore -v .env` confirmou que `.env` é ignorado pela regra `*.env` em `.gitignore`.
- `git check-ignore .env.example` não encontrou regra de exclusão, então o modelo permanece elegível para o Git.
- Não foram criados testes fictícios. A suíte permanece vazia; quando executado sem casos, Pytest deverá retornar código 5 por não encontrar testes, o que não significa aprovação da suíte.
- Ruff, Pytest e Docker não foram executados nesta subetapa. A validação dessas ferramentas fica para o agente principal, que preparou uma imagem Python 3.11 para os checks; nenhum runtime foi instalado no host.
- O arquivo `.env` não foi lido. Nenhum comando Docker foi executado.
