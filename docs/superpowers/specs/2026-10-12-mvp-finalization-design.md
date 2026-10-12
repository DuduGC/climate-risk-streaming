# Etapa 10 — finalização do MVP

Status: proposta para revisão do usuário em 2026-10-12. Base: c5187fb, etapa 9 aceita. Principal orquestra, revisa e verifica; implementadores GPT-6 Luna Max.

## Objetivo e abordagem

Entregar o MVP existente com verificações reproduzíveis e documentação coerente. O fluxo permanece Open-Meteo → Producer → Kafka → Flink → PostgreSQL/weather.alerts → consumer. Não acrescentar funcionalidades, dashboards, serviços ou dependências de produto.

Recomendação: CI com a suíte existente e builds, documentação operacional consolidada e uma verificação final local pertinente. Fazer apenas documentação deixaria a exigência de CI incompleta; automatizar todos os cenários históricos de recuperação exigiria checkpoints locais e ampliaria demais o MVP. Esses cenários continuam opt-in, com suas evidências anteriores preservadas.

## CI e verificações

- Criar `.github/workflows/ci.yml`, para push, pull_request e acionamento manual. Runner Linux, permissões somente de leitura, timeouts explícitos e nenhuma publicação de imagem ou deploy.
- Qualidade: Python 3.11.14, requirements.txt e requirements-dev.txt existentes; `python -m pytest -q` e `python -m ruff check src tests`. Testes HTTP locais entram; Open-Meteo pública e cenários que exigem serviços/checkpoints continuam opt-in. Sem matriz de versões ou novos frameworks.
- Containers: construir as duas imagens existentes, Python e Flink, sem Dockerfiles novos. Verificar imports/versões e os testes de SDK existentes que precisam do runtime PyFlink. Não duplicar o build Python para Producer e consumer, que compartilham imagem. Actions serão fixadas em commits completos conferidos na implementação.
- Uma integração Kafka do consumer existente será reproduzida em ambiente efêmero de CI, com somente Kafka e containers QA necessários, sem acesso à API meteorológica, sem Flink/PostgreSQL ativos e sem credenciais reais. Compose deve receber valores fictícios exclusivos do job para interpolação; não registrar configuração expandida. Cleanup restrito ao projeto CI próprio e em etapa always. Não introduzir uma segunda suíte de entrega.
- O principal verificará comandos equivalentes localmente, sem parar/recriar o pipeline operacional. Não alegar execução no GitHub antes de haver remoto e execução real; o repositório atualmente não tem remoto configurado.

## Documentação

- Atualizar README e `docs/progresso.md`: seis containers, recursos corretos, etapas 1–9 concluídas, instruções de build/start, submissão do job e retomada por checkpoint. Explicar que saúde dos serviços não comprova job Flink ativo; depois de restart do JobManager standalone pode ser necessário reenviar o job pelo checkpoint disponível.
- Criar um documento técnico conciso em `docs/architecture.md`, com diagrama Mermaid, responsabilidade dos componentes, topics/keys/partições, schemas e regras de risco. Referenciar código como fonte de verdade, sem inventar contratos ou copiar toda a implementação.
- Documentar segurança existente, performance observada e limitações: nó único, replicação 1, rede local sem autenticação Kafka, ausência de transação distribuída, logs at-least-once, cache em memória, dados meteorológicos de modelo e fixtures QA. Não apresentar medições ociosas como throughput ou prometer produção/HA.
- Melhorias futuras em lista curta, claramente fora do MVP. Registrar comandos opt-in e quais cenários exigem coordenação do único slot Flink; não executar QA destrutiva ou cancelamento de jobs alheios.

## Aceite e limites

Principal revisa CI/documentação e executa suíte geral/Ruff, builds pertinentes, SDK e uma integração consumer. Reutilizar evidências aceitas das etapas anteriores quando seu código não mudar; repetir apenas verificações justificadas por alterações ou falhas concretas. Revisão final de segurança e recursos adequada ao MVP, sem auditoria extensiva ou benchmark novo.

Aceite: nenhum P1/P2 aberto, comandos reproduzíveis, CI localmente verificada, documentação coerente e consumer/pipeline operacionais preservados. Comparar IDs dos containers e JobID/checkpoints antes/depois. Registrar evidências em `docs/implementation/stage-10-acceptance.md`; provas temporárias em `.venv` ignorada. Scan seguro de credenciais, diff sem erros, commits locais e checkout limpo. Não configurar remoto, fazer push ou publicar sem solicitação.

Método já escolhido: agentes GPT-6 Luna Max implementam, principal revisa e executa QA. Checkout dedicado foundation reutilizado conforme decisão anterior. A próxima decisão é a revisão desta proposta; depois será escrito um plano curto de execução.
