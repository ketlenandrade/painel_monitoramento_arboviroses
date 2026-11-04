# Dashboard de monitoramento – dengue, chikungunya e zika (2022–2026)

## O dashboard é criado automaticamente

O notebook **`30_criar_dashboard`**, chamado pelo `00_executar_tudo`, monta o dashboard por código. Ele o cria na mesma pasta dos notebooks com o nome **"Arboviroses Brasil 2022-2026"** e o publica. Se o dashboard já existir, é atualizado e mantém o mesmo link.

| Widget do 30 | Para que serve |
|---|---|
| `warehouse_id` | Vazio = usa o primeiro SQL Warehouse serverless. |
| `nome_dashboard` | Nome do dashboard. |
| `pasta_destino` | Vazio = a pasta dos notebooks. |
| `publicar` | `sim` publica o dashboard depois de criar. |
| `incluir_mapa` | `nao` tira os mapas de pontos, se o seu workspace não os mostrar. |
| `cor_mapa_calor` | Cor dos mapas e mapas de calor: `amarelo_vermelho` (padrão), `magma`, `inferno`, `YlGnBu` ou `padrao`. |
| `mapa_uf_por` | Como o mapa do Brasil reconhece as UFs: `nome` (padrão) ou `iso` (BR-SP). |
| `pais_mapa` | Nome do país no mapa por UF: `Brasil` (workspace em português, padrão) ou `Brazil`. |

**Importar à mão.** Também dá para importar o dashboard sem rodar o 30: **Dashboards** › **Create** › **Import dashboard from file** › `dashboard_arboviroses_brasil.lvdash.json`. As tabelas precisam existir antes: rode o 00 com `criar_dashboard = nao`, ou do 12 ao 23.

## Páginas e filtros

| Página | Filtros | O que mostra |
|---|---|---|
| **1. Visão geral** | Doença (valor único, padrão Dengue), ano, região, UF, classificação, período | Casos prováveis, confirmados, óbitos, hospitalizados e incidência. Curva epidêmica, mapa do Brasil por UF, mapas de calor (UF × semana e ano × semana), casos e incidência por UF, casos por ano, perfil por idade e sexo, e classificação. |
| **2. Áreas mais afetadas** | Doença, ano, região, UF | Mapa do Brasil por UF, mapa de calor por município (selecione um ano), mapa de calor UF × ano, mapa dos municípios, ranking de UFs, municípios com maior incidência, áreas persistentes (2022–2026), níveis de incidência e co-circulação. |
| **3. Clima e casos** | Doença, ano, região, UF, período, recorte da correlação | Casos, chuva e temperatura por semana. Chuva de 4 semanas antes × incidência. Correlação por defasagem. Perfil climático das áreas mais e menos afetadas. Chuva anual × incidência por UF. Clima mensal da região × casos. |
| **4. Comparação entre doenças** | Ano, região, UF | As três curvas epidêmicas, incidência e letalidade por doença, e sazonalidade por região. As doenças aparecem lado a lado, nunca somadas. |
| **5. Monitoramento** | Doença, região | UFs acima do esperado, razão observado/esperado e principais achados em frases. |

A página 3 só é criada se o notebook 14 (clima) tiver rodado.

**Cores fixas por doença:**

| Doença | Cor |
|---|---|
| Dengue | vermelho `#D62728` |
| Chikungunya | laranja `#FF7F0E` |
| Zika | roxo `#9467BD` |

## Como ler

- **Nível de incidência anual** (casos prováveis por 100 mil hab., Ministério da Saúde):
  - baixa: menos de 100;
  - média: 100 a 299;
  - alta: 300 ou mais.
- **Rankings de municípios:** só entram municípios com 20 mil habitantes ou mais. Em cidades muito pequenas, poucos casos geram taxas enormes.
- **Área persistente:** município em alta incidência em 3 anos ou mais.
- **Semana favorável ao Aedes:** temperatura média entre 22 °C e 32 °C e chuva de 10 mm ou mais na semana.
- **Monitoramento:** compara as últimas 4 semanas consolidadas com a média das mesmas semanas nos anos anteriores. A situação de cada UF é classificada assim:

| Razão observado/esperado | Situação |
|---|---|
| 2 ou mais | Muito acima do esperado |
| de 1,2 a 2 | Acima do esperado |
| de 0,8 a 1,2 | Dentro do esperado |
| até 0,8 | Abaixo do esperado |

## Ajustes no editor

Depois de criado, o dashboard pode ser editado normalmente no Databricks, com **Edit** e **Publish**.

Se você rodar o 30 de novo, ele **substitui** o layout pelo gerado por código. Para manter as suas edições manuais, rode o 00 com `criar_dashboard = nao`: os dados são atualizados e o layout fica como está. Outra opção é mudar `nome_dashboard` para criar um dashboard separado.
