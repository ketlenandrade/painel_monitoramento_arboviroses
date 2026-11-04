# Pipeline ETL de arboviroses – Brasil, 2022 a 2026

Baixa **automaticamente** do OpenDataSUS as bases de **dengue, chikungunya e zika de 2022 a 2026**, trata cada base (Bronze → Silver → Gold), **integra as três** numa base única e cruza com o **clima** (temperatura, chuva e umidade). Depois analisa o comportamento das doenças no Brasil e **cria sozinho o dashboard de monitoramento** no Databricks, com filtros.

Não há filtro de estado: as 27 unidades federativas (26 estados e o Distrito Federal) e os 5.570 municípios são processados. O recorte por doença, estado, município, ano e semana é feito nos filtros do dashboard.

## Como usar (um clique)

1. No Databricks, abra **Workspace** › sua pasta › **Import** › **File** e envie **todos** os `.py` para a **mesma pasta**. Inclua `funcoes_comuns`, `config_agravo` e `base_analitica_agravo`, que os outros notebooks carregam com `%run`.
2. Abra o **`00_executar_tudo`** e clique em **Run all**.
3. No fim, o notebook 30 imprime o link do dashboard **"Arboviroses Brasil 2022-2026"**.

Para o painel se atualizar sozinho, rode o 00 uma vez com **`criar_job = sim`**.

**Histórico para o canal endêmico:** o 00 baixa os arquivos desde **`ano_inicio_historico = 2016`** (padrão). Os anos de 2016 a 2021 entram só como histórico do canal endêmico e do nowcast; tabelas, painel e resultados continuam de 2022 a 2026. Assim, as regras de alerta são avaliadas de 2022 a 2025, com linha de base de até 7 anos anteriores (janela do canal endêmico).

O Job roda toda segunda-feira às 6h (horário de Brasília).

**Versão da lógica (`PIPELINE_VERSION`):** quando uma regra de transformação muda, o número da versão é incrementado e o pipeline reprocessa todos os anos automaticamente na execução seguinte. Depois, as execuções voltam a ser incrementais.

**Primeira execução:** pode levar algumas horas, porque processa o Brasil inteiro. As próximas são rápidas:
- o pipeline só reprocessa os anos cujos arquivos mudaram;
- a vigilância só recalcula se os dados ou os parâmetros mudaram.

## Organização: pipeline separado das análises

| Grupo | Notebook | O que faz |
|---|---|---|
| | `00_executar_tudo` | **Orquestrador:** roda 12 → 13 → 14 → 20 → 23 → 30 e, se você quiser, cria o Job semanal. |
| **1x – Pipeline** (extrai e trata) | `10_populacao_ibge` | População do IBGE por município e ano, **uma vez para as três doenças**. |
| | `11_pipeline_agravo` | Pipeline de **uma** doença (widget `agravo`): download, Bronze, Silver, Gold, snapshots da fonte, testes de qualidade, views, linhagem e métricas de tempo. |
| | `12_rodar_dengue_chikungunya_zika` | Roda o 10 e depois o 11 para as três doenças: arquivos de 2016 a 2026 (2016 a 2021 só como histórico). |
| | `13_integracao_tabelas_painel` | **Integração:** junta as três bases nas tabelas do painel e na base integrada, com as doenças lado a lado. Grava também as coordenadas dos municípios. |
| | `14_clima_etl` | **ETL do clima** (Open-Meteo/ERA5, com a NASA POWER de reserva): capitais e municípios mais afetados, por semana epidemiológica, UF e região. Junta com os casos, com até 12 semanas de defasagem. |
| **2x – Análises** (só leem tabelas) | `20_rodar_analises_por_agravo` | Roda o 21 e o 22 para as três doenças. |
| | `21_vigilancia_agravo` | Canal endêmico, alertas municipais e avaliação das regras, alertas em tempo real, nowcasting, atraso de notificação, completude e validação contra os números oficiais. |
| | `22_resultados_tcc_agravo` | Visualizações, resultados do TCC e CSV da rodada. |
| | `23_areas_afetadas_e_clima` | Áreas mais afetadas, áreas persistentes, co-circulação, sazonalidade, perfil climático, correlação clima × incidência, comparação entre as doenças (CSV C1 a C4), monitoramento e achados. |
| | `24_eficiencia_da_solucao` | Experimentos de eficiência (modos, escalabilidade e granularidade). Roda à parte. |
| | `25_benchmark_granularidade` | Custo de agregar por UF/município × ano/semana/dia. É chamado pelo 24. |
| **3x – Painel** | `30_criar_dashboard` | Cria ou atualiza o dashboard e o publica. |
| **9x – Manutenção** | `90_limpar_tabelas` | Conta as tabelas e views do catálogo e apaga, com confirmação, as que o projeto não usa: schemas de experimento, recortes antigos e views sem uso. Use quando aparecer o erro de limite de 500 objetos. |
| **Carregados com `%run`** | `funcoes_comuns` | Estados e regiões, semana epidemiológica, nomes e cores das doenças, auxiliares de execução. |
| | `config_agravo` | Widgets, schemas e nomes de tabelas de uma doença, constantes das análises e tabela de métricas. |
| | `base_analitica_agravo` | Base analítica (uma linha por notificação) e calendário epidemiológico, montados a partir da Silver. É usada pelo pipeline e pelas análises. |

**Análise de sensibilidade sem refazer o pipeline.** Para mudar parâmetros da análise, rode só o `20` (ou o `21`) com um `rotulo_execucao` diferente. Exemplos: `semanas_consecutivas`, `anos_excluidos_canal`, `data_disponibilidade`.

**Métricas de eficiência do TCC.** O notebook 24 mede os modos de execução (sem mudança, incremental, somente Gold e completo) com repetições em ordem sorteada. A camada `gold_calculo` não inclui a vigilância, que tem a camada própria `vigilancia`.

## Tabelas principais (`workspace.arboviroses_br`)

| Tabela | Conteúdo |
|---|---|
| `dim_municipio_populacao`, `dim_populacao_sexo_uf` | População do IBGE, compartilhada pelas três doenças (gravada pelo notebook 10). |
| `painel_casos`, `painel_uf_semana` | As três doenças, por semana, UF, município, classificação, sexo e faixa etária. |
| `arboviroses_integrada_municipio_semana`, `arboviroses_integrada_uf_semana` | **Base integrada**, com as doenças lado a lado. |
| `populacao`, `dim_municipio_geo` | População (formato do painel) e coordenadas dos municípios. |
| `clima_semanal_municipio`, `clima_semanal_uf`, `clima_semanal_regiao` | Clima por semana epidemiológica. |
| `clima_casos_uf_semana`, `clima_correlacao`, `clima_melhor_defasagem` | Casos × clima com defasagens, e as correlações. |
| `insight_uf_ano`, `insight_municipio_ano` | Incidência, nível (baixa, média ou alta), ranking e variação em relação ao ano anterior. |
| `insight_municipio_periodo` | Áreas **persistentemente** afetadas (3 ou mais anos em alta incidência). |
| `insight_cocirculacao` | Municípios com 2 ou 3 doenças em incidência média ou alta no mesmo ano. |
| `insight_sazonalidade` | Semana e mês do pico, início, fim e duração da temporada, por região. |
| `insight_clima_anual_uf`, `insight_clima_areas_uf`, `insight_perfil_climatico` | Clima das áreas mais afetadas (quartil Q4) comparado ao das menos afetadas (Q1). |
| `insight_clima_regiao_mes` | Clima mensal da região × casos de cada doença. |
| `insight_monitoramento_uf` | Últimas 4 semanas consolidadas comparadas ao esperado. |
| `insight_resumo` | Principais achados em frases, por doença. |

Cada doença tem também o seu schema (`dengue_br`, `chikungunya_br`, `zika_br`):
- **gravadas pelo pipeline (11):** Bronze, Silver e Gold;
- **gravadas pela vigilância (21):** canal endêmico, alertas e nowcasting.

## Erro `QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED ... (limit: 500)`

O plano gratuito do Databricks aceita no máximo **500 tabelas e views** no catálogo. Uma execução completa do projeto usa cerca de 200. O que costuma estourar o limite:
- os **schemas dos experimentos de eficiência** (`dengue_br_exp`, `dengue_br_esc`...), com cerca de 50 tabelas cada;
- **schemas de outros recortes** e views que o dashboard não usa.

Para resolver:
1. Abra o **`90_limpar_tabelas`** e rode com `modo = listar`. Ele mostra quantos objetos existem e quantos cada grupo libera.
2. Escolha os grupos, mude para `modo = apagar` e escreva `APAGAR` em `confirmar`.
3. Rode de novo o `00_executar_tudo`. Ele confere o espaço antes de começar.

As tabelas de produção, os arquivos baixados e as métricas do TCC **nunca** são apagados. Os experimentos podem ser recriados rodando o 24 de novo. Se não quiser que os schemas de experimento se acumulem, use `apagar_schemas_experimento = sim` no 24: as métricas de tempo ficam guardadas.

## Observações

- **As doenças nunca são somadas.** Cada uma tem as suas colunas, a sua cor e o seu filtro.
- **Canal endêmico:** precisa de pelo menos 3 anos anteriores e usa até 7. Com o histórico desde 2016 (padrão), é calculado em todos os anos de 2022 a 2026. Se `ano_inicio_historico = 2022`, só sai para 2025 e 2026.
- **Clima:** a fonte principal é o Open-Meteo (ERA5). O plano gratuito limita as chamadas por hora e por dia, contando por IP, e no Databricks serverless o IP é compartilhado com outros usuários, então o limite pode acabar logo. Com `fonte_clima = automatica` (padrão), o 14:
  - controla o ritmo das chamadas;
  - ao bater no limite, para de chamar o Open-Meteo;
  - baixa os municípios que faltam da **NASA POWER** (gratuita, sem chave).

  Cada município usa uma fonte só, registrada na coluna `fonte_clima`. Tudo fica em cache; anos completos nunca são baixados de novo. Outras opções do 14:
  - `fonte_clima = nasa_power`, para usar só a NASA;
  - `open_meteo` com `esperar_limite = sim`, para esperar o limite liberar;
  - `openmeteo_apikey`, se você tiver uma chave paga.
- **Correlação não é causalidade.** Clima e casos têm a mesma sazonalidade. Olhe também a coluna `rho_sem_sazonalidade`.
- **Ano atual:** é parcial. O monitoramento deixa as 2 semanas mais recentes de fora, porque elas ainda recebem notificações.
- **População:** vem do IBGE. Se a API não responder, o 10 usa uma cópia pública dos mesmos arquivos oficiais.

## Se a fonte do OpenDataSUS recusar a conexão (`Connection reset by peer`)

O notebook 11 tenta de novo cada consulta (espera de 5 s, 15 s e 45 s) e, se o HEAD for recusado, tenta um GET parcial.
Quando a rede continua fora, ele usa a cópia que já estiver no volume. Para os anos que não têm cópia, faça a carga manual:

1. Baixe no seu computador os arquivos anuais (ex.: `DENGBR16.csv.zip` … `DENGBR26.csv.zip`, `CHIKBRaa.csv.zip`, `ZIKABRaa.csv.zip`) do OpenDataSUS.
2. No Databricks: **Catalog → workspace → dengue_raw → Volumes → raw → Upload to this volume** (mesmo para `chikungunya_raw` e `zika_raw`).
   Pode enviar para a pasta do ano (`raw/2016/`) ou para a raiz (`raw/`).
3. Rode de novo. O pipeline encontra o `.zip`, extrai o CSV e continua; quando a fonte voltar, ele confere ETag, data e tamanho e, se o arquivo for baixado, compara o SHA-256: só reprocessa os anos cujo conteúdo mudou.
