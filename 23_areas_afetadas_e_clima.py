# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 23 – ÁREAS MAIS AFETADAS, SAZONALIDADE, CLIMA E MONITORAMENTO (2022-2026)
# ==============================================================================
# Extrai as informações para entender o comportamento da dengue, da chikungunya
# e da zika no Brasil e relaciona as áreas mais afetadas com o clima da região.
# Cada doença é analisada SEPARADAMENTE (nenhuma tabela soma doenças).
#
# Rode DEPOIS do 13 (tabelas do painel e base integrada) e do 14 (ETL do clima).
# Sem o 14, as tabelas de clima são puladas e o resto é gerado normalmente.
#
# Tabelas criadas em workspace.arboviroses_br (prefixo insight_):
#   insight_uf_ano               – casos, incidência, nível (baixa/média/alta),
#                                  posição no ranking e variação vs ano anterior
#   insight_municipio_ano        – o mesmo por município, com coordenadas (mapa)
#   insight_municipio_periodo    – acumulado 2022-2026 por município: incidência
#                                  média anual e nº de anos em alta incidência
#                                  (áreas PERSISTENTEMENTE afetadas)
#   insight_cocirculacao         – municípios com 2 ou 3 doenças em incidência
#                                  média/alta no mesmo ano
#   insight_sazonalidade         – por doença x região x ano: semana e mês do
#                                  pico, início e fim da temporada, duração
#   insight_clima_anual_uf       – clima anual de cada UF (temperatura, chuva,
#                                  umidade, semanas favoráveis ao Aedes)
#   insight_clima_areas_uf       – incidência x clima por doença x UF x ano, com o
#                                  quartil de incidência (áreas mais afetadas)
#   insight_perfil_climatico     – clima médio das áreas em cada quartil de
#                                  incidência (UF e município): as áreas mais
#                                  afetadas são mais quentes? mais chuvosas?
#   insight_clima_regiao_mes     – clima mensal da região x casos de cada doença
#   insight_monitoramento_uf     – últimas semanas x o esperado para as mesmas
#                                  semanas nos anos anteriores (situação atual)
#   insight_resumo               – principais achados em frases, por doença
#   clima_correlacao, clima_melhor_defasagem – correlação de Spearman clima x
#                                  incidência por defasagem (com e sem sazonalidade)
#   CSV C1 a C4 (comparação entre os agravos, para o TCC) em
#     /Volumes/workspace/arboviroses_metricas/arquivos/brasil_<data>/
#
# Níveis de incidência anual (casos prováveis por 100 mil hab., Ministério da
# Saúde): baixa < 100; média 100 a 299; alta >= 300.
# Semana favorável ao Aedes aegypti: temperatura média entre 22 °C e 32 °C e
# chuva >= 10 mm na semana (faixa em que o mosquito se desenvolve mais rápido e
# há criadouros com água). É um indicador simples, não um modelo de risco.
# ==============================================================================

dbutils.widgets.text("ano_inicio", "2022")
dbutils.widgets.text("ano_fim", "2026")
dbutils.widgets.text("pop_minima_ranking", "20000")
dbutils.widgets.text("semanas_monitoramento", "4")
dbutils.widgets.text("semanas_descartar_recentes", "2")
dbutils.widgets.text("ano_comparacao", "2024")

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

CATALOG = "workspace"
DESTINO = f"{CATALOG}.arboviroses_br"
ANO_INI = int(dbutils.widgets.get("ano_inicio").strip() or 2022)
ANO_FIM = int(dbutils.widgets.get("ano_fim").strip() or 2026)
POP_MIN = int(dbutils.widgets.get("pop_minima_ranking").strip() or 20000)
N_SEM_MON = int(dbutils.widgets.get("semanas_monitoramento").strip() or 4)
N_SEM_DESC = int(dbutils.widgets.get("semanas_descartar_recentes").strip() or 2)
ANO_CMP = int(dbutils.widgets.get("ano_comparacao").strip() or 2024)
LIMIAR_MEDIA, LIMIAR_ALTA = 100, 300


def existe(nome):
    return spark.catalog.tableExists(f"{DESTINO}.{nome}")


def salvar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.{nome}")
    print(f"{nome}: {spark.table(f'{DESTINO}.{nome}').count():,} linhas")


for t in ["painel_uf_semana", "painel_casos", "populacao"]:
    if not existe(t):
        raise Exception(f"{DESTINO}.{t} não existe. Rode antes os notebooks 12 e 13.")
TEM_GEO = existe("dim_municipio_geo")
TEM_CLIMA = existe("clima_semanal_uf")
print(f"Período {ANO_INI}-{ANO_FIM} | coordenadas: {TEM_GEO} | clima: {TEM_CLIMA}")

NIVEL_SQL = f"""CASE WHEN incidencia_100mil >= {LIMIAR_ALTA} THEN 'Alta (>= 300)'
                     WHEN incidencia_100mil >= {LIMIAR_MEDIA} THEN 'Média (100-299)'
                     WHEN incidencia_100mil IS NULL THEN 'Sem população'
                     ELSE 'Baixa (< 100)' END"""

# COMMAND ----------

# DBTITLE 1,Ranking por UF e ano (incidência, nível, variação)
salvar(spark.sql(f"""
WITH casos AS (
  SELECT agravo, ano, regiao, uf_sigla, uf_nome,
         SUM(casos_provaveis) AS casos_provaveis, SUM(casos_confirmados) AS casos_confirmados,
         SUM(obitos) AS obitos, SUM(hospitalizados) AS hospitalizados
  FROM {DESTINO}.painel_uf_semana
  WHERE uf_sigla <> 'IGN' AND ano BETWEEN {ANO_INI} AND {ANO_FIM}
  GROUP BY ALL),
pop AS (SELECT ano, uf_sigla, SUM(populacao) AS populacao FROM {DESTINO}.populacao GROUP BY ALL),
base AS (
  SELECT c.*, p.populacao,
         ROUND(c.casos_provaveis / p.populacao * 100000, 2) AS incidencia_100mil,
         ROUND(c.obitos / NULLIF(c.casos_confirmados, 0) * 100, 3) AS letalidade_pct
  FROM casos c LEFT JOIN pop p USING (ano, uf_sigla))
SELECT *, {NIVEL_SQL} AS nivel_incidencia,
       RANK() OVER (PARTITION BY agravo, ano ORDER BY incidencia_100mil DESC) AS posicao_incidencia,
       RANK() OVER (PARTITION BY agravo, ano ORDER BY casos_provaveis DESC) AS posicao_casos,
       ROUND((casos_provaveis - LAG(casos_provaveis) OVER (PARTITION BY agravo, uf_sigla ORDER BY ano))
             / NULLIF(LAG(casos_provaveis) OVER (PARTITION BY agravo, uf_sigla ORDER BY ano), 0) * 100, 1)
         AS variacao_casos_pct_ano_anterior
FROM base
"""), "insight_uf_ano")

# COMMAND ----------

# DBTITLE 1,Ranking por município e ano (com coordenadas para o mapa)
_geo_join = (f"LEFT JOIN {DESTINO}.dim_municipio_geo g USING (municipio_codigo)" if TEM_GEO else "")
_geo_cols = ("g.latitude, g.longitude, g.capital" if TEM_GEO
             else "CAST(NULL AS DOUBLE) AS latitude, CAST(NULL AS DOUBLE) AS longitude, CAST(NULL AS INT) AS capital")
salvar(spark.sql(f"""
WITH casos AS (
  SELECT agravo, ano, regiao, uf_sigla, municipio_codigo, MAX(municipio_nome) AS municipio_nome,
         SUM(casos_provaveis) AS casos_provaveis, SUM(casos_confirmados) AS casos_confirmados,
         SUM(obitos) AS obitos
  FROM {DESTINO}.painel_casos
  WHERE municipio_codigo IS NOT NULL AND ano BETWEEN {ANO_INI} AND {ANO_FIM}
  GROUP BY agravo, ano, regiao, uf_sigla, municipio_codigo),
base AS (
  SELECT c.*, p.populacao, ROUND(c.casos_provaveis / p.populacao * 100000, 2) AS incidencia_100mil, {_geo_cols}
  FROM casos c
  LEFT JOIN {DESTINO}.populacao p USING (ano, municipio_codigo)
  {_geo_join})
SELECT *, {NIVEL_SQL} AS nivel_incidencia,
       populacao >= {POP_MIN} AS entra_ranking,
       CASE WHEN populacao >= {POP_MIN} THEN
         RANK() OVER (PARTITION BY agravo, ano, populacao >= {POP_MIN} ORDER BY incidencia_100mil DESC) END
         AS posicao_brasil,
       RANK() OVER (PARTITION BY agravo, ano, uf_sigla ORDER BY casos_provaveis DESC) AS posicao_casos_na_uf
FROM base
"""), "insight_municipio_ano")

# COMMAND ----------

# DBTITLE 1,Áreas persistentemente afetadas (acumulado 2022-2026)
salvar(spark.sql(f"""
SELECT agravo, regiao, uf_sigla, municipio_codigo, MAX(municipio_nome) AS municipio_nome,
       MAX(latitude) AS latitude, MAX(longitude) AS longitude, MAX(capital) AS capital,
       SUM(casos_provaveis) AS casos_provaveis, SUM(casos_confirmados) AS casos_confirmados,
       SUM(obitos) AS obitos, ROUND(AVG(populacao)) AS populacao_media,
       ROUND(SUM(casos_provaveis) / AVG(populacao) * 100000 / MAX(n.n_anos), 2)
         AS incidencia_media_anual_100mil,
       COUNT(DISTINCT CASE WHEN incidencia_100mil >= {LIMIAR_ALTA} THEN ano END) AS anos_em_alta,
       COUNT(DISTINCT ano) AS anos_com_casos,
       AVG(populacao) >= {POP_MIN} AS entra_ranking,
       CASE WHEN COUNT(DISTINCT CASE WHEN incidencia_100mil >= {LIMIAR_ALTA} THEN ano END) >= 3
              THEN 'Persistente (3+ anos em alta)'
            WHEN COUNT(DISTINCT CASE WHEN incidencia_100mil >= {LIMIAR_ALTA} THEN ano END) >= 1
              THEN 'Episódica (1-2 anos em alta)'
            ELSE 'Sem ano em alta' END AS padrao_area
FROM {DESTINO}.insight_municipio_ano
-- divide pelo nº de anos do período com dados da doença (não só os anos com casos no município)
JOIN (SELECT agravo, COUNT(DISTINCT ano) AS n_anos FROM {DESTINO}.insight_uf_ano GROUP BY agravo) n USING (agravo)
GROUP BY agravo, regiao, uf_sigla, municipio_codigo
"""), "insight_municipio_periodo")

# COMMAND ----------

# DBTITLE 1,Co-circulação: 2 ou 3 doenças no mesmo município e ano
salvar(spark.sql(f"""
WITH w AS (
  SELECT ano, regiao, uf_sigla, municipio_codigo, MAX(municipio_nome) AS municipio_nome,
         MAX(latitude) AS latitude, MAX(longitude) AS longitude, MAX(populacao) AS populacao,
         MAX(CASE WHEN agravo = 'Dengue' THEN incidencia_100mil END) AS incidencia_dengue,
         MAX(CASE WHEN agravo = 'Chikungunya' THEN incidencia_100mil END) AS incidencia_chikungunya,
         MAX(CASE WHEN agravo = 'Zika' THEN incidencia_100mil END) AS incidencia_zika
  FROM {DESTINO}.insight_municipio_ano
  GROUP BY ano, regiao, uf_sigla, municipio_codigo)
SELECT *,
  CAST(COALESCE(incidencia_dengue, 0) >= {LIMIAR_MEDIA} AS INT)
  + CAST(COALESCE(incidencia_chikungunya, 0) >= {LIMIAR_MEDIA} AS INT)
  + CAST(COALESCE(incidencia_zika, 0) >= {LIMIAR_MEDIA} AS INT) AS n_doencas_media_ou_alta,
  CONCAT_WS(' + ',
    CASE WHEN incidencia_dengue >= {LIMIAR_MEDIA} THEN 'Dengue' END,
    CASE WHEN incidencia_chikungunya >= {LIMIAR_MEDIA} THEN 'Chikungunya' END,
    CASE WHEN incidencia_zika >= {LIMIAR_MEDIA} THEN 'Zika' END) AS combinacao
FROM w
"""), "insight_cocirculacao")

# COMMAND ----------

# DBTITLE 1,Sazonalidade: pico, início e fim da temporada por região
# Temporada = semanas entre 10% e 90% dos casos prováveis acumulados do ano.
salvar(spark.sql(f"""
WITH s AS (
  SELECT agravo, regiao, ano, semana, MIN(inicio_semana) AS inicio_semana, SUM(casos_provaveis) AS casos
  FROM {DESTINO}.painel_uf_semana
  WHERE uf_sigla <> 'IGN' AND ano BETWEEN {ANO_INI} AND {ANO_FIM} AND semana IS NOT NULL
  GROUP BY agravo, regiao, ano, semana),
a AS (
  SELECT *, SUM(casos) OVER (PARTITION BY agravo, regiao, ano ORDER BY semana) AS acum,
            SUM(casos) OVER (PARTITION BY agravo, regiao, ano) AS total,
            ROW_NUMBER() OVER (PARTITION BY agravo, regiao, ano ORDER BY casos DESC, semana) AS pos
  FROM s)
SELECT agravo, regiao, ano, MAX(total) AS casos_provaveis_ano,
       MAX(CASE WHEN pos = 1 THEN semana END) AS semana_pico,
       MAX(CASE WHEN pos = 1 THEN casos END) AS casos_semana_pico,
       MAX(CASE WHEN pos = 1 THEN inicio_semana END) AS data_pico,
       MONTH(DATE_ADD(MAX(CASE WHEN pos = 1 THEN inicio_semana END), 3)) AS mes_pico,
       MIN(CASE WHEN acum >= 0.1 * total THEN semana END) AS semana_inicio_temporada,
       MIN(CASE WHEN acum >= 0.9 * total THEN semana END) AS semana_fim_temporada,
       MIN(CASE WHEN acum >= 0.9 * total THEN semana END)
         - MIN(CASE WHEN acum >= 0.1 * total THEN semana END) + 1 AS duracao_temporada_semanas,
       ROUND(MAX(CASE WHEN pos = 1 THEN casos END) / NULLIF(MAX(total), 0) * 100, 1) AS pct_casos_na_semana_pico
FROM a
WHERE total > 0
GROUP BY agravo, regiao, ano
"""), "insight_sazonalidade")

# COMMAND ----------

# DBTITLE 1,Clima das áreas: anual por UF, incidência x clima e perfil por quartil
if TEM_CLIMA:
    # Clima anual por UF (só anos de casos; semanas completas do notebook 14)
    salvar(spark.sql(f"""
    SELECT uf_sigla, ano,
           ROUND(AVG(temp_media), 2) AS temp_media_anual, ROUND(MAX(temp_max), 2) AS temp_max_semanal,
           ROUND(AVG(temp_min), 2) AS temp_min_media, ROUND(SUM(chuva_mm), 1) AS chuva_total_mm,
           ROUND(AVG(umidade_media), 1) AS umidade_media_anual, SUM(dias_chuva) AS dias_chuva,
           SUM(CASE WHEN temp_media BETWEEN 22 AND 32 AND chuva_mm >= 10 THEN 1 ELSE 0 END)
             AS semanas_favoraveis_aedes,
           COUNT(*) AS semanas_com_clima
    FROM {DESTINO}.clima_semanal_uf
    WHERE ano BETWEEN {ANO_INI} AND {ANO_FIM}
    GROUP BY uf_sigla, ano
    """), "insight_clima_anual_uf")

    # Incidência x clima, com quartil de incidência dentro de cada doença e ano
    salvar(spark.sql(f"""
    SELECT i.agravo, i.ano, i.regiao, i.uf_sigla, i.uf_nome, i.casos_provaveis, i.incidencia_100mil,
           i.nivel_incidencia, c.temp_media_anual, c.temp_min_media, c.chuva_total_mm,
           c.umidade_media_anual, c.semanas_favoraveis_aedes, c.semanas_com_clima,
           NTILE(4) OVER (PARTITION BY i.agravo, i.ano ORDER BY i.incidencia_100mil) AS quartil_incidencia
    FROM {DESTINO}.insight_uf_ano i
    JOIN {DESTINO}.insight_clima_anual_uf c USING (uf_sigla, ano)
    WHERE i.incidencia_100mil IS NOT NULL
    """), "insight_clima_areas_uf")

    # Perfil climático por quartil – UF e, quando houver, município
    partes = [spark.sql(f"""
    SELECT agravo, 'UF' AS nivel, quartil_incidencia,
           CASE quartil_incidencia WHEN 4 THEN 'Q4 – mais afetadas' WHEN 1 THEN 'Q1 – menos afetadas'
                ELSE CONCAT('Q', quartil_incidencia) END AS grupo,
           COUNT(*) AS n_areas_ano, ROUND(AVG(incidencia_100mil), 1) AS incidencia_media_100mil,
           ROUND(AVG(temp_media_anual), 2) AS temp_media, ROUND(AVG(temp_min_media), 2) AS temp_min,
           ROUND(AVG(chuva_total_mm), 0) AS chuva_anual_mm, ROUND(AVG(umidade_media_anual), 1) AS umidade,
           ROUND(AVG(semanas_favoraveis_aedes), 1) AS semanas_favoraveis_aedes
    FROM {DESTINO}.insight_clima_areas_uf GROUP BY ALL
    """)]
    if existe("clima_semanal_municipio"):
        partes.append(spark.sql(f"""
        WITH cl AS (
          SELECT municipio_codigo, ano, AVG(temp_media) AS temp_media, AVG(temp_min) AS temp_min,
                 SUM(chuva_mm) AS chuva_mm, AVG(umidade_media) AS umidade,
                 SUM(CASE WHEN temp_media BETWEEN 22 AND 32 AND chuva_mm >= 10 THEN 1 ELSE 0 END) AS sem_fav,
                 COUNT(*) AS semanas
          FROM {DESTINO}.clima_semanal_municipio
          WHERE ano BETWEEN {ANO_INI} AND {ANO_FIM} GROUP BY municipio_codigo, ano),
        m AS (
          SELECT m.agravo, m.ano, m.incidencia_100mil, cl.*,
                 NTILE(4) OVER (PARTITION BY m.agravo, m.ano ORDER BY m.incidencia_100mil) AS quartil_incidencia
          FROM {DESTINO}.insight_municipio_ano m
          JOIN cl ON cl.municipio_codigo = m.municipio_codigo AND cl.ano = m.ano
          WHERE m.incidencia_100mil IS NOT NULL AND cl.semanas >= 40)
        SELECT agravo, 'Município' AS nivel, quartil_incidencia,
               CASE quartil_incidencia WHEN 4 THEN 'Q4 – mais afetadas' WHEN 1 THEN 'Q1 – menos afetadas'
                    ELSE CONCAT('Q', quartil_incidencia) END AS grupo,
               COUNT(*) AS n_areas_ano, ROUND(AVG(incidencia_100mil), 1) AS incidencia_media_100mil,
               ROUND(AVG(temp_media), 2) AS temp_media, ROUND(AVG(temp_min), 2) AS temp_min,
               ROUND(AVG(chuva_mm), 0) AS chuva_anual_mm, ROUND(AVG(umidade), 1) AS umidade,
               ROUND(AVG(sem_fav), 1) AS semanas_favoraveis_aedes
        FROM m GROUP BY ALL
        """))
    perfil = partes[0]
    for p in partes[1:]:
        perfil = perfil.unionByName(p)
    salvar(perfil, "insight_perfil_climatico")

    # Clima mensal da região x casos de cada doença (base integrada do 13)
    if existe("clima_semanal_regiao") and existe("arboviroses_integrada_uf_semana"):
        salvar(spark.sql(f"""
        WITH cl AS (
          SELECT regiao, ano, MONTH(DATE_ADD(inicio_semana, 3)) AS mes,
                 ROUND(AVG(temp_media), 2) AS temp_media, ROUND(SUM(chuva_mm), 1) AS chuva_mm,
                 ROUND(AVG(umidade_media), 1) AS umidade_media
          FROM {DESTINO}.clima_semanal_regiao WHERE ano BETWEEN {ANO_INI} AND {ANO_FIM} GROUP BY ALL),
        cs AS (
          SELECT regiao, ano, MONTH(DATE_ADD(inicio_semana, 3)) AS mes,
                 SUM(casos_dengue) AS casos_dengue, SUM(casos_chikungunya) AS casos_chikungunya,
                 SUM(casos_zika) AS casos_zika
          FROM {DESTINO}.arboviroses_integrada_uf_semana
          WHERE uf_sigla <> 'IGN' AND inicio_semana IS NOT NULL GROUP BY ALL)
        SELECT cl.*, MAKE_DATE(cl.ano, cl.mes, 1) AS mes_data,
               COALESCE(cs.casos_dengue, 0) AS casos_dengue,
               COALESCE(cs.casos_chikungunya, 0) AS casos_chikungunya,
               COALESCE(cs.casos_zika, 0) AS casos_zika
        FROM cl LEFT JOIN cs USING (regiao, ano, mes)
        """), "insight_clima_regiao_mes")
else:
    print("Sem as tabelas de clima (rode o notebook 14): análises de clima puladas.")

# COMMAND ----------

# DBTITLE 1,Monitoramento: últimas semanas x o esperado
# As semanas mais recentes ainda recebem notificações (atraso). Por isso as
# últimas N_SEM_DESC semanas são deixadas de fora da comparação.
salvar(spark.sql(f"""
WITH s AS (
  SELECT agravo, regiao, uf_sigla, ano, semana, SUM(casos_provaveis) AS casos
  FROM {DESTINO}.painel_uf_semana
  WHERE uf_sigla <> 'IGN' AND semana IS NOT NULL AND ano BETWEEN {ANO_INI} AND {ANO_FIM}
  GROUP BY agravo, regiao, uf_sigla, ano, semana),
-- calendário de semanas (semanas sem caso numa UF entram com 0)
cal AS (
  SELECT DISTINCT ano, semana, inicio_semana FROM {DESTINO}.painel_uf_semana
  WHERE semana IS NOT NULL AND inicio_semana IS NOT NULL AND ano BETWEEN {ANO_INI} AND {ANO_FIM}),
ufs AS (SELECT DISTINCT agravo, regiao, uf_sigla FROM s),
ult AS (
  SELECT s.agravo, DATE_SUB(MAX(c.inicio_semana), 7 * {N_SEM_DESC}) AS fim_janela
  FROM s JOIN cal c USING (ano, semana) WHERE s.casos > 0 GROUP BY s.agravo),
atual AS (
  SELECT u.agravo, u.regiao, u.uf_sigla, c.ano, c.semana, c.inicio_semana, COALESCE(s.casos, 0) AS casos
  FROM ufs u
  JOIN ult l ON l.agravo = u.agravo
  JOIN cal c ON c.inicio_semana > DATE_SUB(l.fim_janela, 7 * {N_SEM_MON}) AND c.inicio_semana <= l.fim_janela
  LEFT JOIN s ON s.agravo = u.agravo AND s.uf_sigla = u.uf_sigla AND s.ano = c.ano AND s.semana = c.semana),
-- quantos anos anteriores têm essa semana (os que não tiveram caso contam como 0)
n_ant AS (
  SELECT c.ano, c.semana, COUNT(DISTINCT c2.ano) AS n
  FROM cal c JOIN cal c2 ON c2.semana = c.semana AND c2.ano < c.ano GROUP BY c.ano, c.semana),
soma_ant AS (
  SELECT a.agravo, a.uf_sigla, a.ano, a.semana, SUM(h.casos) AS soma
  FROM atual a JOIN s h ON h.agravo = a.agravo AND h.uf_sigla = a.uf_sigla AND h.semana = a.semana
                       AND h.ano < a.ano
  GROUP BY a.agravo, a.uf_sigla, a.ano, a.semana),
j AS (
  SELECT a.agravo, a.regiao, a.uf_sigla, MIN(a.inicio_semana) AS janela_inicio, MAX(a.inicio_semana) AS janela_fim,
         SUM(a.casos) AS casos_janela,
         ROUND(SUM(COALESCE(e.soma, 0) / n.n), 1) AS esperado_janela,
         MIN(n.n) AS anos_historico
  FROM atual a
  LEFT JOIN soma_ant e USING (agravo, uf_sigla, ano, semana)
  LEFT JOIN n_ant n ON n.ano = a.ano AND n.semana = a.semana
  GROUP BY a.agravo, a.regiao, a.uf_sigla)
SELECT *, ROUND(casos_janela / NULLIF(esperado_janela, 0), 2) AS razao_observado_esperado,
       CASE WHEN esperado_janela IS NULL OR esperado_janela = 0 THEN 'Sem histórico'
            WHEN casos_janela / esperado_janela >= 2 THEN '1. Muito acima do esperado'
            WHEN casos_janela / esperado_janela >= 1.2 THEN '2. Acima do esperado'
            WHEN casos_janela / esperado_janela > 0.8 THEN '3. Dentro do esperado'
            ELSE '4. Abaixo do esperado' END AS situacao
FROM j
"""), "insight_monitoramento_uf")

# COMMAND ----------

# DBTITLE 1,Correlação clima x incidência por defasagem
# (O 14 faz o ETL do clima; as análises ficam aqui.)
TEM_CLIMA_CASOS = existe("clima_casos_uf_semana")
if TEM_CLIMA_CASOS:
    clima_casos_uf = spark.table(f"{DESTINO}.clima_casos_uf_semana")
    LAG_VARS = CLIMA_LAG_VARS  # funcoes_comuns
    MAX_LAG = max(int(c.rsplit("_lag", 1)[1]) for c in clima_casos_uf.columns if "_lag" in c)
    # Spearman (não supõe relação linear). Duas versões:
    #   rho                  – correlação direta das séries semanais
    #   rho_sem_sazonalidade – correlação depois de tirar, de cada série, a média da
    #                          mesma semana do ano naquela UF (anomalias). Se a relação
    #                          continua aparecendo aqui, não é só porque "verão tem chuva
    #                          e tem dengue": semanas mais chuvosas/quentes que o normal
    #                          precedem semanas com mais casos que o normal.
    base = clima_casos_uf.toPandas()


    def anomalia(df, col):
        return df[col] - df.groupby(["agravo", "uf_sigla", "semana"])[col].transform("mean")


    base["inc_anom"] = anomalia(base, "incidencia_100mil")
    resultados = []
    recortes = [("Brasil", "Brasil", base)]
    recortes += [("Região", r, g) for r, g in base.groupby("regiao")]
    recortes += [("UF", u, g) for u, g in base.groupby("uf_sigla")]
    for nivel, nome, g in recortes:
        for agravo, ga in g.groupby("agravo"):
            for v in LAG_VARS:
                for k in range(0, MAX_LAG + 1):
                    col = f"{v}_lag{k}"
                    x = ga[["incidencia_100mil", "inc_anom", col]].dropna()
                    if len(x) < 30:
                        continue
                    anom_clima = (x[col] - ga.loc[x.index].groupby(["uf_sigla", "semana"])[col].transform("mean"))
                    resultados.append({
                        "agravo": agravo, "nivel": nivel, "recorte": nome, "variavel_clima": v,
                        "defasagem_semanas": k, "n_semanas": len(x),
                        "rho": round(x["incidencia_100mil"].corr(x[col], method="spearman"), 3),
                        "rho_sem_sazonalidade": round(x["inc_anom"].corr(anom_clima, method="spearman"), 3),
                    })
    corr = pd.DataFrame(resultados)
    spark.createDataFrame(corr).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_correlacao")

    melhor = (corr.loc[corr.groupby(["agravo", "nivel", "recorte", "variavel_clima"])["rho"].apply(lambda s: s.abs().idxmax())]
              .rename(columns={"defasagem_semanas": "defasagem_mais_forte", "rho": "rho_mais_forte"}))
    spark.createDataFrame(melhor).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_melhor_defasagem")

    print("Brasil – defasagem de correlação mais forte por variável:")
    display(spark.table(f"{DESTINO}.clima_melhor_defasagem").filter("nivel = 'Brasil'").orderBy("agravo", "variavel_clima"))
else:
    print("Sem clima_casos_uf_semana (rode o notebook 14): correlação pulada.")

# COMMAND ----------

# DBTITLE 1,Gráfico: correlação por defasagem (Brasil)
if TEM_CLIMA_CASOS:
    import matplotlib.pyplot as plt

    br = corr[corr["nivel"] == "Brasil"]
    nomes = {"temp_media": "Temperatura média", "temp_min": "Temperatura mínima",
             "chuva_mm": "Chuva (mm)", "umidade_media": "Umidade relativa"}
    fig, eixos = plt.subplots(1, br["agravo"].nunique(), figsize=(5 * br["agravo"].nunique(), 3.6), sharey=True, squeeze=False)
    for ax, (agravo, g) in zip(eixos[0], br.groupby("agravo")):
        for v, gv in g.groupby("variavel_clima"):
            ax.plot(gv["defasagem_semanas"], gv["rho"], marker="o", ms=3, label=nomes.get(v, v))
        ax.axhline(0, color="#999", lw=0.8)
        ax.set_title(agravo, loc="left", fontweight="bold")
        ax.set_xlabel("Clima de quantas semanas antes")
    eixos[0][0].set_ylabel("Correlação de Spearman com a incidência")
    eixos[0][-1].legend(frameon=False, fontsize=8)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,Tabelas de comparação entre os agravos (C1 a C4, CSV para o TCC)
# (O 12 só extrai e trata; as análises ficam aqui.)
import os
from datetime import datetime
import pandas as pd

NOMES = NOMES_AGRAVO
AGRAVOS = list(NOMES_AGRAVO)
UF = "BR"  # schemas dengue_br, chikungunya_br, zika_br
OUT_DIR = f"/Volumes/{CATALOG}/arboviroses_metricas/arquivos/brasil_{datetime.now():%Y%m%d_%H%M}"
os.makedirs(OUT_DIR, exist_ok=True)


def tabela(agravo, sufixo):  # Gold de um agravo (pipeline 11)
    nome = f"{CATALOG}.{agravo}_{UF.lower()}.gold_{agravo}_{sufixo}"
    return spark.table(nome) if spark.catalog.tableExists(nome) else None


def salvar_csv(pdf, nome):
    if pdf is None or pdf.empty:
        print(f"[{nome}] sem dados.")
        return
    caminho = os.path.join(OUT_DIR, f"{nome}.csv")
    pdf.to_csv(caminho, index=False, encoding="utf-8-sig")
    print(f"[{nome}] {len(pdf)} linhas -> {caminho}")
    display(pdf)


# 1) incidência anual no território (Brasil)
partes = []
for ag in AGRAVOS:
    inc = tabela(ag, "incidencia")
    if inc is None:
        print(f"[{ag}] tabela de incidência não encontrada – rode o pipeline (12).")
        continue
    partes.append(
        inc.filter("nivel = 'UF_ANO'")
        .select(F.lit(NOMES[ag]).alias("agravo"), "ano_analise", "casos_provaveis", "populacao",
                "coef_incidencia_100mil", "ic95_inf", "ic95_sup")
        .toPandas()
    )
inc_anual = pd.concat(partes, ignore_index=True).sort_values(["agravo", "ano_analise"]) if partes else None
salvar_csv(inc_anual, "C1_incidencia_anual_brasil")

# 2) incidência por UF no ano escolhido, um agravo por coluna
partes = []
for ag in AGRAVOS:
    inc = tabela(ag, "incidencia")
    if inc is None:
        continue
    partes.append(
        inc.filter((F.col("nivel") == "ESTADO_ANO") & (F.col("ano_analise") == ANO_CMP))
        .select(F.col("nome").alias("uf"), F.col("codigo").alias("codigo_uf"),
                F.lit(NOMES[ag]).alias("agravo"), "casos_provaveis", "coef_incidencia_100mil")
        .toPandas()
    )
if partes:
    longo = pd.concat(partes, ignore_index=True)
    largo = longo.pivot_table(index=["codigo_uf", "uf"], columns="agravo",
                              values=["casos_provaveis", "coef_incidencia_100mil"]).reset_index()
    largo.columns = [" ".join(str(c) for c in col if c).strip() for col in largo.columns]
    salvar_csv(largo.sort_values("codigo_uf"), f"C2_incidencia_por_uf_{ANO_CMP}")

# 3) óbitos confirmados e letalidade por ano
partes = []
for ag in AGRAVOS:
    an = tabela(ag, "annual")
    if an is None:
        continue
    cols = [c for c in ["ano_analise", "casos_provaveis", "casos_confirmados", "obitos", "letalidade_pct",
                        "hospitalizados", "municipios_com_casos"] if c in an.columns]
    partes.append(an.select(F.lit(NOMES[ag]).alias("agravo"), *cols).toPandas())
salvar_csv(pd.concat(partes, ignore_index=True).sort_values(["agravo", "ano_analise"]) if partes else None,
       "C3_obitos_letalidade")

# 4) municípios acima do limiar de alta incidência no ano escolhido
linhas = []
for ag in AGRAVOS:
    inc = tabela(ag, "incidencia")
    if inc is None:
        continue
    m = inc.filter((F.col("nivel") == "MUNICIPIO_ANO") & (F.col("ano_analise") == ANO_CMP))
    r = m.agg(
        F.count(F.when(F.col("coef_incidencia_100mil").isNotNull(), 1)).alias("municipios_com_incidencia"),
        F.count(F.when(F.col("coef_incidencia_100mil") > 300, 1)).alias("municipios_acima_300"),
        F.expr("percentile_approx(coef_incidencia_100mil, 0.5)").alias("mediana_100mil"),
        F.max("coef_incidencia_100mil").alias("maxima_100mil"),
    ).first()
    linhas.append({"agravo": NOMES[ag], "ano": ANO_CMP, **r.asDict()})
salvar_csv(pd.DataFrame(linhas) if linhas else None, f"C4_municipios_alta_incidencia_{ANO_CMP}")

print(f"\nArquivos em: {OUT_DIR}")

# COMMAND ----------

# DBTITLE 1,Principais achados em frases (por doença)
import pandas as pd

achados = []


def add(agravo, tema, texto):
    achados.append({"agravo": agravo, "ordem": len(achados) + 1, "tema": tema, "achado": texto})


def fmt(x, casas=0):
    s = f"{x:,.{casas}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


MESES = ["", "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
uf = spark.table(f"{DESTINO}.insight_uf_ano").toPandas()
pop_br = spark.table(f"{DESTINO}.populacao").groupBy("ano").agg(F.sum("populacao").alias("pop")).toPandas()
saz = spark.table(f"{DESTINO}.insight_sazonalidade").toPandas()
per = spark.table(f"{DESTINO}.insight_municipio_periodo").filter("entra_ranking").toPandas()
coc = spark.table(f"{DESTINO}.insight_cocirculacao").toPandas()
mon = spark.table(f"{DESTINO}.insight_monitoramento_uf").toPandas()

for ag in ["Dengue", "Chikungunya", "Zika"]:
    u = uf[uf["agravo"] == ag]
    if u.empty:
        continue
    anual = u.groupby("ano", as_index=False)["casos_provaveis"].sum().merge(pop_br, on="ano", how="left")
    anual["inc"] = anual["casos_provaveis"] / anual["pop"] * 1e5
    top = anual.sort_values("casos_provaveis", ascending=False).iloc[0]
    add(ag, "Tendência", f"{ag}: {int(top.ano)} foi o ano com mais casos prováveis no período "
        f"({fmt(top.casos_provaveis)}; {fmt(top.inc, 1)} por 100 mil hab.).")
    serie = "; ".join(f"{int(r.ano)}: {fmt(r.casos_provaveis)}" for r in anual.sort_values("ano").itertuples())
    add(ag, "Tendência", f"{ag} – casos prováveis por ano: {serie}.")

    acc = (u.groupby(["uf_sigla", "uf_nome"], as_index=False)
           .agg(casos=("casos_provaveis", "sum"), pop=("populacao", "mean"), anos=("ano", "nunique")))
    acc["inc"] = acc["casos"] / acc["pop"] * 1e5 / u["ano"].nunique()
    t3 = acc.sort_values("inc", ascending=False).head(3)
    add(ag, "Áreas mais afetadas", f"{ag}: UFs com maior incidência média anual em {ANO_INI}-{ANO_FIM}: "
        + ", ".join(f"{r.uf_nome} ({fmt(r.inc, 1)}/100 mil)" for r in t3.itertuples()) + ".")
    reg = u.groupby("regiao", as_index=False).agg(casos=("casos_provaveis", "sum"), pop=("populacao", "sum"))
    reg["inc"] = reg["casos"] / reg["pop"] * 1e5   # soma casos / soma pop dos anos = média anual
    reg = reg.sort_values("inc", ascending=False)
    if not reg.empty:
        add(ag, "Áreas mais afetadas", f"{ag}: a região mais afetada foi o {reg.iloc[0].regiao}; "
            f"a menos afetada, o {reg.iloc[-1].regiao}.")

    p = per[per["agravo"] == ag].sort_values("incidencia_media_anual_100mil", ascending=False)
    if not p.empty:
        persist = int((p["padrao_area"] == "Persistente (3+ anos em alta)").sum())
        add(ag, "Áreas mais afetadas", f"{ag}: municípios (>= {fmt(POP_MIN)} hab.) com maior incidência média "
            "anual: " + ", ".join(f"{r.municipio_nome}/{r.uf_sigla} ({fmt(r.incidencia_media_anual_100mil, 0)})"
                                  for r in p.head(5).itertuples())
            + f". {persist} municípios ficaram em alta incidência em 3 anos ou mais.")

    z = saz[saz["agravo"] == ag]
    if not z.empty:
        pico = z.groupby("regiao")["mes_pico"].agg(lambda s: s.mode().iloc[0] if not s.mode().empty else None)
        add(ag, "Sazonalidade", f"{ag}: mês de pico mais frequente por região – "
            + ", ".join(f"{r}: {MESES[int(m)]}" for r, m in pico.dropna().items()) + ".")
        dur = z["duracao_temporada_semanas"].median()
        add(ag, "Sazonalidade", f"{ag}: a temporada (10% a 90% dos casos do ano) dura em mediana "
            f"{fmt(dur)} semanas.")

    m = mon[mon["agravo"] == ag]
    acima = m[m["situacao"].str.startswith(("1.", "2."))]
    if not m.empty:
        add(ag, "Situação atual", f"{ag}: nas últimas {N_SEM_MON} semanas consolidadas "
            f"(até {m['janela_fim'].max()}), {len(acima)} UFs estão acima do esperado"
            + (": " + ", ".join(acima.sort_values("razao_observado_esperado", ascending=False)["uf_sigla"].head(8))
               if len(acima) else "") + ".")

for ano, g in coc.groupby("ano"):
    n2 = int((g["n_doencas_media_ou_alta"] >= 2).sum())
    n3 = int((g["n_doencas_media_ou_alta"] == 3).sum())
    if n2:
        add("Comparação", "Co-circulação", f"{int(ano)}: {n2} municípios tiveram 2 ou mais doenças com incidência "
            f">= 100/100 mil ({n3} com as três).")

if TEM_CLIMA and existe("insight_perfil_climatico"):
    pc = spark.table(f"{DESTINO}.insight_perfil_climatico").filter("nivel = 'UF'").toPandas()
    for ag, g in pc.groupby("agravo"):
        q4, q1 = g[g["quartil_incidencia"] == 4], g[g["quartil_incidencia"] == 1]
        if q4.empty or q1.empty:
            continue
        q4, q1 = q4.iloc[0], q1.iloc[0]
        add(ag, "Clima", f"{ag}: as UFs mais afetadas (quartil de maior incidência) tiveram em média "
            f"{fmt(q4.temp_media, 1)} °C, {fmt(q4.chuva_anual_mm)} mm de chuva por ano e "
            f"{fmt(q4.semanas_favoraveis_aedes, 0)} semanas favoráveis ao Aedes; as menos afetadas, "
            f"{fmt(q1.temp_media, 1)} °C, {fmt(q1.chuva_anual_mm)} mm e {fmt(q1.semanas_favoraveis_aedes, 0)} semanas.")
    if existe("clima_melhor_defasagem"):
        md = spark.table(f"{DESTINO}.clima_melhor_defasagem").filter("nivel = 'Brasil'").toPandas()
        nomes = {"temp_media": "temperatura média", "temp_min": "temperatura mínima",
                 "chuva_mm": "chuva", "umidade_media": "umidade"}
        for ag, g in md.groupby("agravo"):
            g = g.reindex(g["rho_mais_forte"].abs().sort_values(ascending=False).index)
            r = g.iloc[0]
            add(ag, "Clima", f"{ag}: a variável de clima mais associada à incidência no Brasil foi a "
                f"{nomes.get(r.variavel_clima, r.variavel_clima)}, com {int(r.defasagem_mais_forte)} semanas "
                f"de antecedência (Spearman = {fmt(r.rho_mais_forte, 2)}; sem sazonalidade = "
                f"{fmt(r.rho_sem_sazonalidade, 2)}). Associação, não causalidade.")

resumo = pd.DataFrame(achados)
salvar(spark.createDataFrame(resumo), "insight_resumo")
for r in resumo.itertuples():
    print(f"[{r.agravo} | {r.tema}] {r.achado}")