# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 13 – INTEGRAÇÃO: DENGUE, CHIKUNGUNYA E ZIKA NAS TABELAS DO PAINEL
# ==============================================================================
# Junta a Gold dos três agravos num único schema, pronto para o dashboard do
# Databricks (AI/BI Dashboards), com filtros de agravo, região, estado, município,
# ano, semana epidemiológica e classificação (confirmado ou não).
#
# Rode DEPOIS do notebook 12 (que roda o pipeline 11 para os três agravos).
#
# Tabelas criadas em  workspace.arboviroses_br :
#   painel_casos        – ano x semana x UF x município x classificação x sexo x faixa
#                         etária, por agravo (base de todos os gráficos)
#   painel_uf_semana    – a mesma base somada por UF e semana (gráficos rápidos)
#   populacao           – população IBGE por município e ano (denominador)
#   painel_incidencia   – view: casos prováveis e incidência por 100 mil por
#                         agravo x ano x UF (e Brasil)
#   dim_municipio_geo   – municípios com UF, região, latitude/longitude e capital
#   arboviroses_integrada_municipio_semana
#                       – BASE INTEGRADA: uma linha por município x semana
#                         epidemiológica, com dengue, chikungunya e zika LADO A
#                         LADO (colunas separadas, nunca somadas), população,
#                         incidência de cada doença e coordenadas
#   arboviroses_integrada_uf_semana – a mesma base integrada por UF x semana
#
# Período: 2022 a 2026 (widgets ano_inicio / ano_fim).
#
# Medidas em todas as tabelas:
#   notificacoes, casos_provaveis, casos_confirmados, obitos (confirmados),
#   obitos_investigacao, hospitalizados
# Grupo da classificação (filtro "confirmado ou não"):
#   Confirmado | Provável não confirmado | Descartado
# ==============================================================================

dbutils.widgets.text("agravos", "dengue,chikungunya,zika")
# views separadas por doença (dengue_semanal...): o dashboard não as usa e elas
# ocupam o limite de 500 tabelas/views do Unity Catalog. Desligadas por padrão.
dbutils.widgets.dropdown("criar_views_por_doenca", "nao", ["nao", "sim"])
dbutils.widgets.text("ano_inicio", "2022")
dbutils.widgets.text("ano_fim", "2026")

from pyspark.sql import functions as F

CATALOG = "workspace"
DESTINO = f"{CATALOG}.arboviroses_br"
UF_ORIGEM = "br"  # schemas dengue_br, chikungunya_br, zika_br
AGRAVOS = [a.strip().lower() for a in dbutils.widgets.get("agravos").split(",") if a.strip()]
NOMES = NOMES_AGRAVO  # funcoes_comuns
ANO_INI = int(dbutils.widgets.get("ano_inicio").strip() or 2022)
ANO_FIM = int(dbutils.widgets.get("ano_fim").strip() or 2026)
# URL_MUNICIPIOS e inicio_semana_epi_col (semana epidemiológica) vêm de funcoes_comuns.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DESTINO}")

dim_uf = dim_uf_df()  # uf_codigo, uf_sigla, uf_nome, regiao (funcoes_comuns)

# COMMAND ----------

# DBTITLE 1,População (denominador da incidência)
# Gravada pelo notebook 10 (compartilhada). As versões antigas do pipeline a
# gravavam no schema de cada agravo: elas servem de reserva.
_candidatos_pop = [f"{DESTINO}.dim_municipio_populacao"] + [
    f"{CATALOG}.{a}_{UF_ORIGEM}.dim_municipio_populacao" for a in ["dengue"] + AGRAVOS]
t_pop = next((t for t in _candidatos_pop if spark.catalog.tableExists(t)), None)
if t_pop is None:
    raise Exception("Dimensão de população não encontrada. Rode antes o notebook 12 (ou o 10).")

populacao = (
    spark.table(t_pop)
    .select(
        F.col("municipio_codigo_6").alias("municipio_codigo"),
        F.col("municipio_nome"),
        F.substring("municipio_codigo_6", 1, 2).alias("uf_codigo"),
        F.col("ano_populacao").alias("ano"),
        F.col("populacao").cast("long").alias("populacao"),
        "fonte_populacao",
    )
    .join(dim_uf, "uf_codigo", "left")
)
populacao.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.populacao")
nomes_mun = populacao.select("municipio_codigo", "municipio_nome").dropDuplicates(["municipio_codigo"])
print(f"populacao: {populacao.count():,} linhas (fonte: {t_pop})")

# COMMAND ----------

# DBTITLE 1,painel_casos: os três agravos numa tabela só
partes = []
for ag in AGRAVOS:
    t = f"{CATALOG}.{ag}_{UF_ORIGEM}.gold_{ag}_painel"
    if not spark.catalog.tableExists(t):
        print(f"[{ag}] {t} não existe: rode o 12 (pipeline 11) para este agravo.")
        continue
    partes.append(spark.table(t).withColumn("agravo", F.lit(NOMES.get(ag, ag))))
if not partes:
    raise Exception("Nenhuma tabela gold_<agravo>_painel encontrada.")

base = partes[0]
for p in partes[1:]:
    base = base.unionByName(p, allowMissingColumns=True)

painel = (
    base
    .join(dim_uf, "uf_codigo", "left")
    .join(nomes_mun, "municipio_codigo", "left")
    .select(
        "agravo",
        F.col("ano_analise").alias("ano"),
        "semana",
        inicio_semana_epi_col("ano_analise", "semana").alias("inicio_semana"),
        F.coalesce("regiao", F.lit("Ignorada")).alias("regiao"),
        F.coalesce("uf_sigla", F.lit("IGN")).alias("uf_sigla"),
        F.coalesce("uf_nome", F.lit("Ignorada")).alias("uf_nome"),
        "municipio_codigo",
        F.coalesce("municipio_nome", F.lit("Ignorado")).alias("municipio_nome"),
        "grupo_classificacao", "classificacao_nome", "sexo_nome", "faixa_etaria",
        "notificacoes", "casos_provaveis", "casos_confirmados",
        "obitos", "obitos_investigacao", "hospitalizados",
    )
    .filter(F.col("ano").between(ANO_INI, ANO_FIM))
)
(painel.write.mode("overwrite").option("overwriteSchema", "true")
 .option("delta.autoOptimize.optimizeWrite", "true")
 .saveAsTable(f"{DESTINO}.painel_casos"))
spark.sql(f"OPTIMIZE {DESTINO}.painel_casos ZORDER BY (ano, uf_sigla, semana)")
print(f"painel_casos: {spark.table(f'{DESTINO}.painel_casos').count():,} linhas")

# COMMAND ----------

# DBTITLE 1,painel_uf_semana (agregada por UF, para gráficos rápidos)
(
    spark.table(f"{DESTINO}.painel_casos")
    .groupBy("agravo", "ano", "semana", "inicio_semana", "regiao", "uf_sigla", "uf_nome", "grupo_classificacao")
    .agg(*[F.sum(c).alias(c) for c in
           ["notificacoes", "casos_provaveis", "casos_confirmados", "obitos", "obitos_investigacao", "hospitalizados"]])
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.painel_uf_semana")
)
print(f"painel_uf_semana: {spark.table(f'{DESTINO}.painel_uf_semana').count():,} linhas")

# COMMAND ----------

# DBTITLE 1,painel_incidencia (view): casos e incidência por agravo x ano x UF
spark.sql(f"""
CREATE OR REPLACE VIEW {DESTINO}.painel_incidencia AS
WITH casos AS (
  SELECT agravo, ano, uf_sigla, uf_nome, regiao,
         SUM(casos_provaveis) AS casos_provaveis, SUM(casos_confirmados) AS casos_confirmados,
         SUM(obitos) AS obitos
  FROM {DESTINO}.painel_uf_semana
  WHERE uf_sigla <> 'IGN'
  GROUP BY agravo, ano, uf_sigla, uf_nome, regiao
),
pop AS (
  SELECT ano, uf_sigla, SUM(populacao) AS populacao FROM {DESTINO}.populacao GROUP BY ano, uf_sigla
)
SELECT c.*, p.populacao,
       ROUND(c.casos_provaveis / p.populacao * 100000, 2) AS incidencia_100mil,
       ROUND(c.obitos / NULLIF(c.casos_confirmados, 0) * 100, 3) AS letalidade_pct
FROM casos c LEFT JOIN pop p ON c.ano = p.ano AND c.uf_sigla = p.uf_sigla
""")

# Comentários das colunas (aparecem no dashboard e no Genie)
for col, txt in {
    "agravo": "Dengue, Chikungunya ou Zika",
    "ano": "Ano epidemiológico dos primeiros sintomas",
    "semana": "Semana epidemiológica dos primeiros sintomas (1 a 53)",
    "uf_sigla": "UF de residência (IGN = não informada)",
    "grupo_classificacao": "Confirmado, Provável não confirmado ou Descartado",
    "casos_provaveis": "Notificações exceto descartadas (definição do Ministério da Saúde)",
    "casos_confirmados": "Casos com classificação final de confirmado",
    "obitos": "Óbitos pelo agravo entre casos confirmados",
}.items():
    spark.sql(f"ALTER TABLE {DESTINO}.painel_casos ALTER COLUMN {col} COMMENT '{txt}'")

# Views separadas por doença (o nome já diz qual é): dengue_semanal,
# chikungunya_semanal, zika_semanal, dengue_municipios, ..., dengue_incidencia, ...
for ag in (AGRAVOS if dbutils.widgets.get("criar_views_por_doenca") == "sim" else []):
    nome = NOMES.get(ag, ag)
    spark.sql(f"CREATE OR REPLACE VIEW {DESTINO}.{ag}_semanal AS "
              f"SELECT * FROM {DESTINO}.painel_uf_semana WHERE agravo = '{nome}'")
    spark.sql(f"CREATE OR REPLACE VIEW {DESTINO}.{ag}_municipios AS "
              f"SELECT * FROM {DESTINO}.painel_casos WHERE agravo = '{nome}'")
    spark.sql(f"CREATE OR REPLACE VIEW {DESTINO}.{ag}_incidencia AS "
              f"SELECT * FROM {DESTINO}.painel_incidencia WHERE agravo = '{nome}'")
if dbutils.widgets.get("criar_views_por_doenca") == "sim":
    print("Views por doença:", ", ".join(f"{a}_semanal/_municipios/_incidencia" for a in AGRAVOS))

display(spark.table(f"{DESTINO}.painel_incidencia").filter("ano = 2024").orderBy("agravo", F.desc("incidencia_100mil")))



# COMMAND ----------

# DBTITLE 1,dim_municipio_geo: coordenadas e capital de cada município
# Cópia pública da base de municípios do IBGE com latitude/longitude (a mesma do
# notebook 14). Usada no mapa do dashboard e na análise de áreas x clima.
import pandas as pd

try:
    _geo = pd.read_csv(URL_MUNICIPIOS)
    _geo = pd.DataFrame({
        "municipio_codigo": _geo["codigo_ibge"].astype(str).str[:6],
        "municipio_codigo_7": _geo["codigo_ibge"].astype(str),
        "municipio_nome_geo": _geo["nome"],
        "latitude": _geo["latitude"].astype(float),
        "longitude": _geo["longitude"].astype(float),
        "capital": _geo["capital"].astype(int),
    })
    geo = spark.createDataFrame(_geo)
except Exception as e:  # sem internet: mantém a tabela anterior, se houver
    print(f"[GEO][WARN] não foi possível baixar coordenadas ({str(e)[:150]}); "
          "mantida a dim_municipio_geo anterior, se existir.")
    geo = None

if geo is not None:
    dim_geo = (
        geo.withColumn("uf_codigo", F.substring("municipio_codigo", 1, 2))
        .join(dim_uf, "uf_codigo", "left")
        .join(nomes_mun, "municipio_codigo", "left")
        .withColumn("municipio_nome", F.coalesce("municipio_nome", "municipio_nome_geo"))
        .select("municipio_codigo", "municipio_codigo_7", "municipio_nome", "uf_codigo", "uf_sigla",
                "uf_nome", "regiao", "latitude", "longitude", "capital")
        .dropDuplicates(["municipio_codigo"])
    )
    dim_geo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.dim_municipio_geo")
    print(f"dim_municipio_geo: {spark.table(f'{DESTINO}.dim_municipio_geo').count():,} municípios")

# COMMAND ----------

# DBTITLE 1,BASE INTEGRADA: dengue, chikungunya e zika lado a lado (município x semana)
# Integra as três bases numa só tabela. Cada doença tem as SUAS colunas
# (casos_dengue, casos_chikungunya, casos_zika...), para comparar e cruzar sem
# somar doenças diferentes. Junta também população, incidência e coordenadas.
SUF = {"Dengue": "dengue", "Chikungunya": "chikungunya", "Zika": "zika"}
_por_mun = (
    spark.table(f"{DESTINO}.painel_casos")
    .filter("municipio_codigo IS NOT NULL AND semana IS NOT NULL")
    .groupBy("ano", "semana", "inicio_semana", "regiao", "uf_sigla", "uf_nome",
             "municipio_codigo", "municipio_nome")
    .pivot("agravo", list(SUF))
    .agg(F.sum("casos_provaveis").alias("casos"),
         F.sum("casos_confirmados").alias("confirmados"),
         F.sum("obitos").alias("obitos"),
         F.sum("hospitalizados").alias("hospitalizados"))
)
# renomeia "Dengue_casos" -> "casos_dengue"
for nome, suf in SUF.items():
    for m in ["casos", "confirmados", "obitos", "hospitalizados"]:
        if f"{nome}_{m}" in _por_mun.columns:
            _por_mun = _por_mun.withColumnRenamed(f"{nome}_{m}", f"{m}_{suf}")
        else:
            _por_mun = _por_mun.withColumn(f"{m}_{suf}", F.lit(0).cast("long"))
_metricas = [f"{m}_{suf}" for suf in SUF.values() for m in ["casos", "confirmados", "obitos", "hospitalizados"]]
# Agravo cuja fonte não traz hospitalização (todas as linhas nulas, ex.: zika):
# a coluna continua nula em vez de virar 0 ("sem informação" ≠ "nenhuma internação").
_sem_hosp = [c for c in _metricas if c.startswith("hospitalizados_")
             and _por_mun.filter(F.col(c).isNotNull()).limit(1).count() == 0]
_por_mun = _por_mun.fillna(0, subset=[c for c in _metricas if c not in _sem_hosp])

_pop_mun = spark.table(f"{DESTINO}.populacao").select("ano", "municipio_codigo", "populacao")
integrada = (
    _por_mun.join(_pop_mun, ["ano", "municipio_codigo"], "left")
    .withColumn("n_doencas_com_casos",
                sum((F.col(f"casos_{s}") > 0).cast("int") for s in SUF.values()))
)
for suf in SUF.values():
    integrada = integrada.withColumn(
        f"incidencia_{suf}_100mil",
        F.round(F.col(f"casos_{suf}") / F.col("populacao") * 100000, 3))
if spark.catalog.tableExists(f"{DESTINO}.dim_municipio_geo"):
    integrada = integrada.join(
        spark.table(f"{DESTINO}.dim_municipio_geo").select("municipio_codigo", "latitude", "longitude", "capital"),
        "municipio_codigo", "left")
(integrada.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{DESTINO}.arboviroses_integrada_municipio_semana"))
spark.sql(f"OPTIMIZE {DESTINO}.arboviroses_integrada_municipio_semana ZORDER BY (ano, uf_sigla, municipio_codigo)")
print(f"arboviroses_integrada_municipio_semana: "
      f"{spark.table(f'{DESTINO}.arboviroses_integrada_municipio_semana').count():,} linhas")

# Mesma base integrada por UF x semana (inclui IGN = UF não informada)
_pop_uf = spark.table(f"{DESTINO}.populacao").groupBy("ano", "uf_sigla").agg(F.sum("populacao").alias("populacao"))
integrada_uf = (
    spark.table(f"{DESTINO}.arboviroses_integrada_municipio_semana")
    .groupBy("ano", "semana", "inicio_semana", "regiao", "uf_sigla", "uf_nome")
    .agg(*[F.sum(c).alias(c) for c in _metricas])
    .join(_pop_uf, ["ano", "uf_sigla"], "left")
)
for suf in SUF.values():
    integrada_uf = integrada_uf.withColumn(
        f"incidencia_{suf}_100mil", F.round(F.col(f"casos_{suf}") / F.col("populacao") * 100000, 3))
integrada_uf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{DESTINO}.arboviroses_integrada_uf_semana")
print(f"arboviroses_integrada_uf_semana: {spark.table(f'{DESTINO}.arboviroses_integrada_uf_semana').count():,} linhas")

# Conferência da integração: o total de cada doença na base integrada tem de ser
# igual ao da base longa (painel_casos), com município informado.
_conf_longa = {r["agravo"]: r["c"] for r in spark.table(f"{DESTINO}.painel_casos")
               .filter("municipio_codigo IS NOT NULL AND semana IS NOT NULL")
               .groupBy("agravo").agg(F.sum("casos_provaveis").alias("c")).collect()}
_conf_int = spark.table(f"{DESTINO}.arboviroses_integrada_municipio_semana").agg(
    *[F.sum(f"casos_{s}").alias(n) for n, s in SUF.items()]).first().asDict()
for n in SUF:
    a, b = int(_conf_longa.get(n) or 0), int(_conf_int.get(n) or 0)
    print(f"[CONFERÊNCIA] {n}: base longa = {a:,} | base integrada = {b:,} -> {'OK' if a == b else 'DIFERENTE'}")
    assert a == b, f"Integração com diferença em {n}"

print(f"Pronto. Tabelas em {DESTINO}: painel_casos, painel_uf_semana, populacao, painel_incidencia (view), "
      "dim_municipio_geo, arboviroses_integrada_municipio_semana, arboviroses_integrada_uf_semana.")