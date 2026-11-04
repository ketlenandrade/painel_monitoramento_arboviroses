# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 90 – LIMPEZA DO CATÁLOGO (limite de 500 tabelas e views do Unity Catalog)
# ==============================================================================
# O plano gratuito do Databricks aceita no máximo 500 tabelas + views por
# metastore. Passando disso, qualquer CREATE TABLE / CREATE VIEW falha com
#   QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED ... (limit: 500)
#
# Este notebook conta os objetos e mostra o que pode ser apagado SEM afetar o
# projeto (as tabelas de produção e os arquivos baixados ficam intactos):
#   experimentos   – schemas dos experimentos de eficiência (dengue_br_exp,
#                    dengue_br_esc, ...): cópias completas do pipeline, ~50
#                    tabelas cada. São recriados se você rodar o 24 de novo.
#   outros_recortes– schemas de versões antigas com outro recorte (ex.: dengue_mg)
#   views_sem_uso  – views que o dashboard não usa: vw_painel_* (pipeline antigo)
#                    e dengue_semanal / _municipios / _incidencia (painel antigo)
#   legado_populacao – a população que ficava no schema de cada doença; agora há
#                    uma só, em arboviroses_br (só apaga se a nova existir)
#
# NUNCA apaga: dengue_br / chikungunya_br / zika_br (produção), *_raw (arquivos
# baixados do OpenDataSUS), arboviroses_br (painel e análises) e
# arboviroses_metricas (tempos do TCC).
#
# COMO USAR
#   1. modo = listar (padrão): só mostra.
#   2. modo = apagar, escolha os grupos e escreva APAGAR em confirmar.
# ==============================================================================

dbutils.widgets.dropdown("modo", "listar", ["listar", "apagar"])
dbutils.widgets.multiselect("grupos", "experimentos",
                            ["experimentos", "outros_recortes", "views_sem_uso", "legado_populacao"])
dbutils.widgets.text("confirmar", "")

import re
import pandas as pd
from pyspark.sql import functions as F

CATALOG = "workspace"
LIMITE = 500
MODO = dbutils.widgets.get("modo")
GRUPOS = [g for g in dbutils.widgets.get("grupos").split(",") if g]
AGR = "(dengue|chikungunya|zika)"
PROTEGIDOS = {f"{a}_br" for a in NOMES_AGRAVO} | {f"{a}_raw" for a in NOMES_AGRAVO} | {
    "arboviroses_br", "arboviroses_metricas", "information_schema", "default"}

# COMMAND ----------

# DBTITLE 1,Inventário: tabelas e views do catálogo
inv = contar_objetos_uc(detalhado=True)
total = len(inv)
print(f"Objetos (tabelas + views) no metastore: {total} de {LIMITE}")
display(inv.groupby(["catalogo", "schema"], as_index=False).size()
        .rename(columns={"size": "objetos"}).sort_values("objetos", ascending=False))

# COMMAND ----------

# DBTITLE 1,O que pode ser apagado
ws = inv[inv["catalogo"] == CATALOG]
schemas = sorted(ws["schema"].unique())
alvos = []   # (grupo, tipo, nome completo, n_objetos)

for sch in schemas:
    if sch in PROTEGIDOS:
        continue
    n = int((ws["schema"] == sch).sum())
    if re.fullmatch(rf"{AGR}_br_[a-z0-9_]+", sch):
        alvos.append(("experimentos", "SCHEMA", f"{CATALOG}.{sch}", n))
    elif re.fullmatch(rf"{AGR}_(?!br\b)[a-z]{{2}}(_[a-z0-9_]+)?", sch):
        alvos.append(("outros_recortes", "SCHEMA", f"{CATALOG}.{sch}", n))

for r in ws.itertuples():
    nome = f"{CATALOG}.{r.schema}.{r.tabela}"
    if r.schema in {f"{a}_br" for a in NOMES_AGRAVO} and r.tabela.startswith("vw_painel_") and r.tipo == "VIEW":
        alvos.append(("views_sem_uso", "VIEW", nome, 1))
    if r.schema == "arboviroses_br" and r.tipo == "VIEW" and re.fullmatch(rf"{AGR}_(semanal|municipios|incidencia)", r.tabela):
        alvos.append(("views_sem_uso", "VIEW", nome, 1))

_nova_pop = ((ws["schema"] == "arboviroses_br") & (ws["tabela"] == "dim_municipio_populacao")).any()
if _nova_pop:
    for r in ws.itertuples():
        if r.schema in {f"{a}_br" for a in NOMES_AGRAVO} and r.tabela in ("dim_municipio_populacao", "dim_populacao_sexo_uf"):
            alvos.append(("legado_populacao", "TABLE", f"{CATALOG}.{r.schema}.{r.tabela}", 1))

df_alvos = pd.DataFrame(alvos, columns=["grupo", "tipo", "objeto", "objetos"])
if df_alvos.empty:
    print("Nada a apagar com segurança.")
else:
    resumo = df_alvos.groupby("grupo", as_index=False)["objetos"].sum()
    display(resumo)
    display(df_alvos)
    escolhidos = df_alvos[df_alvos["grupo"].isin(GRUPOS)]
    print(f"Grupos escolhidos {GRUPOS}: liberam {int(escolhidos['objetos'].sum())} objetos "
          f"-> ficariam {total - int(escolhidos['objetos'].sum())} de {LIMITE}.")

# COMMAND ----------

# DBTITLE 1,Apagar (só com modo = apagar e confirmar = APAGAR)
if MODO != "apagar":
    print("modo = listar: nada foi apagado. Para apagar, mude modo para 'apagar' e escreva APAGAR em confirmar.")
elif dbutils.widgets.get("confirmar").strip() != "APAGAR":
    print("Escreva APAGAR no widget confirmar para apagar.")
elif not df_alvos.empty:
    for r in df_alvos[df_alvos["grupo"].isin(GRUPOS)].itertuples():
        sch_ou_tab = r.objeto.split(".")
        if r.tipo == "SCHEMA" and sch_ou_tab[1] in PROTEGIDOS:   # dupla proteção
            continue
        sql = {"SCHEMA": f"DROP SCHEMA IF EXISTS {r.objeto} CASCADE",
               "VIEW": f"DROP VIEW IF EXISTS {r.objeto}",
               "TABLE": f"DROP TABLE IF EXISTS {r.objeto}"}[r.tipo]
        try:
            spark.sql(sql)
            print(f"apagado: {r.objeto} ({r.objetos} objeto(s))")
        except Exception as e:
            print(f"[WARN] {r.objeto}: {str(e)[:200]}")
    print(f"Agora: {len(contar_objetos_uc())} de {LIMITE} objetos.")