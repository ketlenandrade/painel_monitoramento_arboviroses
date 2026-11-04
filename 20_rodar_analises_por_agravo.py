# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 20 – RODAR AS ANÁLISES DE CADA AGRAVO: VIGILÂNCIA (21) E RESULTADOS (22)
# ==============================================================================
# Para dengue, chikungunya e zika, roda:
#   21_vigilancia_agravo     – canal endêmico, alertas, nowcasting, atraso,
#                              completude e validação (grava tabelas Gold de análise)
#   22_resultados_tcc_agravo – visualizações e CSV da rodada (só lê tabelas)
# Rode DEPOIS do 12 (pipeline). Não baixa nem reprocessa dados.
#
# Análise de sensibilidade do artigo: mude semanas_consecutivas,
# anos_excluidos_canal ou data_disponibilidade e use um rotulo_execucao
# diferente; só as análises são refeitas, o pipeline não.
# ==============================================================================

dbutils.widgets.text("agravos", "dengue,chikungunya,zika")
dbutils.widgets.text("ano_inicio_analise", "2022")
dbutils.widgets.text("ano_inicio_fonte", "2016")  # 2016-2021 = só histórico do canal endêmico e do nowcast
dbutils.widgets.text("ano_fim_fonte", "2026")
dbutils.widgets.dropdown("semanas_consecutivas", "2", ["1", "2", "3"])
dbutils.widgets.text("anos_excluidos_canal", "")
dbutils.widgets.dropdown("data_disponibilidade", "DT_DIGITA", ["DT_DIGITA", "DT_NOTIFIC"])
dbutils.widgets.text("rotulo_execucao", "base")
dbutils.widgets.dropdown("exportar_csv", "sim", ["sim", "nao"])
dbutils.widgets.dropdown("forcar_reprocessamento", "nao", ["nao", "sim"])
dbutils.widgets.dropdown("rodar_resultados", "sim", ["sim", "nao"])

import json, time
from datetime import datetime

AGRAVOS = [a.strip().lower() for a in dbutils.widgets.get("agravos").split(",")
           if a.strip().lower() in NOMES_AGRAVO]
TIMEOUT_S = 6 * 3600
COMUNS = {k: dbutils.widgets.get(k).strip() for k in [
    "ano_inicio_analise", "ano_inicio_fonte", "ano_fim_fonte", "semanas_consecutivas",
    "anos_excluidos_canal", "data_disponibilidade", "forcar_reprocessamento"]}
ROTULO = dbutils.widgets.get("rotulo_execucao").strip() or "base"
ETAPAS = ["21_vigilancia_agravo"] + (["22_resultados_tcc_agravo"] if dbutils.widgets.get("rodar_resultados") == "sim" else [])

# COMMAND ----------

# DBTITLE 1,Rodar 21 e 22 para cada agravo
resultado = []
for agravo in AGRAVOS:
    for nb in ETAPAS:
        args = {**COMUNS, "agravo": agravo, "sufixo_schema": "",
                "rotulo_execucao": f"{agravo}_br" if ROTULO == "base" else f"{agravo}_br_{ROTULO}",
                "exportar_csv": dbutils.widgets.get("exportar_csv")}
        t0 = time.perf_counter()
        print(f"[{datetime.now():%H:%M:%S}] {agravo} – {nb} …")
        try:
            saida = dbutils.notebook.run(caminho_absoluto(f"./{nb}"), TIMEOUT_S, args)
            status = json.loads(saida).get("status", saida) if saida and saida.startswith("{") else saida
            resultado.append((agravo, nb, "ok", str(status), round(time.perf_counter() - t0, 1)))
            print(f"    {status} em {time.perf_counter() - t0:,.0f} s")
        except Exception as ex:
            resultado.append((agravo, nb, "erro", motivo_do_erro(ex)[:300], round(time.perf_counter() - t0, 1)))
            print(f"    ERRO: {motivo_do_erro(ex)}")
            break   # sem a vigilância, os resultados desse agravo não fazem sentido
display(spark.createDataFrame(resultado, "agravo string, etapa string, resultado string, detalhe string, segundos double"))

_erros = [r for r in resultado if r[2] == "erro"]
if resultado and len(_erros) == len(AGRAVOS):
    raise Exception("As análises falharam para todos os agravos: " + "; ".join(f"{r[0]}: {r[3][:150]}" for r in _erros))
dbutils.notebook.exit(json.dumps({"agravos": AGRAVOS, "erros": [f"{r[0]}/{r[1]}" for r in _erros]}))