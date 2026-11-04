# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 12 – RODAR O PIPELINE: DENGUE, CHIKUNGUNYA E ZIKA (BRASIL INTEIRO)
# ==============================================================================
# Baixa automaticamente do OpenDataSUS os arquivos anuais de 2022 a 2026 de dengue,
# chikungunya e zika e roda o pipeline (notebook 11) para os três agravos, Brasil inteiro.
# Antes, grava UMA vez a população do IBGE (notebook 10), usada pelos três.
# Este notebook só EXTRAI e TRATA. As análises ficam nos notebooks 2x.
#
# A dimensão de população do IBGE (5.570 municípios) fica em
# workspace.arboviroses_br.dim_municipio_populacao, uma para os três agravos.
#
# Schemas criados: workspace.dengue_br, workspace.chikungunya_br, workspace.zika_br
# Os CSV de cada agravo ficam em /Volumes/workspace/<agravo>_br/raw/exportacoes/<agravo>_br/
# Depois rode o 13_integracao_tabelas_painel.
#
# Tempo: o Brasil inteiro tem muito mais registros que MG. A primeira execução de
# cada agravo (download + Bronze + Silver + Gold) pode levar de 30 min a mais de 1 h
# no serverless.
# ==============================================================================

dbutils.widgets.text("caminho_pipeline", "./11_pipeline_agravo")
dbutils.widgets.text("agravos", "dengue,chikungunya,zika")
dbutils.widgets.dropdown("executar_pipeline", "sim", ["sim", "nao"])
dbutils.widgets.dropdown("forcar_reprocessamento", "nao", ["nao", "sim"])
dbutils.widgets.text("ano_inicio_analise", "2022")
# Período baixado do OpenDataSUS (arquivos anuais DENGBRaa, CHIKBRaa, ZIKABRaa)
dbutils.widgets.text("ano_inicio_fonte", "2016")  # 2016-2021 = só histórico do canal endêmico e do nowcast
dbutils.widgets.text("ano_fim_fonte", "2026")
# Links opcionais, um por base (vazio = endereço padrão do OpenDataSUS). Copie o
# link do botão "Baixar" de qualquer ano da base em dadosabertos.saude.gov.br:
#   arboviroses-dengue | arboviroses-febre-de-chikungunya | arboviroses-zika-virus
dbutils.widgets.text("url_dengue", "")
dbutils.widgets.text("url_chikungunya", "")
dbutils.widgets.text("url_zika", "")

import os
import json
import time
from datetime import datetime

AGRAVOS = [a.strip().lower() for a in dbutils.widgets.get("agravos").split(",")
           if a.strip().lower() in ("dengue", "chikungunya", "zika")]
UF = "BR"  # Brasil inteiro (fixo)
EXECUTAR = dbutils.widgets.get("executar_pipeline") == "sim"
FORCAR = dbutils.widgets.get("forcar_reprocessamento")
ANO_INICIO = dbutils.widgets.get("ano_inicio_analise").strip() or "2022"
ANO_INI_FONTE = dbutils.widgets.get("ano_inicio_fonte").strip() or "2022"
ANO_FIM_FONTE = dbutils.widgets.get("ano_fim_fonte").strip() or "2026"
CATALOG = "workspace"
TIMEOUT_S = 8 * 3600  # sem limite explícito o serverless encerra a execução filha em ~1 h

AGRAVOS = sorted(AGRAVOS, key=lambda a: 0 if a == "dengue" else 1)
print(f"Agravos: {AGRAVOS} | Brasil inteiro | arquivos {ANO_INI_FONTE}-{ANO_FIM_FONTE} | executar pipeline: {EXECUTAR}")

# COMMAND ----------

# DBTITLE 1,População do IBGE (uma vez para os três agravos)
if EXECUTAR:
    t0 = time.perf_counter()
    print(f"[{datetime.now():%H:%M:%S}] população IBGE – iniciando…")
    _ini_ms = int(time.time() * 1000) - 5000
    try:
        saida_pop = dbutils.notebook.run(caminho_absoluto("./10_populacao_ibge"), TIMEOUT_S, {
            "agravo": "dengue", "ano_inicio_fonte": ANO_INI_FONTE, "ano_fim_fonte": ANO_FIM_FONTE,
            "usar_cache_ibge": "sim", "sufixo_schema": "",
        })
        print(f"    ok em {time.perf_counter() - t0:,.0f} s – {saida_pop}")
    except Exception as ex:
        # Sem a população nova, o pipeline ainda roda com a dimensão já gravada
        # (anos que faltarem recebem a população do ano mais próximo).
        print(f"    AVISO: população do IBGE falhou; seguindo com a dimensão existente.\n{erro_detalhado(ex, _ini_ms)}")
        if not spark.catalog.tableExists(f"{CATALOG}.arboviroses_br.dim_municipio_populacao"):
            raise

# COMMAND ----------

# DBTITLE 1,Executar o pipeline para cada agravo
# caminho_absoluto e motivo_do_erro vêm de funcoes_comuns.
CAMINHO_PIPELINE = caminho_absoluto(dbutils.widgets.get("caminho_pipeline").strip())
resultado_execucoes = []

if EXECUTAR:
    for agravo in AGRAVOS:
        args = {
            "agravo": agravo, "sufixo_schema": "",
            "modo_execucao": "completo", "forcar_reprocessamento": FORCAR,
            "usar_cache_ibge": "sim", "exportar_csv": "sim",
            "rotulo_execucao": f"{agravo}_{UF.lower()}",
            "ano_inicio_analise": ANO_INICIO,
            "ano_inicio_fonte": ANO_INI_FONTE, "ano_fim_fonte": ANO_FIM_FONTE,
            "experimento": "", "configuracao": "", "repeticao": "", "aquecimento": "nao",
            "simular_falha": "", "simular_ano_alterado": "",
            "url_fonte": dbutils.widgets.get(f"url_{agravo}").strip(),
        }
        t0 = time.perf_counter()
        _ini_ms = int(time.time() * 1000) - 5000
        print(f"[{datetime.now():%H:%M:%S}] {agravo} – iniciando…")
        try:
            saida = dbutils.notebook.run(CAMINHO_PIPELINE, TIMEOUT_S, args)
            try:
                status = json.loads(saida).get("status", saida)
            except Exception:
                status = saida
            print(f"    ok em {time.perf_counter() - t0:,.0f} s – status: {status}")
            resultado_execucoes.append((agravo, "ok", str(status), round(time.perf_counter() - t0, 1)))
        except Exception as ex:
            motivo = erro_detalhado(ex, _ini_ms)
            print(f"    ERRO em {time.perf_counter() - t0:,.0f} s:\n{motivo}")
            resultado_execucoes.append((agravo, "erro", motivo[:300], round(time.perf_counter() - t0, 1)))
    display(spark.createDataFrame(resultado_execucoes, "agravo string, resultado string, detalhe string, segundos double"))
else:
    print("executar_pipeline = nao: usando as tabelas já gravadas.")

# COMMAND ----------

# DBTITLE 1,Resultado para o notebook 00
_erros = [r for r in resultado_execucoes if r[1] == "erro"]
if EXECUTAR and len(_erros) == len(resultado_execucoes) and resultado_execucoes:
    raise Exception("O pipeline falhou para todos os agravos: " + " || ".join(f"{r[0]}: {r[2][-700:]}" for r in _erros))
dbutils.notebook.exit(json.dumps({"agravos": AGRAVOS, "erros": [r[0] for r in _erros]}))