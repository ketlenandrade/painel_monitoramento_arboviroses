# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./config_agravo

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 22 – RESULTADOS DO TCC DE UM AGRAVO: VISUALIZAÇÕES E CSV DA RODADA
# ==============================================================================
# Mostra as tabelas Gold e de vigilância e exporta os CSV da rodada para o
# artigo (antes, no fim do pipeline). Só lê tabelas; não recalcula nada.
# Rode depois do 11 (pipeline) e do 21 (vigilância) do mesmo agravo.
#   rotulo_execucao : nome da pasta da rodada em <volume>/exportacoes/
#   exportar_csv    : sim = grava os CSV
# ==============================================================================

# COMMAND ----------

# DBTITLE 1,Execução do pipeline a que estes resultados se referem
# Última execução do pipeline (11) neste schema: run_id e anos de arquivo.
_ult = (spark.table(METRICS_TABLE).filter((F.col("schema_analise") == SCHEMA) & (F.col("camada") == "gold_calculo"))
        .orderBy(F.desc("executado_em")).limit(1).collect())
RUN_ID_PIPELINE = _ult[0]["run_id"] if _ult else None
MODO_PIPELINE = _ult[0]["modo_execucao"] if _ult else None
downloaded_paths = [(int(r[0]), None) for r in spark.table(SILVER_TABLE).select("_source_year").distinct()
                    .orderBy("_source_year").collect() if r[0] is not None]
print(f"[RESULTADOS] {CFG_AGRAVO['nome']} | pipeline run_id {RUN_ID_PIPELINE} | anos {[a for a, _ in downloaded_paths]}")

# COMMAND ----------

# DBTITLE 1,Visualização - Gold Semanal
display(spark.table(GOLD_WEEKLY))

# COMMAND ----------

# DBTITLE 1,Tabela - Casos prováveis por ano (agravo e UF dos widgets)
# Caso provável = notificado e NÃO descartado (códigos em CFG_AGRAVO), por ano
# epidemiológico de início dos sintomas, residentes na UF do widget.
_cols = spark.table(GOLD_ANNUAL).columns
_sel = [c for c in [
    "ano_analise", "total_notificacoes", "casos_provaveis", "casos_confirmados",
    "obitos", "incidencia_provaveis_100mil"
] if c in _cols]
tabela_provaveis_ano = spark.table(GOLD_ANNUAL).select(*_sel).orderBy("ano_analise")
display(tabela_provaveis_ano)
print(
    f"Ano corrente ({END_YEAR}) é PARCIAL: contém só as semanas já notificadas "
    "até a data da última atualização da fonte."
)

# COMMAND ----------

# DBTITLE 1,Visualização - Gold Anual
display(spark.table(GOLD_ANNUAL))

# COMMAND ----------

# DBTITLE 1,Visualização - Gold Municipal
display(spark.table(GOLD_MUNICIPAL))

# COMMAND ----------

# DBTITLE 1,Visualização - Gold Sexo (resumo, anual, faixa etária)
display(spark.table(GOLD_SEXO_SUMMARY))
display(spark.table(GOLD_SEXO_ANUAL).orderBy("ano_analise", "sexo_nome"))
display(spark.table(GOLD_SEXO_FAIXA).orderBy("faixa_etaria", "sexo_nome"))

# COMMAND ----------

# DBTITLE 1,Resultados para o TCC
print("1) Validação contra números oficiais")
if spark.catalog.tableExists(GOLD_VALIDACAO):
    display(spark.table(GOLD_VALIDACAO))
else:
    print(f"   Preencha {VALIDACAO_CSV} e rode novamente.")

print("2a) Atraso de notificação por ano")
display(spark.table(GOLD_ATRASO))

print("2b) Revisão dos dados: % do valor final conhecido por idade da semana")
display(spark.table(GOLD_REVISAO_RESUMO))

print("3a) Canal endêmico estadual")
display(spark.table(GOLD_CANAL_UF).orderBy("ano_epi", "semana_epi"))

print("3b) Avaliação das regras de alerta")
display(spark.table(GOLD_AVALIACAO_ALERTAS))

print("3c) Alertas em tempo real: desempenho por defasagem de informação e limiar")
display(spark.table(GOLD_ALERTAS_RT))

print("3d) Nowcast estadual: erro do dado bruto x dado corrigido, por defasagem")
display(spark.table(GOLD_NOWCAST_RESUMO))

print("3e) Testes de qualidade automatizados (somente o que não foi aprovado)")
display(spark.table(GOLD_TESTES_QUALIDADE).filter(F.col("status") != "APROVADO"))

print("3f) Perfil clínico: sintomas mais frequentes (todos os anos, todas as idades)")
display(
    spark.table(GOLD_SINTOMAS)
    .filter(F.col("faixa_etaria") == "Todas")
    .groupBy("sintoma")
    .agg(F.sum("n_presente").alias("n_presente"), F.sum("n_informado").alias("n_informado"))
    .withColumn("pct_presente", F.round(F.col("n_presente") / F.col("n_informado") * 100, 2))
    .orderBy(F.desc("pct_presente"))
)

print("3g) Fatores de risco (risco relativo bruto, todos os anos)")
display(
    spark.table(GOLD_FATORES_RISCO).filter(F.col("ano") == "Todos")
    .orderBy("desfecho", F.desc("risco_relativo"))
)

print("4) Completude das variáveis (Romero & Cunha, 2006)")
display(spark.table(GOLD_COMPLETUDE).orderBy("variavel", "ano"))

print("5) Custo do processamento: incremental x completo")
display(
    spark.table(METRICS_TABLE)
    .filter(
        (F.col("schema_analise") == SCHEMA)
        & F.col("modo_execucao").isin("completo", "incremental")
        & ~F.coalesce(F.col("aquecimento"), F.lit(False))
    )
    .groupBy("camada", "modo_execucao")
    .agg(
        F.count("*").alias("execucoes"),
        F.round(F.avg("duracao_segundos"), 2).alias("duracao_media_s"),
        F.round(F.avg("n_anos_processados"), 2).alias("anos_processados_medio"),
    )
    .orderBy("camada", "modo_execucao")
)

# COMMAND ----------

# DBTITLE 1,EXPORTAR RESULTADOS DA RODADA (CSV para o artigo)
# ==============================================================================
# EXPORTAR RESULTADOS DA RODADA
# ==============================================================================
# As tabelas Gold são sobrescritas a cada execução. Para comparar rodadas
# (1, 2 e 3 semanas consecutivas; com e sem anos epidêmicos), esta célula
# salva uma cópia das tabelas de resultado em CSV, numa pasta por rodada:
#   /Volumes/<catalogo>/<agravo>_br/raw/exportacoes/<rotulo_execucao>/
# Use um "rotulo_execucao" diferente em cada rodada (ex.: base, cons1, cons3,
# sem_epidemicos). Os CSV abrem direto no Excel e servem para montar as
# tabelas e figuras do artigo. Baixe-os pelo Catalog Explorer (Volumes).
# ==============================================================================

import json

if not EXPORTAR_CSV:
    print("[EXPORT] Desligado (widget exportar_csv = nao): nada foi exportado.")
else:
    EXPORT_DIR = os.path.join(VOLUME_PATH, "exportacoes", ROTULO_EXECUCAO)
    os.makedirs(EXPORT_DIR, exist_ok=True)

    tabelas_export = {
        "avaliacao_alertas": GOLD_AVALIACAO_ALERTAS,
        "alertas_tempo_real": GOLD_ALERTAS_RT,
        "nowcast_estado": GOLD_NOWCAST,
        "nowcast_resumo": GOLD_NOWCAST_RESUMO,
        "fatores_completude": GOLD_FATORES_COMPLETUDE,
        "sintomas": GOLD_SINTOMAS,
        "perfil": GOLD_PERFIL,
        "laboratorio": GOLD_LABORATORIO,
        "autoctonia": GOLD_AUTOCTONIA,
        "tempos": GOLD_TEMPOS,
        "gravidade_sinais": GOLD_GRAVIDADE_SINAIS,
        "cubo_semanal": GOLD_CUBO_SEMANAL,
        "regional": GOLD_REGIONAL,
        "incidencia": GOLD_INCIDENCIA,
        "demografico_semanal": GOLD_DEMOGRAFICO_SEMANAL,
        "clinico_semanal": GOLD_CLINICO_SEMANAL,
        "hospitalizacao_semanal": GOLD_HOSPITALIZACAO_SEMANAL,
        "municipal_semanal": GOLD_MUNICIPAL_WEEKLY,
        "fatores_risco": GOLD_FATORES_RISCO,
        "testes_qualidade": GOLD_TESTES_QUALIDADE,
        "linhagem": GOLD_LINHAGEM,
        "distribuicao_classificacao": GOLD_DIST_CLASSIFICACAO,
        "canal_endemico_uf": GOLD_CANAL_UF,
        "atraso_notificacao": GOLD_ATRASO,
        "completude": GOLD_COMPLETUDE,
        "revisao_resumo": GOLD_REVISAO_RESUMO,
        "anual": GOLD_ANNUAL,
        "municipal": GOLD_MUNICIPAL,
        "validacao_oficial": GOLD_VALIDACAO,
    }

    exportados = []
    for nome, tabela in tabelas_export.items():
        try:
            if not spark.catalog.tableExists(tabela):
                print(f"[EXPORT] {nome}: tabela inexistente, pulada.")
                continue
            _n_total = spark.table(tabela).count()
            caminho = os.path.join(EXPORT_DIR, f"{nome}.csv")
            if _n_total > 300000:
                # Tabelas grandes (comuns no escopo nacional): o Spark grava o CSV
                # direto no Volume, sem passar pela memória do driver.
                _tmp = os.path.join(EXPORT_DIR, f"_tmp_{nome}")
                (spark.table(tabela).withColumn("rotulo_execucao", F.lit(ROTULO_EXECUCAO))
                 .coalesce(1).write.mode("overwrite").option("header", True).csv(_tmp))
                _parte = next(f for f in os.listdir(_tmp) if f.startswith("part-") and f.endswith(".csv"))
                shutil.move(os.path.join(_tmp, _parte), caminho)
                shutil.rmtree(_tmp, ignore_errors=True)
                exportados.append((nome, _n_total))
                print(f"[EXPORT] {nome}: {_n_total:,} linhas (gravação pelo Spark) -> {caminho}")
                continue
            pdf = spark.table(tabela).toPandas()
            pdf.insert(0, "rotulo_execucao", ROTULO_EXECUCAO)
            pdf.to_csv(caminho, index=False, encoding="utf-8-sig")
            exportados.append((nome, len(pdf)))
            print(f"[EXPORT] {nome}: {len(pdf):,} linhas -> {caminho}")
        except Exception as e:
            print(f"[EXPORT][WARN] {nome}: {str(e)[:150]}")

    # Microdados tratados: exportados pelo pipeline (11), widget exportar_microdados.

    parametros = {
        "rotulo_execucao": ROTULO_EXECUCAO,
        "run_id": RUN_ID_PIPELINE,
        "run_id_resultados": RUN_ID,
        "pipeline_version": PIPELINE_VERSION,
        "modo_execucao": MODO_PIPELINE,
        "anos_fonte": [int(a) for a, _ in downloaded_paths],
        "periodo_analise": [ANO_INICIO_ANALISE, END_YEAR],
        "semanas_consecutivas_alerta": SEMANAS_CONSECUTIVAS_ALERTA,
        "anos_excluidos_canal": ANOS_EXCLUIDOS_CANAL,
        "janela_anos_canal": JANELA_ANOS_CANAL,
        "min_anos_historico": MIN_ANOS_HISTORICO,
        "limiar_alta_incidencia": LIMIAR_ALTA_INCIDENCIA,
        "limiares_sensibilidade": LIMIARES_SENSIBILIDADE,
        "defasagens_informacao": DEFASAGENS_INFORMACAO,
        "data_disponibilidade": DATA_DISPONIBILIDADE,
        "remover_duplicados_sinan": REMOVER_DUPLICADOS_SINAN,
        "excluir_chikungunya": EXCLUIR_CHIKUNGUNYA,
        "exportar_microdados": EXPORTAR_MICRODADOS,
        "nowcast_min_casos": NOWCAST_MIN_CASOS,
        "agravo": AGRAVO,
        "uf": UF_SIGLA,
        "schema": SCHEMA,
        "ano_inicio_fonte": START_YEAR,
        "codigos_confirmado": list(CFG_AGRAVO["confirmado"]),
        "codigos_nao_provavel": list(CODIGOS_NAO_PROVAVEIS),
        "notebook_version": NOTEBOOK_VERSION,
        "executado_em_utc": datetime.now(timezone.utc).isoformat(),
    }
    with open(os.path.join(EXPORT_DIR, "parametros.json"), "w", encoding="utf-8") as f:
        json.dump(parametros, f, ensure_ascii=False, indent=2)

    print(f"\n[EXPORT] Rodada '{ROTULO_EXECUCAO}' salva em: {EXPORT_DIR}")
    print(f"[EXPORT] Tabelas exportadas: {len(exportados)}")

# COMMAND ----------

# DBTITLE 1,Saída
import json as _json_res
dbutils.notebook.exit(_json_res.dumps({"status": "OK", "agravo": AGRAVO}))