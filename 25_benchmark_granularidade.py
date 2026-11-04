# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./config_agravo

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 25 – BENCHMARK DE GRANULARIDADE (custo por UF/município x ano/semana/dia)
# ==============================================================================
# Medição de eficiência que antes ficava no fim do pipeline (widget
# benchmark_granularidade). Agora é chamada pelo 24_eficiencia_da_solucao
# (experimento "granularidade") ou rodada à mão. Usa a base analítica montada
# a partir da Silver gravada. Widget: repeticoes_benchmark.
# ==============================================================================

# COMMAND ----------

# MAGIC %run ./base_analitica_agravo

# COMMAND ----------

# DBTITLE 1,BENCHMARK DE GRANULARIDADE (UF/município x ano/semana/dia)
# ==============================================================================
# BENCHMARK DE GRANULARIDADE (pedido dos revisores: custo por ano, por dia, por
# estado e por município)
# ==============================================================================
# Mede quanto custa produzir a contagem de casos prováveis e a incidência por
# 100 mil habitantes em seis níveis de agregação, a partir da Silver:
#   uf_ano, uf_semana, uf_dia, municipio_ano, municipio_semana, municipio_dia
# Cada nível é calculado REPETICOES_BENCHMARK vezes (mais 1 aquecimento
# descartado), em ordem sorteada a cada rodada para não favorecer nenhum nível.
# A gravação usa o formato "noop": o Spark calcula tudo, mas não escreve em
# disco, então o tempo medido é só o da agregação.
# Resultados em arboviroses_metricas.benchmark_granularidade.
# ==============================================================================

import random as _random

if True:  # neste notebook o benchmark sempre roda
    _pop_mun = (
        spark.table(POPULATION_TABLE)
        .select(
            F.col("municipio_codigo_6").alias("_mun"),
            F.col("ano_populacao").alias("_ano"),
            F.col("populacao").alias("_pop"),
        )
    )
    _pop_uf = pop_territorio_ano.select(F.col("_ano_pop").alias("_ano"), F.col("populacao_territorio").alias("_pop"))

    _base_bench = gold_base.select(
        "ID_MN_RESI_CODIGO", "ano_analise", "SEM_PRI_Int", "DT_SIN_PRI_Parsed",
        "flag_caso_provavel", "flag_confirmado", "flag_obito", "flag_hospitalizado",
    )
    _n_entrada = _base_bench.count()

    _NIVEIS = {
        "uf_ano": (["ano_analise"], "uf"),
        "uf_semana": (["ano_analise", "SEM_PRI_Int"], "uf"),
        "uf_dia": (["DT_SIN_PRI_Parsed"], "uf"),
        "municipio_ano": (["ID_MN_RESI_CODIGO", "ano_analise"], "mun"),
        "municipio_semana": (["ID_MN_RESI_CODIGO", "ano_analise", "SEM_PRI_Int"], "mun"),
        "municipio_dia": (["ID_MN_RESI_CODIGO", "DT_SIN_PRI_Parsed"], "mun"),
    }

    def _consulta_nivel(nivel):
        chaves, territorio = _NIVEIS[nivel]
        agg = _base_bench.groupBy(*chaves).agg(
            F.sum("flag_caso_provavel").alias("casos_provaveis"),
            F.sum("flag_confirmado").alias("casos_confirmados"),
            F.sum("flag_obito").alias("obitos"),
            F.sum("flag_hospitalizado").alias("hospitalizados"),
        )
        # ano da população: ano epidemiológico, ou o ano da data no nível diário
        ano_ref = F.year("DT_SIN_PRI_Parsed") if "DT_SIN_PRI_Parsed" in chaves else F.col("ano_analise")
        agg = agg.withColumn("_ano_ref", ano_ref)
        if territorio == "mun":
            agg = agg.join(
                _pop_mun,
                (F.col("ID_MN_RESI_CODIGO") == F.col("_mun")) & (F.col("_ano_ref") == F.col("_ano")),
                "left",
            )
        else:
            agg = agg.join(_pop_uf, F.col("_ano_ref") == F.col("_ano"), "left")
        return agg.withColumn(
            "incidencia_100mil",
            F.when(F.col("_pop") > 0, F.col("casos_provaveis") / F.col("_pop") * 100000),
        )

    _linhas_bench = []
    _linhas_saida = {}
    _rng = _random.Random(20261023)
    for _rodada in range(REPETICOES_BENCHMARK + 1):  # rodada 0 = aquecimento
        _ordem = list(_NIVEIS)
        _rng.shuffle(_ordem)
        for _nivel in _ordem:
            _df = _consulta_nivel(_nivel)
            _t0 = time.perf_counter()
            try:
                _df.write.format("noop").mode("overwrite").save()
            except Exception:
                # ambiente sem o formato noop: força o cálculo com uma soma total
                _df.agg(F.sum("casos_provaveis"), F.sum("incidencia_100mil")).collect()
            _dur = time.perf_counter() - _t0
            if _nivel not in _linhas_saida:
                _linhas_saida[_nivel] = _df.count()  # fora do cronômetro
            _linhas_bench.append((
                RUN_ID, AGRAVO, UF_SIGLA, SCHEMA, EXPERIMENTO or None, _nivel,
                _rodada, _rodada == 0, float(_dur), int(_linhas_saida[_nivel]),
                int(_n_entrada), datetime.now(timezone.utc), AMBIENTE,
            ))
            print(f"[BENCHMARK] rodada {_rodada} | {_nivel:<17} {_dur:7.3f}s "
                  f"| {_linhas_saida[_nivel]:,} linhas")

    (
        spark.createDataFrame(
            _linhas_bench,
            "run_id string, agravo string, uf string, schema_analise string, "
            "experimento string, nivel string, repeticao_interna int, aquecimento boolean, "
            "duracao_segundos double, linhas_saida long, registros_entrada long, "
            "executado_em timestamp, ambiente string"
        )
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable(BENCH_TABLE)
    )
    display(
        spark.table(BENCH_TABLE)
        .filter((F.col("run_id") == RUN_ID) & ~F.col("aquecimento"))
        .groupBy("nivel", "linhas_saida")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("duracao_segundos"), 3).alias("media_s"),
            F.round(F.stddev("duracao_segundos"), 3).alias("dp_s"),
            F.round(F.percentile_approx("duracao_segundos", 0.5), 3).alias("mediana_s"),
        )
        .orderBy("mediana_s")
    )

# COMMAND ----------

# DBTITLE 1,Saída
import json as _json_b
dbutils.notebook.exit(_json_b.dumps({"status": "OK", "agravo": AGRAVO, "run_id": RUN_ID}))