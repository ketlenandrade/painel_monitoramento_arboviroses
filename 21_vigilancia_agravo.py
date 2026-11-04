# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./config_agravo

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 21 – VIGILÂNCIA DE UM AGRAVO: CANAL ENDÊMICO, ALERTAS, NOWCASTING E QUALIDADE
# ==============================================================================
# Análises do TCC que antes rodavam dentro do pipeline (seções 35, 36 e 39.x).
# O código é o mesmo; agora elas leem a Silver e as tabelas Gold gravadas pelo
# 11_pipeline_agravo e gravam as mesmas tabelas de antes, no mesmo schema:
#   comparação histórica (35), alertas municipais (36), atraso de notificação
#   (39.2), canal endêmico (39.4), avaliação das regras de alerta (39.5),
#   alertas em tempo real (39.5b), nowcasting (39.5c), completude (39.6) e
#   validação contra números oficiais (39.7).
#
# Vantagem: a análise de sensibilidade (semanas_consecutivas, anos_excluidos_canal,
# data_disponibilidade...) roda só este notebook, sem refazer o pipeline.
# Se nada mudou desde a última vez (mesma Silver, mesma Gold e mesmos parâmetros),
# o notebook termina logo com SEM_MUDANCA; use forcar_reprocessamento = sim para
# recalcular mesmo assim.
# ==============================================================================

# COMMAND ----------

# DBTITLE 1,Entradas e verificação de mudança
import json as _json_vig
import csv as _csv

for _t in [SILVER_TABLE, GOLD_WEEKLY, GOLD_ANNUAL, GOLD_MUNICIPAL_WEEKLY]:
    if not spark.catalog.tableExists(_t):
        raise Exception(f"{_t} não existe. Rode antes o 11_pipeline_agravo para {AGRAVO}.")


def _versao(t):
    return spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").collect()[0]["version"] if spark.catalog.tableExists(t) else -1


PARAMETROS_VIGILANCIA = {
    "ano_inicio_analise": ANO_INICIO_ANALISE, "semanas_consecutivas_alerta": SEMANAS_CONSECUTIVAS_ALERTA,
    "anos_excluidos_canal": ANOS_EXCLUIDOS_CANAL, "janela_anos_canal": JANELA_ANOS_CANAL,
    "min_anos_historico": MIN_ANOS_HISTORICO, "defasagens_informacao": DEFASAGENS_INFORMACAO,
    "limiares_sensibilidade": LIMIARES_SENSIBILIDADE, "data_disponibilidade": DATA_DISPONIBILIDADE,
    "remover_duplicados_sinan": REMOVER_DUPLICADOS_SINAN, "excluir_chikungunya": EXCLUIR_CHIKUNGUNYA,
    "nowcast_min_casos": NOWCAST_MIN_CASOS, "pipeline_version": PIPELINE_VERSION,
}
import hashlib as _hashlib_vig
# versões Delta das entradas + parâmetros -> uma impressão digital (hash)
CHAVE_VIGILANCIA = _hashlib_vig.md5(_json_vig.dumps({
    "silver": _versao(SILVER_TABLE), "gold_weekly": _versao(GOLD_WEEKLY),
    "populacao": _versao(POPULATION_TABLE), "parametros": PARAMETROS_VIGILANCIA,
}, sort_keys=True, default=str).encode()).hexdigest()


def _chave_anterior():
    try:
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {GOLD_AVALIACAO_ALERTAS}").collect()}
        return props.get("vigilancia.chave")
    except Exception:
        return None


if _chave_anterior() == CHAVE_VIGILANCIA and not FORCAR_REPROCESSAMENTO:
    print("[VIGILÂNCIA] Nada mudou desde a última execução (dados e parâmetros iguais).")
    dbutils.notebook.exit(_json_vig.dumps({"status": "SEM_MUDANCA", "agravo": AGRAVO}))
t_start_vigilancia = time.perf_counter()

# COMMAND ----------

# MAGIC %run ./base_analitica_agravo

# COMMAND ----------

# DBTITLE 1,Tabelas Gold e anos usados pelas análises
# Gravadas pelo pipeline (11): mesmo conteúdo que as análises usavam em memória.
df_gold_annual = spark.table(GOLD_ANNUAL)
df_gold_municipal_weekly = spark.table(GOLD_MUNICIPAL_WEEKLY)
# Anos de arquivo carregados (no pipeline: os anos baixados do OpenDataSUS)
anos_disponiveis = sorted(
    int(r[0]) for r in spark.table(SILVER_TABLE).select("_source_year").distinct().collect() if r[0] is not None
)
print(f"[VIGILÂNCIA] {CFG_AGRAVO['nome']}: anos de arquivo {anos_disponiveis}")

# COMMAND ----------

# DBTITLE 1,35. GOLD COMPARAÇÃO HISTÓRICA
# ==============================================================================
# 35. GOLD COMPARAÇÃO HISTÓRICA
#
# Importante:
# a média histórica de uma semana NÃO inclui a própria observação ao calcular
# a média de referência daquela observação.
# ==============================================================================

weekly_base = (
    gold_base
    .groupBy(
        "ano_analise",
        "SEM_PRI_Int"
    )
    .agg(
        F.sum("flag_confirmado")
        .alias("casos_confirmados"),

        F.sum("flag_obito")
        .alias("obitos")
    )
)

window_week = Window.partitionBy(
    "SEM_PRI_Int"
)

df_gold_comparison = (
    weekly_base
    .withColumn(
        "media_historica",
        F.avg("casos_confirmados")
        .over(window_week)
    )
    .withColumn(
        "maximo_historico",
        F.max("casos_confirmados")
        .over(window_week)
    )
    .withColumn(
        "minimo_historico",
        F.min("casos_confirmados")
        .over(window_week)
    )
    .withColumn(
        "media_sem_a_observacao",
        F.when(
            F.count("*").over(window_week) > 1,
            (
                F.sum("casos_confirmados")
                .over(window_week)
                -
                F.col("casos_confirmados")
            )
            /
            (
                F.count("*").over(window_week) - 1
            )
        )
    )
    .withColumn(
        "desvio_vs_media_pct",
        F.when(
            F.col("media_sem_a_observacao") > 0,
            F.round(
                (
                    F.col("casos_confirmados")
                    -
                    F.col("media_sem_a_observacao")
                )
                /
                F.col("media_sem_a_observacao")
                * 100,
                2
            )
        )
    )
    .withColumn(
        "situacao_semana",
        F.when(
            F.col("media_sem_a_observacao").isNull(),
            "Sem histórico suficiente"
        )
        .when(
            F.col("casos_confirmados")
            >
            F.col("media_sem_a_observacao") * 1.20,
            "Acima da média"
        )
        .when(
            F.col("casos_confirmados")
            <
            F.col("media_sem_a_observacao") * 0.80,
            "Abaixo da média"
        )
        .otherwise(
            "Próxima da média"
        )
    )
)

# COMMAND ----------

# DBTITLE 1,36. GOLD ALERTAS MUNICIPAIS
# ==============================================================================
# 36. GOLD ALERTAS MUNICIPAIS
#
# Regra operacional para o dashboard:
# ALTO  = >= 50 casos na semana e aumento >= 50%
# MÉDIO = >= 20 casos na semana e aumento >= 25%
#
# Essas regras são regras analíticas do projeto, NÃO uma classificação oficial
# do Ministério da Saúde.
# ==============================================================================

municipal_week_base = df_gold_municipal_weekly

window_municipio = Window.partitionBy(
    "ID_MN_RESI_CODIGO"
).orderBy(
    "ano_analise",
    "SEM_PRI_Int"
)

df_gold_alertas = (
    municipal_week_base
    .withColumn(
        "casos_semana_anterior",
        F.lag("casos_confirmados")
        .over(window_municipio)
    )
    .withColumn(
        "variacao_percentual",
        F.when(
            F.col("casos_semana_anterior") > 0,
            F.round(
                (
                    F.col("casos_confirmados")
                    -
                    F.col("casos_semana_anterior")
                )
                /
                F.col("casos_semana_anterior")
                * 100,
                2
            )
        )
    )
    .withColumn(
        "nivel_alerta",
        F.when(
            (
                (F.col("casos_confirmados") >= 50)
                &
                (F.col("variacao_percentual") >= 50)
            ),
            "ALTO"
        )
        .when(
            (
                (F.col("casos_confirmados") >= 20)
                &
                (F.col("variacao_percentual") >= 25)
            ),
            "MEDIO"
        )
        .otherwise("NORMAL")
    )
    .filter(
        F.col("nivel_alerta") != "NORMAL"
    )
)

# COMMAND ----------

# DBTITLE 1,39.2 TCC – ATRASO DE NOTIFICAÇÃO E DIGITAÇÃO
# ==============================================================================
# 39.2. ATRASO DE NOTIFICAÇÃO
# ==============================================================================
# atraso_notificacao = DT_NOTIFIC - DT_SIN_PRI   (sintoma -> notificação)
# atraso_digitacao   = DT_DIGITA  - DT_NOTIFIC   (notificação -> sistema),
#                      se a coluna DT_DIGITA existir no arquivo do SINAN.
# Valores negativos ou acima de 365 dias são tratados como erro de registro
# e contados à parte (n_invalidos).
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_atraso_notificacao...")

dt_digita_col = first_existing_column(gold_base.columns, ["DT_DIGITA"])

base_atraso = (
    gold_base
    .filter(F.col("flag_caso_provavel") == 1)
    .withColumn(
        "_atraso_not",
        F.datediff("DT_NOTIFIC_Parsed", "DT_SIN_PRI_Parsed")
    )
    .withColumn(
        "_atraso_dig",
        F.datediff(parse_date_flexible(gold_base, dt_digita_col), F.col("DT_NOTIFIC_Parsed"))
        if dt_digita_col else F.lit(None).cast("int")
    )
)

for c in ["_atraso_not", "_atraso_dig"]:
    base_atraso = (
        base_atraso
        .withColumn(c + "_invalido", (F.col(c) < 0) | (F.col(c) > 365))
        .withColumn(c, F.when(F.col(c).between(0, 365), F.col(c)))
    )


def resumo_atraso(col, prefixo):
    return [
        F.count(col).alias(f"{prefixo}_n_validos"),
        F.sum(F.col(col + "_invalido").cast("int")).alias(f"{prefixo}_n_invalidos"),
        F.round(F.avg(col), 2).alias(f"{prefixo}_media_dias"),
        F.percentile_approx(col, 0.5).alias(f"{prefixo}_mediana_dias"),
        F.percentile_approx(col, 0.75).alias(f"{prefixo}_p75_dias"),
        F.percentile_approx(col, 0.9).alias(f"{prefixo}_p90_dias"),
        F.round(
            F.avg(F.when(F.col(col).isNotNull(), (F.col(col) <= 7).cast("int"))) * 100, 2
        ).alias(f"{prefixo}_pct_ate_7_dias"),
    ]


AGG_ATRASO = resumo_atraso("_atraso_not", "notificacao") + resumo_atraso("_atraso_dig", "digitacao")

df_gold_atraso = (
    base_atraso
    .groupBy("ano_analise")
    .agg(F.count("*").alias("casos_provaveis"), *AGG_ATRASO)
    .orderBy("ano_analise")
)

# Por semana: permite mostrar se o atraso aumenta durante o pico epidêmico
df_gold_atraso_semanal = (
    base_atraso
    .groupBy("ano_analise", "SEM_PRI_Int")
    .agg(F.count("*").alias("casos_provaveis"), *resumo_atraso("_atraso_not", "notificacao"))
)

print(f"[TCC] Coluna de digitação: {dt_digita_col or 'não existe no arquivo'}")

# COMMAND ----------

# DBTITLE 1,39.4 TCC – CANAL ENDÊMICO (DIAGRAMA DE CONTROLE)
# ==============================================================================
# 39.4. CANAL ENDÊMICO
# ==============================================================================
# Para cada ano-alvo e semana, compara a incidência semanal com a dos
# MESMOS semanas nos anos ANTERIORES (até JANELA_ANOS_CANAL anos, sem olhar
# o futuro e sem usar o próprio ano). Dois métodos clássicos:
#   * Quartis (zonas): êxito < Q1 <= segurança < Q2 <= alerta < Q3 <= epidêmica
#   * Média + 1,96 desvio-padrão (limite superior)
# Anos epidêmicos podem ser excluídos do histórico via ANOS_EXCLUIDOS_CANAL.
# Grade completa município x semana: semanas sem caso entram com zero
# (sem isso, a média histórica seria superestimada).
# ==============================================================================

print("[GOLD] Criando canal endêmico...")


def canal_endemico(df, chaves):
    """df: chaves + ano_epi + semana_epi + incidencia (grade completa)."""
    t, h = df.alias("t"), df.alias("h")
    cond = (
        (F.col("t.semana_epi") == F.col("h.semana_epi"))
        & (F.col("h.ano_epi") < F.col("t.ano_epi"))
        & (F.col("h.ano_epi") >= F.col("t.ano_epi") - JANELA_ANOS_CANAL)
    )
    for k in chaves:
        cond = cond & (F.col(f"t.{k}") == F.col(f"h.{k}"))
    if ANOS_EXCLUIDOS_CANAL:
        cond = cond & ~F.col("h.ano_epi").isin(ANOS_EXCLUIDOS_CANAL)

    outras = [c for c in df.columns if c not in chaves + ["ano_epi", "semana_epi"]]
    canal = (
        t.join(h, cond, "left")
        .groupBy(*[F.col(f"t.{c}").alias(c) for c in chaves + ["ano_epi", "semana_epi"] + outras])
        .agg(
            F.countDistinct("h.ano_epi").alias("n_anos_historico"),
            F.avg("h.incidencia").alias("media_historica"),
            F.stddev_samp("h.incidencia").alias("dp_historico"),
            F.percentile_approx("h.incidencia", 0.25).alias("q1"),
            F.percentile_approx("h.incidencia", 0.50).alias("q2_mediana"),
            F.percentile_approx("h.incidencia", 0.75).alias("q3"),
        )
        .withColumn("limite_superior_dp", F.col("media_historica") + 1.96 * F.col("dp_historico"))
    )
    suficiente = F.col("n_anos_historico") >= MIN_ANOS_HISTORICO
    return (
        canal
        .withColumn(
            "zona_quartis",
            F.when(~suficiente, "Histórico insuficiente")
            .when(F.col("incidencia") > F.col("q3"), "Epidêmica")
            .when(F.col("incidencia") > F.col("q2_mediana"), "Alerta")
            .when(F.col("incidencia") > F.col("q1"), "Segurança")
            .otherwise("Êxito")
        )
        .withColumn(
            "acima_limite_dp",
            F.when(suficiente, F.col("incidencia") > F.col("limite_superior_dp"))
        )
    )


# --- Grade estadual ------------------------------------------------------------
semanas_encerradas = (
    df_cal_epi
    # Só anos cujo arquivo do SINAN foi carregado: um ano sem dados entraria
    # no histórico como "zero casos" e puxaria a média para baixo.
    .filter(F.col("ano_epi").isin(anos_disponiveis) & (F.col("fim_semana") < F.lit(HOJE)))
    .select("ano_epi", "semana_epi")
)

grade_uf = (
    semanas_encerradas
    .join(
        # semanal com TODOS os anos (histórico do canal), não só 2022+
        gold_base_full
        .groupBy(
            F.col("ano_analise").alias("ano_epi"),
            F.col("SEM_PRI_Int").alias("semana_epi"),
        )
        .agg(F.sum("flag_caso_provavel").alias("casos_provaveis")),
        ["ano_epi", "semana_epi"], "left"
    )
    .join(pop_territorio_ano.withColumnRenamed("_ano_pop", "ano_epi"), "ano_epi", "left")
    .fillna(0, subset=["casos_provaveis"])
    .withColumn("incidencia", F.col("casos_provaveis") / F.col("populacao_territorio") * 100000)
    .withColumn("uf", F.lit(UF_SIGLA))
)

df_gold_canal_uf = canal_endemico(grade_uf, ["uf"])

# --- Grade municipal -------------------------------------------------------------
casos_mun_semana = (
    gold_base_full
    .groupBy(
        F.col("ID_MN_RESI_CODIGO").alias("municipio_codigo_6"),
        F.col("ano_analise").alias("ano_epi"),
        F.col("SEM_PRI_Int").alias("semana_epi"),
    )
    .agg(
        F.sum("flag_caso_provavel").alias("casos_provaveis"),
        F.sum("flag_confirmado").alias("casos_confirmados"),
    )
)

grade_mun = (
    spark.table(POPULATION_TABLE)
    .select(
        "municipio_codigo_6", "municipio_nome",
        F.col("ano_populacao").alias("ano_epi"),
        F.col("populacao").alias("populacao")
    )
    .filter(F.col("populacao") > 0)
    .join(semanas_encerradas, "ano_epi")
    .join(casos_mun_semana, ["municipio_codigo_6", "ano_epi", "semana_epi"], "left")
    .fillna(0, subset=["casos_provaveis", "casos_confirmados"])
    .withColumn("incidencia", F.col("casos_provaveis") / F.col("populacao") * 100000)
)

df_gold_canal_mun = canal_endemico(grade_mun, ["municipio_codigo_6"])

# COMMAND ----------

# DBTITLE 1,39.5 TCC – AVALIAÇÃO DAS REGRAS DE ALERTA
# ==============================================================================
# 39.5. AVALIAÇÃO DAS REGRAS DE ALERTA
# ==============================================================================
# Verdade de referência: município-ano com incidência anual de casos
# prováveis >= LIMIAR_ALTA_INCIDENCIA por 100 mil hab. (faixa de "alta
# incidência" usada pelo Ministério da Saúde).
#
# Regras comparadas (todas por município e semana):
#   A_regra_original : >= 20 confirmados na semana e alta >= 25% vs semana
#                      anterior (regra MÉDIO/ALTO do dashboard; agora com a
#                      semana anterior de fato, inclusive semanas com zero)
#   B_canal_quartis  : incidência na zona epidêmica (> Q3)
#   C_canal_media_dp : incidência > média + 1,96 DP
#   (B e C exigem SEMANAS_CONSECUTIVAS_ALERTA semanas seguidas acima do limite)
#
# Métricas: matriz de confusão, sensibilidade, especificidade, VPP, F1 e
# antecedência (semanas entre o 1º alerta e o pico do município).
# Só entram anos completos e com histórico suficiente para B e C, para que
# as três regras sejam avaliadas exatamente no mesmo conjunto.
# ==============================================================================

print("[GOLD] Avaliando regras de alerta...")

w_mun = Window.partitionBy("municipio_codigo_6").orderBy("ano_epi", "semana_epi")

alertas_semanais = (
    df_gold_canal_mun
    .withColumn("_conf_ant", F.lag("casos_confirmados").over(w_mun))
    .withColumn(
        "alerta_A_regra_original",
        (F.col("casos_confirmados") >= 20)
        & (F.col("_conf_ant") > 0)
        & ((F.col("casos_confirmados") - F.col("_conf_ant")) / F.col("_conf_ant") >= 0.25)
    )
    .withColumn("_b", (F.col("zona_quartis") == "Epidêmica").cast("int"))
    .withColumn("_c", F.coalesce(F.col("acima_limite_dp"), F.lit(False)).cast("int"))
    # alerta só quando o limite é ultrapassado em N semanas seguidas
    .withColumn(
        "alerta_B_canal_quartis",
        F.sum("_b").over(w_mun.rowsBetween(-(SEMANAS_CONSECUTIVAS_ALERTA - 1), 0))
        == SEMANAS_CONSECUTIVAS_ALERTA
    )
    .withColumn(
        "alerta_C_canal_media_dp",
        F.sum("_c").over(w_mun.rowsBetween(-(SEMANAS_CONSECUTIVAS_ALERTA - 1), 0))
        == SEMANAS_CONSECUTIVAS_ALERTA
    )
    .drop("_b", "_c", "_conf_ant")
)

REGRAS = ["A_regra_original", "B_canal_quartis", "C_canal_media_dp"]

df_gold_alertas_mun_ano = (
    alertas_semanais
    .groupBy("municipio_codigo_6", "municipio_nome", "ano_epi", "populacao")
    .agg(
        F.sum("casos_provaveis").alias("casos_provaveis_ano"),
        # max: a semana 53 raramente tem histórico e não deve excluir o ano
        F.max("n_anos_historico").alias("n_anos_historico"),
        F.count("*").alias("semanas_observadas"),
        F.max_by("semana_epi", "casos_provaveis").alias("semana_pico"),
        *[F.min(F.when(F.col(f"alerta_{r}"), F.col("semana_epi"))).alias(f"semana_alerta_{r}") for r in REGRAS],
    )
    .withColumn("incidencia_anual_100mil", F.round(F.col("casos_provaveis_ano") / F.col("populacao") * 100000, 2))
    .withColumn("alta_incidencia", F.col("incidencia_anual_100mil") >= LIMIAR_ALTA_INCIDENCIA)
)

# entra_avaliacao fica gravada na tabela: a análise estatística das regras de
# alerta usa exatamente o mesmo conjunto de município-ano (bootstrap e McNemar).
df_gold_alertas_mun_ano = df_gold_alertas_mun_ano.withColumn(
    "entra_avaliacao",
    (F.col("ano_epi") >= ANO_INICIO_ANALISE)
    & (F.col("ano_epi") < ANO_CORRENTE)
    & (F.col("n_anos_historico") >= MIN_ANOS_HISTORICO)
    & (F.col("semanas_observadas") >= 52)
)
base_avaliacao = df_gold_alertas_mun_ano.filter(F.col("entra_avaliacao"))


def com_ic_wilson(df):
    """IC 95% de Wilson para sensibilidade, especificidade e VPP (em %).
    Wilson se comporta bem com proporções perto de 0 ou 100% e n pequeno."""
    z = 1.96
    for nome, num, den in [
        ("sensibilidade", F.col("VP"), F.col("VP") + F.col("FN")),
        ("especificidade", F.col("VN"), F.col("VN") + F.col("FP")),
        ("vpp", F.col("VP"), F.col("VP") + F.col("FP")),
    ]:
        n = den.cast("double")
        p = num.cast("double") / n
        centro = (p + z * z / (2 * n)) / (1 + z * z / n)
        meia = z * F.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
        df = (
            df.withColumn(f"{nome}_ic95_inf", F.when(n > 0, F.round((centro - meia) * 100, 2)))
            .withColumn(f"{nome}_ic95_sup", F.when(n > 0, F.round((centro + meia) * 100, 2)))
        )
    return df

avaliacoes = []
for r in REGRAS:
    alertou = F.col(f"semana_alerta_{r}").isNotNull()
    antecedencia = F.col("semana_pico") - F.col(f"semana_alerta_{r}")
    avaliacoes.append(
        base_avaliacao.agg(
            F.lit(r).alias("regra"),
            F.count("*").alias("municipios_ano_avaliados"),
            F.sum((alertou & F.col("alta_incidencia")).cast("int")).alias("VP"),
            F.sum((alertou & ~F.col("alta_incidencia")).cast("int")).alias("FP"),
            F.sum((~alertou & F.col("alta_incidencia")).cast("int")).alias("FN"),
            F.sum((~alertou & ~F.col("alta_incidencia")).cast("int")).alias("VN"),
            F.percentile_approx(
                F.when(alertou & F.col("alta_incidencia"), antecedencia), 0.5
            ).alias("antecedencia_mediana_semanas"),
            F.round(
                F.avg(F.when(alertou & F.col("alta_incidencia"), (antecedencia > 0).cast("int"))) * 100, 2
            ).alias("pct_alertas_antes_do_pico"),
        )
    )

df_gold_avaliacao_alertas = (
    reduce(lambda a, b: a.unionByName(b), avaliacoes)
    .withColumn("sensibilidade_pct", F.round(F.col("VP") / (F.col("VP") + F.col("FN")) * 100, 2))
    .withColumn("especificidade_pct", F.round(F.col("VN") / (F.col("VN") + F.col("FP")) * 100, 2))
    .withColumn("vpp_pct", F.round(F.col("VP") / (F.col("VP") + F.col("FP")) * 100, 2))
    .withColumn(
        "f1",
        F.round(2 * F.col("VP") / (2 * F.col("VP") + F.col("FP") + F.col("FN")), 4)
    )
    .withColumn("limiar_alta_incidencia", F.lit(LIMIAR_ALTA_INCIDENCIA))
    .withColumn("semanas_consecutivas", F.lit(SEMANAS_CONSECUTIVAS_ALERTA))
)
df_gold_avaliacao_alertas = com_ic_wilson(df_gold_avaliacao_alertas)

# COMMAND ----------

# DBTITLE 1,39.5b TCC – ALERTAS EM TEMPO REAL (reconstrução "as-of")
# ==============================================================================
# 39.5b. ALERTAS COM A INFORMAÇÃO QUE EXISTIA NA SEMANA (reconstrução "as-of")
# ==============================================================================
# Pergunta: o canal endêmico continua detectando surtos quando o alerta é
# calculado com dado INCOMPLETO, isto é, só com os casos já notificados na época?
#
# Como funciona (sem depender de snapshots antigos da fonte):
#   * Os limites do canal (Q1/Q2/Q3, média e desvio-padrão) vêm do histórico
#     FINAL de anos anteriores (seção 39.4): no mundo real, esses anos já estão
#     consolidados quando o alerta é emitido.
#   * A incidência da semana S, porém, é recalculada usando apenas os casos cuja
#     data de DISPONIBILIDADE (DT_DIGITA, ou DT_NOTIFIC conforme o widget
#     "data_disponibilidade") é <= fim da semana S + D semanas, em que
#     D = defasagem de informação (DEFASAGENS_INFORMACAO). O cenário "final"
#     usa todos os casos e reproduz a avaliação da seção 39.5.
#   * Para cada defasagem há duas versões: "bruto" (só o que já estava na base)
#     e "nowcast" (o bruto corrigido pela completude histórica da defasagem,
#     ver abaixo). A pergunta central do trabalho é: o nowcast recupera a
#     capacidade de detecção que o atraso de notificação tira do alerta?
#   * A "verdade de referência" (município-ano com incidência anual alta) e a
#     semana de pico continuam sendo calculadas com o dado FINAL.
#   * Cada regra (B quartis, C média+DP) é avaliada para cada limiar em
#     LIMIARES_SENSIBILIDADE. A regra A (confirmados) não entra aqui porque usa
#     outro insumo; comparar regras com insumos diferentes não seria justo.
#
# NOWCAST: a fração dos casos de uma semana que já está na base depois de D
# semanas (p) é estimada SOMENTE com anos anteriores ao ano avaliado (sem olhar o
# futuro). O nowcast da semana é  casos_conhecidos / p.  A estimativa de p é
# agregada para o estado inteiro e não varia por semana do ano; municípios
# pequenos têm muito ruído. Declarar essas limitações no artigo.
#
# Limitação geral: a reconstrução usa a data de digitação ATUAL do registro. Se o
# registro foi alterado depois, a data pode não refletir o primeiro momento em
# que ele apareceu na base pública.
# ==============================================================================

print("[GOLD] Avaliando alertas com informação disponível em tempo real...")

cal_fim_semana = df_cal_epi.select("ano_epi", "semana_epi", "fim_semana")

_col_disponivel = (
    F.col("DT_DISPONIVEL")
    if (DATA_DISPONIBILIDADE == "DT_DIGITA" and "DT_DISPONIVEL" in gold_base_full.columns)
    else F.col("DT_NOTIFIC_Parsed")
)
print(f"[GOLD] Data de disponibilidade usada nas análises em tempo real: {DATA_DISPONIBILIDADE}")

base_rt = (
    gold_base_full
    .filter(
        (F.col("flag_caso_provavel") == 1)
        & _col_disponivel.isNotNull()
        & F.col("ID_MN_RESI_CODIGO").isNotNull()
    )
    .select(
        F.col("ID_MN_RESI_CODIGO").alias("municipio_codigo_6"),
        F.col("ano_analise").alias("ano_epi"),
        F.col("SEM_PRI_Int").alias("semana_epi"),
        _col_disponivel.alias("data_disponivel"),
    )
    .join(cal_fim_semana, ["ano_epi", "semana_epi"])
    # dias entre o fim da semana de sintomas e a data em que o caso aparece na base
    .withColumn("dias_ate_disp", F.datediff("data_disponivel", "fim_semana"))
    # defasagem em semanas inteiras (0 = já estava na base ao fim da própria semana)
    .withColumn(
        "lag_sem",
        F.greatest(F.lit(0), F.ceil(F.col("dias_ate_disp") / 7)).cast("int")
    )
)


# --- Fatores de completude (base do nowcast) ----------------------------------
def calcular_fatores_completude(contagens, anos_alvo, defasagens, janela, ano_corrente):
    """contagens: {ano_epi: {lag_semanas: n_casos}} (estado inteiro).
    Para cada ano-alvo Y e defasagem D devolve a fração dos casos que já estava
    na base após D semanas, estimada só com os anos de Y-janela a Y-1 (e nunca
    com o ano corrente, ainda incompleto). Sem histórico -> sem fator."""
    saida = []
    for ano in anos_alvo:
        refs = [
            a for a in sorted(contagens)
            if ano - janela <= a < ano and a < ano_corrente
        ]
        total = sum(sum(contagens[a].values()) for a in refs)
        if not refs or total <= 0:
            continue
        for d in defasagens:
            acumulado = sum(n for a in refs for lag, n in contagens[a].items() if lag <= d)
            saida.append((int(ano), int(d), acumulado / total, ",".join(str(a) for a in refs)))
    return saida


_contagens = {}
for _r in base_rt.groupBy("ano_epi", "lag_sem").count().collect():
    _contagens.setdefault(int(_r["ano_epi"]), {})[int(_r["lag_sem"])] = int(_r["count"])

fatores_lista = calcular_fatores_completude(
    _contagens,
    sorted(_contagens),
    [int(d) for d in DEFASAGENS_INFORMACAO],
    JANELA_ANOS_CANAL,
    ANO_CORRENTE,
)

df_fatores_completude = spark.createDataFrame(
    fatores_lista,
    "ano_epi int, defasagem_semanas int, p_completude double, anos_referencia string"
)
print(f"[GOLD] Fatores de completude calculados: {len(fatores_lista)} (ano x defasagem).")

REGRAS_RT = [
    # (nome da regra, coluna 0/1 de acima-do-limite, coluna da 1ª semana de alerta)
    ("B_canal_quartis", "_b", "sem_alerta_B"),
    ("C_canal_media_dp", "_c", "sem_alerta_C"),
]


def cenario_rt(defasagem, nowcast=False):
    """Grade município x semana com a incidência conhecida na época.
    defasagem=None -> dado final (todos os casos).
    nowcast=True   -> casos conhecidos divididos pela completude histórica."""
    d = df_gold_canal_mun

    if defasagem is None:
        d = d.withColumn("casos_rt", F.col("casos_provaveis"))
    else:
        conhecidos = (
            base_rt
            .filter(F.col("dias_ate_disp") <= 7 * int(defasagem))
            .groupBy("municipio_codigo_6", "ano_epi", "semana_epi")
            .agg(F.count("*").alias("casos_rt"))
        )
        d = (
            d.join(conhecidos, ["municipio_codigo_6", "ano_epi", "semana_epi"], "left")
            .fillna(0, subset=["casos_rt"])
        )
        if nowcast:
            fator = (
                df_fatores_completude
                .filter(F.col("defasagem_semanas") == int(defasagem))
                .select("ano_epi", "p_completude")
            )
            d = (
                d.join(fator, "ano_epi", "left")
                .withColumn(
                    "casos_rt",
                    F.when(F.col("p_completude") > 0, F.col("casos_rt") / F.col("p_completude"))
                    .otherwise(F.col("casos_rt"))
                )
                .drop("p_completude")
            )

    suficiente = F.col("n_anos_historico") >= MIN_ANOS_HISTORICO
    d = d.withColumn("incidencia_rt", F.col("casos_rt") / F.col("populacao") * 100000)

    return (
        d
        .withColumn(
            "_b",
            F.coalesce((suficiente & (F.col("incidencia_rt") > F.col("q3"))).cast("int"), F.lit(0))
        )
        .withColumn(
            "_c",
            F.coalesce(
                (suficiente & (F.col("incidencia_rt") > F.col("limite_superior_dp"))).cast("int"),
                F.lit(0)
            )
        )
    )


def avaliar_cenario_rt(defasagem, nowcast=False):
    metodo = "final" if defasagem is None else ("nowcast" if nowcast else "bruto")
    rotulo = "final" if defasagem is None else f"D{int(defasagem)}_{metodo}"
    janela = w_mun.rowsBetween(-(SEMANAS_CONSECUTIVAS_ALERTA - 1), 0)

    d = cenario_rt(defasagem, nowcast)
    for _, flag, _ in REGRAS_RT:
        # alerta só quando o limite é ultrapassado em N semanas seguidas
        d = d.withColumn(
            f"alerta{flag}",
            F.sum(flag).over(janela) == SEMANAS_CONSECUTIVAS_ALERTA
        )

    mun_ano = (
        d.groupBy("municipio_codigo_6", "ano_epi", "populacao")
        .agg(
            F.sum("casos_provaveis").alias("casos_ano"),
            F.max("n_anos_historico").alias("n_anos_historico"),
            F.count("*").alias("semanas_observadas"),
            F.max_by("semana_epi", "casos_provaveis").alias("semana_pico"),
            *[
                F.min(F.when(F.col(f"alerta{flag}"), F.col("semana_epi"))).alias(col_sem)
                for _, flag, col_sem in REGRAS_RT
            ],
        )
        .withColumn("incid_anual", F.col("casos_ano") / F.col("populacao") * 100000)
        .filter(
            (F.col("ano_epi") >= ANO_INICIO_ANALISE)
            & (F.col("ano_epi") < ANO_CORRENTE)
            & (F.col("n_anos_historico") >= MIN_ANOS_HISTORICO)
            & (F.col("semanas_observadas") >= 52)
        )
    )

    # Uma única passada pelos dados: todas as combinações regra x limiar
    aggs = [F.count("*").alias("n")]
    for regra, _, col_sem in REGRAS_RT:
        alertou = F.col(col_sem).isNotNull()
        antecedencia = F.col("semana_pico") - F.col(col_sem)
        for lim in LIMIARES_SENSIBILIDADE:
            alta = F.col("incid_anual") >= lim
            k = f"{regra}__{int(lim)}"
            aggs += [
                F.sum((alertou & alta).cast("int")).alias(f"{k}__VP"),
                F.sum((alertou & ~alta).cast("int")).alias(f"{k}__FP"),
                F.sum((~alertou & alta).cast("int")).alias(f"{k}__FN"),
                F.sum((~alertou & ~alta).cast("int")).alias(f"{k}__VN"),
                F.percentile_approx(F.when(alertou & alta, antecedencia), 0.5).alias(f"{k}__ant"),
                F.avg(F.when(alertou & alta, (antecedencia > 0).cast("int"))).alias(f"{k}__antes"),
            ]

    res = mun_ano.agg(*aggs).first()

    linhas = []
    for regra, _, _ in REGRAS_RT:
        for lim in LIMIARES_SENSIBILIDADE:
            k = f"{regra}__{int(lim)}"
            ant, antes = res[f"{k}__ant"], res[f"{k}__antes"]
            linhas.append((
                rotulo,
                metodo,
                None if defasagem is None else int(defasagem),
                regra,
                float(lim),
                int(res["n"] or 0),
                int(res[f"{k}__VP"] or 0),
                int(res[f"{k}__FP"] or 0),
                int(res[f"{k}__FN"] or 0),
                int(res[f"{k}__VN"] or 0),
                float(ant) if ant is not None else None,
                round(float(antes) * 100, 2) if antes is not None else None,
            ))
    return linhas


schema_alertas_rt = (
    "cenario string, metodo string, defasagem_semanas int, regra string, limiar_alta_incidencia double, "
    "municipios_ano_avaliados bigint, VP bigint, FP bigint, FN bigint, VN bigint, "
    "antecedencia_mediana_semanas double, pct_alertas_antes_do_pico double"
)

linhas_alertas_rt = []
print("[GOLD]   cenário de informação: final (referência)")
linhas_alertas_rt += avaliar_cenario_rt(None)
for _defasagem in DEFASAGENS_INFORMACAO:
    for _nowcast in (False, True):
        _nome = "nowcast" if _nowcast else "bruto"
        print(f"[GOLD]   cenário: {_defasagem} semana(s) de defasagem, {_nome}")
        linhas_alertas_rt += avaliar_cenario_rt(_defasagem, _nowcast)


def _razao_pct(num, den):
    # F.when evita divisão por zero (que falha com ANSI mode ligado)
    return F.when(den > 0, F.round(num / den * 100, 2))


df_gold_alertas_rt = (
    spark.createDataFrame(linhas_alertas_rt, schema_alertas_rt)
    .withColumn("sensibilidade_pct", _razao_pct(F.col("VP"), F.col("VP") + F.col("FN")))
    .withColumn("especificidade_pct", _razao_pct(F.col("VN"), F.col("VN") + F.col("FP")))
    .withColumn("vpp_pct", _razao_pct(F.col("VP"), F.col("VP") + F.col("FP")))
    .withColumn(
        "f1",
        F.when(
            (2 * F.col("VP") + F.col("FP") + F.col("FN")) > 0,
            F.round(2 * F.col("VP") / (2 * F.col("VP") + F.col("FP") + F.col("FN")), 4)
        )
    )
    .withColumn("semanas_consecutivas", F.lit(SEMANAS_CONSECUTIVAS_ALERTA))
    .orderBy("limiar_alta_incidencia", "regra", "defasagem_semanas", "metodo")
)
df_gold_alertas_rt = com_ic_wilson(df_gold_alertas_rt)

# COMMAND ----------

# DBTITLE 1,39.5c TCC – NOWCASTING DO ATRASO DE NOTIFICAÇÃO (estado)
# ==============================================================================
# 39.5c. NOWCASTING: quão bem se recupera o número real de casos de cada semana?
# ==============================================================================
# Para o estado inteiro, compara por semana epidemiológica e defasagem D:
#   casos_final    : casos da semana com a base completa (referência)
#   casos_bruto    : casos que já estavam na base D semanas após o fim da semana
#   casos_nowcast  : casos_bruto / p   (p = completude histórica da defasagem D,
#                    estimada só com anos anteriores)
#   erro_pct_*     : erro relativo com sinal (negativo = subestimação)
# O resumo traz o erro absoluto mediano de cada método e em que % das semanas o
# nowcast ficou mais perto do valor final que o dado bruto.
# ==============================================================================

print("[GOLD] Calculando nowcast estadual...")

_agg_nc = [F.count("*").alias("casos_final")] + [
    F.sum((F.col("dias_ate_disp") <= 7 * int(d)).cast("int")).alias(f"bruto_D{int(d)}")
    for d in DEFASAGENS_INFORMACAO
]
_semanal_estado = base_rt.groupBy("ano_epi", "semana_epi").agg(*_agg_nc).collect()
_fat = {(f[0], f[1]): f[2] for f in fatores_lista}

linhas_nowcast = []
for _r in _semanal_estado:
    for _d in DEFASAGENS_INFORMACAO:
        _final = int(_r["casos_final"])
        _bruto = int(_r[f"bruto_D{int(_d)}"] or 0)
        _p = _fat.get((int(_r["ano_epi"]), int(_d)))
        _now = (_bruto / _p) if (_p and _p > 0) else None
        linhas_nowcast.append((
            int(_r["ano_epi"]), int(_r["semana_epi"]), int(_d),
            _final, _bruto,
            float(_now) if _now is not None else None,
            float(_p) if _p is not None else None,
            round((_bruto - _final) / _final * 100, 2) if _final else None,
            round((_now - _final) / _final * 100, 2) if (_now is not None and _final) else None,
        ))

df_gold_nowcast = spark.createDataFrame(
    linhas_nowcast,
    "ano_epi int, semana_epi int, defasagem_semanas int, casos_final long, casos_bruto long, "
    "casos_nowcast double, p_completude double, erro_pct_bruto double, erro_pct_nowcast double"
).orderBy("ano_epi", "semana_epi", "defasagem_semanas")

df_gold_nowcast_resumo = (
    df_gold_nowcast
    .filter(
        (F.col("ano_epi") >= ANO_INICIO_ANALISE)
        & (F.col("ano_epi") < ANO_CORRENTE)
        & (F.col("casos_final") >= NOWCAST_MIN_CASOS)
        & F.col("casos_nowcast").isNotNull()
    )
    .groupBy("defasagem_semanas")
    .agg(
        F.count("*").alias("n_semanas"),
        F.round(F.percentile_approx(F.abs("erro_pct_bruto"), 0.5), 2).alias("erro_abs_mediano_bruto_pct"),
        F.round(F.percentile_approx(F.abs("erro_pct_nowcast"), 0.5), 2).alias("erro_abs_mediano_nowcast_pct"),
        F.round(F.percentile_approx("erro_pct_bruto", 0.5), 2).alias("vies_mediano_bruto_pct"),
        F.round(F.percentile_approx("erro_pct_nowcast", 0.5), 2).alias("vies_mediano_nowcast_pct"),
        F.round(
            F.avg((F.abs("erro_pct_nowcast") < F.abs("erro_pct_bruto")).cast("int")) * 100, 2
        ).alias("pct_semanas_nowcast_mais_proximo"),
    )
    .orderBy("defasagem_semanas")
)

# COMMAND ----------

# DBTITLE 1,39.6 TCC – COMPLETUDE DAS VARIÁVEIS (Romero & Cunha, 2006)
# ==============================================================================
# 39.6. COMPLETUDE
# ==============================================================================
# % de campos em branco ou "ignorado" por variável e ano, com a escala de
# Romero & Cunha (2006): incompletude < 5% excelente; 5–10% bom;
# 10–20% regular; 20–50% ruim; >= 50% muito ruim.
# Cada variável usa o denominador em que ela é aplicável (ex.: evolução e
# hospitalização só fazem sentido para casos confirmados).
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_completude...")

todos = F.lit(True)
confirmados = F.col("flag_confirmado") == 1
classificados = F.col("CLASSI_FIN_Int").isNotNull()

VARIAVEIS_COMPLETUDE = [
    # (variável, denominador, em_branco, ignorado)
    ("sexo", todos, F.col("CS_SEXO_PADRAO").isNull(), ~F.col("CS_SEXO_PADRAO").isin("M", "F")),
    ("raca_cor", todos, F.col("CS_RACA_PADRAO").isNull() | (F.col("CS_RACA_PADRAO") == ""), F.col("CS_RACA_PADRAO") == "9"),
    ("idade", todos, F.col("idade_anos").isNull(), F.lit(False)),
    ("municipio_residencia", todos, F.col("ID_MN_RESI_CODIGO").isNull(), F.lit(False)),
    ("data_inicio_sintomas", todos, F.col("DT_SIN_PRI_Parsed").isNull(), F.lit(False)),
    ("classificacao_final", todos, F.col("CLASSI_FIN_Int").isNull(), F.lit(False)),
    ("criterio_confirmacao", classificados, F.col("CRITERIO_Int").isNull(), F.lit(False)),
    ("evolucao", confirmados, F.col("EVOLUCAO_Int").isNull(), F.col("EVOLUCAO_Int") == 9),
    ("hospitalizacao", confirmados, F.col("HOSPITALIZ_Int").isNull(), F.col("HOSPITALIZ_Int") == 9),
]

aggs = []
for nome, den, branco, ignorado in VARIAVEIS_COMPLETUDE:
    aggs += [
        F.sum(den.cast("int")).alias(f"{nome}__den"),
        F.sum((den & branco).cast("int")).alias(f"{nome}__branco"),
        F.sum((den & ~branco & F.coalesce(ignorado, F.lit(False))).cast("int")).alias(f"{nome}__ignorado"),
    ]

linhas_comp = []
for row in gold_base.groupBy("_source_year").agg(*aggs).collect():
    for nome, *_ in VARIAVEIS_COMPLETUDE:
        den = row[f"{nome}__den"] or 0
        br = row[f"{nome}__branco"] or 0
        ig = row[f"{nome}__ignorado"] or 0
        pct = round((br + ig) / den * 100, 2) if den else None
        if pct is None:
            classe = None
        elif pct < 5:
            classe = "Excelente"
        elif pct < 10:
            classe = "Bom"
        elif pct < 20:
            classe = "Regular"
        elif pct < 50:
            classe = "Ruim"
        else:
            classe = "Muito ruim"
        linhas_comp.append((int(row["_source_year"]), nome, int(den), int(br), int(ig), pct, classe))

df_gold_completude = spark.createDataFrame(
    linhas_comp,
    "ano int, variavel string, denominador bigint, em_branco bigint, ignorado bigint, "
    "pct_incompletude double, classificacao_romero_cunha string"
)

# COMMAND ----------

# DBTITLE 1,39.7 TCC – VALIDAÇÃO CONTRA NÚMEROS OFICIAIS
# ==============================================================================
# 39.7. VALIDAÇÃO EXTERNA
# ==============================================================================
# Preencha o arquivo  <volume>/validacao/numeros_oficiais.csv  com os números
# do Painel de Arboviroses do Ministério da Saúde
# (Brasil, por ano epidemiológico e residência). Na primeira execução o pipeline
# cria o modelo com as linhas a preencher.
#
# Indicadores aceitos: casos_provaveis, casos_confirmados, obitos (óbitos
# CONFIRMADOS, como no painel), obitos_investigacao e incidencia_provaveis_100mil.
# Compare com a data de atualização do painel igual à coluna
# versao_fonte_last_modified de gold_dengue_annual. Anote a data_consulta: os números oficiais
# também mudam com o tempo (veja a análise de revisão, seção 39.3).
# ==============================================================================

# VALIDACAO_DIR e VALIDACAO_CSV: definidos em config_agravo.
INDICADORES_VALIDACAO = [
    "casos_provaveis", "casos_confirmados", "obitos", "obitos_investigacao",
    "incidencia_provaveis_100mil",
]
os.makedirs(VALIDACAO_DIR, exist_ok=True)

if not os.path.exists(VALIDACAO_CSV):
    with open(VALIDACAO_CSV, "w", encoding="utf-8", newline="") as f:
        w = _csv.writer(f, delimiter=";")
        w.writerow(["ano", "indicador", "valor_oficial", "fonte", "data_consulta"])
        for ano in anos_disponiveis:
            for ind in INDICADORES_VALIDACAO:
                w.writerow([ano, ind, "", "", ""])
    print(f"[TCC] Modelo de validação criado em: {VALIDACAO_CSV} – preencha os valores oficiais.")

linhas_oficiais = []
with open(VALIDACAO_CSV, encoding="utf-8-sig") as f:
    for row in _csv.DictReader(f, delimiter=";"):
        valor = str(row.get("valor_oficial") or "").strip().replace(" ", "")
        if "," in valor:  # formato brasileiro: 1.234,56
            valor = valor.replace(".", "").replace(",", ".")
        if not valor:
            continue
        try:
            linhas_oficiais.append((
                int(row["ano"]), row["indicador"].strip(), float(valor),
                (row.get("fonte") or "").strip(), (row.get("data_consulta") or "").strip()
            ))
        except Exception:
            print(f"[TCC] Linha ignorada no CSV de validação: {row}")

df_gold_validacao = None
if linhas_oficiais:
    oficiais = spark.createDataFrame(
        linhas_oficiais, "ano int, indicador string, valor_oficial double, fonte string, data_consulta string"
    )
    pipeline_longo = df_gold_annual.select(
        F.col("ano_analise").alias("ano"),
        F.explode(F.array(*[
            F.struct(F.lit(i).alias("indicador"), F.col(i).cast("double").alias("valor_pipeline"))
            for i in INDICADORES_VALIDACAO
        ])).alias("x")
    ).select("ano", "x.*")

    df_gold_validacao = (
        oficiais.join(pipeline_longo, ["ano", "indicador"], "left")
        .withColumn("diferenca_absoluta", F.round(F.col("valor_pipeline") - F.col("valor_oficial"), 2))
        .withColumn(
            "diferenca_pct",
            F.round((F.col("valor_pipeline") - F.col("valor_oficial")) / F.col("valor_oficial") * 100, 2)
        )
        .withColumn(
            "avaliacao",
            F.when(F.abs("diferenca_pct") <= 1, "Concordante (<=1%)")
            .when(F.abs("diferenca_pct") <= 5, "Pequena divergência (1-5%)")
            .otherwise("Divergência relevante (>5%)")
        )
        .withColumn("validado_em", F.current_timestamp())
        .orderBy("ano", "indicador")
    )
    print(f"[TCC] Validação: {len(linhas_oficiais)} valores oficiais comparados.")
else:
    print("[TCC] Nenhum valor oficial preenchido ainda; validação pulada.")

# COMMAND ----------

# DBTITLE 1,Salvar as tabelas de vigilância
# Canal, alertas e nowcasting foram calculados com o histórico completo; na
# saída ficam só os anos do período de análise (antes isso era feito na seção 40 do pipeline).
df_gold_canal_uf = df_gold_canal_uf.filter(F.col("ano_epi") >= ANO_INICIO_ANALISE)
df_gold_canal_mun = df_gold_canal_mun.filter(F.col("ano_epi") >= ANO_INICIO_ANALISE)
df_gold_alertas_mun_ano = df_gold_alertas_mun_ano.filter(F.col("ano_epi") >= ANO_INICIO_ANALISE)
df_gold_nowcast = df_gold_nowcast.filter(F.col("ano_epi") >= ANO_INICIO_ANALISE)
df_fatores_completude = df_fatores_completude.filter(F.col("ano_epi") >= ANO_INICIO_ANALISE)

tabelas_vigilancia = [
    (df_gold_comparison, GOLD_COMPARISON),
    (df_gold_alertas, GOLD_ALERTAS),
    (df_gold_atraso, GOLD_ATRASO),
    (df_gold_atraso_semanal, GOLD_ATRASO_SEMANAL),
    (df_gold_canal_uf, GOLD_CANAL_UF),
    (df_gold_canal_mun, GOLD_CANAL_MUN),
    (df_gold_alertas_mun_ano, GOLD_ALERTAS_MUN_ANO),
    (df_gold_avaliacao_alertas, GOLD_AVALIACAO_ALERTAS),
    (df_gold_alertas_rt, GOLD_ALERTAS_RT),
    (df_gold_nowcast, GOLD_NOWCAST),
    (df_gold_nowcast_resumo, GOLD_NOWCAST_RESUMO),
    (df_fatores_completude, GOLD_FATORES_COMPLETUDE),
    (df_gold_completude, GOLD_COMPLETUDE),
]
if df_gold_validacao is not None:
    tabelas_vigilancia.append((df_gold_validacao, GOLD_VALIDACAO))

_assinatura = None
try:
    _assinatura = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {GOLD_WEEKLY}").collect()
                   }.get("pipeline.assinatura_fonte")
except Exception:
    pass

_linhas_linhagem = []
for dataframe, table_name in tabelas_vigilancia:
    print(f"[VIGILÂNCIA] Salvando {table_name}...")
    (dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name))
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('vigilancia.run_id' = '{RUN_ID}', "
              f"'pipeline.versao' = '{PIPELINE_VERSION}', 'pipeline.assinatura_fonte' = '{_assinatura or ''}')")
    _linhas_linhagem.append((RUN_ID, table_name, int(spark.table(table_name).count()), int(_versao(table_name)),
                             PIPELINE_VERSION, _assinatura, _json_vig.dumps(PARAMETROS_VIGILANCIA, default=str),
                             datetime.now(timezone.utc)))

(spark.createDataFrame(_linhas_linhagem, "run_id string, tabela string, linhas long, versao_delta long, "
                       "pipeline_version string, assinatura_fonte string, parametros string, executado_em timestamp")
 .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(GOLD_LINHAGEM))

for _t in [GOLD_COMPARISON, GOLD_ALERTAS, GOLD_CANAL_MUN, GOLD_ALERTAS_MUN_ANO, GOLD_NOWCAST]:
    try:
        spark.sql(f"OPTIMIZE {_t}")
    except Exception as e:
        print(f"[WARN] OPTIMIZE não executado em {_t}: {str(e)[:150]}")

# a chave fica gravada na última tabela: se a execução parar no meio, recalcula
spark.sql(f"ALTER TABLE {GOLD_AVALIACAO_ALERTAS} SET TBLPROPERTIES ('vigilancia.chave' = '{CHAVE_VIGILANCIA}')")

print("\nAlertas:")
display(spark.table(GOLD_ALERTAS).orderBy(F.desc("ano_analise"), F.desc("casos_confirmados")).limit(50))

registrar_metrica(RUN_ID, "vigilancia", time.perf_counter() - t_start_vigilancia, modo="analise",
                  observacao="canal endêmico, alertas, nowcasting, atraso, completude, validação")
dbutils.notebook.exit(_json_vig.dumps({"status": "OK", "agravo": AGRAVO, "run_id": RUN_ID,
                                       "tabelas": len(tabelas_vigilancia)}))