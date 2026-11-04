# Databricks notebook source
# DBTITLE 1,Sobre este notebook
# ==============================================================================
# BASE ANALÍTICA DE UM AGRAVO (a partir da Silver gravada)
# ==============================================================================
# Carregado com  %run ./base_analitica_agravo  pelo pipeline (11), pela vigilância
# (21) e pelo benchmark (25). Lê a Silver e a população e monta, sem gravar nada:
#   gold_base_full / gold_base  – uma linha por notificação, com ano/semana
#                                 epidemiológicos, flags e população
#   pop_territorio_ano          – população do Brasil por ano
#   anos_analise                – anos epidemiológicos presentes
#   df_cal_epi, HOJE            – calendário epidemiológico
# Precisa do config_agravo carregado antes.
# ==============================================================================
if "df_silver_persisted" not in globals():
    df_silver_persisted = spark.table(SILVER_TABLE)

# COMMAND ----------
# DBTITLE 1,26. BASE ANALÍTICA DA GOLD
# ==============================================================================
# 26. BASE ANALÍTICA DA GOLD
# ==============================================================================
# CORREÇÃO – semana epidemiológica:
# No SINAN, SEM_PRI vem no formato AAAASS (ex.: 202405). O código anterior
# usava esse valor inteiro como "semana", então a comparação histórica
# (partição por semana) nunca encontrava outros anos e o semanal ficava com
# semanas como 202405. Aqui separamos:
#   ano_epi     = AAAA   (ano epidemiológico oficial)
#   SEM_PRI_Int = SS     (1..53) – mesmo nome de antes, para não quebrar o dashboard
# Feito na Gold (a partir da Silver) para funcionar também com partições antigas.
#
# Casos prováveis (critério do Ministério da Saúde para incidência):
# todas as notificações, exceto descartadas (5) e chikungunya (13).
# ==============================================================================

sem_raw = F.col("SEM_PRI_Int").cast("int")

silver_gold = (
    df_silver_persisted
    .withColumn(
        "ano_epi",
        F.when(sem_raw >= 190001, F.floor(sem_raw / 100).cast("int"))
    )
    .withColumn(
        "SEM_PRI_Int",
        F.when(sem_raw >= 190001, (sem_raw % 100).cast("int"))
        .when(sem_raw.between(1, 53), sem_raw)
    )
    .withColumn(
        "ano_analise",
        F.coalesce(
            F.col("ano_epi"),
            F.year("DT_SIN_PRI_Parsed"),
            F.year("DT_NOTIFIC_Parsed"),
            F.col("_source_year").cast("int")
        )
    )
    .withColumn(
        "flag_caso_provavel",
        F.when(
            F.col("CLASSI_FIN_Int").isNull()
            | ~F.col("CLASSI_FIN_Int").isin(*CODIGOS_NAO_PROVAVEIS),
            1
        ).otherwise(0)
    )
)

# ------------------------------------------------------------------------------
# ÓBITOS – mesma regra do painel do Ministério da Saúde (versão 10)
#   obitos (flag_obito)       : evolução = óbito pelo agravo (EVOLUCAO 2) E caso
#                               CONFIRMADO (dengue: 10/11/12). É o "óbito
#                               confirmado" do painel e o numerador da letalidade.
#   obitos_evolucao_agravo    : EVOLUCAO 2 em qualquer classificação (regra das
#                               versões <= 9; inclui inconclusivos e casos ainda
#                               sem classificação). Fica como coluna de conferência.
#   obitos_investigacao       : EVOLUCAO 4, como antes.
# Feito aqui (Gold) e não na Silver: vale também no modo somente_gold, sem
# reprocessar a Silver.
# ------------------------------------------------------------------------------
silver_gold = (
    silver_gold
    .withColumn("flag_obito_evolucao_agravo", F.col("flag_obito"))
    .withColumn(
        "flag_obito",
        F.when((F.col("flag_obito_evolucao_agravo") == 1) & (F.col("flag_confirmado") == 1), 1).otherwise(0),
    )
)

if REMOVER_DUPLICADOS_SINAN:
    silver_gold = silver_gold.filter(F.col("flag_duplicado_sinan") == 0)
    print("[GOLD] Registros marcados como duplicados pelo SINAN (NDUPLIC_N=1) removidos.")

pop_dim = spark.table(POPULATION_TABLE).alias("pop")

gold_base = (
    silver_gold.alias("s")
    .join(
        pop_dim,
        (F.col("s.ID_MN_RESI_CODIGO") == F.col("pop.municipio_codigo_6"))
        & (F.col("s.ano_analise") == F.col("pop.ano_populacao")),
        "left"
    )
    .select(
        F.col("s.*"),
        F.col("pop.municipio_codigo_ibge"),
        F.col("pop.municipio_nome"),
        F.col("pop.uf").alias("uf_municipio"),
        F.col("pop.populacao").alias("populacao_ano"),
        F.col("pop.ano_populacao").alias("ano_populacao_referencia"),
        F.col("pop.fonte_populacao")
    )
)

# Fallback: população mais recente do município (para anos fora da dimensão)
latest_population = (
    spark.table(POPULATION_TABLE)
    .filter(F.col("populacao").isNotNull())
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("municipio_codigo_6").orderBy(F.desc("ano_populacao"))
        )
    )
    .filter(F.col("_rn") == 1)
    .select(
        F.col("municipio_codigo_6").alias("_mun6"),
        F.col("municipio_codigo_ibge").alias("_cod7"),
        F.col("municipio_nome").alias("_nome"),
        F.col("populacao").alias("populacao_mais_recente"),
        F.col("ano_populacao").alias("ano_populacao_mais_recente")
    )
)

gold_base = (
    gold_base
    .join(latest_population, F.col("ID_MN_RESI_CODIGO") == F.col("_mun6"), "left")
    .withColumn("municipio_codigo_ibge", F.coalesce("municipio_codigo_ibge", "_cod7"))
    .withColumn("municipio_nome", F.coalesce("municipio_nome", "_nome"))
    .withColumn(
        "populacao_referencia",
        F.coalesce(F.col("populacao_ano"), F.col("populacao_mais_recente"))
    )
    .withColumn(
        "ano_populacao_referencia",
        F.coalesce(F.col("ano_populacao_referencia"), F.col("ano_populacao_mais_recente"))
    )
    .drop("_mun6", "_cod7", "_nome")
)

# ------------------------------------------------------------------------------
# PERÍODO DE ANÁLISE x HISTÓRICO DE REFERÊNCIA
#   gold_base_full : todos os anos carregados (START_YEAR..END_YEAR). Usada SÓ
#                    para o histórico do canal endêmico e dos limiares de alerta.
#   gold_base      : apenas ANO_INICIO_ANALISE..END_YEAR (2022-2026). Todas as
#                    tabelas descritivas e os resultados do artigo usam esta.
# ------------------------------------------------------------------------------
gold_base_full = gold_base
gold_base = gold_base.filter(F.col("ano_analise") >= ANO_INICIO_ANALISE)
print(
    f"[GOLD] Período de análise: {ANO_INICIO_ANALISE}-{END_YEAR}. "
    f"Anos anteriores servem apenas de histórico do canal endêmico."
)

# População total do Brasil por ano (soma dos 5.570 municípios) – denominador nacional
pop_territorio_ano = (
    spark.table(POPULATION_TABLE)
    .groupBy(F.col("ano_populacao").alias("_ano_pop"))
    .agg(F.sum("populacao").alias("populacao_territorio"))
    .filter(F.col("populacao_territorio") > 0)
)


def com_incidencia(df, col_ano, col_pop="populacao_territorio"):
    """Junta a população do Brasil no ano e calcula incidência por 100 mil hab."""
    if col_pop == "populacao_territorio":
        df = (
            df.join(pop_territorio_ano, F.col(col_ano) == F.col("_ano_pop"), "left")
            .drop("_ano_pop")
        )
    for casos, nome in [
        ("casos_provaveis", "incidencia_provaveis_100mil"),
        ("casos_confirmados", "incidencia_confirmados_100mil"),
    ]:
        if casos in df.columns:
            df = df.withColumn(
                nome,
                F.when(
                    F.col(col_pop) > 0,
                    F.round(F.col(casos) / F.col(col_pop) * 100000, 2)
                )
            )
    return df

# COMMAND ----------
# DBTITLE 1,Anos epidemiológicos presentes na base
anos_analise = [
    r[0] for r in gold_base.select("ano_analise").distinct().collect()
    if r[0] is not None
]

# COMMAND ----------
# DBTITLE 1,39.1 TCC – CALENDÁRIO EPIDEMIOLÓGICO
# ==============================================================================
# 39.1. CALENDÁRIO EPIDEMIOLÓGICO
# ==============================================================================
# A semana epidemiológica (SE) vai de domingo a sábado. A SE 1 de um ano é a
# primeira semana que tem pelo menos 4 dias em janeiro (regra do Ministério
# da Saúde). Esse calendário é usado pelas análises de revisão dos dados e
# pelo canal endêmico (para criar a grade completa de semanas, inclusive as
# semanas sem nenhum caso).
# ==============================================================================

from datetime import date, timedelta


# Regra do SINAN (semana 1 contém 4 de janeiro): função única em funcoes_comuns.
inicio_ano_epidemiologico = inicio_ano_epi


anos_calendario = sorted(
    set(POPULATION_YEARS) | {int(a) for a in anos_analise if a is not None}
)

linhas_calendario = []
for ano in range(min(anos_calendario) - 1, max(anos_calendario) + 2):
    inicio = inicio_ano_epidemiologico(ano)
    n_semanas = (inicio_ano_epidemiologico(ano + 1) - inicio).days // 7
    for semana in range(1, n_semanas + 1):
        ini = inicio + timedelta(weeks=semana - 1)
        linhas_calendario.append((ano, semana, ini, ini + timedelta(days=6)))

df_cal_epi = spark.createDataFrame(
    linhas_calendario,
    "ano_epi int, semana_epi int, inicio_semana date, fim_semana date"
)

# Só semanas que já terminaram entram na análise
HOJE = datetime.now(timezone.utc).date()
ANO_CORRENTE = HOJE.year

print(f"[TCC] Calendário epidemiológico: {len(linhas_calendario)} semanas.")
