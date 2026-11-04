# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 24 – EFICIÊNCIA DA SOLUÇÃO (experimentos com repetições + estatística)
# ==============================================================================
# Mede, com repetições, quanto a arquitetura economiza e como ela escala:
#
#   EXPERIMENTO 1 – custo_modos: tempo de cada modo de execução
#     verificacao_sem_mudanca : a fonte não mudou -> só confere os metadados e
#                               encerra (é o que acontece na maioria dos dias)
#     incremental_1_ano       : só o arquivo do ano corrente mudou
#     somente_gold            : recalcula só os indicadores (Gold)
#     completo_forcado        : reprocessa todos os anos do zero
#   -> mostra a ECONOMIA da detecção de mudança e da execução incremental.
#
#   EXPERIMENTO 2 – escalabilidade_volume: reprocessamento completo com cada vez
#     mais anos de dados (ex.: 1, 2, 4, 7 e 11 arquivos anuais)
#   -> mostra como o tempo cresce com o volume (linear? sublinear?).
#
#   EXPERIMENTO 3 – granularidade: custo de agregar casos e incidência por
#     UF/município x ano/semana/dia (notebook 25_benchmark_granularidade, que
#     antes rodava no fim do pipeline com o widget benchmark_granularidade).
#
# Desde a versão 12 o pipeline (11) NÃO inclui as análises de vigilância (canal
# endêmico, alertas, nowcasting...), que rodam no 21 e têm a camada própria
# "vigilancia" na tabela de métricas. Por isso os tempos de gold_calculo não são
# comparáveis com os medidos com o pipeline antigo (01): rode os experimentos
# de novo.
#
# Protocolo: 1 execução de aquecimento descartada por configuração; depois N
# repetições de cada configuração em ORDEM SORTEADA (semente fixa), para que
# variações do ambiente ao longo do tempo não se confundam com o efeito da
# configuração. O 11 grava o tempo de cada camada (verificação, Bronze, Silver,
# população, Gold) em arboviroses_metricas.pipeline_metrics.
#
# Estatística: média, desvio-padrão, coeficiente de variação, mediana, IQR,
# IC 95% (t e bootstrap); Kruskal-Wallis + Mann-Whitney com correção de Holm e
# delta de Cliff; economia anual por frequência de execução; regressão do tempo
# em função do volume (elasticidade log-log).
#
# Os experimentos usam schemas próprios (dengue_br_exp, dengue_br_esc) e NÃO
# alteram as tabelas do dashboard.
#
# COMO USAR
#   1. modo = planejar  -> só lista as execuções e estima o tempo total.
#   2. modo = executar  -> roda as execuções e, no fim, a análise.
#   3. modo = analisar  -> só refaz a análise com o que já foi medido.
# Brasil inteiro é grande: comece com poucas repetições (3) e com o experimento
# de custo; a escalabilidade pode ser feita depois.
# ==============================================================================

dbutils.widgets.dropdown("modo", "executar", ["planejar", "executar", "analisar"])
dbutils.widgets.text("experimentos", "custo_modos")
dbutils.widgets.dropdown("agravo", "dengue", ["dengue", "chikungunya", "zika"])
dbutils.widgets.text("n_repeticoes", "5")
dbutils.widgets.text("n_aquecimento", "1")
dbutils.widgets.text("ano_incremental", "2026")
dbutils.widgets.text("anos_inicio_escalabilidade", "2026,2025,2024,2023,2022")
dbutils.widgets.text("atualizacoes_por_semana", "1")
dbutils.widgets.text("caminho_pipeline", "./11_pipeline_agravo")
dbutils.widgets.text("nome_rodada", "")
# Cada schema de experimento (dengue_br_exp, dengue_br_esc) é uma cópia do pipeline
# com ~50 tabelas, e o plano gratuito aceita 500 tabelas/views no total.
# sim = apaga esses schemas no fim (as métricas de tempo ficam guardadas).
dbutils.widgets.dropdown("apagar_schemas_experimento", "sim", ["nao", "sim"])

import os, json, time, math, random
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from scipy import stats
from pyspark.sql import functions as F

MODO = dbutils.widgets.get("modo")
EXPERIMENTOS = [e.strip() for e in dbutils.widgets.get("experimentos").split(",") if e.strip()]
AGRAVO = dbutils.widgets.get("agravo")
N_REP = max(1, int(dbutils.widgets.get("n_repeticoes") or 3))
N_AQUEC = max(0, int(dbutils.widgets.get("n_aquecimento") or 1))
ANO_INC = dbutils.widgets.get("ano_incremental").strip() or "2026"
ANOS_ESC = [int(a) for a in dbutils.widgets.get("anos_inicio_escalabilidade").replace(" ", "").split(",") if a.isdigit()]
ATUALIZ_SEMANA = float(dbutils.widgets.get("atualizacoes_por_semana") or 1)
RODADA = dbutils.widgets.get("nome_rodada").strip()
SEMENTE = 20261030
TIMEOUT_S = 8 * 3600
CATALOG = "workspace"
METRICAS = f"{CATALOG}.arboviroses_metricas"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METRICAS}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {METRICAS}.arquivos")


def nome_exp(e):
    return f"{e}_{RODADA}" if RODADA else e


print(f"modo={MODO} | experimentos={EXPERIMENTOS} | agravo={AGRAVO} | repetições={N_REP} | aquecimento={N_AQUEC}")

# COMMAND ----------

# DBTITLE 1,Plano de execuções
PADRAO = {  # todos os widgets do pipeline (11), para nenhuma execução herdar valores de outra
    "forcar_reprocessamento": "nao", "modo_execucao": "completo", "semanas_consecutivas": "2",
    "anos_excluidos_canal": "", "rotulo_execucao": "base", "ano_inicio_analise": "2022",
    "data_disponibilidade": "DT_DIGITA", "remover_duplicados_sinan": "nao", "exportar_microdados": "nao",
    "excluir_chikungunya": "nao", "agravo": AGRAVO, "ano_inicio_fonte": "2022", "ano_fim_fonte": "2026", "sufixo_schema": "",
    "usar_cache_ibge": "sim", "simular_ano_alterado": "", "benchmark_granularidade": "nao",
    "repeticoes_benchmark": "1", "exportar_csv": "nao", "experimento": "", "configuracao": "",
    "repeticao": "", "aquecimento": "nao", "simular_falha": "", "simular_falha_ano": "2026", "url_fonte": "",
}


def silver_existe(sufixo):
    base = f"{AGRAVO}_br"
    return spark.catalog.tableExists(f"{CATALOG}.{base}_{sufixo}.silver_{base}")


plano = []
for exp in EXPERIMENTOS:
    if exp == "custo_modos":
        base = {"sufixo_schema": "exp"}
        configs = {
            "verificacao_sem_mudanca": {**base},
            "incremental_1_ano": {**base, "simular_ano_alterado": ANO_INC},
            "somente_gold": {**base, "modo_execucao": "somente_gold"},
            "completo_forcado": {**base, "forcar_reprocessamento": "sim"},
        }
        if not silver_existe("exp"):  # 1ª vez: cria o schema de experimento
            plano.append(("preparacao", exp, -1, True, {**base, "forcar_reprocessamento": "sim"}))
    elif exp == "escalabilidade_volume":
        configs = {
            f"{2026 - a + 1:02d}_anos_{a}_2026": {"sufixo_schema": "esc", "ano_inicio_fonte": str(a),
                                                  "forcar_reprocessamento": "sim"}
            for a in sorted(ANOS_ESC, reverse=True)
        }
    elif exp == "granularidade":
        continue  # roda à parte, no notebook 25 (célula abaixo)
    else:
        print(f"Experimento desconhecido: {exp}")
        continue
    for nome, args in configs.items():
        for r in range(N_AQUEC):
            plano.append((nome, exp, -(r + 1), True, args))
    validas = [(nome, exp, r, False, args) for nome, args in configs.items() for r in range(1, N_REP + 1)]
    random.Random(SEMENTE).shuffle(validas)
    plano += validas

df_plano = pd.DataFrame([{"ordem": i + 1, "experimento": nome_exp(e), "configuracao": c, "repeticao": r,
                          "aquecimento": a} for i, (c, e, r, a, _) in enumerate(plano)])
print(f"{len(plano)} execuções no plano.")

# estimativa de tempo com medições anteriores (se houver)
try:
    hist = (spark.table(f"{METRICAS}.pipeline_metrics")
            .filter((F.col("agravo") == AGRAVO) & F.col("configuracao").isNotNull())
            .groupBy("run_id", "configuracao").agg(F.sum("duracao_segundos").alias("s"))
            .groupBy("configuracao").agg(F.expr("percentile_approx(s, 0.5)").alias("mediana_s")).toPandas())
    est = df_plano.merge(hist, on="configuracao", how="left")
    if est["mediana_s"].notna().any():
        print(f"Tempo estimado (com base em execuções anteriores): ~{est['mediana_s'].sum() / 3600:.1f} h "
              f"({est['mediana_s'].isna().sum()} execuções sem estimativa).")
except Exception:
    pass
display(df_plano)

# COMMAND ----------

# DBTITLE 1,Executar
# caminho_absoluto e motivo_do_erro vêm de funcoes_comuns.

if MODO == "executar":
    caminho = caminho_absoluto(dbutils.widgets.get("caminho_pipeline").strip())
    registro, falhas_seguidas = [], 0
    for i, (conf, exp, rep, aquec, args) in enumerate(plano, 1):
        a = {**PADRAO, **args, "experimento": nome_exp(exp), "configuracao": conf,
             "repeticao": str(rep), "aquecimento": "sim" if aquec else "nao"}
        t0 = time.perf_counter()
        print(f"[{i:>3}/{len(plano)}] {conf} rep {rep}{' (aquecimento)' if aquec else ''} ... ", end="")
        try:
            saida = dbutils.notebook.run(caminho, TIMEOUT_S, a)
            st = json.loads(saida).get("status", "ok") if saida and saida.startswith("{") else (saida or "ok")
            print(f"{time.perf_counter() - t0:,.1f} s – {st}")
            registro.append((nome_exp(exp), conf, rep, aquec, "ok", str(st), time.perf_counter() - t0))
            falhas_seguidas = 0
        except Exception as ex:
            print(f"ERRO após {time.perf_counter() - t0:,.0f} s\n{motivo_do_erro(ex)}")
            registro.append((nome_exp(exp), conf, rep, aquec, "erro", motivo_do_erro(ex)[:500], time.perf_counter() - t0))
            falhas_seguidas += 1
            if conf == "preparacao" or falhas_seguidas >= 3:
                print("Parando: a preparação falhou ou houve 3 falhas seguidas. Corrija o erro acima e rode de novo.")
                break
    (spark.createDataFrame(registro, "experimento string, configuracao string, repeticao int, aquecimento boolean, "
                                     "resultado string, detalhe string, segundos_parede double")
     .withColumn("executado_em", F.current_timestamp())
     .write.mode("append").saveAsTable(f"{METRICAS}.execucoes_eficiencia"))
    if "granularidade" in EXPERIMENTOS:
        t0 = time.perf_counter()
        print(f"[granularidade] notebook 25, {N_REP} repetições … ", end="")
        try:
            saida = dbutils.notebook.run(caminho_absoluto("./25_benchmark_granularidade"), TIMEOUT_S, {
                **PADRAO, "repeticoes_benchmark": str(N_REP), "experimento": nome_exp("granularidade")})
            print(f"{time.perf_counter() - t0:,.1f} s – {saida}")
        except Exception as ex:
            print(f"ERRO\n{motivo_do_erro(ex)}")
elif MODO == "planejar":
    if "granularidade" in EXPERIMENTOS:
        print(f"+ granularidade: 6 níveis x ({N_REP} repetições + 1 aquecimento) no notebook 25")
    dbutils.notebook.exit("Plano listado acima. Para rodar, mude o widget modo para executar.")

# COMMAND ----------

# DBTITLE 1,Funções estatísticas
def bootstrap_ic(x, est="media", B=5000):
    x = np.asarray(x, float); x = x[~np.isnan(x)]
    if len(x) < 2:
        return (np.nan, np.nan)
    a = x[np.random.default_rng(SEMENTE).integers(0, len(x), (B, len(x)))]
    v = a.mean(1) if est == "media" else np.median(a, 1)
    return tuple(float(q) for q in np.quantile(v, [0.025, 0.975]))


def descritiva(x):
    x = np.asarray(x, float); x = x[~np.isnan(x)]; n = len(x)
    if n == 0:
        return {"n": 0}
    m = x.mean(); dp = x.std(ddof=1) if n > 1 else np.nan
    q1, med, q3 = np.quantile(x, [.25, .5, .75])
    t = stats.t.ppf(.975, n - 1) if n > 1 else np.nan
    bm, bmed = bootstrap_ic(x, "media"), bootstrap_ic(x, "mediana")
    return {"n": n, "media": m, "dp": dp, "cv_pct": dp / m * 100 if m else np.nan, "mediana": med,
            "q1": q1, "q3": q3, "iqr": q3 - q1, "minimo": x.min(), "maximo": x.max(),
            "ic95_media_t_inf": m - t * dp / math.sqrt(n) if n > 1 else np.nan,
            "ic95_media_t_sup": m + t * dp / math.sqrt(n) if n > 1 else np.nan,
            "ic95_mediana_boot_inf": bmed[0], "ic95_mediana_boot_sup": bmed[1]}


def tabela(df, grupos, col):
    out = pd.DataFrame([{**dict(zip(grupos, k if isinstance(k, tuple) else (k,))), **descritiva(g[col])}
                        for k, g in df.groupby(grupos)])
    num = out.select_dtypes("number").columns.difference(["n"])
    out[num] = out[num].round(3)
    return out


def holm(p):
    p = np.asarray(p, float); o = np.argsort(p); aj = np.empty(len(p)); acc = 0
    for pos, i in enumerate(o):
        acc = max(acc, (len(p) - pos) * p[i]); aj[i] = min(1, acc)
    return aj


def cliff(a, b):
    a, b = np.asarray(a, float), np.asarray(b, float)
    return float(((a[:, None] > b).sum() - (a[:, None] < b).sum()) / (len(a) * len(b)))


def comparar(df, grupo, col):
    am = {k: g[col].dropna().values for k, g in df.groupby(grupo)}
    am = {k: v for k, v in am.items() if len(v) >= 2}
    if len(am) < 2:
        return None, pd.DataFrame()
    h, p = stats.kruskal(*am.values())
    ks = sorted(am); pares = []
    for i in range(len(ks)):
        for j in range(i + 1, len(ks)):
            a, b = am[ks[i]], am[ks[j]]
            u, pp = stats.mannwhitneyu(a, b, alternative="two-sided"); d = cliff(a, b)
            pares.append({"grupo_a": ks[i], "grupo_b": ks[j], "mediana_a": np.median(a), "mediana_b": np.median(b),
                          "vezes_mais_lento": np.median(a) / np.median(b) if np.median(b) else np.nan,
                          "U": u, "p": pp, "cliff_delta": d,
                          "efeito": "desprezível" if abs(d) < .147 else "pequeno" if abs(d) < .33
                          else "médio" if abs(d) < .474 else "grande"})
    pares = pd.DataFrame(pares); pares["p_holm"] = holm(pares["p"])
    return {"teste": "Kruskal-Wallis", "H": h, "gl": len(am) - 1, "p": p}, pares

# COMMAND ----------

# DBTITLE 1,Análise: tempos por modo e por camada
import matplotlib.pyplot as plt

OUT = f"/Volumes/{CATALOG}/arboviroses_metricas/arquivos/eficiencia_{datetime.now():%Y%m%d_%H%M}"
os.makedirs(OUT, exist_ok=True)


def salvar(pdf, nome):
    if pdf is None or len(pdf) == 0:
        print(f"[{nome}] sem dados"); return
    pdf.to_csv(f"{OUT}/{nome}.csv", index=False, encoding="utf-8-sig")
    spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{METRICAS}.eficiencia_{nome.lower()}")
    print(f"[{nome}] {len(pdf)} linhas"); display(pdf)


m = (spark.table(f"{METRICAS}.pipeline_metrics")
     .filter((F.col("agravo") == AGRAVO) & F.col("experimento").isin([nome_exp(e) for e in EXPERIMENTOS])
             & (F.col("aquecimento") == False) & F.col("configuracao").isNotNull())
     .toPandas())
if m.empty:
    dbutils.notebook.exit("Ainda não há medições. Rode com modo = executar.")

# execuções em que a fonte foi baixada de verdade (OpenDataSUS publicou versão
# nova durante o experimento) saem da comparação, porque o tempo não é comparável
verif = m[m["camada"] == "verificacao_fonte"]
com_download = verif.loc[~verif["observacao"].fillna("").str.contains("baixados=nenhum"), "run_id"]
m = m[~m["run_id"].isin(com_download) | m["configuracao"].str.contains("completo|anos_")]

por_run = m.groupby("run_id").agg(experimento=("experimento", "first"), configuracao=("configuracao", "first"),
                                  n_anos=("n_anos_fonte", "first")).reset_index()
pivo = m.pivot_table(index="run_id", columns="camada", values="duracao_segundos", aggfunc="sum")
pivo["TOTAL"] = pivo.sum(axis=1)
reg = m[m["camada"] == "bronze"].groupby("run_id")["registros_processados"].max().rename("registros_bronze")
tempos = (pivo.reset_index().melt(id_vars="run_id", var_name="camada", value_name="segundos").dropna()
          .merge(por_run, on="run_id"))

salvar(tabela(tempos, ["experimento", "configuracao", "camada"], "segundos"), "E1_tempo_por_modo_e_camada")

custo = tempos[(tempos["experimento"] == nome_exp("custo_modos")) & (tempos["camada"] == "TOTAL")]
if len(custo):
    glob, pares = comparar(custo, "configuracao", "segundos")
    if glob:
        salvar(pd.DataFrame([glob]), "E2_kruskal_wallis_modos")
        salvar(pares, "E3_comparacoes_entre_modos")
    ordem = ["verificacao_sem_mudanca", "incremental_1_ano", "somente_gold", "completo_forcado"]
    ordem = [o for o in ordem if o in set(custo["configuracao"])]
    fig, ax = plt.subplots(figsize=(7, 3.8))
    ax.boxplot([custo.loc[custo["configuracao"] == o, "segundos"] for o in ordem], showfliers=True)
    for i, o in enumerate(ordem, 1):
        y = custo.loc[custo["configuracao"] == o, "segundos"]
        ax.scatter(np.random.default_rng(1).normal(i, .04, len(y)), y, s=12, alpha=.7, color="#2a78d6")
    rotulos = {"verificacao_sem_mudanca": "Fonte sem\nmudança", "incremental_1_ano": "Incremental\n(1 ano)",
               "somente_gold": "Só Gold", "completo_forcado": "Completo"}
    ax.set_xticks(range(1, len(ordem) + 1), [rotulos[o] for o in ordem])
    ax.set_yscale("log"); ax.set_ylabel("Tempo total (s, escala log)")
    ax.set_title(f"{AGRAVO.capitalize()} – tempo por modo de execução (Brasil)", loc="left", fontweight="bold")
    fig.tight_layout(); fig.savefig(f"{OUT}/F1_tempo_por_modo.png", dpi=200); plt.show()

# COMMAND ----------

# DBTITLE 1,Análise: economia anual da detecção de mudança
# Em 365 dias, com a fonte atualizada ATUALIZ_SEMANA vezes por semana:
#   sempre reprocessar tudo          : N execuções x tempo completo
#   detecção + incremental            : (N - u) x tempo sem mudança + u x tempo incremental
#   detecção + completo               : (N - u) x tempo sem mudança + u x tempo completo
# IC 95% por bootstrap das medições.
if len(custo) and {"verificacao_sem_mudanca", "completo_forcado"} <= set(custo["configuracao"]):
    amostra = {c: custo.loc[custo["configuracao"] == c, "segundos"].values for c in set(custo["configuracao"])}
    rng = np.random.default_rng(SEMENTE)
    u_ano = ATUALIZ_SEMANA * 52.14
    linhas = []
    for freq, n_ano in [("horária", 8760), ("diária", 365), ("semanal", 52.14)]:
        u = min(u_ano, n_ano)
        boots = {c: np.median(v[rng.integers(0, len(v), (3000, len(v)))], axis=1) for c, v in amostra.items()}
        sempre = n_ano * boots["completo_forcado"]
        estr = {"sempre_reprocessar_tudo": sempre,
                "deteccao_mais_completo": (n_ano - u) * boots["verificacao_sem_mudanca"] + u * boots["completo_forcado"]}
        if "incremental_1_ano" in boots:
            estr["deteccao_mais_incremental"] = (n_ano - u) * boots["verificacao_sem_mudanca"] + u * boots["incremental_1_ano"]
        for nome, v in estr.items():
            eco = (1 - v / sempre) * 100
            linhas.append({"frequencia": freq, "estrategia": nome, "atualizacoes_por_semana": ATUALIZ_SEMANA,
                           "horas_por_ano": round(float(np.median(v)) / 3600, 2),
                           "economia_pct": round(float(np.median(eco)), 2),
                           "economia_ic95_inf": round(float(np.quantile(eco, .025)), 2),
                           "economia_ic95_sup": round(float(np.quantile(eco, .975)), 2)})
    eco = pd.DataFrame(linhas)
    salvar(eco, "E4_economia_anual_por_frequencia")
    fig, ax = plt.subplots(figsize=(7, 3.6))
    cores = {"sempre_reprocessar_tudo": "#2a78d6", "deteccao_mais_completo": "#eb6834", "deteccao_mais_incremental": "#1baf7a"}
    freqs = ["horária", "diária", "semanal"]; w = .26
    for i, (e, cor) in enumerate(cores.items()):
        s = eco[eco["estrategia"] == e].set_index("frequencia").reindex(freqs)
        ax.bar(np.arange(3) + (i - 1) * w, s["horas_por_ano"], w, color=cor, label=e.replace("_", " "))
    ax.set_yscale("log"); ax.set_xticks(range(3), ["Execução horária", "Diária", "Semanal"])
    ax.set_ylabel("Horas de processamento / ano"); ax.legend(frameon=False, fontsize=8)
    ax.set_title(f"{AGRAVO.capitalize()} – custo anual por estratégia", loc="left", fontweight="bold")
    fig.tight_layout(); fig.savefig(f"{OUT}/F2_economia_anual.png", dpi=200); plt.show()

# COMMAND ----------

# DBTITLE 1,Análise: escalabilidade com o volume de dados
esc = (tempos[(tempos["experimento"] == nome_exp("escalabilidade_volume")) & (tempos["camada"] == "TOTAL")]
       .merge(reg.reset_index(), on="run_id", how="left"))
if len(esc) and esc["configuracao"].nunique() >= 2:
    salvar(tabela(esc, ["configuracao", "n_anos"], "segundos"), "E5_escalabilidade_tempo")
    ok = esc.dropna(subset=["registros_bronze"])
    ok = ok[(ok["registros_bronze"] > 0) & (ok["segundos"] > 0)]
    if ok["registros_bronze"].nunique() >= 2:
        lin = stats.linregress(ok["registros_bronze"], ok["segundos"])
        log = stats.linregress(np.log10(ok["registros_bronze"]), np.log10(ok["segundos"]))
        t = stats.t.ppf(.975, len(ok) - 2)
        salvar(pd.DataFrame([{
            "n_execucoes": len(ok), "segundos_por_milhao_registros": lin.slope * 1e6, "r2_linear": lin.rvalue ** 2,
            "elasticidade": log.slope, "elasticidade_ic95_inf": log.slope - t * log.stderr,
            "elasticidade_ic95_sup": log.slope + t * log.stderr, "r2_loglog": log.rvalue ** 2,
            "interpretacao": "linear (≈1)" if abs(log.slope - 1) < 2 * log.stderr else
                             ("sublinear (<1)" if log.slope < 1 else "superlinear (>1)"),
        }]), "E6_escalabilidade_regressao")
        fig, ax = plt.subplots(figsize=(6, 3.6))
        ax.scatter(ok["registros_bronze"] / 1e6, ok["segundos"], color="#2a78d6", s=18)
        xs = np.linspace(ok["registros_bronze"].min(), ok["registros_bronze"].max(), 50)
        ax.plot(xs / 1e6, lin.intercept + lin.slope * xs, color="#eb6834", lw=1.2)
        ax.set_xlabel("Registros brutos processados (milhões)"); ax.set_ylabel("Tempo total (s)")
        ax.set_title(f"{AGRAVO.capitalize()} – tempo × volume (reprocessamento completo)", loc="left", fontweight="bold")
        fig.tight_layout(); fig.savefig(f"{OUT}/F3_escalabilidade.png", dpi=200); plt.show()

# COMMAND ----------

# DBTITLE 1,Análise: custo por granularidade (experimento 3)
BENCH = f"{METRICAS}.benchmark_granularidade"
if "granularidade" in EXPERIMENTOS and spark.catalog.tableExists(BENCH):
    b = (spark.table(BENCH).filter((F.col("agravo") == AGRAVO) & ~F.col("aquecimento")
                                   & (F.col("experimento") == nome_exp("granularidade"))).toPandas())
    if len(b):
        e7 = tabela(b.rename(columns={"duracao_segundos": "segundos"}), ["nivel", "linhas_saida"], "segundos")
        salvar(e7, "E7_custo_por_granularidade")

print(f"\nResultados em {OUT} e nas tabelas {METRICAS}.eficiencia_*")

# COMMAND ----------

# DBTITLE 1,Limpeza dos schemas de experimento (limite de 500 tabelas/views)
_sufixos = {"custo_modos": "exp", "escalabilidade_volume": "esc"}
_schemas_exp = [f"{CATALOG}.{AGRAVO}_br_{_sufixos[e]}" for e in EXPERIMENTOS if e in _sufixos]
if dbutils.widgets.get("apagar_schemas_experimento") == "sim" and MODO != "planejar":
    for _s in _schemas_exp:
        spark.sql(f"DROP SCHEMA IF EXISTS {_s} CASCADE")
        print(f"apagado: {_s}")
else:
    print(f"Schemas de experimento mantidos: {_schemas_exp}. Para liberar espaço no catálogo, use "
          "apagar_schemas_experimento = sim ou o notebook 90_limpar_tabelas.")
avisar_limite_uc()