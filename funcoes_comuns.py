# Databricks notebook source
# DBTITLE 1,Funções e tabelas de referência usadas por todos os notebooks
# ==============================================================================
# FUNÇÕES COMUNS – carregado pelos outros notebooks com  %run ./funcoes_comuns
# ==============================================================================
# Um só lugar para o que antes estava repetido em vários notebooks:
#   * estados (sigla, código IBGE, nome) e regiões
#   * semana epidemiológica (regra do SINAN), em Python e em Spark
#   * nomes e cores fixas das doenças
#   * variáveis de clima usadas nas defasagens
#   * caminho de outro notebook e mensagem de erro de uma execução filha
# Não define widgets nem grava tabelas: pode ser carregado em qualquer ordem.
# ==============================================================================

import os
from datetime import date, timedelta

# ---------------- estados e regiões ----------------
# sigla -> (código IBGE, nome)
UFS = {
    "RO": (11, "Rondônia"), "AC": (12, "Acre"), "AM": (13, "Amazonas"),
    "RR": (14, "Roraima"), "PA": (15, "Pará"), "AP": (16, "Amapá"),
    "TO": (17, "Tocantins"), "MA": (21, "Maranhão"), "PI": (22, "Piauí"),
    "CE": (23, "Ceará"), "RN": (24, "Rio Grande do Norte"), "PB": (25, "Paraíba"),
    "PE": (26, "Pernambuco"), "AL": (27, "Alagoas"), "SE": (28, "Sergipe"),
    "BA": (29, "Bahia"), "MG": (31, "Minas Gerais"), "ES": (32, "Espírito Santo"),
    "RJ": (33, "Rio de Janeiro"), "SP": (35, "São Paulo"), "PR": (41, "Paraná"),
    "SC": (42, "Santa Catarina"), "RS": (43, "Rio Grande do Sul"),
    "MS": (50, "Mato Grosso do Sul"), "MT": (51, "Mato Grosso"),
    "GO": (52, "Goiás"), "DF": (53, "Distrito Federal"),
}
# a região sai do 1º dígito do código IBGE da UF
_REGIAO_POR_DIGITO = {"1": "Norte", "2": "Nordeste", "3": "Sudeste", "4": "Sul", "5": "Centro-Oeste"}
REGIAO_POR_UF = {s: _REGIAO_POR_DIGITO[str(c)[0]] for s, (c, _) in UFS.items()}
UF_POR_CODIGO = {str(c): s for s, (c, _) in UFS.items()}          # "31" -> "MG"
# linhas (uf_codigo, uf_sigla, uf_nome, regiao) para montar a dimensão de UF
UFS_LINHAS = [(str(c), s, n, REGIAO_POR_UF[s]) for s, (c, n) in UFS.items()]


def dim_uf_df():
    """Dimensão de UF em Spark: uf_codigo, uf_sigla, uf_nome, regiao."""
    return spark.createDataFrame(UFS_LINHAS, "uf_codigo string, uf_sigla string, uf_nome string, regiao string")


def coluna_regiao(col_uf_sigla):
    """Expressão Spark que devolve a região a partir da sigla da UF."""
    from pyspark.sql import functions as F
    return F.create_map(*[x for k, v in REGIAO_POR_UF.items() for x in (F.lit(k), F.lit(v))])[F.col(col_uf_sigla)]


# ---------------- doenças ----------------
NOMES_AGRAVO = {"dengue": "Dengue", "chikungunya": "Chikungunya", "zika": "Zika"}
CORES_AGRAVO = {"Dengue": "#D62728", "Chikungunya": "#FF7F0E", "Zika": "#9467BD"}

# ---------------- clima ----------------
URL_MUNICIPIOS = ("https://raw.githubusercontent.com/kelvins/municipios-brasileiros/"
                  "503e2f70bbf1b4b7ec0b1f68b09086ccc38fe861/csv/municipios.csv")
CLIMA_LAG_VARS = ["temp_media", "temp_min", "chuva_mm", "umidade_media"]


# ---------------- semana epidemiológica (regra do SINAN) ----------------
# A semana vai de domingo a sábado; a semana 1 é a que contém 4 de janeiro
# (tem pelo menos 4 dias em janeiro).
def inicio_ano_epi(ano):
    """Domingo em que começa a semana 1 do ano epidemiológico."""
    d = date(ano, 1, 4)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def semana_epi(d):
    """Devolve (ano_epi, semana_epi, inicio_da_semana) de uma data."""
    for ano in (d.year + 1, d.year, d.year - 1):
        ini = inicio_ano_epi(ano)
        if d >= ini:
            sem = (d - ini).days // 7 + 1
            return ano, sem, ini + timedelta(days=7 * (sem - 1))


def inicio_semana_epi_col(col_ano, col_semana):
    """Mesma regra em Spark: data (domingo) de início da semana epidemiológica."""
    from pyspark.sql import functions as F
    jan4 = F.make_date(F.col(col_ano).cast("int"), F.lit(1), F.lit(4))
    ini_ano = F.date_sub(jan4, F.dayofweek(jan4) - 1)
    return F.when(F.col(col_semana).isNotNull(),
                  F.date_add(ini_ano, (F.col(col_semana).cast("int") - 1) * 7))


assert semana_epi(date(2024, 1, 1))[:2] == (2024, 1)
assert semana_epi(date(2022, 1, 1))[:2] == (2021, 52)
assert semana_epi(date(2024, 12, 31))[:2] == (2025, 1)
assert semana_epi(date(2020, 12, 31))[:2] == (2020, 53)


# ---------------- execução de notebooks ----------------
def caminho_absoluto(caminho):
    """Caminho absoluto de outro notebook, relativo à pasta do notebook atual."""
    if caminho.startswith("/"):
        return caminho
    atual = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    return os.path.normpath(os.path.join(os.path.dirname(atual), caminho))


def motivo_do_erro(ex):
    """Trecho útil da mensagem de erro de um dbutils.notebook.run."""
    texto = str(ex)
    k = texto.rfind("FAILED:")
    return (texto[k:k + 1500] if k >= 0 else texto[-1500:]).strip()


ORQUESTRADORES = ("00_executar_tudo", "12_rodar_dengue_chikungunya_zika", "20_rodar_analises_por_agravo")


def erro_detalhado(ex, desde_ms=None):
    """O dbutils.notebook.run só devolve "Workload failed, see run output". Aqui
    buscamos, na API de Jobs, a execução filha que falhou (10, 11, 13, 14, 21...)
    a partir de desde_ms e devolvemos a mensagem real e o fim do traceback."""
    base = motivo_do_erro(ex)
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import RunType
        w = WorkspaceClient()
        for r in w.jobs.list_runs(run_type=RunType.WORKFLOW_RUN, limit=25, expand_tasks=True):
            if desde_ms and (r.start_time or 0) < desde_ms:
                continue
            res = getattr(getattr(r.state, "result_state", None), "value", None) if r.state else None
            if res not in ("FAILED", "TIMEDOUT", "INTERNAL_ERROR"):
                continue
            nome = r.run_name or ""
            if any(o in nome for o in ORQUESTRADORES):
                continue
            tid = r.tasks[0].run_id if r.tasks else r.run_id
            out = w.jobs.get_run_output(tid)
            trace = [l for l in (out.error_trace or "").splitlines() if l.strip()]
            return (f"{base}\n--- ERRO REAL em {nome} (run {r.run_id}) ---\n{out.error or ''}\n"
                    + "\n".join(trace[-30:]) + f"\nDetalhes: {r.run_page_url or ''}")
    except Exception as e2:
        return f"{base}\n(não foi possível buscar o erro do notebook filho: {str(e2)[:200]})"
    return base


# ---------------- limite do Unity Catalog (plano gratuito: 500 tabelas + views) -
LIMITE_OBJETOS_UC = 500


def contar_objetos_uc(detalhado=False):
    """Tabelas e views do metastore (sem system, samples e information_schema),
    como pandas: catalogo, schema, tabela, tipo."""
    import pandas as _pd
    consultas = [
        "SELECT table_catalog, table_schema, table_name, table_type FROM system.information_schema.tables",
        "SELECT table_catalog, table_schema, table_name, table_type FROM workspace.information_schema.tables",
    ]
    for q in consultas:
        try:
            df = spark.sql(q + " WHERE table_schema <> 'information_schema' "
                               "AND table_catalog NOT IN ('system', 'samples', '__databricks_internal')").toPandas()
            break
        except Exception:
            df = None
    if df is None:
        return _pd.DataFrame(columns=["catalogo", "schema", "tabela", "tipo"])
    df.columns = ["catalogo", "schema", "tabela", "tipo"]
    df["tipo"] = df["tipo"].astype(str).str.upper().map(lambda t: "VIEW" if "VIEW" in t else "TABLE")
    return df


def avisar_limite_uc(novos_previstos=0):
    """Mostra quantos objetos existem; devolve False se não couberem os novos."""
    n = len(contar_objetos_uc())
    livre = LIMITE_OBJETOS_UC - n
    print(f"[CATÁLOGO] {n} de {LIMITE_OBJETOS_UC} tabelas/views em uso ({livre} livres).")
    if livre < max(novos_previstos, 20):
        print("[CATÁLOGO][AVISO] Pouco espaço no limite do Unity Catalog. Rode o notebook "
              "90_limpar_tabelas (modo listar e depois apagar) para liberar objetos.")
    return livre >= novos_previstos
