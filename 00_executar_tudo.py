# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 00 – EXECUTAR TUDO: DOWNLOAD -> ETL -> INTEGRAÇÃO -> CLIMA -> ANÁLISES -> DASHBOARD
# ==============================================================================
# Um clique ("Run all") faz o processo inteiro, em ordem.
#
# PIPELINE (extrair e tratar)
#   12  grava a população do IBGE (10, uma vez), baixa do OpenDataSUS os arquivos
#       anuais de DENGUE, CHIKUNGUNYA e ZIKA de 2022 a 2026 (só o que mudou) e
#       roda o pipeline de cada agravo (11: Bronze -> Silver -> Gold)
#   13  integra as três bases (tabelas do painel e base integrada lado a lado)
#   14  baixa o clima (Open-Meteo) das capitais e dos municípios mais afetados
#       e junta com os casos (defasagens)
# ANÁLISES
#   20  para cada agravo: vigilância (21 – canal endêmico, alertas, nowcasting,
#       atraso, completude) e resultados do TCC (22 – visualizações e CSV)
#   23  áreas mais afetadas, sazonalidade, co-circulação, perfil climático,
#       correlação clima x casos, comparação entre os agravos e achados
# DASHBOARD
#   30  cria ou atualiza o dashboard de monitoramento e o publica
#
# Notebooks carregados com %run (não precisam ser rodados): funcoes_comuns,
# config_agravo e base_analitica_agravo.
# Eficiência (24 e 25) fica fora deste encadeamento: rode à parte.
#
# criar_job = sim  -> cria (ou atualiza) um Job do Databricks que roda este
#                     mesmo encadeamento toda segunda-feira às 6h (horário de
#                     Brasília), para o painel se atualizar sozinho.
# ==============================================================================

dbutils.widgets.text("ano_inicio", "2022")
# Primeiro arquivo anual baixado. Os anos antes de ano_inicio entram só como HISTÓRICO
# do canal endêmico e dos fatores de completude do nowcast (os resultados, o painel e
# as tabelas continuam de ano_inicio a ano_fim). Com 2016, o canal tem de 6 a 10 anos
# de histórico e as regras de alerta são avaliadas de 2022 a 2025.
dbutils.widgets.text("ano_inicio_historico", "2016")
dbutils.widgets.text("ano_fim", "2026")
dbutils.widgets.dropdown("forcar_reprocessamento", "nao", ["nao", "sim"])
dbutils.widgets.dropdown("rodar_clima", "sim", ["sim", "nao"])
dbutils.widgets.dropdown("nivel_clima", "mais_afetados", ["capitais", "maiores_municipios", "mais_afetados"])
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.dropdown("criar_dashboard", "sim", ["sim", "nao"])
dbutils.widgets.dropdown("criar_job", "nao", ["nao", "sim"])

import os, json, time
from datetime import datetime

INI = dbutils.widgets.get("ano_inicio").strip() or "2022"
HIST = dbutils.widgets.get("ano_inicio_historico").strip() or INI
FIM = dbutils.widgets.get("ano_fim").strip() or "2026"
PASTA = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
TIMEOUT = 10 * 3600

ETAPAS = [
    ("12_rodar_dengue_chikungunya_zika", {"ano_inicio_fonte": HIST, "ano_fim_fonte": FIM,
                                          "ano_inicio_analise": INI,
                                          "forcar_reprocessamento": dbutils.widgets.get("forcar_reprocessamento")}),
    ("13_integracao_tabelas_painel", {"ano_inicio": INI, "ano_fim": FIM}),
    ("14_clima_etl", {"nivel_clima": dbutils.widgets.get("nivel_clima"), "baixar_clima": "sim"}),
    ("20_rodar_analises_por_agravo", {"ano_inicio_analise": INI, "ano_inicio_fonte": HIST, "ano_fim_fonte": FIM}),
    ("23_areas_afetadas_e_clima", {"ano_inicio": INI, "ano_fim": FIM}),
    ("30_criar_dashboard", {"warehouse_id": dbutils.widgets.get("warehouse_id").strip()}),
]
if dbutils.widgets.get("criar_dashboard") == "nao":   # mantém edições manuais do dashboard
    ETAPAS = [e for e in ETAPAS if e[0] != "30_criar_dashboard"]
if dbutils.widgets.get("rodar_clima") == "nao":
    ETAPAS = [e for e in ETAPAS if e[0] != "14_clima_etl"]

# COMMAND ----------

# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Espaço no catálogo (limite de 500 tabelas e views)
# A primeira execução completa cria cerca de 200 tabelas/views (3 doenças +
# painel + clima + análises). As seguintes só substituem as que já existem.
# Tabelas que já existem são só substituídas (não ocupam espaço novo): conta
# apenas o que o projeto ainda vai criar.
_inv = contar_objetos_uc()
_schemas_projeto = ({f"{a}_br" for a in NOMES_AGRAVO} | {f"{a}_raw" for a in NOMES_AGRAVO}
                    | {"arboviroses_br", "arboviroses_metricas"})
_do_projeto = int(_inv["schema"].isin(_schemas_projeto).sum())
_previstos = max(0, 200 - _do_projeto)
print(f"[CATÁLOGO] objetos do projeto já existentes: {_do_projeto}; ainda a criar: ~{_previstos}")
if not avisar_limite_uc(_previstos):
    raise Exception("Não há espaço no limite de 500 tabelas/views do Unity Catalog para a primeira execução. "
                    "Rode o notebook 90_limpar_tabelas (modo listar e depois apagar) e tente de novo.")

# COMMAND ----------

# DBTITLE 1,Rodar as etapas em ordem
resultado = []
for nb, args in ETAPAS:
    t0 = time.perf_counter()
    inicio_ms = int(time.time() * 1000) - 5000
    print(f"[{datetime.now():%H:%M:%S}] {nb} …")
    try:
        saida = dbutils.notebook.run(f"{PASTA}/{nb}", TIMEOUT, args)
        resultado.append((nb, "ok", (saida or "")[:300], round(time.perf_counter() - t0)))
        print(f"    ok em {time.perf_counter() - t0:,.0f} s {saida or ''}")
    except Exception as ex:
        msg = erro_detalhado(ex, inicio_ms)
        resultado.append((nb, "erro", msg[:300], round(time.perf_counter() - t0)))
        print(f"    ERRO: {msg}")
        if nb == "14_clima_etl":   # sem clima o resto ainda funciona
            print("    Seguindo sem o clima (a página de clima do dashboard fica de fora).")
            continue
        display(spark.createDataFrame(resultado, "etapa string, status string, detalhe string, segundos long"))
        raise
display(spark.createDataFrame(resultado, "etapa string, status string, detalhe string, segundos long"))

# COMMAND ----------

# DBTITLE 1,(Opcional) Job semanal para atualizar tudo sozinho
if dbutils.widgets.get("criar_job") == "sim":
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    nome_job = "Arboviroses Brasil – atualização semanal"
    tarefas, anterior = [], None
    for nb, args in ETAPAS:
        t = {"task_key": nb[:45],
             "notebook_task": {"notebook_path": f"{PASTA}/{nb}", "base_parameters": args},
             "timeout_seconds": TIMEOUT}
        if anterior:
            t["depends_on"] = [{"task_key": anterior}]
            if nb == "20_rodar_analises_por_agravo":   # roda mesmo se o clima falhar
                t["run_if"] = "ALL_DONE"
        tarefas.append(t)
        anterior = t["task_key"]
    config = {"name": nome_job, "tasks": tarefas, "max_concurrent_runs": 1,
              "schedule": {"quartz_cron_expression": "0 0 6 ? * MON", "timezone_id": "America/Sao_Paulo",
                           "pause_status": "UNPAUSED"}}
    existentes = [j for j in w.api_client.do("GET", "/api/2.1/jobs/list", query={"name": nome_job}).get("jobs", [])]
    if existentes:
        jid = existentes[0]["job_id"]
        w.api_client.do("POST", "/api/2.1/jobs/reset", body={"job_id": jid, "new_settings": config})
        print(f"Job atualizado: {nome_job} (id {jid})")
    else:
        jid = w.api_client.do("POST", "/api/2.1/jobs/create", body=config)["job_id"]
        print(f"Job criado: {nome_job} (id {jid}) – toda segunda às 6h")
    print(f"{w.config.host.rstrip('/')}/jobs/{jid}")