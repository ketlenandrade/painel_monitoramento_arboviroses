# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 14 – ETL DO CLIMA E JUNÇÃO COM OS CASOS (DENGUE / CHIKUNGUNYA / ZIKA)
# ==============================================================================
# Extrai dados de clima diários (temperatura, chuva e umidade), agrega por semana
# epidemiológica e cruza com os casos do pipeline, incluindo o clima das semanas
# ANTERIORES (defasagem). O Aedes aegypti leva semanas entre a chuva/calor, a
# eclosão dos ovos, a picada e o caso notificado; por isso a relação aparece
# com atraso de algumas semanas.
#
# FONTES DO CLIMA (widget fonte_clima):
#   automatica (padrão) – usa o Open-Meteo; se o limite gratuito acabar (por hora
#                 ou por dia), os municípios que faltam vêm da NASA POWER. Cada
#                 município usa UMA fonte só em todos os anos (coluna fonte_clima).
#   open_meteo  – só Open-Meteo (com esperar_limite = sim, espera o limite por
#                 hora liberar, até espera_maxima_min minutos no total)
#   nasa_power  – só NASA POWER
# NASA POWER (power.larc.nasa.gov): gratuita, sem chave, dados diários da
#   reanálise MERRA-2 / GEOS (grade de ~0,5°), atraso de poucos dias. É menos
#   fina que o ERA5 (~0,25°), mas suficiente para o clima semanal por município.
# No Databricks serverless o Open-Meteo gratuito costuma bater no limite logo:
# ele conta as chamadas por IP, e o IP de saída do serverless é compartilhado
# com outros usuários. Com uma chave paga do Open-Meteo, use openmeteo_apikey.
#
# Open-Meteo Historical Weather API (reanálise ERA5, ECMWF),
#   https://archive-api.open-meteo.com – gratuita para uso não comercial, sem
#   chave, cobertura de todo o Brasil desde 1940, atraso de ~5 dias.
#   Limite gratuito: 10.000 chamadas "ponderadas" por dia; 10 anos de 5 variáveis
#   de um local pesam ~140 chamadas. Por isso:
#     nivel_clima = capitais            -> 27 capitais (1 por UF), cabe num dia
#     nivel_clima = maiores_municipios  -> capitais + os N municípios mais
#                   populosos
#     nivel_clima = mais_afetados (padrão) -> capitais + os N municípios com
#                   maior incidência 2022-2026 de CADA doença (dengue,
#                   chikungunya e zika; só municípios com >= pop_minima hab.,
#                   para evitar taxas instáveis de cidades muito pequenas).
#                   É o que permite relacionar as ÁREAS MAIS AFETADAS com o clima.
#   Se o limite do dia acabar, o notebook para, guarda o que já baixou (cache
#   por local e por ano no Volume) e continua de onde parou na próxima execução.
#   Anos completos ficam no cache para sempre; o ano corrente é atualizado quando
#   o arquivo tem mais de 7 dias.
#
# Rode DEPOIS do 13_integracao_tabelas_painel (usa painel_uf_semana, painel_casos,
# populacao e dim_municipio_geo do schema workspace.arboviroses_br).
# Período: clima de out/2021 (defasagens) até hoje; casos de 2022 a 2026.
#
# Tabelas criadas em workspace.arboviroses_br:
#   clima_semanal_municipio   – clima por município x semana epidemiológica
#   clima_locais              – municípios com clima baixado e o motivo (capital / top doença)
#   clima_semanal_uf          – clima por UF x semana (capital ou média ponderada
#                               pela população dos municípios baixados)
#   clima_semanal_regiao      – clima por região x semana (média ponderada pela população)
#   clima_casos_uf_semana     – casos e incidência semanal por agravo x UF x semana,
#                               com o clima da mesma semana e de 1 a N semanas antes
#   clima_casos_municipio_semana – idem por município (só municípios com clima)
#   (a correlação clima x incidência por defasagem – clima_correlacao e
#    clima_melhor_defasagem – é calculada no notebook 23, junto com as análises)
#
# IMPORTANTE: correlação não é causalidade. Clima e casos têm a mesma sazonalidade
# (verão chuvoso e quente); a tabela mostra uma ASSOCIAÇÃO possível, não prova que
# o clima causou os casos. Ver a coluna rho_sem_sazonalidade, que remove a média
# de cada semana do ano antes de correlacionar.
# ==============================================================================

dbutils.widgets.dropdown("nivel_clima", "mais_afetados", ["capitais", "maiores_municipios", "mais_afetados"])
dbutils.widgets.text("n_municipios", "40")
dbutils.widgets.text("pop_minima", "20000")
# 2021-10-01: 12 semanas antes de 2022, para as defasagens do clima de 2022
dbutils.widgets.text("data_inicio", "2021-10-01")
dbutils.widgets.text("defasagem_maxima_semanas", "12")
dbutils.widgets.dropdown("baixar_clima", "sim", ["sim", "nao"])
dbutils.widgets.dropdown("fonte_clima", "automatica", ["automatica", "open_meteo", "nasa_power"])
dbutils.widgets.dropdown("esperar_limite", "nao", ["nao", "sim"])
dbutils.widgets.text("espera_maxima_min", "90")
dbutils.widgets.text("openmeteo_apikey", "")

import os, json, time, math
from datetime import date, datetime, timedelta
import requests
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

CATALOG = "workspace"
DESTINO = f"{CATALOG}.arboviroses_br"
NIVEL = dbutils.widgets.get("nivel_clima")
N_MUN = int(dbutils.widgets.get("n_municipios") or 40)
POP_MIN = int(dbutils.widgets.get("pop_minima") or 20000)
DATA_INICIO = dbutils.widgets.get("data_inicio").strip() or "2021-10-01"
DATA_FIM = (date.today() - timedelta(days=7)).isoformat()   # ERA5 tem ~5 dias de atraso
MAX_LAG = int(dbutils.widgets.get("defasagem_maxima_semanas") or 12)
BAIXAR = dbutils.widgets.get("baixar_clima") == "sim"
FONTE = dbutils.widgets.get("fonte_clima")
ESPERAR_LIMITE = dbutils.widgets.get("esperar_limite") == "sim"
ESPERA_MAX_S = 60 * float(dbutils.widgets.get("espera_maxima_min") or 90)
OM_APIKEY = dbutils.widgets.get("openmeteo_apikey").strip()

VARIAVEIS = {
    "temperature_2m_mean": "temp_media",
    "temperature_2m_max": "temp_max",
    "temperature_2m_min": "temp_min",
    "precipitation_sum": "chuva_mm",
    "relative_humidity_2m_mean": "umidade_media",
}
URL_CLIMA = ("https://customer-archive-api.open-meteo.com/v1/archive" if OM_APIKEY
             else "https://archive-api.open-meteo.com/v1/archive")
URL_NASA = "https://power.larc.nasa.gov/api/temporal/daily/point"
# variáveis da NASA POWER -> mesmos nomes do Open-Meteo (o resto do notebook não muda)
NASA_PARA_OM = {"T2M": "temperature_2m_mean", "T2M_MAX": "temperature_2m_max", "T2M_MIN": "temperature_2m_min",
                "PRECTOTCORR": "precipitation_sum", "RH2M": "relative_humidity_2m_mean"}
# URL_MUNICIPIOS vem de funcoes_comuns.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DESTINO}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DESTINO}.raw")
CACHE = f"/Volumes/{CATALOG}/arboviroses_br/raw/clima"
# um diretório por fonte (o do Open-Meteo é a pasta raiz, como nas versões anteriores)
DIR_FONTE = {"open_meteo": CACHE, "nasa_power": os.path.join(CACHE, "nasa_power")}
NOME_FONTE = {"open_meteo": "Open-Meteo (ERA5)", "nasa_power": "NASA POWER (MERRA-2)"}
for _d in DIR_FONTE.values():
    os.makedirs(_d, exist_ok=True)

print(f"Nível: {NIVEL} | período: {DATA_INICIO} a {DATA_FIM} | defasagem até {MAX_LAG} semanas | fonte: {FONTE}")

# COMMAND ----------

# DBTITLE 1,Locais: capitais ou maiores municípios
if spark.catalog.tableExists(f"{DESTINO}.dim_municipio_geo"):   # gravada pelo 13
    mun = spark.table(f"{DESTINO}.dim_municipio_geo").select(
        "municipio_codigo", "municipio_nome", "uf_sigla", "latitude", "longitude", "capital").toPandas()
else:
    mun = pd.read_csv(URL_MUNICIPIOS)
    mun = mun.rename(columns={"codigo_ibge": "codigo7", "nome": "municipio_nome"})
    mun["municipio_codigo"] = mun["codigo7"].astype(str).str[:6]
    mun["uf_sigla"] = mun["codigo_uf"].astype(str).map(UF_POR_CODIGO)

capitais = mun[mun["capital"] == 1]
motivo = {c: "capital" for c in capitais["municipio_codigo"]}
if NIVEL == "capitais":
    locais = capitais
elif NIVEL == "maiores_municipios":
    pop = (spark.table(f"{DESTINO}.populacao").filter("ano = 2024")
           .select("municipio_codigo", "populacao").toPandas())
    maiores = mun.merge(pop, on="municipio_codigo").nlargest(N_MUN, "populacao")
    for c in maiores["municipio_codigo"]:
        motivo.setdefault(c, "mais populoso")
    locais = pd.concat([capitais, maiores]).drop_duplicates("municipio_codigo")
else:  # mais_afetados: maior incidência acumulada 2022-2026 de cada doença
    afetados = spark.sql(f"""
        WITH casos AS (
          SELECT agravo, municipio_codigo, SUM(casos_provaveis) AS casos
          FROM {DESTINO}.painel_casos WHERE municipio_codigo IS NOT NULL
          GROUP BY agravo, municipio_codigo),
        pop AS (
          SELECT municipio_codigo, AVG(populacao) AS pop_media
          FROM {DESTINO}.populacao GROUP BY municipio_codigo),
        inc AS (
          SELECT c.agravo, c.municipio_codigo, c.casos, p.pop_media,
                 c.casos / p.pop_media * 100000 AS incidencia_acumulada
          FROM casos c JOIN pop p USING (municipio_codigo)
          WHERE p.pop_media >= {POP_MIN}),
        rk AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY agravo ORDER BY incidencia_acumulada DESC) AS pos
          FROM inc)
        SELECT agravo, municipio_codigo FROM rk WHERE pos <= {N_MUN}
    """).toPandas()
    for r in afetados.itertuples():
        motivo[r.municipio_codigo] = (motivo.get(r.municipio_codigo, "") + f" top {r.agravo}").strip()
    locais = pd.concat([capitais, mun[mun["municipio_codigo"].isin(afetados["municipio_codigo"])]]).drop_duplicates("municipio_codigo")
locais = locais[["municipio_codigo", "municipio_nome", "uf_sigla", "latitude", "longitude", "capital"]].reset_index(drop=True)
locais["motivo_selecao"] = locais["municipio_codigo"].map(motivo).fillna("")
spark.createDataFrame(locais).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_locais")
print(f"{len(locais)} locais ({int(locais['capital'].sum())} capitais)")

# COMMAND ----------

# DBTITLE 1,Baixar o clima diário (cache por ano, controle do limite e fonte de reserva)
from collections import deque


def periodos():
    """Um período por ano civil, de DATA_INICIO até DATA_FIM."""
    ini, fim = date.fromisoformat(DATA_INICIO), date.fromisoformat(DATA_FIM)
    for ano in range(ini.year, fim.year + 1):
        a, b = max(ini, date(ano, 1, 1)), min(fim, date(ano, 12, 31))
        yield ano, a.isoformat(), b.isoformat(), b == date(ano, 12, 31)


def arquivos_cache(cod, fonte="open_meteo"):
    """Arquivos de cache do local numa fonte (um por ano): (caminho, início, fim, existe)."""
    pasta = DIR_FONTE[fonte]
    saida = []
    for ano, a, b, completo in periodos():
        prefixo = f"{cod}_{ano}_{a}_"
        existentes = sorted(f for f in os.listdir(pasta) if f.startswith(prefixo) and f.endswith(".json"))
        if completo:
            alvo = os.path.join(pasta, f"{prefixo}{b}.json")
            saida.append((alvo, a, b, os.path.exists(alvo)))
        else:  # ano corrente: aceita o arquivo mais recente se tiver até 7 dias de atraso
            ok = [f for f in existentes
                  if date.fromisoformat(f[len(prefixo):-5]) >= date.fromisoformat(b) - timedelta(days=7)]
            caminho = os.path.join(pasta, ok[-1] if ok else f"{prefixo}{b}.json")
            saida.append((caminho, a, b, bool(ok)))
    return saida


def situacao(cod, fonte):
    """completo | so_ano_corrente (falta só atualizar o ano em curso) | parcial | vazio"""
    arqs = arquivos_cache(cod, fonte)
    falta = [x for x in arqs if not x[3]]
    if not falta:
        return "completo"
    if len(falta) == len(arqs):
        return "vazio"
    if len(falta) == 1 and falta[0] is arqs[-1]:
        return "so_ano_corrente"
    return "parcial"


def fonte_do_local(cod):
    """Fonte usada na leitura: a que tem o local completo (Open-Meteo primeiro);
    senão, a que tem mais anos. Cada local fica com uma fonte só."""
    sit = {f: situacao(cod, f) for f in DIR_FONTE}
    for f in ("open_meteo", "nasa_power"):
        if sit[f] == "completo":
            return f
    n = {f: sum(x[3] for x in arquivos_cache(cod, f)) for f in DIR_FONTE}
    melhor = max(n, key=lambda f: (n[f], f == "open_meteo"))
    return melhor if n[melhor] else None


def _salvar(caminho, conteudo):
    with open(caminho + ".tmp", "w") as f:
        f.write(conteudo)
    os.replace(caminho + ".tmp", caminho)
    # remove versões antigas do mesmo ano (ano corrente)
    pasta, nome = os.path.split(caminho)
    pref = nome.rsplit("_", 1)[0] + "_"
    for f in os.listdir(pasta):
        if f.startswith(pref) and f != nome and f.endswith(".json"):
            os.remove(os.path.join(pasta, f))


# ---------------- Open-Meteo: controle do limite gratuito -------------------
# Cada chamada "pesa" (variáveis / 10) x (dias / 14), no mínimo 1. Limites
# gratuitos: 600 por minuto, 5.000 por hora e 10.000 por dia. O notebook soma o
# peso das chamadas e espera antes de passar do limite (com folga de 10%).
LIMITES_OM = {60: 540, 3600: 4500, 86400: 9000}
_historico_om = deque()   # (instante, peso)
estado = {"om_bloqueado": None, "espera_total_s": 0.0, "chamadas_om": 0, "chamadas_nasa": 0}


def peso_om(inicio, fim):
    dias = (date.fromisoformat(fim) - date.fromisoformat(inicio)).days + 1
    return max(1.0, len(VARIAVEIS) / 10 * dias / 14)


def _esperar(segundos, motivo):
    if estado["espera_total_s"] + segundos > ESPERA_MAX_S:
        return False
    print(f"   {motivo}: aguardando {segundos / 60:.1f} min")
    time.sleep(segundos)
    estado["espera_total_s"] += segundos
    return True


def _respeitar_ritmo(peso):
    agora = time.time()
    while _historico_om and agora - _historico_om[0][0] > 86400:
        _historico_om.popleft()
    for janela, limite in LIMITES_OM.items():
        usados = [(t, w) for t, w in _historico_om if agora - t < janela]
        if sum(w for _, w in usados) + peso > limite:
            if janela == 86400:
                return "limite_diario"
            # espera até a chamada mais antiga da janela sair dela
            soma, liberar = sum(w for _, w in usados) + peso - limite, 0
            for t, w in usados:
                soma -= w
                if soma <= 0:
                    liberar = janela - (agora - t) + 1
                    break
            if janela == 60:
                time.sleep(max(1, liberar))
            elif not (ESPERAR_LIMITE and _esperar(liberar, "ritmo próximo do limite por hora")):
                return "limite_hora"
    return None


def baixar_open_meteo(row, caminho, inicio, fim):
    """'ok' | 'limite_hora' | 'limite_diario' | 'erro'"""
    peso = peso_om(inicio, fim)
    bloqueio = _respeitar_ritmo(peso)
    if bloqueio:
        return bloqueio
    params = {"latitude": row.latitude, "longitude": row.longitude, "start_date": inicio, "end_date": fim,
              "daily": ",".join(VARIAVEIS), "timezone": "America/Sao_Paulo"}
    if OM_APIKEY:
        params["apikey"] = OM_APIKEY
    for tentativa in range(4):
        try:
            r = requests.get(URL_CLIMA, params=params, timeout=120)
        except requests.RequestException as ex:
            print(f"   Open-Meteo sem resposta ({str(ex)[:80]}); nova tentativa")
            time.sleep(10 * (tentativa + 1))
            continue
        estado["chamadas_om"] += 1
        _historico_om.append((time.time(), peso))
        if r.status_code == 200:
            _salvar(caminho, r.text)
            return "ok"
        motivo = r.text[:160].lower()
        if r.status_code == 429 and "daily" in motivo:
            return "limite_diario"
        if r.status_code == 429 and "hourly" in motivo:
            # só libera na próxima hora: esperar 65 s não adianta
            agora = datetime.now()
            ate_proxima_hora = 3600 - (agora.minute * 60 + agora.second) + 60
            if ESPERAR_LIMITE and _esperar(ate_proxima_hora, "limite por hora do Open-Meteo"):
                continue
            return "limite_hora"
        if r.status_code == 429:   # limite por minuto
            time.sleep(65)
            continue
        print(f"   Open-Meteo HTTP {r.status_code} em {row.municipio_nome}: {r.text[:120]}")
        time.sleep(5 * (tentativa + 1))
    return "erro"


# ---------------- NASA POWER (reserva) -------------------------------------
def baixar_nasa(row, caminho, inicio, fim):
    """Grava no MESMO formato do Open-Meteo ({"daily": {...}}). 'ok' | 'erro'"""
    fim_pedido = fim
    for tentativa in range(5):
        params = {"parameters": ",".join(NASA_PARA_OM), "community": "AG",
                  "latitude": round(row.latitude, 4), "longitude": round(row.longitude, 4),
                  "start": inicio.replace("-", ""), "end": fim_pedido.replace("-", ""), "format": "JSON"}
        try:
            r = requests.get(URL_NASA, params=params, timeout=180)
        except requests.RequestException as ex:
            print(f"   NASA POWER sem resposta ({str(ex)[:80]}); nova tentativa")
            time.sleep(15 * (tentativa + 1))
            continue
        estado["chamadas_nasa"] += 1
        if r.status_code == 200:
            js = r.json()
            par = js["properties"]["parameter"]
            vazio = float(js.get("header", {}).get("fill_value", -999.0))
            dias = sorted(par[next(iter(NASA_PARA_OM))])
            daily = {"time": [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in dias]}
            for k_nasa, k_om in NASA_PARA_OM.items():
                daily[k_om] = [None if (v is None or float(v) <= vazio + 1e-6) else float(v)
                               for v in (par[k_nasa].get(d) for d in dias)]
            _salvar(caminho, json.dumps({"daily": daily, "fonte": "NASA POWER", "latitude": row.latitude,
                                         "longitude": row.longitude}))
            time.sleep(0.5)
            return "ok"
        if r.status_code == 422 and tentativa < 2:
            # data final além do que já foi publicado: pede até 30 dias antes
            fim_pedido = (date.fromisoformat(fim_pedido) - timedelta(days=30)).isoformat()
            if fim_pedido < inicio:
                return "erro"
            continue
        if r.status_code in (429, 503):
            time.sleep(60)
            continue
        print(f"   NASA POWER HTTP {r.status_code} em {row.municipio_nome}: {r.text[:120]}")
        time.sleep(5 * (tentativa + 1))
    return "erro"


BAIXAR_POR_FONTE = {"open_meteo": baixar_open_meteo, "nasa_power": baixar_nasa}


def baixar_local(row, fonte):
    """Baixa os anos que faltam do local na fonte escolhida."""
    status = "cache"
    for caminho, a, b, existe in arquivos_cache(row.municipio_codigo, fonte):
        if existe:
            continue
        st = BAIXAR_POR_FONTE[fonte](row, caminho, a, b)
        if st != "ok":
            return st
        status = "ok"
    return status


def escolher_fonte(cod):
    """Fonte para BAIXAR o que falta deste local, mantendo uma fonte por local."""
    if FONTE != "automatica":
        return FONTE
    om, nasa = situacao(cod, "open_meteo"), situacao(cod, "nasa_power")
    if om in ("completo", "so_ano_corrente") and not estado["om_bloqueado"]:
        return "open_meteo"
    if om == "so_ano_corrente" and estado["om_bloqueado"]:
        return None   # mantém o Open-Meteo; atualiza o ano corrente na próxima execução
    if nasa != "vazio":
        return "nasa_power"
    return "nasa_power" if estado["om_bloqueado"] else "open_meteo"


if BAIXAR:
    contagem = {}
    for n_local, row in enumerate(locais.itertuples(), 1):
        fonte = escolher_fonte(row.municipio_codigo)
        if fonte is None:
            contagem["mantido (atualiza depois)"] = contagem.get("mantido (atualiza depois)", 0) + 1
            continue
        if fonte == "open_meteo" and estado["om_bloqueado"]:
            contagem["pulado (limite Open-Meteo)"] = contagem.get("pulado (limite Open-Meteo)", 0) + 1
            continue
        st = baixar_local(row, fonte)
        if st in ("limite_hora", "limite_diario"):
            estado["om_bloqueado"] = st
            print(f"[{n_local}/{len(locais)}] Open-Meteo: {'limite por hora' if st == 'limite_hora' else 'limite diário'} "
                  "atingido." + (" Seguindo com a NASA POWER para os locais que faltam." if FONTE == "automatica"
                                 else " O que já foi baixado fica no cache; rode de novo mais tarde."))
            if FONTE == "automatica":
                st = baixar_local(row, "nasa_power") if situacao(row.municipio_codigo, "open_meteo") not in (
                    "completo", "so_ano_corrente") else "mantido (atualiza depois)"
                fonte = "nasa_power"
        chave = f"{st} ({NOME_FONTE[fonte]})" if st in ("ok", "cache", "erro") else st
        contagem[chave] = contagem.get(chave, 0) + 1
        if n_local % 25 == 0:
            print(f"[{n_local}/{len(locais)}] {contagem}")
    print("Download:", contagem)
    print(f"Chamadas: Open-Meteo {estado['chamadas_om']} | NASA POWER {estado['chamadas_nasa']} | "
          f"espera total {estado['espera_total_s'] / 60:.0f} min")

# COMMAND ----------

# DBTITLE 1,Clima semanal por município (semana epidemiológica)
linhas, fonte_por_local = [], {}
for row in locais.itertuples():
    fonte = fonte_do_local(row.municipio_codigo)
    if fonte is None:
        continue
    fonte_por_local[row.municipio_codigo] = NOME_FONTE[fonte]
    for caminho, _a, _b, _ in arquivos_cache(row.municipio_codigo, fonte):
        if not os.path.exists(caminho):
            continue
        d = json.load(open(caminho))["daily"]
        df = pd.DataFrame({VARIAVEIS[k]: pd.to_numeric(pd.Series(d[k]), errors="coerce") for k in VARIAVEIS})
        df["data"] = pd.to_datetime(d["time"]).date
        df["municipio_codigo"], df["municipio_nome"], df["uf_sigla"] = row.municipio_codigo, row.municipio_nome, row.uf_sigla
        df["fonte_clima"] = NOME_FONTE[fonte]
        linhas.append(df)
print("Locais por fonte:", pd.Series(fonte_por_local).value_counts().to_dict(),
      f"| sem clima: {len(locais) - len(fonte_por_local)}")
locais["fonte_clima"] = locais["municipio_codigo"].map(fonte_por_local)
spark.createDataFrame(locais).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_locais")
if not linhas:
    raise Exception("Nenhum dado de clima disponível. Rode com baixar_clima = sim.")
diario = pd.concat(linhas, ignore_index=True).drop_duplicates(["municipio_codigo", "data"])

ep = diario["data"].map(semana_epi)
diario["ano"] = ep.map(lambda x: x[0]); diario["semana"] = ep.map(lambda x: x[1]); diario["inicio_semana"] = ep.map(lambda x: x[2])
diario["dia_chuva"] = (diario["chuva_mm"] >= 1).astype(int)

semanal = (diario.groupby(["municipio_codigo", "municipio_nome", "uf_sigla", "fonte_clima", "ano", "semana",
                           "inicio_semana"], as_index=False)
           .agg(temp_media=("temp_media", "mean"), temp_max=("temp_max", "mean"), temp_min=("temp_min", "mean"),
                chuva_mm=("chuva_mm", "sum"), dias_chuva=("dia_chuva", "sum"),
                umidade_media=("umidade_media", "mean"),
                dias=("chuva_mm", "count"), dias_temp=("temp_media", "count")))
# só semanas completas: 7 dias com chuva e temperatura (dias sem dado ficam de fora)
semanal = semanal[(semanal["dias"] == 7) & (semanal["dias_temp"] == 7)].drop(columns=["dias", "dias_temp"])
for c in ["temp_media", "temp_max", "temp_min", "umidade_media", "chuva_mm"]:
    semanal[c] = semanal[c].round(2)

sdf_mun = spark.createDataFrame(semanal)
sdf_mun.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_semanal_municipio")
print(f"clima_semanal_municipio: {len(semanal):,} linhas, {semanal['municipio_codigo'].nunique()} municípios")

# COMMAND ----------

# DBTITLE 1,Clima semanal por UF
# Capitais: o clima da UF é o da capital. Com mais municípios: média ponderada
# pela população de cada município baixado (representa onde as pessoas vivem).
pop_mun = spark.table(f"{DESTINO}.populacao").filter("ano = 2024").select("municipio_codigo", F.col("populacao").alias("_peso"))
CLIMA_VARS = ["temp_media", "temp_max", "temp_min", "chuva_mm", "dias_chuva", "umidade_media"]
clima_uf = (
    sdf_mun.join(pop_mun, "municipio_codigo", "left").fillna({"_peso": 1})
    .groupBy("uf_sigla", "ano", "semana", "inicio_semana")
    .agg(*[F.round(F.sum(F.col(c) * F.col("_peso")) / F.sum("_peso"), 2).alias(c) for c in CLIMA_VARS],
         F.count("*").alias("municipios_com_clima"))
)
clima_uf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_semanal_uf")
print(f"clima_semanal_uf: {clima_uf.count():,} linhas")

# Clima por REGIÃO (média das UFs ponderada pela população): o "clima da região"
_pop_uf_peso = spark.table(f"{DESTINO}.populacao").filter("ano = 2024").groupBy("uf_sigla").agg(F.sum("populacao").alias("_peso"))
clima_regiao = (
    spark.table(f"{DESTINO}.clima_semanal_uf").withColumn("regiao", coluna_regiao("uf_sigla"))
    .join(_pop_uf_peso, "uf_sigla", "left").fillna({"_peso": 1})
    .groupBy("regiao", "ano", "semana", "inicio_semana")
    .agg(*[F.round(F.sum(F.col(c) * F.col("_peso")) / F.sum("_peso"), 2).alias(c) for c in CLIMA_VARS],
         F.count("*").alias("ufs_com_clima"))
)
clima_regiao.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_semanal_regiao")
print(f"clima_semanal_regiao: {clima_regiao.count():,} linhas")

# COMMAND ----------

# DBTITLE 1,Casos + clima da mesma semana e das semanas anteriores (defasagens)
LAG_VARS = CLIMA_LAG_VARS  # funcoes_comuns


def com_defasagens(casos, clima, chave):
    """Junta, para cada semana, o clima de 0 a MAX_LAG semanas antes."""
    out = casos
    for k in range(0, MAX_LAG + 1):
        c = clima.select(
            *chave,
            F.date_add("inicio_semana", 7 * k).alias("inicio_semana"),
            *[F.col(v).alias(f"{v}_lag{k}") for v in LAG_VARS],
        )
        out = out.join(c, chave + ["inicio_semana"], "left")
    return out


# Só as semanas do período dos casos (2022-2026): o clima de out-dez/2021 serve
# apenas para as defasagens e não pode virar "semanas com 0 casos".
_anos_casos = spark.table(f"{DESTINO}.painel_uf_semana").agg(F.min("ano"), F.max("ano")).first()
calendario = (spark.table(f"{DESTINO}.clima_semanal_uf").select("ano", "semana", "inicio_semana").distinct()
              .filter(F.col("ano").between(int(_anos_casos[0]), int(_anos_casos[1]))))

# UF
pop_uf = spark.table(f"{DESTINO}.populacao").groupBy("ano", "uf_sigla").agg(F.sum("populacao").alias("populacao"))
_casos_uf = (
    spark.table(f"{DESTINO}.painel_uf_semana").filter("uf_sigla <> 'IGN'")
    .groupBy("agravo", "uf_sigla", "ano", "semana")
    .agg(F.sum("casos_provaveis").alias("casos_provaveis"), F.sum("casos_confirmados").alias("casos_confirmados"))
)
# Grade completa agravo x UF x semana: semanas sem nenhum caso entram com 0
# (sem isso, as semanas "zeradas" sumiriam e a correlação ficaria enviesada).
_grade_uf = (
    spark.table(f"{DESTINO}.painel_uf_semana").filter("uf_sigla <> 'IGN'")
    .select("agravo", "regiao", "uf_sigla").distinct()
    .crossJoin(calendario)
)
casos_uf = (
    _grade_uf.join(_casos_uf, ["agravo", "uf_sigla", "ano", "semana"], "left")
    .fillna({"casos_provaveis": 0, "casos_confirmados": 0})
    .join(pop_uf, ["ano", "uf_sigla"], "left")
    .withColumn("incidencia_100mil", F.round(F.col("casos_provaveis") / F.col("populacao") * 100000, 3))
)
clima_casos_uf = com_defasagens(casos_uf, spark.table(f"{DESTINO}.clima_semanal_uf"), ["uf_sigla"])
clima_casos_uf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_casos_uf_semana")
print(f"clima_casos_uf_semana: {clima_casos_uf.count():,} linhas")

# Município (só os que têm clima)
cods = [r[0] for r in sdf_mun.select("municipio_codigo").distinct().collect()]
pop_m = spark.table(f"{DESTINO}.populacao").select("ano", "municipio_codigo", "populacao")
_casos_mun = (
    spark.table(f"{DESTINO}.painel_casos").filter(F.col("municipio_codigo").isin(cods))
    .groupBy("agravo", "municipio_codigo", "ano", "semana")
    .agg(F.sum("casos_provaveis").alias("casos_provaveis"), F.sum("casos_confirmados").alias("casos_confirmados"))
)
_grade_mun = (
    spark.table(f"{DESTINO}.painel_casos").select("agravo").distinct()
    .crossJoin(sdf_mun.select("municipio_codigo", "municipio_nome", "uf_sigla").distinct())
    .crossJoin(calendario)
)
casos_mun = (
    _grade_mun.join(_casos_mun, ["agravo", "municipio_codigo", "ano", "semana"], "left")
    .fillna({"casos_provaveis": 0, "casos_confirmados": 0})
    .join(pop_m, ["ano", "municipio_codigo"], "left")
    .withColumn("incidencia_100mil", F.round(F.col("casos_provaveis") / F.col("populacao") * 100000, 3))
)
clima_casos_mun = com_defasagens(casos_mun, spark.table(f"{DESTINO}.clima_semanal_municipio"), ["municipio_codigo"])
clima_casos_mun.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.clima_casos_municipio_semana")
print(f"clima_casos_municipio_semana: {clima_casos_mun.count():,} linhas")