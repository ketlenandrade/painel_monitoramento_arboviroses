# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./funcoes_comuns

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 30 – CRIA (OU ATUALIZA) O DASHBOARD DE MONITORAMENTO NO DATABRICKS
# ==============================================================================
# Monta, por código, um AI/BI Dashboard (Lakeview) com as tabelas dos notebooks
# 13, 14 e 23, e o cria na mesma pasta deste notebook. Se o dashboard já existir,
# ele é ATUALIZADO (mesmo link). Depois é publicado.
#
# Páginas:
#   1. Visão geral         – uma doença por vez: indicadores, curva epidêmica,
#                             mapa do Brasil por UF, mapas de calor
#                             (UF x semana e ano x semana),
#                             casos e incidência por UF, perfil, classificação
#   2. Áreas mais afetadas – mapa do Brasil por UF (choropleth) e mapa de calor
#                             por município,
#                             mapa de municípios, mapa de calor UF x ano,
#                             ranking de UFs e municípios,
#                             áreas persistentes, níveis de incidência e
#                             co-circulação das três doenças
#   3. Clima e casos       – casos x chuva x temperatura por semana, chuva de 4
#                             semanas antes x incidência, correlação por
#                             defasagem, perfil climático das áreas mais afetadas
#                             e clima mensal da região (só se o 14 foi rodado)
#   4. Comparação          – dengue, chikungunya e zika lado a lado (nunca somadas)
#   5. Monitoramento       – últimas semanas x o esperado por UF e principais achados
#
# Filtros: doença (valor único), ano, região, UF, período (semanas), classificação
# (confirmado / provável não confirmado / descartado) e recorte da correlação.
#
# Precisa de um SQL Warehouse (o serverless "Serverless Starter Warehouse" serve).
# Deixe o widget warehouse_id vazio para escolher sozinho.
# Uma cópia do JSON do dashboard (.lvdash.json) fica no Volume; dá para importá-lo
# à mão em Dashboards > Create > Import dashboard from file.
# ==============================================================================

dbutils.widgets.text("nome_dashboard", "Arboviroses Brasil 2022-2026")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("pasta_destino", "")
dbutils.widgets.dropdown("publicar", "sim", ["sim", "nao"])
dbutils.widgets.dropdown("incluir_mapa", "sim", ["sim", "nao"])
# Escala de cor dos mapas de calor e do mapa do Brasil.
#   amarelo_vermelho : gradiente próprio, do amarelo-claro ao vermelho-escuro (padrão)
#   magma, inferno, YlGnBu : esquemas prontos aceitos pelo Databricks
#   padrao           : escala azul do próprio Databricks
dbutils.widgets.dropdown("cor_mapa_calor", "amarelo_vermelho",
                         ["amarelo_vermelho", "magma", "inferno", "YlGnBu", "padrao"])
# Mapa do Brasil por UF (choropleth): casar as UFs pelo nome ("São Paulo") ou pelo
# código ISO ("BR-SP"). Se algum estado ficar cinza, troque para iso.
dbutils.widgets.dropdown("mapa_uf_por", "nome", ["nome", "iso"])
# País do mapa por UF, com o nome que o editor do Databricks usa no seu idioma.
dbutils.widgets.dropdown("pais_mapa", "Brasil", ["Brasil", "Brazil"])

import json, os, hashlib
from datetime import datetime

CATALOG = "workspace"
DESTINO = f"{CATALOG}.arboviroses_br"
NOME = dbutils.widgets.get("nome_dashboard").strip() or "Arboviroses Brasil 2022-2026"
INCLUIR_MAPA = dbutils.widgets.get("incluir_mapa") == "sim"
COR_CALOR = dbutils.widgets.get("cor_mapa_calor")
MAPA_UF_POR = dbutils.widgets.get("mapa_uf_por")
PAIS_MAPA = dbutils.widgets.get("pais_mapa")

# COMMAND ----------

# DBTITLE 1,Construtor do dashboard (JSON do Lakeview)
CORES = [{"value": k, "color": v} for k, v in CORES_AGRAVO.items()]  # cores fixas (funcoes_comuns)


def _id(*partes):
    return hashlib.md5("|".join(partes).encode()).hexdigest()[:8]


def campo(nome, expr=None):
    return {"name": nome, "expression": expr or f"`{nome}`"}


def soma(col):
    return {"name": f"sum({col})", "expression": f"SUM(`{col}`)"}


def media(col):
    return {"name": f"avg({col})", "expression": f"AVG(`{col}`)"}


def rampa(esquema):
    """colorRamp do Lakeview: gradiente próprio ou esquema pronto; None = padrão."""
    if not esquema or esquema == "padrao":
        return None
    if esquema == "amarelo_vermelho":
        return {"mode": "custom-sequential", "colors": {"start": "#FFF3B0", "end": "#99000D"}}
    return {"mode": "scheme", "scheme": esquema}


def enc(campo_, titulo, tipo=None, cores=False):
    e = {"fieldName": campo_["name"] if isinstance(campo_, dict) else campo_, "displayName": titulo}
    if tipo:
        e["scale"] = {"type": tipo}
        if cores:
            e["scale"]["mappings"] = CORES
    return e


class Dashboard:
    """Monta o serialized_dashboard do Lakeview: datasets, páginas, filtros e gráficos."""

    def __init__(self):
        self.datasets, self.pages, self.ds_cols = [], [], {}

    def dataset(self, nome, sql, colunas):
        self.datasets.append({"name": nome, "displayName": nome, "queryLines": [sql]})
        self.ds_cols[nome] = set(colunas)
        return nome

    def pagina(self, nome, titulo):
        p = {"name": _id("page", nome), "displayName": titulo, "pageType": "PAGE_TYPE_CANVAS", "layout": []}
        self.pages.append(p)
        self._p, self._y = p, 0
        return p

    # --- posição: grade de 6 colunas ------------------------------------------
    def linha(self, widgets, altura):
        x = 0
        for w, largura in widgets:
            self._p["layout"].append({"widget": w, "position": {"x": x, "y": self._y, "width": largura, "height": altura}})
            x += largura
        self._y += altura

    # --- widgets ----------------------------------------------------------------
    def texto(self, nome, linhas):
        # cada linha do markdown precisa terminar em "\n" (senão o Lakeview emenda as
        # linhas); título com "##" para caber na altura do bloco
        linhas = [("##" + l[1:] if l.startswith("# ") else l).rstrip("\n") + "\n\n" for l in linhas]
        return {"name": _id(self._p["name"], nome), "multilineTextboxSpec": {"lines": linhas}}

    def filtro(self, titulo, coluna, tipo="filter-multi-select", padrao=None, datasets=None):
        nome = _id(self._p["name"], "f", coluna, titulo)
        alvo = [d for d in (datasets or self._ds_pagina) if coluna in self.ds_cols[d]]
        queries, fields = [], []
        for d in alvo:
            qn = f"f_{d}_{coluna}"
            queries.append({"name": qn, "query": {"datasetName": d, "fields": [
                campo(coluna), campo(f"{coluna}_associativity", "COUNT_IF(`associative_filter_predicate_group`)")],
                "disaggregated": False}})
            fields.append({"fieldName": coluna, "displayName": coluna, "queryName": qn})
        spec = {"version": 2, "widgetType": tipo, "encodings": {"fields": fields},
                "frame": {"showTitle": True, "title": titulo}}
        if padrao is not None:
            spec["selection"] = {"defaultSelection": {"values": {"dataType": "STRING",
                                                                 "values": [{"value": padrao}]}}}
        return {"name": nome, "queries": queries, "spec": spec}

    def grafico(self, nome, tipo, ds, campos, encodings, titulo, versao=3, desagregado=False, extra=None):
        spec = {"version": versao, "widgetType": tipo, "encodings": encodings,
                "frame": {"showTitle": True, "title": titulo}}
        if extra:
            spec.update(extra)
        return {"name": _id(self._p["name"], nome),
                "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": campos,
                                                             "disaggregated": desagregado}}],
                "spec": spec}

    def contador(self, nome, ds, f, titulo):
        return self.grafico(nome, "counter", ds, [f], {"value": enc(f, titulo)}, titulo, versao=2)

    def barra(self, nome, ds, x, y, titulo, cor=None, x_tipo="categorical", cores_doenca=False, agrupado=False,
              ordenar_y=False):
        campos = [x, y] + ([cor] if cor else [])
        e = {"x": enc(x, x["name"], x_tipo), "y": enc(y, titulo, "quantitative")}
        if ordenar_y:
            e["x"]["scale"]["sort"] = {"by": "y-reversed"}
        if cor:
            e["color"] = enc(cor, cor["name"], "categorical", cores_doenca)
        return self.grafico(nome, "bar", ds, campos, e, titulo,
                            extra={"mark": {"layout": "group"}} if agrupado else None)

    def linha_g(self, nome, ds, x, y, titulo, cor=None, cores_doenca=False, x_tipo="temporal"):
        campos = [x, y] + ([cor] if cor else [])
        e = {"x": enc(x, x["name"], x_tipo), "y": enc(y, titulo, "quantitative")}
        if cor:
            e["color"] = enc(cor, cor["name"], "categorical", cores_doenca)
        return self.grafico(nome, "line", ds, campos, e, titulo)

    def pizza(self, nome, ds, angulo, cor, titulo):
        return self.grafico(nome, "pie", ds, [angulo, cor],
                            {"angle": enc(angulo, titulo, "quantitative"),
                             "color": enc(cor, cor["name"], "categorical")}, titulo)

    def dispersao(self, nome, ds, x, y, cor, titulo, rx, ry):
        return self.grafico(nome, "scatter", ds, [x, y, cor],
                            {"x": enc(x, rx, "quantitative"), "y": enc(y, ry, "quantitative"),
                             "color": enc(cor, cor["name"], "categorical")}, titulo, desagregado=True)

    def mapa(self, nome, ds, titulo):
        """Mapa de pontos (symbol-map, versão 2): tamanho = casos, cor = nível de incidência."""
        campos = [campo("latitude"), campo("longitude"), campo("casos_provaveis"), campo("nivel_incidencia")]
        e = {"coordinates": {"latitude": {"fieldName": "latitude"}, "longitude": {"fieldName": "longitude"}},
             "size": enc(campo("casos_provaveis"), "Casos prováveis", "quantitative"),
             "color": enc(campo("nivel_incidencia"), "Nível de incidência", "categorical")}
        return self.grafico(nome, "symbol-map", ds, campos, e, titulo, versao=2, desagregado=True,
                            extra={"mark": {"opacity": 0.7}})

    def mapa_calor(self, nome, ds, titulo, esquema=None):
        """Mapa de calor do Brasil: um ponto por município, cor = incidência (escala contínua).
        Usa o widget symbol-map (versão 2), que aceita cor quantitativa com colorRamp."""
        campos = [campo("latitude"), campo("longitude"), campo("incidencia_cor")]
        cor = enc(campo("incidencia_cor"), "Incidência por 100 mil (cor até 3.000)", "quantitative")
        if rampa(esquema):
            cor["scale"]["colorRamp"] = rampa(esquema)
        e = {"coordinates": {"latitude": {"fieldName": "latitude"}, "longitude": {"fieldName": "longitude"}},
             "color": cor}
        return self.grafico(nome, "symbol-map", ds, campos, e, titulo, versao=2, desagregado=True,
                            extra={"mark": {"opacity": 0.8}})

    def mapa_uf(self, nome, ds, valor, titulo, rv, esquema=None, por="nome"):
        """Mapa do Brasil com os estados preenchidos pela cor do valor (choropleth)."""
        col, papel = ("uf_nome", "admin1-name") if por == "nome" else ("uf_iso", "admin1-iso")
        cor = enc(valor, rv, "quantitative")
        if rampa(esquema):
            cor["scale"]["colorRamp"] = rampa(esquema)
        e = {"region": {"regionType": "mapbox-v4-admin",
                        # nome do país como o Databricks grava no editor (workspace em português: "Brasil")
                        "admin0": {"type": "value", "value": PAIS_MAPA, "geographicRole": "admin0-name"},
                        "admin1": {"fieldName": col, "type": "field", "geographicRole": papel}},
             "color": cor}
        return self.grafico(nome, "choropleth-map", ds, [campo(col), valor], e, titulo, versao=1)

    def calor(self, nome, ds, x, y, valor, titulo, rx, ry, rv, esquema=None):
        """Mapa de calor: x e y categóricos, cor = valor agregado (quantitativo)."""
        cor = enc(valor, rv, "quantitative")
        if rampa(esquema):
            cor["scale"]["colorRamp"] = rampa(esquema)
        e = {"x": enc(x, rx, "categorical"), "y": enc(y, ry, "categorical"), "color": cor}
        return self.grafico(nome, "heatmap", ds, [x, y, valor], e, titulo)

    def tabela(self, nome, ds, colunas, titulo):
        cols = []
        for i, (c, t) in enumerate(colunas):
            cols.append({"fieldName": c, "displayName": t, "title": t, "type": "string", "displayAs": "string",
                         "visible": True, "order": 100000 + i, "booleanValues": ["false", "true"],
                         "imageUrlTemplate": "{{ @ }}", "imageTitleTemplate": "{{ @ }}", "imageWidth": "",
                         "imageHeight": "", "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}",
                         "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": True, "allowSearch": False,
                         "alignContent": "left", "allowHTML": False, "highlightLinks": False,
                         "useMonospaceFont": False, "preserveWhitespace": False})
        return self.grafico(nome, "table", ds, [campo(c) for c, _ in colunas],
                            {"columns": cols}, titulo, versao=1, desagregado=True,
                            extra={"invisibleColumns": [], "allowHTMLByDefault": False, "itemsPerPage": 25,
                                   "paginationSize": "default", "condensed": True, "withRowNumber": True})

    def json(self):
        return {"datasets": self.datasets, "pages": self.pages,
                "uiSettings": {"theme": {"widgetHeaderAlignment": "LEFT"}}}


def construir(tabelas, incluir_mapa=True, cor_calor="amarelo_vermelho", mapa_uf_por="nome"):
    """tabelas: conjunto com os nomes das tabelas que existem em DESTINO."""
    d = Dashboard()
    T = lambda t: f"{DESTINO}.{t}"
    tem = lambda *ts: all(t in tabelas for t in ts)

    # ---------------- datasets ----------------
    MED = ["notificacoes", "casos_provaveis", "casos_confirmados", "obitos", "hospitalizados"]
    semanal = d.dataset("semanal", f"""SELECT agravo, ano, semana, inicio_semana, regiao, uf_sigla, uf_nome,
  grupo_classificacao, {', '.join(MED)}
FROM {T('painel_uf_semana')} WHERE uf_sigla <> 'IGN'""",
        ["agravo", "ano", "semana", "inicio_semana", "regiao", "uf_sigla", "uf_nome", "grupo_classificacao"] + MED)
    perfil = d.dataset("perfil", f"""SELECT agravo, ano, inicio_semana, regiao, uf_sigla, grupo_classificacao,
  sexo_nome, faixa_etaria, SUM(casos_provaveis) AS casos_provaveis, SUM(obitos) AS obitos
FROM {T('painel_casos')} GROUP BY ALL""",
        ["agravo", "ano", "inicio_semana", "regiao", "uf_sigla", "grupo_classificacao", "sexo_nome",
         "faixa_etaria", "casos_provaveis", "obitos"])
    # mapas de calor: casos e incidência por UF x semana, já calculada no SQL (uma linha
    # por doença x UF x semana), para o gráfico usar AVG(incidencia_semana) sem depender
    # de fórmula no widget. População da UF vem do insight_uf_ano (mesma do artigo).
    calor = d.dataset("calor_uf_semana", f"""SELECT s.agravo, s.ano, s.semana, s.inicio_semana, s.regiao, s.uf_sigla,
  CONCAT(CAST(s.ano AS STRING), '-', LPAD(CAST(s.semana AS STRING), 2, '0')) AS ano_semana,
  LPAD(CAST(s.semana AS STRING), 2, '0') AS semana_epi,
  CAST(s.ano AS STRING) AS ano_txt,
  SUM(s.casos_provaveis) AS casos_provaveis,
  MAX(p.populacao) AS populacao,
  ROUND(SUM(s.casos_provaveis) * 100000 / MAX(p.populacao), 2) AS incidencia_semana
FROM {T('painel_uf_semana')} s
LEFT JOIN {T('insight_uf_ano')} p ON s.agravo = p.agravo AND s.ano = p.ano AND s.uf_sigla = p.uf_sigla
WHERE s.uf_sigla <> 'IGN'
GROUP BY s.agravo, s.ano, s.semana, s.inicio_semana, s.regiao, s.uf_sigla""",
        ["agravo", "ano", "semana", "inicio_semana", "regiao", "uf_sigla", "ano_semana", "semana_epi", "ano_txt",
         "casos_provaveis", "populacao", "incidencia_semana"])
    uf_ano = d.dataset("uf_ano", f"SELECT *, CONCAT('BR-', uf_sigla) AS uf_iso FROM {T('insight_uf_ano')}",
                       ["uf_iso", "agravo", "ano", "regiao", "uf_sigla", "uf_nome", "casos_provaveis", "casos_confirmados",
                        "obitos", "populacao", "incidencia_100mil", "letalidade_pct", "nivel_incidencia",
                        "posicao_incidencia", "variacao_casos_pct_ano_anterior"])
    mun_ano = d.dataset("municipio_ano", f"""SELECT agravo, ano, regiao, uf_sigla, municipio_nome, populacao,
  casos_provaveis, casos_confirmados, obitos, incidencia_100mil, nivel_incidencia, posicao_brasil,
  entra_ranking, latitude, longitude,
  -- cor do mapa de calor: limitada a 3.000/100 mil para poucos municípios pequenos
  -- não apagarem a escala (o valor real aparece na dica do ponto)
  LEAST(incidencia_100mil, 3000) AS incidencia_cor
FROM {T('insight_municipio_ano')} WHERE casos_provaveis > 0""",
        ["agravo", "ano", "regiao", "uf_sigla", "municipio_nome", "populacao", "casos_provaveis",
         "casos_confirmados", "obitos", "incidencia_100mil", "nivel_incidencia", "posicao_brasil",
         "entra_ranking", "latitude", "longitude", "incidencia_cor"])
    top_mun = d.dataset("top_municipios", f"""SELECT agravo, ano, regiao, uf_sigla, municipio_nome, populacao,
  casos_provaveis, incidencia_100mil, nivel_incidencia
FROM {T('insight_municipio_ano')} WHERE entra_ranking ORDER BY incidencia_100mil DESC""",
        ["agravo", "ano", "regiao", "uf_sigla", "municipio_nome", "populacao", "casos_provaveis",
         "incidencia_100mil", "nivel_incidencia"])
    persist = d.dataset("areas_persistentes", f"""SELECT agravo, regiao, uf_sigla, municipio_nome,
  populacao_media, casos_provaveis, incidencia_media_anual_100mil, anos_em_alta, padrao_area
FROM {T('insight_municipio_periodo')} WHERE entra_ranking
ORDER BY anos_em_alta DESC, incidencia_media_anual_100mil DESC""",
        ["agravo", "regiao", "uf_sigla", "municipio_nome", "populacao_media", "casos_provaveis",
         "incidencia_media_anual_100mil", "anos_em_alta", "padrao_area"])
    cocirc = d.dataset("cocirculacao", f"""SELECT ano, regiao, uf_sigla, municipio_nome, combinacao,
  n_doencas_media_ou_alta, incidencia_dengue, incidencia_chikungunya, incidencia_zika
FROM {T('insight_cocirculacao')} WHERE n_doencas_media_ou_alta >= 2""",
        ["ano", "regiao", "uf_sigla", "municipio_nome", "combinacao", "n_doencas_media_ou_alta",
         "incidencia_dengue", "incidencia_chikungunya", "incidencia_zika"])
    saz = d.dataset("sazonalidade", f"SELECT * FROM {T('insight_sazonalidade')}",
                    ["agravo", "regiao", "ano", "casos_provaveis_ano", "semana_pico", "mes_pico",
                     "semana_inicio_temporada", "semana_fim_temporada", "duracao_temporada_semanas"])
    mon = d.dataset("monitoramento", f"""SELECT * FROM {T('insight_monitoramento_uf')}
ORDER BY razao_observado_esperado DESC""",
        ["agravo", "regiao", "uf_sigla", "janela_inicio", "janela_fim", "casos_janela", "esperado_janela",
         "razao_observado_esperado", "situacao"])
    resumo = d.dataset("achados", f"SELECT agravo, ordem, tema, achado FROM {T('insight_resumo')} ORDER BY ordem",
                       ["agravo", "ordem", "tema", "achado"])

    clima = tem("clima_casos_uf_semana", "clima_correlacao")
    if clima:
        cl_sem = d.dataset("clima_semanal", f"""SELECT agravo, regiao, uf_sigla, ano, semana, inicio_semana,
  casos_provaveis, incidencia_100mil, temp_media_lag0 AS temp_media, chuva_mm_lag0 AS chuva_mm,
  umidade_media_lag0 AS umidade_media, chuva_mm_lag4 AS chuva_4_semanas_antes,
  temp_media_lag4 AS temp_4_semanas_antes
FROM {T('clima_casos_uf_semana')}""",
            ["agravo", "regiao", "uf_sigla", "ano", "semana", "inicio_semana", "casos_provaveis",
             "incidencia_100mil", "temp_media", "chuva_mm", "umidade_media", "chuva_4_semanas_antes",
             "temp_4_semanas_antes"])
        corr = d.dataset("correlacao", f"""SELECT agravo, nivel, recorte, variavel_clima, defasagem_semanas, rho,
  rho_sem_sazonalidade, n_semanas FROM {T('clima_correlacao')}""",
            ["agravo", "nivel", "recorte", "variavel_clima", "defasagem_semanas", "rho",
             "rho_sem_sazonalidade", "n_semanas"])
    perfil_cl = (d.dataset("perfil_climatico", f"SELECT * FROM {T('insight_perfil_climatico')}",
                           ["agravo", "nivel", "quartil_incidencia", "grupo", "n_areas_ano",
                            "incidencia_media_100mil", "temp_media", "temp_min", "chuva_anual_mm", "umidade",
                            "semanas_favoraveis_aedes"])
                 if tem("insight_perfil_climatico") else None)
    cl_areas = (d.dataset("clima_areas_uf", f"SELECT * FROM {T('insight_clima_areas_uf')}",
                          ["agravo", "ano", "regiao", "uf_sigla", "uf_nome", "casos_provaveis", "incidencia_100mil",
                           "nivel_incidencia", "temp_media_anual", "chuva_total_mm", "umidade_media_anual",
                           "semanas_favoraveis_aedes", "quartil_incidencia"])
                if tem("insight_clima_areas_uf") else None)
    cl_reg = (d.dataset("clima_regiao_mes", f"""SELECT regiao, ano, mes_data, temp_media, chuva_mm, umidade_media,
  agravo, casos
FROM {T('insight_clima_regiao_mes')}
LATERAL VIEW STACK(3, 'Dengue', casos_dengue, 'Chikungunya', casos_chikungunya, 'Zika', casos_zika) s AS agravo, casos""",
                        ["regiao", "ano", "mes_data", "temp_media", "chuva_mm", "umidade_media", "agravo", "casos"])
              if tem("insight_clima_regiao_mes") else None)

    AVISO = ("Cada doença é mostrada separadamente (os números nunca são somados). Casos prováveis = notificações "
             "exceto descartadas (SINAN/OpenDataSUS, 2022-2026). O ano corrente é parcial: os arquivos abertos são "
             "publicados com algumas semanas de atraso em relação ao painel do Ministério da Saúde.")

    # ---------------- 1. Visão geral ----------------
    d.pagina("visao", "1. Visão geral")
    d._ds_pagina = [semanal, perfil, uf_ano, resumo, calor]
    d.linha([(d.texto("t", ["# Arboviroses no Brasil – doença selecionada", AVISO]), 6)], 2)
    d.linha([(d.filtro("Doença", "agravo", "filter-single-select", "Dengue"), 1),
             (d.filtro("Ano", "ano"), 1), (d.filtro("Região", "regiao"), 1), (d.filtro("UF", "uf_sigla"), 1),
             (d.filtro("Classificação", "grupo_classificacao"), 1),
             (d.filtro("Período (semanas)", "inicio_semana", "filter-date-range-picker"), 1)], 1)
    d.linha([(d.contador("c1", semanal, soma("casos_provaveis"), "Casos prováveis"), 1),
             (d.contador("c2", semanal, soma("casos_confirmados"), "Casos confirmados"), 1),
             (d.contador("c3", semanal, soma("obitos"), "Óbitos confirmados"), 1),
             (d.contador("c4", semanal, soma("hospitalizados"), "Hospitalizados"), 1),
             (d.contador("c5", uf_ano, {"name": "incidencia", "expression":
                                        "SUM(`casos_provaveis`) * 100000 / SUM(`populacao`)"},
                         "Incidência por 100 mil hab. (ano/UF filtrados)"), 1),
             # o arquivo aberto do OpenDataSUS é publicado com atraso em relação ao
             # painel do Ministério: mostra até que semana o ano selecionado tem dados
             (d.contador("c6", semanal, {"name": "ultima_se", "expression": "MAX(`semana`)"},
                         "Última semana epidemiológica com dados"), 1)], 2)
    d.linha([(d.linha_g("curva", semanal, campo("inicio_semana"), soma("casos_provaveis"),
                        "Curva epidêmica – casos prováveis por semana", cor=campo("grupo_classificacao")), 6)], 6)
    d.linha([(d.mapa_uf("mapa_br_uf", uf_ano, media("incidencia_100mil"),
                        "Mapa do Brasil – incidência por UF (casos por 100 mil hab.)", "Incidência por 100 mil",
                        cor_calor, mapa_uf_por), 3),
             (d.barra("incuf", uf_ano, campo("uf_sigla"), media("incidencia_100mil"),
                      "Incidência por 100 mil hab. por UF", cor=campo("nivel_incidencia"), ordenar_y=True), 3)], 9)
    d.linha([(d.barra("uf", semanal, campo("uf_sigla"), soma("casos_provaveis"), "Casos prováveis por UF",
                      cor=campo("grupo_classificacao"), ordenar_y=True), 6)], 6)
    # incidência da célula = incidencia_semana (já calculada no SQL; uma linha por célula)
    d.linha([(d.calor("calor_uf", calor, campo("ano_semana"), campo("uf_sigla"),
                      media("incidencia_semana"),
                      "Mapa de calor – incidência semanal por UF (casos por 100 mil hab.)",
                      "Ano-semana epidemiológica", "UF", "Incidência por 100 mil", cor_calor), 6)], 9)
    d.linha([(d.calor("calor_ano", calor, campo("semana_epi"), campo("ano_txt"), soma("casos_provaveis"),
                      "Mapa de calor – sazonalidade: casos prováveis por ano e semana epidemiológica",
                      "Semana epidemiológica", "Ano", "Casos prováveis", cor_calor), 6)], 5)
    d.linha([(d.barra("ano", semanal, campo("ano"), soma("casos_provaveis"), "Casos prováveis por ano",
                      cor=campo("grupo_classificacao")), 2),
             (d.barra("idade", perfil, campo("faixa_etaria"), soma("casos_provaveis"),
                      "Perfil por faixa etária e sexo", cor=campo("sexo_nome")), 2),
             (d.pizza("classif", semanal, soma("notificacoes"), campo("grupo_classificacao"),
                      "Notificações por classificação"), 2)], 6)

    # ---------------- 2. Áreas mais afetadas ----------------
    d.pagina("areas", "2. Áreas mais afetadas")
    d._ds_pagina = [mun_ano, top_mun, uf_ano, persist, cocirc]
    d.linha([(d.texto("t", ["# Áreas mais afetadas – doença selecionada",
                            "Níveis de incidência anual (Ministério da Saúde): baixa < 100, média 100-299 e "
                            "alta >= 300 casos por 100 mil hab. Rankings só com municípios de 20 mil hab. ou mais."]),
               6)], 2)
    d.linha([(d.filtro("Doença", "agravo", "filter-single-select", "Dengue"), 2), (d.filtro("Ano", "ano"), 1),
             (d.filtro("Região", "regiao"), 1), (d.filtro("UF", "uf_sigla"), 2)], 1)
    ln_br = [(d.mapa_uf("mapa_br_uf2", uf_ano, media("incidencia_100mil"),
                        "Mapa do Brasil – incidência por UF (casos por 100 mil hab.)", "Incidência por 100 mil",
                        cor_calor, mapa_uf_por), 3 if incluir_mapa else 6)]
    if incluir_mapa:
        ln_br.append((d.mapa_calor("mapa_calor_br", mun_ano,
                                   "Mapa de calor – incidência por município (selecione um ano)", cor_calor), 3))
    d.linha(ln_br, 10)
    linha_mapa = [(d.barra("rkuf", uf_ano, campo("uf_sigla"), media("incidencia_100mil"),
                           "Ranking de UFs – incidência por 100 mil", cor=campo("regiao"), ordenar_y=True),
                   3 if incluir_mapa else 6)]
    if incluir_mapa:
        linha_mapa.insert(0, (d.mapa("mapa", mun_ano, "Mapa – municípios por casos e nível de incidência"), 3))
    d.linha(linha_mapa, 8)
    d.linha([(d.tabela("top", top_mun, [("ano", "Ano"), ("uf_sigla", "UF"), ("municipio_nome", "Município"),
                                         ("populacao", "População"), ("casos_provaveis", "Casos prováveis"),
                                         ("incidencia_100mil", "Incidência/100 mil"),
                                         ("nivel_incidencia", "Nível")],
                       "Municípios com maior incidência"), 3),
             (d.tabela("pers", persist, [("uf_sigla", "UF"), ("municipio_nome", "Município"),
                                          ("anos_em_alta", "Anos em alta"),
                                          ("incidencia_media_anual_100mil", "Incidência média anual"),
                                          ("casos_provaveis", "Casos 2022-2026"), ("padrao_area", "Padrão")],
                       "Áreas persistentemente afetadas (2022-2026)"), 3)], 7)
    d.linha([(d.calor("calor_ufano", uf_ano, campo("ano"), campo("uf_sigla"), media("incidencia_100mil"),
                      "Mapa de calor – incidência anual por UF (casos por 100 mil hab.)",
                      "Ano", "UF", "Incidência por 100 mil", cor_calor), 6)], 8)
    d.linha([(d.barra("niv", mun_ano, campo("ano"), {"name": "municipios", "expression": "COUNT(*)"},
                      "Municípios com casos, por nível de incidência", cor=campo("nivel_incidencia")), 3),
             (d.barra("coc", cocirc, campo("ano"), {"name": "municipios", "expression": "COUNT(*)"},
                      "Co-circulação: municípios com 2+ doenças em incidência média/alta",
                      cor=campo("combinacao")), 3)], 6)

    # ---------------- 3. Clima e casos ----------------
    if clima:
        d.pagina("clima", "3. Clima e casos")
        d._ds_pagina = [cl_sem, corr] + [x for x in (perfil_cl, cl_areas, cl_reg) if x]
        d.linha([(d.texto("t", ["# Clima e casos – doença selecionada",
                                "Clima: Open-Meteo (reanálise ERA5) ou, quando o limite gratuito acaba, NASA POWER (MERRA-2) – a fonte de cada município está em clima_locais. A correlação mostra ASSOCIAÇÃO, não causa: "
                                "clima e casos têm a mesma sazonalidade. Veja rho_sem_sazonalidade. "
                                "Semana favorável ao Aedes: temperatura média 22-32 °C e chuva >= 10 mm."]), 6)], 2)
        d.linha([(d.filtro("Doença", "agravo", "filter-single-select", "Dengue"), 1), (d.filtro("Ano", "ano"), 1),
                 (d.filtro("Região", "regiao"), 1), (d.filtro("UF", "uf_sigla"), 1),
                 (d.filtro("Período", "inicio_semana", "filter-date-range-picker"), 1),
                 (d.filtro("Recorte da correlação", "recorte", "filter-single-select", "Brasil", [corr]), 1)], 1)
        d.linha([(d.linha_g("cs", cl_sem, campo("inicio_semana"), soma("casos_provaveis"),
                            "Casos prováveis por semana"), 2),
                 (d.barra("ch", cl_sem, campo("inicio_semana"), media("chuva_mm"), "Chuva semanal (mm, média das UFs)",
                          x_tipo="temporal"), 2),
                 (d.linha_g("tp", cl_sem, campo("inicio_semana"), media("temp_media"),
                            "Temperatura média semanal (°C)"), 2)], 6)
        d.linha([(d.dispersao("sc", cl_sem, campo("chuva_4_semanas_antes"), campo("incidencia_100mil"),
                              campo("regiao"), "Chuva de 4 semanas antes x incidência semanal (UF)",
                              "Chuva 4 semanas antes (mm)", "Incidência por 100 mil"), 3),
                 (d.linha_g("corr", corr, campo("defasagem_semanas"), media("rho"),
                            "Correlação clima x incidência por defasagem (semanas)", cor=campo("variavel_clima"),
                            x_tipo="quantitative"), 3)], 7)
        if perfil_cl or cl_areas:
            ln = []
            if perfil_cl:
                ln.append((d.barra("pq", perfil_cl, campo("grupo"), media("semanas_favoraveis_aedes"),
                                   "Semanas favoráveis ao Aedes por quartil de incidência", cor=campo("nivel"),
                                   agrupado=True), 2))
                ln.append((d.tabela("pqt", perfil_cl, [("nivel", "Nível"), ("grupo", "Quartil"),
                                                       ("incidencia_media_100mil", "Incidência média"),
                                                       ("temp_media", "Temp. média °C"),
                                                       ("chuva_anual_mm", "Chuva anual mm"),
                                                       ("umidade", "Umidade %"),
                                                       ("semanas_favoraveis_aedes", "Semanas favoráveis")],
                                    "Perfil climático das áreas: mais afetadas (Q4) x menos afetadas (Q1)"), 2))
            if cl_areas:
                ln.append((d.dispersao("sa", cl_areas, campo("chuva_total_mm"), campo("incidencia_100mil"),
                                       campo("regiao"), "UFs: chuva anual x incidência anual",
                                       "Chuva anual (mm)", "Incidência por 100 mil"), 6 - 2 * len(ln)))
            d.linha(ln, 7)
        if cl_reg:
            d.linha([(d.linha_g("rc", cl_reg, campo("mes_data"), soma("casos"), "Casos por mês na região",
                                cor=campo("regiao")), 2),
                     (d.linha_g("rch", cl_reg, campo("mes_data"), media("chuva_mm"), "Chuva mensal da região (mm)",
                                cor=campo("regiao")), 2),
                     (d.linha_g("rt", cl_reg, campo("mes_data"), media("temp_media"),
                                "Temperatura média mensal da região (°C)", cor=campo("regiao")), 2)], 6)

    # ---------------- 4. Comparação entre doenças ----------------
    d.pagina("comp", "4. Comparação entre doenças")
    d._ds_pagina = [semanal, uf_ano, saz]
    d.linha([(d.texto("t", ["# Dengue, chikungunya e zika – lado a lado",
                            "Nenhum gráfico soma as doenças: cada uma tem a sua cor. Compare pela incidência – a "
                            "dengue tem muito mais casos que as outras."]), 6)], 2)
    d.linha([(d.filtro("Ano", "ano"), 2), (d.filtro("Região", "regiao"), 2), (d.filtro("UF", "uf_sigla"), 2)], 1)
    d.linha([(d.linha_g("cv", semanal, campo("inicio_semana"), soma("casos_provaveis"),
                        "Curvas epidêmicas – dengue, chikungunya e zika", cor=campo("agravo"), cores_doenca=True), 6)], 6)
    d.linha([(d.barra("iu", uf_ano, campo("uf_sigla"), media("incidencia_100mil"),
                      "Incidência por UF e doença", cor=campo("agravo"), cores_doenca=True, agrupado=True), 4),
             (d.barra("let", uf_ano, campo("ano"), {"name": "letalidade", "expression":
                                                    "SUM(`obitos`) * 100 / SUM(`casos_confirmados`)"},
                      "Letalidade (%) por ano", cor=campo("agravo"), cores_doenca=True, agrupado=True), 2)], 6)
    d.linha([(d.tabela("saz", saz, [("agravo", "Doença"), ("regiao", "Região"), ("ano", "Ano"),
                                     ("semana_pico", "Semana do pico"), ("mes_pico", "Mês do pico"),
                                     ("semana_inicio_temporada", "Início temporada"),
                                     ("semana_fim_temporada", "Fim temporada"),
                                     ("duracao_temporada_semanas", "Duração (sem.)")],
                       "Sazonalidade por região"), 4),
             (d.barra("pico", saz, campo("regiao"), media("semana_pico"), "Semana epidemiológica média do pico",
                      cor=campo("agravo"), cores_doenca=True, agrupado=True), 2)], 7)

    # ---------------- 5. Monitoramento ----------------
    d.pagina("mon", "5. Monitoramento")
    d._ds_pagina = [mon, resumo]
    d.linha([(d.texto("t", ["# Situação atual por UF",
                            "Últimas semanas consolidadas x média das mesmas semanas nos anos anteriores "
                            "(as 2 semanas mais recentes ficam de fora: ainda recebem notificações)."]), 6)], 2)
    d.linha([(d.filtro("Doença", "agravo", "filter-single-select", "Dengue"), 3), (d.filtro("Região", "regiao"), 3)], 1)
    d.linha([(d.contador("acima", mon, {"name": "ufs_acima", "expression":
                                        "COUNT_IF(`situacao` LIKE '1.%' OR `situacao` LIKE '2.%')"},
                         "UFs acima do esperado"), 2),
             (d.contador("casosj", mon, soma("casos_janela"), "Casos nas últimas semanas"), 2),
             (d.contador("espj", mon, soma("esperado_janela"), "Esperado (média anos anteriores)"), 2)], 2)
    d.linha([(d.barra("razao", mon, campo("uf_sigla"), media("razao_observado_esperado"),
                      "Observado / esperado por UF", cor=campo("situacao"), ordenar_y=True), 3),
             (d.tabela("mt", mon, [("uf_sigla", "UF"), ("janela_inicio", "De"), ("janela_fim", "Até"),
                                    ("casos_janela", "Casos"), ("esperado_janela", "Esperado"),
                                    ("razao_observado_esperado", "Razão"), ("situacao", "Situação")],
                       "Situação por UF"), 3)], 7)
    d.linha([(d.tabela("ach", resumo, [("tema", "Tema"), ("achado", "Achado")],
                       "Principais achados (doença selecionada)"), 6)], 8)
    return d.json()

# COMMAND ----------

# DBTITLE 1,Criar ou atualizar o dashboard no workspace
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
tabelas = {r.tableName for r in spark.sql(f"SHOW TABLES IN {DESTINO}").collect()}
faltam = [t for t in ["painel_uf_semana", "painel_casos", "insight_uf_ano", "insight_municipio_ano",
                      "insight_resumo", "populacao"] if t not in tabelas]
if faltam:
    raise Exception(f"Faltam tabelas em {DESTINO}: {faltam}. Rode antes os notebooks 13 e 23.")
if "clima_casos_uf_semana" not in tabelas:
    print("[AVISO] Sem as tabelas de clima (notebook 14): a página 'Clima e casos' não será criada.")

serial = construir(tabelas, INCLUIR_MAPA, COR_CALOR, MAPA_UF_POR)

# SQL Warehouse: o do widget ou o primeiro serverless (senão, o primeiro da lista)
wh = dbutils.widgets.get("warehouse_id").strip()
if not wh:
    lista = w.api_client.do("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    if not lista:
        raise Exception("Nenhum SQL Warehouse encontrado. Crie um (SQL > SQL Warehouses) ou informe warehouse_id.")
    lista.sort(key=lambda x: (not x.get("enable_serverless_compute", False), x.get("state") != "RUNNING"))
    wh = lista[0]["id"]
    print(f"SQL Warehouse: {lista[0].get('name')} ({wh})")

pasta = dbutils.widgets.get("pasta_destino").strip() or os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
caminho = f"{pasta}/{NOME}.lvdash.json"

dash_id = None
try:
    st = w.api_client.do("GET", "/api/2.0/workspace/get-status", query={"path": caminho})
    dash_id = st.get("resource_id")
except Exception:
    pass

corpo = {"display_name": NOME, "warehouse_id": wh, "serialized_dashboard": json.dumps(serial, ensure_ascii=False)}
if dash_id:
    r = w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dash_id}", body=corpo)
    print(f"Dashboard ATUALIZADO: {NOME}")
else:
    r = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body={**corpo, "parent_path": pasta})
    dash_id = r["dashboard_id"]
    print(f"Dashboard CRIADO: {NOME}")

if dbutils.widgets.get("publicar") == "sim":
    w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dash_id}/published",
                    body={"warehouse_id": wh, "embed_credentials": True})
    print("Dashboard publicado.")

host = w.config.host.rstrip("/")
print(f"Abrir para editar: {host}/sql/dashboardsv3/{dash_id}")
print(f"Versão publicada:  {host}/dashboardsv3/{dash_id}/published")

# Cópia do JSON no Volume, para importar à mão se preferir
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DESTINO}.raw")
copia = f"/Volumes/{CATALOG}/arboviroses_br/raw/dashboard/{NOME}_{datetime.now():%Y%m%d_%H%M}.lvdash.json"
os.makedirs(os.path.dirname(copia), exist_ok=True)
with open(copia, "w", encoding="utf-8") as f:
    json.dump(serial, f, ensure_ascii=False, indent=2)
print(f"Cópia do JSON: {copia}")
dbutils.notebook.exit(json.dumps({"dashboard_id": dash_id, "url": f"{host}/sql/dashboardsv3/{dash_id}"}))