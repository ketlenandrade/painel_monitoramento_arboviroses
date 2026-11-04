# Databricks notebook source
# MAGIC %run ./funcoes_comuns

# COMMAND ----------
# DBTITLE 1,Sobre este notebook
# ==============================================================================
# CONFIGURAÇÃO DE UM AGRAVO (dengue, chikungunya ou zika)
# ==============================================================================
# Carregado com  %run ./config_agravo  pelos notebooks que trabalham com UM agravo:
#   10_populacao_ibge, 11_pipeline_agravo, 21_vigilancia_agravo,
#   22_resultados_tcc_agravo e 25_benchmark_granularidade.
# Define os widgets, os nomes de schemas e tabelas, as constantes das análises,
# a identificação da execução (RUN_ID) e a tabela de métricas de tempo.
# Não baixa nem grava dados (só cria schemas/volumes, se não existirem).
#
# Widgets que importam em cada notebook:
#   pipeline (11)  : agravo, forcar_reprocessamento, modo_execucao,
#                    ano_inicio_fonte, ano_fim_fonte, url_fonte, sufixo_schema...
#   vigilância (21): agravo, ano_inicio_analise, semanas_consecutivas,
#                    anos_excluidos_canal, data_disponibilidade,
#                    remover_duplicados_sinan, excluir_chikungunya
#   resultados (22): agravo, rotulo_execucao, exportar_csv
# ==============================================================================

# COMMAND ----------
# DBTITLE 1,Parâmetros (widgets) do agravo
# ==============================================================================
# WIDGET: FORÇAR REPROCESSAMENTO
# ==============================================================================
# Por padrão, o pipeline só reprocessa Bronze/Silver/Gold quando detecta que
# a fonte oficial do SINAN mudou (ver seção 9, mais adiante). Se nada mudou,
# o notebook chama dbutils.notebook.exit('NO_UPDATE') e todas as células
# seguintes aparecem como "Command skipped" — é o comportamento esperado,
# não um erro.
#
# Para forçar uma atualização completa mesmo sem mudança na fonte (ex.: para
# testar), mude este widget para "sim" antes de rodar o notebook.
# ==============================================================================

dbutils.widgets.dropdown("forcar_reprocessamento", "nao", ["sim", "nao"])

# ------------------------------------------------------------------------------
# Widgets para as RODADAS DE ANÁLISE do artigo (não precisam de reprocessar
# Bronze/Silver):
#   modo_execucao          : "completo" (padrão) ou "somente_gold". No modo
#                            somente_gold o pipeline NÃO consulta nem baixa a
#                            fonte e NÃO regrava Bronze/Silver: lê a Silver já
#                            pronta e recalcula só a Gold (rápido).
#   semanas_consecutivas   : 1, 2 ou 3 semanas seguidas acima do limite para
#                            disparar o alerta (análise de sensibilidade).
#   anos_excluidos_canal   : anos epidêmicos a tirar do histórico do canal,
#                            separados por vírgula (ex.: "2019,2024"). Vazio =
#                            nenhum.
#   rotulo_execucao        : nome da pasta onde os resultados serão exportados
#                            em CSV (ex.: "base", "cons1", "sem_epidemicos").
# ------------------------------------------------------------------------------
dbutils.widgets.dropdown("modo_execucao", "completo", ["completo", "somente_gold"])
dbutils.widgets.dropdown("semanas_consecutivas", "2", ["1", "2", "3"])
dbutils.widgets.text("anos_excluidos_canal", "")
dbutils.widgets.text("rotulo_execucao", "base")
# Primeiro ano que entra nos RESULTADOS (tabelas Gold, avaliação, artigo).
# Os anos anteriores (a partir de START_YEAR) são carregados só como HISTÓRICO
# de referência do canal endêmico. Padrão 2022 = período prometido no projeto.
dbutils.widgets.text("ano_inicio_analise", "2022")
# Data que define quando um caso "passa a existir" para a vigilância nas
# análises em tempo real e no nowcasting: DT_DIGITA (data de digitação no
# SINAN, mais fiel à disponibilidade do dado) ou DT_NOTIFIC.
dbutils.widgets.dropdown("data_disponibilidade", "DT_DIGITA", ["DT_DIGITA", "DT_NOTIFIC"])
# Remover (sim) ou manter (nao) os registros que o próprio SINAN marca como
# duplicados (NDUPLIC_N = 1). Padrão "nao": mantém a mesma regra do painel
# oficial, o que facilita a validação externa.
dbutils.widgets.dropdown("remover_duplicados_sinan", "nao", ["nao", "sim"])
dbutils.widgets.dropdown("exportar_microdados", "nao", ["nao", "sim"])
# Regra de caso provável. "nao" = igual ao painel oficial do Ministério da Saúde
# (notificados, exceto descartados, CLASSI_FIN 5). "sim" = exclui também os
# classificados como chikungunya (CLASSI_FIN 13).
dbutils.widgets.dropdown("excluir_chikungunya", "nao", ["nao", "sim"])

# ------------------------------------------------------------------------------
# Widgets novos (versão 10)
#   agravo                 : dengue, chikungunya ou zika. Cada agravo tem o seu
#                            schema: dengue_br, chikungunya_br, zika_br.
#   (não há widget de UF: o pipeline processa sempre o Brasil inteiro)
#   ano_inicio_fonte       : primeiro arquivo anual baixado (padrão 2022).
#   ano_fim_fonte          : último arquivo anual baixado (padrão 2026).
#                            Usados também no experimento de escalabilidade.
#   sufixo_schema          : vazio = schema de produção. Use "exp", "esc"... para
#                            rodar experimentos sem tocar nas tabelas do artigo.
#   usar_cache_ibge        : "sim" (padrão) reaproveita a dimensão de população já
#                            gravada, se ela cobrir todos os anos, sem chamar a API
#                            do IBGE (que costuma ficar fora do ar). Use "nao" só
#                            quando quiser baixar estimativas novas do IBGE.
#   simular_ano_alterado   : ex. "2026". Faz o pipeline agir como se só esse ano
#                            tivesse mudado na fonte (atualização incremental),
#                            sem precisar esperar a publicação do OpenDataSUS.
#   benchmark_granularidade: "sim" mede, ao final, o tempo de agregar os dados
#                            por UF/município x ano/semana/dia.
#   repeticoes_benchmark   : quantas vezes repetir cada nível do benchmark.
#   exportar_csv           : "nao" nas repetições de tempo (não sobrescreve os
#                            CSV do artigo nem gasta tempo exportando).
#   experimento, configuracao, repeticao, aquecimento: etiquetas gravadas junto
#                            com cada métrica de tempo (preenchidas pelo
#                            notebook 24_eficiencia_da_solucao).
# ------------------------------------------------------------------------------
dbutils.widgets.dropdown("agravo", "dengue", ["dengue", "chikungunya", "zika"])
dbutils.widgets.text("ano_inicio_fonte", "2016")  # 2016-2021 = só histórico do canal endêmico e do nowcast
dbutils.widgets.text("ano_fim_fonte", "2026")
# Link de UM arquivo anual da base (opcional; só se o Ministério mudar o endereço).
# do recurso "Zika - 2024" em dadosabertos.saude.gov.br; o pipeline troca o ano
# sozinho. Vazio = usa o endereço padrão do OpenDataSUS para o agravo.
dbutils.widgets.text("url_fonte", "")
dbutils.widgets.text("sufixo_schema", "")
dbutils.widgets.dropdown("usar_cache_ibge", "sim", ["sim", "nao"])
dbutils.widgets.text("simular_ano_alterado", "")
dbutils.widgets.dropdown("benchmark_granularidade", "nao", ["nao", "sim"])
dbutils.widgets.text("repeticoes_benchmark", "10")
dbutils.widgets.dropdown("exportar_csv", "sim", ["sim", "nao"])
# views vw_painel_* do painel antigo: o dashboard novo não as usa e elas ocupam
# o limite de 500 tabelas/views do Unity Catalog. Desligadas por padrão.
dbutils.widgets.dropdown("criar_views_painel", "nao", ["nao", "sim"])
dbutils.widgets.text("experimento", "")
dbutils.widgets.text("configuracao", "")
dbutils.widgets.text("repeticao", "")
dbutils.widgets.dropdown("aquecimento", "nao", ["nao", "sim"])
# Teste de auditabilidade: injeta uma falha na fonte de UM ano para
# provar que o pipeline rejeita a entrada ruim e mantém a versão anterior intacta.
# Só é aceito com sufixo_schema preenchido (nunca no schema de produção).
dbutils.widgets.dropdown(
    "simular_falha", "",
    ["", "rede", "zip_truncado", "zip_invalido", "csv_vazio", "download_interrompido"],
)
dbutils.widgets.text("simular_falha_ano", "2026")

FORCAR_REPROCESSAMENTO = dbutils.widgets.get("forcar_reprocessamento") == "sim"
SOMENTE_GOLD = dbutils.widgets.get("modo_execucao") == "somente_gold"
ANO_INICIO_ANALISE = int(dbutils.widgets.get("ano_inicio_analise").strip() or 2022)
DATA_DISPONIBILIDADE = dbutils.widgets.get("data_disponibilidade")
REMOVER_DUPLICADOS_SINAN = dbutils.widgets.get("remover_duplicados_sinan") == "sim"
EXPORTAR_MICRODADOS = dbutils.widgets.get("exportar_microdados") == "sim"
EXCLUIR_CHIKUNGUNYA = dbutils.widgets.get("excluir_chikungunya") == "sim"
ROTULO_EXECUCAO = (
    "".join(c for c in dbutils.widgets.get("rotulo_execucao").strip() if c.isalnum() or c in "-_")
    or "base"
)

# ------------------------------------------------------------------------------
# Agravo: pasta e prefixo do arquivo no OpenDataSUS e codificação de CLASSI_FIN.
#   URL: https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/<pasta>/csv/
#        <prefixo>BR<aa>.csv.zip
# Dengue e chikungunya são notificadas no Sinan Online (mesma ficha): 5 =
# descartado, 8 = inconclusivo, 10/11/12 = dengue, 13 = chikungunya. O
# dicionário publicado para chikungunya também lista 1 = confirmado e
# 2 = descartado; os dois esquemas são aceitos. Zika: 1 = confirmado,
# 2 = descartado, 8 = inconclusivo.
# CONFIRA a tabela gold_<agravo>_distribuicao_classificacao depois da primeira
# rodada: ela mostra quais códigos aparecem de fato em cada arquivo anual.
# ------------------------------------------------------------------------------
AGRAVOS = {
    "dengue": {
        "nome": "Dengue", "pasta": "Dengue", "prefixo": "DENG",
        "confirmado": (10, 11, 12), "descartado": (5,),
        "nao_provavel": (5, 13) if EXCLUIR_CHIKUNGUNYA else (5,),
        "dominio": (5, 8, 10, 11, 12, 13),
        "rotulos": {5: "Descartado", 8: "Inconclusivo", 10: "Dengue",
                    11: "Dengue com sinais de alarme", 12: "Dengue grave",
                    13: "Chikungunya"},
    },
    "chikungunya": {
        "nome": "Chikungunya", "pasta": "Chikungunya", "prefixo": "CHIK",
        "confirmado": (13, 1), "descartado": (5, 2),
        # classificado como dengue (10/11/12) também não é chikungunya provável
        "nao_provavel": (5, 2, 10, 11, 12),
        "dominio": (1, 2, 5, 8, 10, 11, 12, 13),
        "rotulos": {1: "Chikungunya", 2: "Descartado", 5: "Descartado",
                    8: "Inconclusivo", 10: "Dengue",
                    11: "Dengue com sinais de alarme", 12: "Dengue grave",
                    13: "Chikungunya"},
    },
    "zika": {
        "nome": "Zika", "pasta": "Zikavirus", "prefixo": "ZIKA",
        "confirmado": (1,), "descartado": (2,),
        "nao_provavel": (2,),
        "dominio": (1, 2, 8),
        "rotulos": {1: "Zika", 2: "Descartado", 8: "Inconclusivo"},
    },
}
AGRAVO = dbutils.widgets.get("agravo")
CFG_AGRAVO = AGRAVOS[AGRAVO]
PREFIXO_ARQUIVO = CFG_AGRAVO["prefixo"]
CODIGOS_NAO_PROVAVEIS = CFG_AGRAVO["nao_provavel"]

# COMMAND ----------
# DBTITLE 1,1b. Estados e escopo
# UFS (sigla -> (código IBGE, nome)) e UF_POR_CODIGO vêm do notebook funcoes_comuns.

# Escopo: BRASIL INTEIRO (sem filtro de estado). Incidência nacional, por UF e
# por município (5.570 municípios). Código IBGE do Brasil = 1 (nível N1).
UF_SIGLA = "BR"
ESCOPO_NACIONAL = True
UF_CODIGO_IBGE, UF_NOME = 1, "Brasil"
# Códigos de UF aceitos no município de residência: todas as 27 UFs
PREFIXOS_UF = list(UF_POR_CODIGO)
SIGLAS_UF = list(UFS)

ANO_INICIO_FONTE = int(dbutils.widgets.get("ano_inicio_fonte").strip() or 2022)
ANO_FIM_FONTE = int(dbutils.widgets.get("ano_fim_fonte").strip() or 2026)
SUFIXO_SCHEMA = "".join(
    c for c in dbutils.widgets.get("sufixo_schema").strip().lower() if c.isalnum() or c == "_"
)
USAR_CACHE_IBGE = dbutils.widgets.get("usar_cache_ibge") == "sim"
_simular = dbutils.widgets.get("simular_ano_alterado").strip()
SIMULAR_ANO_ALTERADO = int(_simular) if _simular.isdigit() else None
BENCHMARK_GRANULARIDADE = dbutils.widgets.get("benchmark_granularidade") == "sim"
REPETICOES_BENCHMARK = max(1, int(dbutils.widgets.get("repeticoes_benchmark").strip() or 10))
EXPORTAR_CSV = dbutils.widgets.get("exportar_csv") == "sim"
CRIAR_VIEWS_PAINEL = dbutils.widgets.get("criar_views_painel") == "sim"
EXPERIMENTO = dbutils.widgets.get("experimento").strip()
CONFIGURACAO = dbutils.widgets.get("configuracao").strip()
_rep = dbutils.widgets.get("repeticao").strip()
REPETICAO = int(_rep) if _rep.lstrip("-").isdigit() else None
AQUECIMENTO = dbutils.widgets.get("aquecimento") == "sim"
SIMULAR_FALHA = dbutils.widgets.get("simular_falha").strip()
_ano_falha = dbutils.widgets.get("simular_falha_ano").strip()
SIMULAR_FALHA_ANO = int(_ano_falha) if _ano_falha.isdigit() else None
if SIMULAR_FALHA and not SUFIXO_SCHEMA:
    raise ValueError("simular_falha só pode ser usado com sufixo_schema (ex.: 'aud').")

# Versão do NOTEBOOK (registrada nas métricas). A versão da lógica da Silver é
# PIPELINE_VERSION, na seção 2: ela não mudou para dengue, então a Silver
# existente continua válida e não é reprocessada.
NOTEBOOK_VERSION = "11"

# COMMAND ----------
# DBTITLE 1,1. IMPORTAÇÕES
# ==============================================================================
# 1. IMPORTAÇÕES
# ==============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType,
    LongType,
    DoubleType,
    StringType
)

from datetime import datetime, timezone

import requests
import os
import hashlib
import zipfile
import shutil
import time

# Modo ANSI desligado nesta sessão. Com ele ligado (padrão em computação
# serverless), divisão por zero e conversões inválidas LANÇAM ERRO em vez de
# retornar NULL. As tabelas Gold têm dezenas de taxas (letalidade, incidência,
# % de hospitalização...) cujo denominador pode ser 0 em algum ano/sexo/município
# da série ampliada; o comportamento desejado é NULL, não interromper o pipeline.
try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    print("[CONFIG] spark.sql.ansi.enabled = false (divisão por zero -> NULL)")
except Exception as e:
    print(f"[CONFIG][WARN] Não foi possível desligar o modo ANSI: {e}")

# COMMAND ----------
# DBTITLE 1,2. CONFIGURAÇÕES GLOBAIS
# ==============================================================================
# 2. CONFIGURAÇÕES GLOBAIS
# ==============================================================================

# Período baixado: ano_inicio_fonte..ano_fim_fonte (padrão 2022-2026).
# O canal endêmico precisa de pelo menos MIN_ANOS_HISTORICO anos anteriores
# para cada ano-alvo: com 2022-2026 ele é calculado para 2025 e 2026. Para ter
# canal em todos os anos, rode com ano_inicio_fonte = 2016.
# Se alguma URL de ano antigo não existir, o pipeline registra "INDISPONIVEL_*"
# e segue com os anos disponíveis (ver seção 9).
START_YEAR = ANO_INICIO_FONTE  # widget ano_inicio_fonte (padrão 2022)
END_YEAR = max(ANO_FIM_FONTE, START_YEAR)  # widget ano_fim_fonte (padrão 2026)

SOURCE_YEARS = list(range(START_YEAR, END_YEAR + 1))

# Cada agravo é uma BASE SEPARADA no portal de dados abertos do Ministério da Saúde,
# com os arquivos anuais no S3 do OpenDataSUS (endereços conferidos no portal):
#   Dengue      : dadosabertos.saude.gov.br/dataset/arboviroses-dengue
#                 .../SINAN/Dengue/csv/DENGBR24.csv.zip
#   Chikungunya : dadosabertos.saude.gov.br/dataset/arboviroses-febre-de-chikungunya
#                 .../SINAN/Chikungunya/csv/CHIKBR22.csv.zip
#   Zika        : dadosabertos.saude.gov.br/dataset/arboviroses-zika-virus
#                 .../SINAN/Zikavirus/csv/ZIKABR22.csv.zip
# {yy} = 2 últimos dígitos do ano. Se o Ministério mudar o endereço, cole no
# widget url_fonte o link de um arquivo anual novo (o ano é trocado sozinho).
import re as _re_url
_S3 = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN"
URL_PADRAO = {
    "dengue": f"{_S3}/Dengue/csv/DENGBR{{yy}}.csv.zip",
    "chikungunya": f"{_S3}/Chikungunya/csv/CHIKBR{{yy}}.csv.zip",
    "zika": f"{_S3}/Zikavirus/csv/ZIKABR{{yy}}.csv.zip",
}


def _modelo_da_url(url):
    """Troca os 2 dígitos do ano no nome do arquivo por {yy}."""
    url = url.strip()
    if "{yy}" in url:
        return url
    m = _re_url.search(r"(\d{2})(\.csv\.zip|\.json\.zip|\.zip)$", url)
    if not m:
        raise ValueError(f"url_fonte não parece um arquivo anual do SINAN: {url}")
    return url[:m.start(1)] + "{yy}" + url[m.end(1):]


_url_widget = dbutils.widgets.get("url_fonte").strip()
SOURCE_URL_TEMPLATE = _modelo_da_url(_url_widget) if _url_widget else URL_PADRAO[AGRAVO]
print(f"[FONTE] {CFG_AGRAVO['nome']}: {SOURCE_URL_TEMPLATE}")

HEADERS = {
    "User-Agent": "Databricks-Arboviroses-Brasil/11.0"
}

# Versão da lógica da Silver. Sempre que a regra de transformação mudar,
# incremente este valor: o pipeline detecta a diferença e reprocessa todos
# os anos automaticamente (não é preciso apagar tabelas nem usar o widget).
PIPELINE_VERSION = "10"  # v10: hospitalização nula (e não zero) quando o arquivo não tem HOSPITALIZ (zika). v9: decodificação completa (laboratório, sorotipo, autoctonia, gravidade, tempos). v8: sintomas/comorbidades, DT_DISPONIVEL, código 8


# ------------------------------------------------------------------------------
# Parâmetros das análises do TCC (notebook 21_vigilancia_agravo)
# ------------------------------------------------------------------------------
# Guarda uma cópia de cada versão baixada do SINAN em raw/historico/. Permite
# reconstruir, no futuro, como a base estava em cada data (ocupa espaço).
ARQUIVAR_VERSOES_FONTE = True

# Incidência anual (casos prováveis/100 mil hab.) considerada "alta" pelo MS
LIMIAR_ALTA_INCIDENCIA = 300.0

# Canal endêmico: quantos anos anteriores usar, mínimo para calcular e anos
# epidêmicos a excluir do histórico (ex.: [2019, 2024]). O padrão é não excluir
# nenhum; rode uma segunda vez com os anos epidêmicos excluídos para a análise
# de sensibilidade do artigo.
JANELA_ANOS_CANAL = 7
MIN_ANOS_HISTORICO = 3
ANOS_EXCLUIDOS_CANAL = [
    int(a) for a in dbutils.widgets.get("anos_excluidos_canal").replace(" ", "").split(",")
    if a.strip().isdigit()
]

# Nowcasting (seção 39.5c): semanas com menos casos que isso (estado) são
# ignoradas no resumo de erro, para o erro percentual não ser dominado por ruído.
NOWCAST_MIN_CASOS = 50

# Campos clínicos do SINAN (codificação 1 = sim, 2 = não, demais = ignorado).
# nome_padronizado -> coluna original do arquivo
SINTOMAS = {
    "febre": "FEBRE", "mialgia": "MIALGIA", "cefaleia": "CEFALEIA",
    "exantema": "EXANTEMA", "vomito": "VOMITO", "nausea": "NAUSEA",
    "dor_costas": "DOR_COSTAS", "conjuntivite": "CONJUNTVIT",
    "artrite": "ARTRITE", "artralgia": "ARTRALGIA", "petequias": "PETEQUIA_N",
    "leucopenia": "LEUCOPENIA", "prova_do_laco": "LACO", "dor_retroorbitaria": "DOR_RETRO",
}
COMORBIDADES = {
    "diabetes": "DIABETES", "doenca_hematologica": "HEMATOLOG",
    "hepatopatia": "HEPATOPAT", "doenca_renal": "RENAL",
    "hipertensao": "HIPERTENSA", "doenca_acido_peptica": "ACIDO_PEPT",
    "doenca_autoimune": "AUTO_IMUNE",
}
SINAIS_ALARME = {
    "hipotensao": "ALRM_HIPOT", "plaquetopenia": "ALRM_PLAQ", "vomitos_persistentes": "ALRM_VOM",
    "sangramento": "ALRM_SANG", "hematocrito_aumentado": "ALRM_HEMAT",
    "dor_abdominal": "ALRM_ABDOM", "letargia": "ALRM_LETAR",
    "hepatomegalia": "ALRM_HEPAT", "acumulo_liquidos": "ALRM_LIQ",
}

# Alertas em tempo real (seção 39.5b)
# Defasagem de informação, em semanas: um caso só "existe" para o alerta da
# semana S se foi notificado até o fim da semana S + defasagem. 0 = só o que já
# estava notificado ao fim da própria semana. "final" (sem filtro) é calculado
# sempre, como referência.
DEFASAGENS_INFORMACAO = [0, 1, 2, 4]
# Limiares de incidência anual (casos prováveis/100 mil) usados como "verdade
# de referência" na análise de sensibilidade.
LIMIARES_SENSIBILIDADE = [100.0, 300.0, 500.0]

# Revisão dos dados: idade mínima (em semanas) para considerar uma semana
# "consolidada" e usá-la como valor final de referência
SEMANAS_CONSOLIDACAO = 12

# Canal endêmico: nº de semanas seguidas acima do limite para disparar alerta.
# Rode com 1, 2 e 3 para a análise de sensibilidade do TCC.
SEMANAS_CONSECUTIVAS_ALERTA = int(dbutils.widgets.get("semanas_consecutivas"))

# COMMAND ----------
# DBTITLE 1,3. DESCOBRIR CATÁLOGO DISPONÍVEL
# ==============================================================================
# 3. DESCOBRIR CATÁLOGO DISPONÍVEL
# ==============================================================================

catalog_rows = spark.sql("SHOW CATALOGS").collect()

catalogs = [r[0] for r in catalog_rows]

preferred = [
    c for c in catalogs
    if c.lower() in {"workspace", "main"}
]

if preferred:

    CATALOG = preferred[0]

else:

    available_catalogs = [
        c for c in catalogs
        if c.lower() not in {
            "hive_metastore",
            "system",
            "samples"
        }
    ]

    if not available_catalogs:
        raise Exception(
            "Nenhum catálogo apropriado foi encontrado."
        )

    CATALOG = available_catalogs[0]

# COMMAND ----------
# DBTITLE 1,4. BANCO / VOLUME
# ==============================================================================
# 4. BANCO / VOLUME
# ==============================================================================

# Schema de análise: <agravo>_br (dengue_br, chikungunya_br, zika_br).
# Com sufixo_schema (ex.: "exp") vira dengue_br_exp: testes ficam separados.
SCHEMA_BASE = f"{AGRAVO}_{UF_SIGLA.lower()}"
SCHEMA = SCHEMA_BASE + (f"_{SUFIXO_SCHEMA}" if SUFIXO_SCHEMA else "")

# Os arquivos do SINAN são NACIONAIS (um por ano, todas as UFs). Por isso o
# download fica num volume por AGRAVO, compartilhado por todas as UFs e
# execuções: ninguém baixa o mesmo arquivo duas vezes.
RAW_SCHEMA = f"{AGRAVO}_raw"   # dengue_raw, chikungunya_raw, zika_raw

VOLUME = "raw"

# Volume do schema de análise: ibge/, validacao/, exportacoes/
VOLUME_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
)

# Volume dos arquivos baixados do SINAN (por agravo)
RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"

# Métricas de tempo de TODAS as execuções (todos os agravos, UFs e
# experimentos) numa tabela só, lida pelo notebook 24_eficiencia_da_solucao.
METRICAS_SCHEMA = "arboviroses_metricas"

# Prefixo das tabelas Gold: gold_dengue_*, gold_chikungunya_*, gold_zika_*
GOLD_PREFIX = f"gold_{AGRAVO}"

# COMMAND ----------
# DBTITLE 1,5. TABELAS
# ==============================================================================
# 5. TABELAS
# ==============================================================================

BRONZE_TABLE = (
    f"{CATALOG}.{SCHEMA}.bronze_{SCHEMA_BASE}"
)

SILVER_TABLE = (
    f"{CATALOG}.{SCHEMA}.silver_{SCHEMA_BASE}"
)

MANIFEST_TABLE = (
    f"{CATALOG}.{RAW_SCHEMA}.source_manifest"
)


# -----------------------------
# GOLD
# -----------------------------

GOLD_WEEKLY = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_weekly"
)

GOLD_ANNUAL = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_annual"
)

GOLD_MUNICIPAL = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_municipal"
)

GOLD_DEMOGRAPHIC = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_demographic"
)

GOLD_SEVERITY = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_severity"
)

GOLD_HOSPITALIZATION = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_hospitalization"
)

GOLD_MONTHLY = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_monthly"
)

GOLD_COMPARISON = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_comparison"
)

GOLD_QUALITY = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_data_quality"
)

GOLD_ALERTAS = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_alertas"
)

GOLD_INDICATORS = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_indicators"
)

GOLD_MUNICIPAL_WEEKLY = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_municipal_weekly"
)

# -----------------------------
# GOLD - SEXO
# -----------------------------

GOLD_SEXO_SUMMARY = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_sexo_summary"
)

GOLD_SEXO_ANUAL = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_sexo_anual"
)

GOLD_SEXO_FAIXA = (
    f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_sexo_faixa"
)

# -----------------------------
# GOLD - ANÁLISES DO TCC
# -----------------------------

GOLD_ATRASO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_atraso_notificacao"
GOLD_ATRASO_SEMANAL = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_atraso_semanal"
GOLD_SNAPSHOT = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_snapshot_semanal"
GOLD_REVISAO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_revisao_dados"
GOLD_REVISAO_RESUMO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_revisao_resumo"
GOLD_CANAL_UF = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_canal_endemico"
GOLD_CANAL_MUN = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_canal_endemico_municipal"
GOLD_ALERTAS_MUN_ANO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_alertas_municipio_ano"
GOLD_AVALIACAO_ALERTAS = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_avaliacao_alertas"
GOLD_COMPLETUDE = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_completude"
GOLD_VALIDACAO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_validacao_oficial"
GOLD_ALERTAS_RT = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_alertas_tempo_real"
GOLD_SINTOMAS = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_sintomas"
GOLD_PERFIL = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_perfil"
GOLD_LABORATORIO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_laboratorio"
GOLD_AUTOCTONIA = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_autoctonia"
GOLD_TEMPOS = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_tempos"
GOLD_GRAVIDADE_SINAIS = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_gravidade_sinais"
GOLD_CUBO_SEMANAL = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_cubo_semanal"
# Tabela para o painel (dashboard): ano x semana x município x classificação x sexo x faixa etária
GOLD_PAINEL = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_painel"
GOLD_REGIONAL = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_regional"
GOLD_INCIDENCIA = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_incidencia"
GOLD_DEMOGRAFICO_SEMANAL = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_demografico_semanal"
GOLD_CLINICO_SEMANAL = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_clinico_semanal"
GOLD_HOSPITALIZACAO_SEMANAL = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_hospitalizacao_semanal"
GOLD_FATORES_RISCO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_fatores_risco"
GOLD_NOWCAST = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_nowcast_estado"
GOLD_NOWCAST_RESUMO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_nowcast_resumo"
GOLD_FATORES_COMPLETUDE = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_fatores_completude"
GOLD_TESTES_QUALIDADE = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_testes_qualidade"
GOLD_LINHAGEM = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_linhagem"
VOLUME_ANO_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_volume_por_ano"
GOLD_DIST_CLASSIFICACAO = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}_distribuicao_classificacao"

# COMMAND ----------
# DBTITLE 1,6. CRIAÇÃO DO SCHEMA E VOLUME
# ==============================================================================
# 6. CRIAÇÃO DO SCHEMA E VOLUME
# ==============================================================================

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{CATALOG}.{SCHEMA}"
)

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"{CATALOG}.{SCHEMA}.{VOLUME}"
)

# Schema/volume dos arquivos baixados (compartilhado pelo agravo)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME}")

# Schema das métricas de tempo (compartilhado por todas as execuções)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{METRICAS_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{METRICAS_SCHEMA}.arquivos")

# COMMAND ----------
# DBTITLE 1,7. IDENTIFICAÇÃO DA EXECUÇÃO
# ==============================================================================
# 7. IDENTIFICAÇÃO DA EXECUÇÃO
# ==============================================================================

RUN_ID = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%dT%H%M%S%fZ"
)

# perf_counter: relógio monotônico, próprio para medir durações
RUN_START = time.perf_counter()


print("=" * 80)

print(
    f"[INFO] Catálogo : {CATALOG}"
)

print(
    f"[INFO] Schema   : {SCHEMA}"
)

print(
    f"[INFO] Volume   : {VOLUME_PATH}"
)

print(
    f"[INFO] Run ID   : {RUN_ID}"
)
print(f"[INFO] Agravo   : {CFG_AGRAVO['nome']} | UF: {UF_SIGLA} ({UF_NOME}) | "
      f"arquivos {START_YEAR}-{END_YEAR} | fonte em {RAW_VOLUME_PATH}")
if EXPERIMENTO:
    print(f"[INFO] Experimento: {EXPERIMENTO} | configuração: {CONFIGURACAO or '-'} | "
          f"repetição: {REPETICAO} | aquecimento: {AQUECIMENTO}")

print("=" * 80)

# COMMAND ----------
# DBTITLE 1,Tabela de métricas do pipeline (tempo real por camada)
# ==============================================================================
# TABELA DE MÉTRICAS DO PIPELINE (tempo real de execução por camada)
# ==============================================================================
# Uma linha por camada e por execução. Camadas medidas:
#   verificacao_fonte : HEAD no OpenDataSUS + download/extração quando houve
#                       mudança (depende da rede; reportar à parte no artigo)
#   bronze, silver    : leitura do CSV, padronização e gravação Delta
#   ibge              : dimensão de população (API do IBGE ou cache)
#   gold_calculo      : todas as tabelas analíticas, até gravar as Gold e views
#   gold_manutencao   : OPTIMIZE, linhagem e validação final
# As etiquetas experimento/configuracao/repeticao/aquecimento vêm dos widgets e
# permitem calcular média, mediana, IQR e IC no notebook 24_eficiencia_da_solucao.
# ==============================================================================

METRICS_TABLE = f"{CATALOG}.{METRICAS_SCHEMA}.pipeline_metrics"
BENCH_TABLE = f"{CATALOG}.{METRICAS_SCHEMA}.benchmark_granularidade"

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {METRICS_TABLE}
    (
        run_id STRING,
        camada STRING,
        duracao_segundos DOUBLE,
        registros_processados BIGINT,
        executado_em TIMESTAMP,
        modo_execucao STRING,
        anos_processados STRING,
        n_anos_processados INT,
        forcado BOOLEAN,
        agravo STRING,
        uf STRING,
        schema_analise STRING,
        ano_inicio_fonte INT,
        n_anos_fonte INT,
        experimento STRING,
        configuracao STRING,
        repeticao INT,
        aquecimento BOOLEAN,
        observacao STRING,
        notebook_version STRING,
        ambiente STRING
    )
    USING DELTA
    """
)


def _descrever_ambiente():
    """Versão do runtime e tipo de máquina, gravados junto com cada métrica
    (em serverless o hardware não é fixo; isso precisa constar no artigo)."""
    partes = [f"spark={spark.version}"]
    for rotulo, chave in [
        ("dbr", "spark.databricks.clusterUsageTags.sparkVersion"),
        ("node", "spark.databricks.clusterUsageTags.nodeTypeId"),
        ("workers", "spark.databricks.clusterUsageTags.clusterWorkers"),
    ]:
        try:
            partes.append(f"{rotulo}={spark.conf.get(chave)}")
        except Exception:
            pass
    if len(partes) == 1:
        partes.append("serverless/sem tags de cluster")
    return "; ".join(partes)


AMBIENTE = _descrever_ambiente()


def registrar_metrica(run_id, camada, duracao_segundos, registros_processados=None,
                      modo=None, observacao=None):
    """Grava a duração real (medida com time.perf_counter()) de uma camada do
    pipeline, com o modo de execução (incremental x completo x somente_gold), os
    anos processados e as etiquetas do experimento."""
    anos_proc = sorted(int(y) for y in globals().get("changed_years", []) or [])
    anos_disp = sorted(int(y) for y, _ in globals().get("downloaded_paths", []) or [])
    if modo is None:
        modo = "completo" if anos_proc and anos_proc == anos_disp else "incremental"
        if globals().get("SOMENTE_GOLD"):
            modo = "somente_gold"  # não entra na comparação incremental x completo
    row = [(
        run_id,
        camada,
        float(duracao_segundos),
        int(registros_processados) if registros_processados is not None else None,
        datetime.now(timezone.utc),
        modo,
        ",".join(str(a) for a in anos_proc),
        len(anos_proc),
        bool(FORCAR_REPROCESSAMENTO),
        AGRAVO,
        UF_SIGLA,
        SCHEMA,
        int(START_YEAR),
        len(SOURCE_YEARS),
        EXPERIMENTO or None,
        CONFIGURACAO or None,
        REPETICAO,
        bool(AQUECIMENTO),
        observacao,
        NOTEBOOK_VERSION,
        AMBIENTE,
    )]
    df = spark.createDataFrame(
        row,
        schema=(
            "run_id string, camada string, duracao_segundos double, "
            "registros_processados bigint, executado_em timestamp, "
            "modo_execucao string, anos_processados string, n_anos_processados int, "
            "forcado boolean, agravo string, uf string, schema_analise string, "
            "ano_inicio_fonte int, n_anos_fonte int, experimento string, "
            "configuracao string, repeticao int, aquecimento boolean, "
            "observacao string, notebook_version string, ambiente string"
        )
    )
    (
        df.write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(METRICS_TABLE)
    )
    print(f"[MÉTRICA] {camada} ({modo}): {duracao_segundos:.2f}s"
          + (f" | {registros_processados:,} registros" if registros_processados is not None else ""))

# COMMAND ----------
# DBTITLE 1,Funções auxiliares (colunas, datas, números) usadas pelo pipeline e pelas análises
import re
import csv as _csv

def first_existing_column(
    dataframe_columns,
    candidates
):

    for candidate in candidates:

        if candidate in dataframe_columns:

            return candidate

    return None


def to_int_pop(value):
    if value is None:
        return None
    digits = re.sub(r"[^0-9]", "", str(value))
    return int(digits) if digits else None



# DBTITLE 1,16. FUNÇÕES AUXILIARES PARA PADRONIZAÇÃO
# ==============================================================================
# 16. FUNÇÕES AUXILIARES PARA PADRONIZAÇÃO
# ==============================================================================

from functools import reduce

def safe_int(column_name):
    """Converte coluna de texto em INT sem falhar no modo ANSI.
    Os CSVs de anos antigos trazem inteiros como '4064.0'; o cast direto para
    INT quebra com CAST_INVALID_INPUT. Aqui: texto -> DOUBLE -> INT, e valor
    malformado vira NULL em vez de derrubar o pipeline."""
    return F.expr(f"try_cast(try_cast(trim(`{column_name}`) AS DOUBLE) AS INT)")


def parse_date_flexible(dataframe, column_name):
    if column_name is None or column_name not in dataframe.columns:
        return F.lit(None).cast("date")

    # try_to_timestamp devolve NULL quando o texto não bate com o formato
    # (to_date lança erro no modo ANSI). O primeiro formato que funcionar vence.
    raw = f"trim(cast(`{column_name}` as string))"
    return F.coalesce(
        *[
            F.expr(f"cast(try_to_timestamp({raw}, '{fmt}') as date)")
            for fmt in ("yyyy-MM-dd", "yyyyMMdd", "dd/MM/yyyy", "yyyy/MM/dd")
        ]
    )


# first_existing_column já foi definida na seção de padronização da Silver.

# COMMAND ----------
# DBTITLE 1,Tabelas compartilhadas pelos três agravos e caminhos das análises
# ==============================================================================
# POPULAÇÃO (IBGE): uma dimensão só, para dengue, chikungunya e zika, gravada
# pelo notebook 10_populacao_ibge em workspace.arboviroses_br.
# ==============================================================================
SCHEMA_COMPARTILHADO = f"{CATALOG}.arboviroses_br"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_COMPARTILHADO}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_COMPARTILHADO}.raw")
VOLUME_COMPARTILHADO = f"/Volumes/{CATALOG}/arboviroses_br/raw"

POPULATION_TABLE = f"{SCHEMA_COMPARTILHADO}.dim_municipio_populacao"
POPULATION_SEXO_TABLE = f"{SCHEMA_COMPARTILHADO}.dim_populacao_sexo_uf"
# Inclui o ano anterior ao início: casos das primeiras semanas epidemiológicas
# do arquivo de 2022 podem pertencer ao ano epidemiológico de 2021.
POPULATION_YEARS = list(range(START_YEAR - 1, END_YEAR + 1))

# Validação contra os números oficiais (preenchida à mão; ver 21_vigilancia_agravo)
VALIDACAO_DIR = os.path.join(VOLUME_PATH, "validacao")
VALIDACAO_CSV = os.path.join(VALIDACAO_DIR, "numeros_oficiais.csv")
