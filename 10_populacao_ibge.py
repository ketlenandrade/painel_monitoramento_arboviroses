# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./config_agravo

# COMMAND ----------

# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 10 – DIMENSÃO DE MUNICÍPIOS E POPULAÇÃO (IBGE) – UMA VEZ PARA OS TRÊS AGRAVOS
# ==============================================================================
# Grava  workspace.arboviroses_br.dim_municipio_populacao  (município x ano) e
# dim_populacao_sexo_uf, usadas como denominador da incidência de dengue,
# chikungunya e zika. Antes ficava dentro do pipeline e era refeita por agravo.
#
# Chamado pelo 12_rodar_dengue_chikungunya_zika antes dos agravos, e pelo próprio
# pipeline (11) quando falta algum ano. Widgets: ano_inicio_fonte, ano_fim_fonte
# (anos de população necessários) e usar_cache_ibge (sim = não consulta a API do
# IBGE se já houver uma dimensão gravada).
# ==============================================================================

# COMMAND ----------

# DBTITLE 1,Preparação
# Último recurso da seção abaixo (sem IBGE e sem lista de municípios): usar os
# municípios de uma Silver já gravada. Sem nenhuma, a lista fica vazia.
_silvers = [f"{CATALOG}.{a}_{UF_SIGLA.lower()}.silver_{a}_{UF_SIGLA.lower()}" for a in ("dengue", "chikungunya", "zika")]
_silver_existente = next((t for t in _silvers if spark.catalog.tableExists(t)), None)
df_silver_persisted = (spark.table(_silver_existente) if _silver_existente
                       else spark.createDataFrame([], "ID_MN_RESI_CODIGO string"))
t_start_ibge = time.perf_counter()

# COMMAND ----------

# DBTITLE 1,25. DIMENSÃO MUNICÍPIOS + POPULAÇÃO IBGE
# ==============================================================================
# 25. DIMENSÃO MUNICÍPIOS + POPULAÇÃO IBGE
# ==============================================================================
# Fontes oficiais (API de Agregados v3 do IBGE / SIDRA):
#
#   * Tabela 6579 (EstimaPop), variável 9324 – população residente estimada
#     (1º de julho). Existe para 2021, 2024, 2025, 2026...  NÃO existe para
#     2022 e 2023 – pedir esses anos à tabela 6579 é o que provocava HTTP 500.
#   * Tabela 4709 (Censo 2022), variável 93 – população residente em 2022.
#   * 2023: o IBGE não publicou estimativa municipal. Usamos interpolação
#     geométrica entre o Censo 2022 e a estimativa 2024 (indicado na coluna
#     fonte_populacao, para ficar transparente no TCC/artigo).
#   * Anos ainda não publicados: repete o último ano disponível (também
#     indicado em fonte_populacao).
#
# Fallback manual: se a API estiver fora do ar, coloque um CSV em
#   /Volumes/<catalogo>/arboviroses_br/raw/ibge/populacao_municipios.csv
# com as colunas  municipio_codigo_ibge;ano;populacao  (6 ou 7 dígitos).
# Valores do CSV têm prioridade sobre a API.
#
# Se nada puder ser obtido, a dimensão já existente NÃO é sobrescrita.
# ==============================================================================

import re
import csv as _csv

# POPULATION_TABLE e POPULATION_SEXO_TABLE (compartilhadas): definidas em config_agravo.

IBGE_API_V3 = "https://servicodados.ibge.gov.br/api/v3/agregados"
IBGE_API_SIDRA = "https://apisidra.ibge.gov.br/values"
IBGE_LOCALIDADES_URL = (
    "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    if ESCOPO_NACIONAL else
    "https://servicodados.ibge.gov.br/api/v1/localidades/estados/"
    f"{UF_CODIGO_IBGE}/municipios"
)
# Localidades nas consultas do IBGE: municípios da UF ou de todo o Brasil;
# território inteiro (para a população por sexo): N3 da UF ou N1 do Brasil.
IBGE_LOC_MUNICIPIOS = "N6[all]" if ESCOPO_NACIONAL else f"N6[N3[{UF_CODIGO_IBGE}]]"
IBGE_LOC_TERRITORIO = "N1[all]" if ESCOPO_NACIONAL else f"N3[{UF_CODIGO_IBGE}]"
SIDRA_LOC_MUNICIPIOS = "n6/all" if ESCOPO_NACIONAL else f"n6/in%20n3%20{UF_CODIGO_IBGE}"

IBGE_DIR = os.path.join(VOLUME_COMPARTILHADO, "ibge")
IBGE_CSV_MANUAL = os.path.join(IBGE_DIR, "populacao_municipios.csv")
os.makedirs(IBGE_DIR, exist_ok=True)

# POPULATION_YEARS: definido em config_agravo.

ANO_CENSO = 2022
ANOS_SEM_ESTIMATIVA = {2007, 2010, 2022, 2023}  # não existem na tabela 6579


def _bundles_ca_candidatos():
    """CAs alternativos (verificação de certificado SEMPRE ligada)."""
    candidatos = []
    for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        if os.environ.get(var):
            candidatos.append(os.environ[var])
    candidatos += [
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
    ]
    try:
        import certifi
        candidatos.append(certifi.where())
    except Exception:
        pass
    vistos = []
    for c in candidatos:
        if c and os.path.exists(c) and c not in vistos:
            vistos.append(c)
    return vistos


_IBGE_SSL_AVISADO = False
# Se o IBGE não responder (timeout de conexão), as próximas chamadas desta
# execução nem são tentadas: sem isso, cada uma das ~15 consultas esperaria
# 4 x 120 s antes de desistir.
_IBGE_FORA_DO_AR = False
_IBGE_CA_OK = None  # bundle que funcionou (reaproveitado nas próximas chamadas)


def http_get_json(url, tentativas=4, timeout=(15, 120)):
    """GET com retentativas e backoff para a API do IBGE (que oscila bastante).
    Em erro de certificado SSL tenta outros bundles de CA instalados no cluster
    (sem desligar a verificação). Se nenhum funcionar, não insiste: use o CSV
    manual ibge/populacao_municipios.csv."""
    global _IBGE_SSL_AVISADO, _IBGE_CA_OK, _IBGE_FORA_DO_AR
    if _IBGE_FORA_DO_AR:
        return None
    ultimo_erro = None
    falhas_conexao = 0
    for tentativa in range(1, tentativas + 1):
        try:
            verify = _IBGE_CA_OK if _IBGE_CA_OK else True
            try:
                r = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify)
            except requests.exceptions.SSLError as e_ssl:
                r = None
                for bundle in _bundles_ca_candidatos():
                    try:
                        r = requests.get(url, headers=HEADERS, timeout=timeout, verify=bundle)
                        _IBGE_CA_OK = bundle
                        break
                    except requests.exceptions.SSLError:
                        continue
                if r is None:
                    if not _IBGE_SSL_AVISADO:
                        print(
                            "[IBGE][WARN] O cluster não consegue validar o certificado SSL "
                            "do IBGE. A verificação NÃO foi desligada. Use o CSV manual "
                            f"{IBGE_CSV_MANUAL} (municipio_codigo_ibge;ano;populacao"
                            "[;municipio_nome]) ou peça ao administrador para instalar a "
                            "cadeia de CA no cluster."
                        )
                        _IBGE_SSL_AVISADO = True
                    ultimo_erro = "SSL: " + str(e_ssl)[:120]
                    break  # sem sentido repetir
            if r.status_code == 200:
                return r.json()
            ultimo_erro = f"HTTP {r.status_code}"
            # 4xx (exceto 429) não melhora tentando de novo
            if 400 <= r.status_code < 500 and r.status_code != 429:
                break
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
            ultimo_erro = str(e)[:200]
            falhas_conexao += 1
            if falhas_conexao >= 2:
                _IBGE_FORA_DO_AR = True
                print(
                    "[IBGE][WARN] O IBGE não respondeu (timeout de conexão). As demais "
                    "consultas desta execução foram puladas; a dimensão de população já "
                    "gravada é mantida. Para não depender da API, use usar_cache_ibge = sim "
                    f"ou o CSV manual {IBGE_CSV_MANUAL}."
                )
                break
        except Exception as e:
            ultimo_erro = str(e)[:200]
        time.sleep(min(2 ** tentativa, 20))
    print(f"[IBGE] Falha em {url}: {ultimo_erro}")
    return None


# to_int_pop: definida em config_agravo.


def ibge_v3(tabela, variavel, periodo, localidades, classificacao=None):
    """Consulta a API de Agregados v3. Retorna lista de
    (codigo_localidade, nome_localidade, categorias{id_class: id_cat}, valor)."""
    url = (
        f"{IBGE_API_V3}/{tabela}/periodos/{periodo}/variaveis/{variavel}"
        f"?localidades={localidades}"
    )
    if classificacao:
        url += f"&classificacao={classificacao}"

    data = http_get_json(url)
    saida = []
    if not data:
        return saida

    for var in data:
        for res in var.get("resultados", []):
            categorias = {}
            for cl in res.get("classificacoes", []) or []:
                cats = list((cl.get("categoria") or {}).keys())
                if cats:
                    categorias[str(cl.get("id"))] = str(cats[0])
            for s in res.get("series", []):
                loc = s.get("localidade", {})
                valor = to_int_pop((s.get("serie") or {}).get(str(periodo)))
                if valor:
                    saida.append(
                        (str(loc.get("id")), loc.get("nome"), categorias, valor)
                    )
    return saida


def sidra_values_municipios(tabela, variavel, ano):
    """Fallback pela API antiga do SIDRA (apisidra.ibge.gov.br)."""
    url = (
        f"{IBGE_API_SIDRA}/t/{tabela}/{SIDRA_LOC_MUNICIPIOS}"
        f"/v/{variavel}/p/{ano}"
    )
    data = http_get_json(url)
    saida = {}
    if not data or len(data) < 2:
        return saida

    header = data[0]
    chave_mun = next(
        (k for k, v in header.items() if str(v).startswith("Município (Código)")),
        None
    )
    if not chave_mun:
        return saida

    for row in data[1:]:
        valor = to_int_pop(row.get("V"))
        if valor:
            saida[str(row.get(chave_mun))] = valor
    return saida


# --- Espelho automático da população (quando a API do IBGE não responde) -------
# Os mesmos arquivos oficiais do IBGE (Censo 2022 – SIDRA 9514; estimativas –
# SIDRA 6579 e DOU 2024; estimativas TCU 2025, revisão de 13/01/2026), versionados
# no repositório público do pacote brpop (github.com/rfsaldanha/brpop), fixados
# num commit para a execução ser reprodutível. Nomes dos municípios: repositório
# kelvins/municipios-brasileiros. Uma cópia fica no Volume, e as próximas
# execuções não precisam baixar de novo.
ESPELHO_POP_COMMIT = "08137ea140484da72f32e3232a6baf45ccc3156a"
ESPELHO_POP_BASE = (
    f"https://raw.githubusercontent.com/rfsaldanha/brpop/{ESPELHO_POP_COMMIT}/data-raw/ibge/"
)
ESPELHO_POP_ARQUIVOS = [
    "tabela9514.csv",              # Censo 2022
    "tabela6579_v2.csv",           # estimativas 2001-2021
    "estimativa_dou_2024.csv",     # estimativa 2024
    "estimativa_tcu_2025.csv",     # estimativa 2025
]
ESPELHO_NOMES_URL = (
    "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/"
    "503e2f70bbf1b4b7ec0b1f68b09086ccc38fe861/csv/municipios.csv"
)
ESPELHO_DIR = os.path.join(IBGE_DIR, "espelho")


def _texto_espelho(url, nome_local):
    """Lê do Volume se já existir; senão baixa e grava a cópia."""
    os.makedirs(ESPELHO_DIR, exist_ok=True)
    caminho = os.path.join(ESPELHO_DIR, nome_local)
    if os.path.exists(caminho) and os.path.getsize(caminho) > 0:
        with open(caminho, encoding="utf-8-sig") as f:
            return f.read()
    r = requests.get(url, timeout=(15, 120))
    r.raise_for_status()
    texto = r.content.decode("utf-8-sig")
    with open(caminho + ".tmp", "w", encoding="utf-8") as f:
        f.write(texto)
    os.replace(caminho + ".tmp", caminho)
    return texto


def baixar_populacao_espelho(prefixos_uf):
    """Retorna ({ano: {codigo_7: pop}}, {codigo_7: nome}, descricao_fonte) dos
    municípios cujos 2 primeiros dígitos estão em prefixos_uf (lista de códigos de UF)."""
    pops, nomes = {}, {}
    prefixos = {str(p) for p in prefixos_uf}
    for arq in ESPELHO_POP_ARQUIVOS:
        try:
            linhas = list(_csv.reader(_texto_espelho(ESPELHO_POP_BASE + arq, arq).splitlines(),
                                      delimiter=";"))
        except Exception as e:
            print(f"[IBGE][espelho] {arq}: {str(e)[:150]}")
            continue
        cab = [c.strip() for c in linhas[0]]
        for row in linhas[1:]:
            if not row or row[0][:2] not in prefixos:
                continue
            cod = re.sub(r"[^0-9]", "", row[0])
            for j, col in enumerate(cab[1:], start=1):
                ano = to_int_pop(col)
                pop = to_int_pop(row[j]) if j < len(row) else None
                if ano and pop and len(cod) == 7:
                    pops.setdefault(ano, {})[cod] = pop
    try:
        for row in _csv.DictReader(_texto_espelho(ESPELHO_NOMES_URL, "municipios.csv").splitlines()):
            if str(row.get("codigo_uf")) in prefixos:
                nomes[str(row["codigo_ibge"])] = row["nome"]
    except Exception as e:
        print(f"[IBGE][espelho] nomes: {str(e)[:150]}")
    if pops:
        print(f"[IBGE][espelho] População obtida: "
              f"{ {a: len(v) for a, v in sorted(pops.items())} } | nomes: {len(nomes)}")
    return pops, nomes, "IBGE (arquivos oficiais via espelho brpop/GitHub)"


def populacao_municipios(tabela, variavel, ano):
    """Retorna {codigo_ibge_7: populacao} e {codigo_ibge_7: nome}."""
    pops, nomes = {}, {}
    for cod, nome, _, valor in ibge_v3(
        tabela, variavel, ano, IBGE_LOC_MUNICIPIOS
    ):
        pops[cod] = valor
        if nome:
            nomes[cod] = re.sub(r"\s*-\s*[A-Z]{2}$", "", nome)
    if not pops:
        pops = sidra_values_municipios(tabela, variavel, ano)
    return pops, nomes


def _atualizar_dimensao_ibge():
    """Consulta o IBGE e regrava as dimensões de população (municípios e sexo)."""
    # ==============================================================================
    # MUNICÍPIOS (API de Localidades)
    # ==============================================================================

    municipios = {}  # codigo_7 -> nome
    dados_localidades = http_get_json(IBGE_LOCALIDADES_URL)
    for item in dados_localidades or []:
        codigo_7 = str(item.get("id", "")).strip()
        if len(codigo_7) == 7 and item.get("nome"):
            municipios[codigo_7] = item["nome"]

    print(f"[IBGE] Municípios obtidos na API de Localidades: {len(municipios)}")


    # ==============================================================================
    # POPULAÇÃO POR ANO
    # ==============================================================================

    pop_por_ano = {}    # ano -> {codigo_7: populacao}
    fonte_por_ano = {}  # ano -> descrição da fonte

    # 1) Censo 2022
    if ANO_CENSO in POPULATION_YEARS:
        for tabela_censo in ("4709", "4714"):
            pops, nomes = populacao_municipios(tabela_censo, "93", ANO_CENSO)
            if pops:
                pop_por_ano[ANO_CENSO] = pops
                fonte_por_ano[ANO_CENSO] = f"IBGE Censo 2022 (tabela {tabela_censo})"
                for k, v in nomes.items():
                    municipios.setdefault(k, v)
                break

    # 2) Estimativas anuais (tabela 6579)
    for ano in POPULATION_YEARS:
        if ano in ANOS_SEM_ESTIMATIVA:
            continue
        pops, nomes = populacao_municipios("6579", "9324", ano)
        if pops:
            pop_por_ano[ano] = pops
            fonte_por_ano[ano] = "IBGE Estimativas (tabela 6579)"
            for k, v in nomes.items():
                municipios.setdefault(k, v)
        else:
            print(f"[IBGE] Estimativa {ano} ainda não disponível.")

    # 3) CSV manual (sobrescreve a API quando existir)
    if os.path.exists(IBGE_CSV_MANUAL):
        with open(IBGE_CSV_MANUAL, encoding="utf-8-sig") as f:
            amostra = f.read(4096)
            f.seek(0)
            delim = ";" if amostra.count(";") >= amostra.count(",") else ","
            n_manual = 0
            for row in _csv.DictReader(f, delimiter=delim):
                cod = re.sub(r"[^0-9]", "", str(row.get("municipio_codigo_ibge", "")))
                if cod[:2] not in PREFIXOS_UF:
                    continue  # município de outra UF
                ano = to_int_pop(row.get("ano"))
                pop = to_int_pop(row.get("populacao"))
                if not cod or not ano or not pop:
                    continue
                # aceita 6 dígitos: completa pelo código de 7 conhecido
                if len(cod) == 6:
                    cod = next((k for k in municipios if k[:6] == cod), None)
                    if not cod:
                        continue
                pop_por_ano.setdefault(ano, {})[cod] = pop
                nome_csv = (row.get("municipio_nome") or "").strip()
                if len(cod) == 7:
                    municipios.setdefault(cod, nome_csv or cod)
                fonte_por_ano[ano] = "CSV manual (ibge/populacao_municipios.csv)"
                n_manual += 1
        print(f"[IBGE] Registros do CSV manual: {n_manual}")

    # 4) Espelho automático dos arquivos oficiais do IBGE no GitHub
    #    (usado quando a API não respondeu; o IBGE costuma recusar conexões de
    #    servidores fora do Brasil, como os do Databricks).
    _faltando = [a for a in POPULATION_YEARS
                 if a not in pop_por_ano and a not in ANOS_SEM_ESTIMATIVA]
    if ANO_CENSO in POPULATION_YEARS and ANO_CENSO not in pop_por_ano:
        _faltando.append(ANO_CENSO)
    if _faltando or not municipios:
        _pops_esp, _nomes_esp, _fonte_esp = baixar_populacao_espelho(PREFIXOS_UF)
        for _ano, _pops in _pops_esp.items():
            if _ano in _faltando and _pops:
                pop_por_ano[_ano] = _pops
                fonte_por_ano[_ano] = _fonte_esp
        for _c, _n in _nomes_esp.items():
            municipios.setdefault(_c, _n)
        if _pops_esp:
            print(f"[IBGE] Espelho automático usado para os anos: "
                  f"{sorted(a for a in _pops_esp if a in _faltando)}")

    print(
        "[IBGE] Anos obtidos diretamente: "
        f"{ {a: len(v) for a, v in sorted(pop_por_ano.items())} }"
    )


    # ==============================================================================
    # PREENCHER ANOS SEM DADO (interpolação geométrica / repetição)
    # ==============================================================================

    # Fallback: API de Localidades fora do ar -> usa códigos já conhecidos
    if not municipios and pop_por_ano:
        nomes_antigos = {}
        if spark.catalog.tableExists(POPULATION_TABLE):
            try:
                nomes_antigos = {
                    r["municipio_codigo_ibge"]: r["municipio_nome"]
                    for r in spark.table(POPULATION_TABLE)
                    .select("municipio_codigo_ibge", "municipio_nome").distinct().collect()
                    if r["municipio_codigo_ibge"]
                }
            except Exception:
                pass
        for a in pop_por_ano:
            for c in pop_por_ano[a]:
                if len(c) == 7:
                    municipios.setdefault(c, nomes_antigos.get(c) or c)
        print(f"[IBGE] Municípios reconstruídos a partir dos dados de população: {len(municipios)}")

    anos_com_dado = sorted(pop_por_ano)
    linhas_pop = []

    if anos_com_dado and municipios:
        for cod7, nome in sorted(municipios.items()):
            serie = {a: pop_por_ano[a][cod7] for a in anos_com_dado if cod7 in pop_por_ano[a]}
            anos_mun = sorted(serie)

            for ano in POPULATION_YEARS:
                if ano in serie:
                    pop, fonte = serie[ano], fonte_por_ano[ano]
                elif not anos_mun:
                    pop, fonte = None, "Sem dado IBGE"
                else:
                    antes = [a for a in anos_mun if a < ano]
                    depois = [a for a in anos_mun if a > ano]
                    if antes and depois:
                        a0, a1 = antes[-1], depois[0]
                        p0, p1 = serie[a0], serie[a1]
                        taxa = (p1 / p0) ** (1.0 / (a1 - a0))
                        pop = int(round(p0 * taxa ** (ano - a0)))
                        fonte = f"Interpolação geométrica {a0}-{a1}"
                    elif antes:
                        pop, fonte = serie[antes[-1]], f"Repetida de {antes[-1]}"
                    else:
                        pop, fonte = serie[depois[0]], f"Repetida de {depois[0]}"

                linhas_pop.append(
                    (cod7[:6], cod7, nome, UF_POR_CODIGO.get(cod7[:2], UF_SIGLA), int(ano), pop, fonte)
                )


    schema_dim = (
        "municipio_codigo_6 string, municipio_codigo_ibge string, "
        "municipio_nome string, uf string, ano_populacao int, "
        "populacao bigint, fonte_populacao string"
    )

    tem_populacao = any(l[5] for l in linhas_pop)

    if tem_populacao:
        df_dim_municipio = spark.createDataFrame(linhas_pop, schema=schema_dim)
        (
            df_dim_municipio
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(POPULATION_TABLE)
        )
        print(f"[IBGE] Dimensão salva em: {POPULATION_TABLE} ({len(linhas_pop):,} linhas)")

    elif spark.catalog.tableExists(POPULATION_TABLE):
        print(
            "[IBGE][WARN] Não foi possível obter população nesta execução. "
            f"Mantendo a versão anterior de {POPULATION_TABLE}."
        )
        if "fonte_populacao" not in spark.table(POPULATION_TABLE).columns:
            (
                spark.table(POPULATION_TABLE)
                .withColumn("fonte_populacao", F.lit("Versão anterior (fonte não registrada)"))
                .write.format("delta").mode("overwrite")
                .option("overwriteSchema", "true").saveAsTable(POPULATION_TABLE)
            )
            print("[IBGE] Coluna fonte_populacao adicionada à tabela anterior.")

    else:
        print(
            "[IBGE][WARN] Sem população disponível. Criando dimensão só com os "
            "municípios (incidência ficará nula). Use o CSV manual se a API falhar."
        )
        if municipios:
            linhas_vazias = [
                (c[:6], c, n, UF_POR_CODIGO.get(c[:2], UF_SIGLA), int(a), None, "Sem dado IBGE")
                for c, n in municipios.items() for a in POPULATION_YEARS
            ]
            df_dim_municipio = spark.createDataFrame(linhas_vazias, schema=schema_dim)
        else:
            df_dim_municipio = (
                df_silver_persisted
                .select(F.col("ID_MN_RESI_CODIGO").alias("municipio_codigo_6"))
                .filter(F.col("municipio_codigo_6").isNotNull())
                .distinct()
                .crossJoin(
                    spark.createDataFrame([(int(a),) for a in POPULATION_YEARS], "ano_populacao int")
                )
                .select(
                    "municipio_codigo_6",
                    F.lit(None).cast("string").alias("municipio_codigo_ibge"),
                    F.lit(None).cast("string").alias("municipio_nome"),
                    F.lit(UF_SIGLA).alias("uf"),
                    "ano_populacao",
                    F.lit(None).cast("bigint").alias("populacao"),
                    F.lit("Sem dado IBGE").alias("fonte_populacao"),
                )
            )
        (
            df_dim_municipio
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(POPULATION_TABLE)
        )


    # ==============================================================================
    # POPULAÇÃO DO BRASIL POR SEXO (Censo 2022, tabela 9514) – denominador da
    # incidência por sexo usada nas tabelas gold_dengue_sexo_*.
    # Classificações: 2 = sexo (4 Homens, 5 Mulheres); 287 = idade (100362 Total);
    # 286 = forma de declaração da idade (113635 Total).
    # ==============================================================================

    mapa_sexo = {"4": "Masculino", "5": "Feminino"}
    linhas_sexo = []
    for _, _, cats, valor in ibge_v3(
        "9514", "93", ANO_CENSO, IBGE_LOC_TERRITORIO,
        classificacao="2[4,5]|287[100362]|286[113635]"
    ):
        sexo = mapa_sexo.get(cats.get("2"))
        if sexo:
            linhas_sexo.append((sexo, int(valor), ANO_CENSO, "IBGE Censo 2022 (tabela 9514)"))

    if linhas_sexo:
        (
            spark.createDataFrame(
                linhas_sexo,
                "sexo_nome string, populacao bigint, ano_populacao int, fonte_populacao string"
            )
            .write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(POPULATION_SEXO_TABLE)
        )
        print(f"[IBGE] População por sexo salva em: {POPULATION_SEXO_TABLE}")
    elif not spark.catalog.tableExists(POPULATION_SEXO_TABLE):
        print("[IBGE][WARN] População por sexo indisponível; incidência por sexo ficará nula.")


def _dimensao_ibge_em_cache():
    """Procura uma dimensão de população já gravada que tenha dados: primeiro a
    compartilhada (arboviroses_br) e depois as das versões anteriores do pipeline,
    que ficavam no schema de cada agravo (dengue_br, ...). Se achar uma antiga,
    copia para a compartilhada. Devolve o nome da tabela de origem, ou None."""
    candidatos = list(dict.fromkeys(
        [POPULATION_TABLE]
        + [f"{CATALOG}.{a}_{UF_SIGLA.lower()}.dim_municipio_populacao" for a in ("dengue", "chikungunya", "zika")]
    ))
    for t_pop in candidatos:
        t_sexo = t_pop.replace("dim_municipio_populacao", "dim_populacao_sexo_uf")
        if not spark.catalog.tableExists(t_pop):
            continue
        anos = {
            int(r[0]) for r in spark.table(t_pop).filter(F.col("populacao").isNotNull())
            .select("ano_populacao").distinct().collect()
        }
        if not anos:
            continue
        # anos que faltarem são completados depois (_completar_anos_populacao)
        if t_pop != POPULATION_TABLE:
            (spark.table(t_pop).write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(POPULATION_TABLE))
            if spark.catalog.tableExists(t_sexo):
                (spark.table(t_sexo).write.format("delta").mode("overwrite")
                 .option("overwriteSchema", "true").saveAsTable(POPULATION_SEXO_TABLE))
        return t_pop
    return None


def _completar_anos_populacao():
    """Garante uma linha de população para TODOS os anos de POPULATION_YEARS.
    Ano sem dado (estimativa não publicada, API fora do ar, dimensão antiga com
    menos anos) recebe a população do ano mais próximo que existe, e isso fica
    escrito em fonte_populacao. Sem isso, a incidência desses anos ficaria nula
    e o canal endêmico perderia anos de histórico."""
    if not spark.catalog.tableExists(POPULATION_TABLE):
        return []
    dim = spark.table(POPULATION_TABLE)
    tem = sorted({
        int(r[0]) for r in dim.filter(F.col("populacao").isNotNull()).select("ano_populacao").distinct().collect()
    })
    faltam = [a for a in POPULATION_YEARS if a not in tem]
    if not tem or not faltam:
        return []
    mapa = [(int(a), int(min(tem, key=lambda x: (abs(x - a), -x)))) for a in faltam]
    novos = (
        dim.filter(F.col("populacao").isNotNull())
        .join(spark.createDataFrame(mapa, "_ano_novo int, _ano_origem int"),
              F.col("ano_populacao") == F.col("_ano_origem"))
        .withColumn("ano_populacao", F.col("_ano_novo"))
        .withColumn("fonte_populacao", F.concat(F.lit("Repetida de "), F.col("_ano_origem").cast("string"),
                                                F.lit(" (ano sem dado do IBGE)")))
        .drop("_ano_novo", "_ano_origem")
    )
    # A tabela é pequena (municípios x anos): traz para o driver e regrava, para
    # não sobrescrever uma tabela enquanto ela é lida.
    linhas = (
        dim.filter(~F.col("ano_populacao").isin(faltam))
        .unionByName(novos.select(*dim.columns))
        .collect()
    )
    (spark.createDataFrame(linhas, dim.schema)
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(POPULATION_TABLE))
    print(f"[IBGE] Anos sem população completados com o ano mais próximo: "
          f"{', '.join(f'{a}←{o}' for a, o in mapa)}")
    return mapa


_origem_cache_ibge = _dimensao_ibge_em_cache() if USAR_CACHE_IBGE else None
if _origem_cache_ibge:
    print(f"[IBGE] usar_cache_ibge = sim: dimensão reaproveitada de {_origem_cache_ibge} "
          "(API do IBGE não consultada).")
else:
    if USAR_CACHE_IBGE:
        print("[IBGE] Nenhuma dimensão de população gravada para esta UF; consultando a API.")
    _atualizar_dimensao_ibge()
_completar_anos_populacao()
if not spark.catalog.tableExists(POPULATION_SEXO_TABLE):
    print("[IBGE][WARN] Sem população por sexo (a API do IBGE não respondeu e não há cópia gravada): "
          "a incidência por sexo fica nula nesta execução. Rode de novo quando a API voltar.")

display(
    spark.table(POPULATION_TABLE)
    .groupBy("ano_populacao", "fonte_populacao")
    .agg(
        F.count("*").alias("municipios"),
        F.sum("populacao").alias("populacao_territorio")
    )
    .orderBy("ano_populacao")
)

# COMMAND ----------

# DBTITLE 1,Registrar métrica e saída
registrar_metrica(
    RUN_ID, "ibge", time.perf_counter() - t_start_ibge,
    observacao=(f"cache de {_origem_cache_ibge}" if _origem_cache_ibge else "API do IBGE"),
)
import json as _json_pop
dbutils.notebook.exit(_json_pop.dumps({
    "status": "OK", "tabela": POPULATION_TABLE,
    "anos": sorted(int(r[0]) for r in spark.table(POPULATION_TABLE).select("ano_populacao").distinct().collect()),
}))