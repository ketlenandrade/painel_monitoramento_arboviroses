# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Sobre este notebook
# ==============================================================================
# 11 – PIPELINE ETL DE UM AGRAVO (DENGUE, CHIKUNGUNYA OU ZIKA) – BRASIL INTEIRO
# (SINAN / OpenDataSUS) – notebook versão 12
# ==============================================================================
# Fonte : Notificações de dengue, chikungunya e zika (SINAN), OpenDataSUS,
#         arquivos anuais DENGBRyy / CHIKBRyy / ZIKABRyy.csv.zip (widget
#         "agravo"). Período baixado e analisado: 2022-2026 (padrão).
#         O canal endêmico usa os anos anteriores disponíveis como
#         histórico (com 2022-2026, ele sai para 2025 e 2026; para ter canal
#         em todos os anos, use ano_inicio_fonte = 2016).
# Escopo : BRASIL INTEIRO (todas as 27 UFs e 5.570 municípios), SEM filtro de
#         estado. Os filtros por estado/município ficam para o dashboard.
# Camadas: Bronze (arquivo bruto) -> Silver (limpo e padronizado, residentes
#         no Brasil) -> Gold (tabelas analíticas em Delta Lake, por município,
#         por UF e para o Brasil).
# Versão 12 – SÓ O PIPELINE (extração -> Bronze -> Silver -> Gold):
#   * configuração e widgets: config_agravo (%run)
#   * população do IBGE: notebook 10_populacao_ibge (uma vez para os 3 agravos)
#   * base analítica e calendário: base_analitica_agravo (%run, também usado
#     pelas análises)
#   * canal endêmico, alertas, nowcasting, atraso, completude e validação:
#     21_vigilancia_agravo
#   * visualizações, resultados do TCC e CSV da rodada: 22_resultados_tcc_agravo
#   * benchmark de granularidade: 25_benchmark_granularidade
# Ficam aqui: download, Bronze, Silver, tabelas Gold, snapshots da fonte,
# testes de qualidade, views, OPTIMIZE, linhagem e métricas de tempo.
#
# Novidades da versão 10 (pedidos dos revisores):
#   * widget agravo (dengue/chikungunya/zika); escopo nacional fixo;
#   * widgets de experimento (experimento, configuracao, repeticao,
#     aquecimento) que etiquetam as métricas de tempo;
#   * cronômetros separados: a API do IBGE e o OPTIMIZE não contaminam mais
#     o tempo da Gold; o download/checagem da fonte tem métrica própria;
#   * usar_cache_ibge, simular_ano_alterado, benchmark_granularidade,
#     exportar_csv, ano_inicio_fonte e sufixo_schema para os experimentos;
#   * IC 95% de Wilson nas métricas de alerta e coluna entra_avaliacao;
#   * distribuição de CLASSI_FIN por ano (conferência da codificação).
# ==============================================================================

# COMMAND ----------

# MAGIC %run ./config_agravo

# COMMAND ----------

# DBTITLE 1,8. MANIFESTO DAS FONTES
# ==============================================================================
# 8. MANIFESTO DAS FONTES
# ==============================================================================

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE}
    (
        execution_id STRING,
        year INT,
        url STRING,
        etag STRING,
        last_modified STRING,
        content_length BIGINT,
        sha256 STRING,
        downloaded_at TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------

# DBTITLE 1,9. VERIFICAÇÃO AUTOMÁTICA DA FONTE E DOWNLOAD
# ==============================================================================
# 9. VERIFICAÇÃO AUTOMÁTICA DA FONTE E DOWNLOAD
# ==============================================================================
# A cada execução o pipeline consulta a fonte oficial.
# Se ETag/Last-Modified/Content-Length não mudaram, mantém o cache local.
# Se houver alteração, baixa o novo ZIP, valida, extrai e somente então
# substitui o arquivo anterior. O SHA-256 funciona como validação adicional.
# ==============================================================================

def normalize_etag(value):
    if value is None:
        return None
    return str(value).strip().strip('\"')


def sha256_file(file_path, chunk_size=8 * 1024 * 1024):
    digest = hashlib.sha256()
    with open(file_path, 'rb') as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _tentar(func, descricao, tentativas=4, espera_inicial=5):
    """Repete chamadas de rede com espera crescente (5 s, 15 s, 45 s).
    'Connection reset by peer' e timeouts do S3 costumam ser transitórios."""
    import time as _t
    ultimo = None
    for n in range(1, tentativas + 1):
        try:
            return func()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            ultimo = e
            if n < tentativas:
                espera = espera_inicial * (3 ** (n - 1))
                print(f"[RETRY] {descricao}: tentativa {n}/{tentativas} falhou "
                      f"({type(e).__name__}); nova tentativa em {espera}s")
                _t.sleep(espera)
    raise ultimo


def _metadados_da_resposta(headers):
    content_length = headers.get('Content-Length')
    faixa = headers.get('Content-Range')          # "bytes 0-0/123456" no GET com Range
    if faixa and '/' in faixa:
        content_length = faixa.split('/')[-1]
    try:
        content_length = int(content_length) if content_length and content_length != '*' else None
    except Exception:
        content_length = None
    return {
        'etag': normalize_etag(headers.get('ETag')),
        'last_modified': headers.get('Last-Modified'),
        'content_length': content_length
    }


_FALHAS_SEGUIDAS = [0]   # anos seguidos sem resposta: evita esperar minutos em cada ano


def get_source_metadata(url):
    """Consulta metadados da fonte oficial sem baixar o arquivo.
    1) HEAD com novas tentativas; 2) se o HEAD for recusado, GET de 1 byte (Range)."""
    md = _get_source_metadata(url, rapido=_FALHAS_SEGUIDAS[0] >= 2)
    _FALHAS_SEGUIDAS[0] = 0 if md else _FALHAS_SEGUIDAS[0] + 1
    return md


def _get_source_metadata(url, rapido=False):
    try:
        r = _tentar(lambda: requests.head(url, headers=HEADERS, allow_redirects=True, timeout=60),
                    "HEAD " + url.rsplit('/', 1)[-1], tentativas=1 if rapido else 4)
        if 200 <= r.status_code < 300:
            return _metadados_da_resposta(r.headers)
        print(f"[WARN] HEAD {r.status_code} para {url}")
    except Exception as e:
        print(f"[WARN] HEAD falhou: {e}")

    try:
        h = dict(HEADERS); h['Range'] = 'bytes=0-0'
        def _get_1byte():
            resp = requests.get(url, headers=h, stream=True, allow_redirects=True, timeout=60)
            resp.close()
            return resp
        r = _tentar(_get_1byte, "GET(Range) " + url.rsplit('/', 1)[-1], tentativas=1 if rapido else 2)
        if r.status_code in (200, 206):
            print("[INFO] Metadados obtidos por GET parcial (HEAD indisponível).")
            return _metadados_da_resposta(r.headers)
        print(f"[WARN] GET parcial {r.status_code} para {url}")
    except Exception as e:
        print(f"[WARN] Não foi possível consultar metadados: {e}")
    return None


def localizar_cache(year, year_dir, target_csv, zip_path):
    """Procura o arquivo do ano já presente no volume (inclusive enviado à mão
    pelo Catalog Explorer) e, se só houver o .zip, extrai o CSV.
    Locais aceitos: <raw>/<ano>/, <raw>/ (raiz) e <raw>/historico/<ano>/."""
    if os.path.exists(target_csv) and os.path.getsize(target_csv) > 0:
        return True
    yy = str(year)[-2:]
    nomes_zip = [f"{PREFIXO_ARQUIVO}BR{yy}.csv.zip", f"{PREFIXO_ARQUIVO}BR{yy}.zip"]
    nome_csv = f"{PREFIXO_ARQUIVO}BR{yy}.csv"
    candidatos = []
    for pasta in (year_dir, RAW_VOLUME_PATH):
        candidatos += [os.path.join(pasta, n) for n in nomes_zip + [nome_csv]]
    hist = os.path.join(RAW_VOLUME_PATH, "historico", str(year))
    if os.path.isdir(hist):
        candidatos += sorted((os.path.join(hist, f) for f in os.listdir(hist) if f.lower().endswith('.zip')),
                             key=os.path.getmtime, reverse=True)
    for c in candidatos:
        if not os.path.isfile(c) or os.path.getsize(c) == 0:
            continue
        try:
            if c.lower().endswith('.csv'):
                if os.path.abspath(c) != os.path.abspath(target_csv):
                    shutil.copy2(c, target_csv)
            else:
                tmp = extract_csv_from_zip(c, target_csv, year)
                os.replace(tmp, target_csv)
                if os.path.abspath(c) != os.path.abspath(zip_path):
                    shutil.copy2(c, zip_path)
            print(f"[CACHE] {year}: arquivo local encontrado em {c}")
            return True
        except Exception as e:
            print(f"[WARN] {year}: {c} não pôde ser usado ({e})")
    return False


def get_latest_manifest_by_year():
    """Retorna o último manifesto conhecido de cada ano."""
    if not spark.catalog.tableExists(MANIFEST_TABLE):
        return {}

    manifest = spark.table(MANIFEST_TABLE)

    window_manifest = Window.partitionBy('year').orderBy(
        F.desc('downloaded_at')
    )

    rows = (
        manifest
        .withColumn('_rn', F.row_number().over(window_manifest))
        .filter(F.col('_rn') == 1)
        .drop('_rn')
        .collect()
    )

    return {int(row['year']): row.asDict() for row in rows}


def extract_csv_from_zip(zip_file_path, destination_csv, year):
    """Valida o ZIP e extrai o primeiro CSV para um arquivo temporário."""
    if not zipfile.is_zipfile(zip_file_path):
        raise Exception(f"Arquivo baixado não é um ZIP válido: {zip_file_path}")

    temp_csv = f"{destination_csv}.{RUN_ID}.part"

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as archive:
            bad_member = archive.testzip()
            if bad_member:
                raise Exception(
                    f"ZIP corrompido; membro inválido: {bad_member}"
                )

            csv_members = [
                member for member in archive.namelist()
                if member.lower().endswith('.csv')
                and not member.endswith('/')
            ]

            if not csv_members:
                raise Exception(
                    f"Nenhum CSV encontrado no ZIP de {year}."
                )

            # Preferir o CSV esperado; caso não exista, usar o primeiro CSV.
            expected_name = f"{PREFIXO_ARQUIVO}BR{str(year)[-2:]}.csv".lower()
            member = next(
                (m for m in csv_members if os.path.basename(m).lower() == expected_name),
                csv_members[0]
            )

            print(f"[EXTRACT] {year}: {member}")

            with archive.open(member) as source, open(temp_csv, 'wb') as destination:
                shutil.copyfileobj(source, destination, length=8 * 1024 * 1024)

        if not os.path.exists(temp_csv) or os.path.getsize(temp_csv) == 0:
            raise Exception(f"CSV extraído está vazio ou não foi criado: {temp_csv}")

        return temp_csv

    except Exception:
        if os.path.exists(temp_csv):
            os.remove(temp_csv)
        raise


class _RespostaFalsa:
    """Resposta HTTP simulada para os testes de auditabilidade (simular_falha)."""

    def __init__(self, conteudo, interromper=False):
        self.conteudo = conteudo
        self.interromper = interromper
        self.headers = {}

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=None):
        if self.interromper:
            yield self.conteudo[: max(1, len(self.conteudo) // 3)]
            raise requests.exceptions.ConnectionError("conexão interrompida (simulada)")
        yield self.conteudo


def _conteudo_com_falha(tipo, year, zip_path):
    import io
    if tipo in ("zip_truncado", "download_interrompido") and os.path.exists(zip_path):
        with open(zip_path, "rb") as f:
            original = f.read(64 * 1024 * 1024)
        return original[: len(original) // 2] if tipo == "zip_truncado" else original
    if tipo == "csv_vazio":
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr(f"{PREFIXO_ARQUIVO}BR{str(year)[-2:]}.csv", "")
        return buf.getvalue()
    return b"isto nao e um arquivo zip " * 4096  # zip_invalido (ou sem arquivo local)


def download_and_replace_source(year, url, year_dir, target_csv, zip_path, metadata):
    """Baixa para arquivo temporário e só substitui a fonte atual após validação."""
    temp_zip = f"{zip_path}.{RUN_ID}.part"

    if os.path.exists(temp_zip):
        os.remove(temp_zip)

    print(f"[DOWNLOAD] Baixando nova versão de {year}...")

    if SIMULAR_FALHA and SIMULAR_FALHA != "rede" and int(year) == SIMULAR_FALHA_ANO:
        print(f"[TESTE] Falha simulada no download de {year}: {SIMULAR_FALHA}")
        response = _RespostaFalsa(
            _conteudo_com_falha(SIMULAR_FALHA, year, zip_path),
            interromper=(SIMULAR_FALHA == "download_interrompido"),
        )
    else:
        response = _tentar(
            lambda: requests.get(url, headers=HEADERS, stream=True, timeout=300),
            f"download {year}"
        )
    response.raise_for_status()

    response_etag = normalize_etag(response.headers.get('ETag'))
    response_last_modified = response.headers.get('Last-Modified')
    response_content_length = response.headers.get('Content-Length')

    try:
        response_content_length = (
            int(response_content_length)
            if response_content_length
            else metadata.get('content_length')
        )
    except Exception:
        response_content_length = metadata.get('content_length')

    try:
        with open(temp_zip, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    file.write(chunk)

        if not os.path.exists(temp_zip) or os.path.getsize(temp_zip) == 0:
            raise Exception(f"Download vazio para {year}.")

        new_sha256 = sha256_file(temp_zip)

        if not zipfile.is_zipfile(temp_zip):
            raise Exception(f"ZIP inválido após download de {year}.")

        temp_csv = extract_csv_from_zip(
            temp_zip,
            target_csv,
            year
        )

        # Se o conteúdo é exatamente igual ao manifesto, não reprocessa.
        manifest_sha = metadata.get('sha256') if metadata else None
        if (
            manifest_sha
            and manifest_sha.lower() == new_sha256.lower()
            and os.path.exists(target_csv)
        ):
            os.remove(temp_csv)
            if os.path.exists(temp_zip):
                os.remove(temp_zip)
            print(f"[CACHE] {year}: SHA-256 igual ao último download.")
            return False, new_sha256, {
                'etag': response_etag or metadata.get('etag'),
                'last_modified': response_last_modified or metadata.get('last_modified'),
                'content_length': response_content_length
            }

        # Substituição atômica: o arquivo antigo permanece se algo falhar.
        os.replace(temp_csv, target_csv)
        os.replace(temp_zip, zip_path)

        if ARQUIVAR_VERSOES_FONTE:
            hist_dir = os.path.join(RAW_VOLUME_PATH, "historico", str(year))
            os.makedirs(hist_dir, exist_ok=True)
            shutil.copy2(
                zip_path,
                os.path.join(hist_dir, f"{PREFIXO_ARQUIVO}BR{str(year)[-2:]}_{RUN_ID}_{new_sha256[:12]}.csv.zip")
            )

        final_metadata = {
            'etag': response_etag or metadata.get('etag'),
            'last_modified': response_last_modified or metadata.get('last_modified'),
            'content_length': response_content_length
        }

        print(
            f"[UPDATE] Fonte {year} atualizada. "
            f"SHA-256: {new_sha256[:16]}..."
        )

        return True, new_sha256, final_metadata

    except Exception:
        if os.path.exists(temp_zip):
            os.remove(temp_zip)
        raise


# Cronômetro da checagem da fonte (HEAD + download quando houve mudança)
t_start_verificacao = time.perf_counter()

latest_manifest = get_latest_manifest_by_year()
downloaded_paths = []
changed_years = []
sources_updated = False
source_status = []

for year in SOURCE_YEARS:
    yy = str(year)[-2:]
    year_dir = os.path.join(RAW_VOLUME_PATH, str(year))
    os.makedirs(year_dir, exist_ok=True)

    target_csv = os.path.join(year_dir, f"{PREFIXO_ARQUIVO}BR{yy}.csv")
    zip_path = os.path.join(year_dir, f"{PREFIXO_ARQUIVO}BR{yy}.csv.zip")
    url = SOURCE_URL_TEMPLATE.format(yy=yy)

    previous = latest_manifest.get(year, {})
    local_exists = localizar_cache(year, year_dir, target_csv, zip_path)
    zip_exists = os.path.exists(zip_path)

    # Modo somente_gold: não consulta a internet; só registra os anos que já
    # estão no volume (a Gold usa essa lista e o manifesto para o snapshot).
    if SOMENTE_GOLD:
        if local_exists:
            downloaded_paths.append((year, target_csv))
            source_status.append((year, 'SOMENTE_GOLD_USOU_CACHE'))
        else:
            source_status.append((year, 'SOMENTE_GOLD_SEM_ARQUIVO_LOCAL'))
        continue

    print(f"\n[CHECK] Verificando fonte oficial de {year}...")

    if SIMULAR_FALHA == "rede" and year == SIMULAR_FALHA_ANO:
        print(f"[TESTE] Falha de rede simulada na consulta de {year}.")
        metadata = None
    else:
        metadata = get_source_metadata(url)

    # --------------------------------------------------------------------------
    # Fonte indisponível
    # --------------------------------------------------------------------------
    if metadata is None:
        if local_exists:
            print(
                f"[WARN] Fonte {year} indisponível agora. "
                f"Mantendo arquivo local existente."
            )
            downloaded_paths.append((year, target_csv))
            source_status.append((year, 'INDISPONIVEL_MANTEVE_CACHE'))
        else:
            print(
                f"[SKIP] Fonte {year} indisponível e não existe cache local."
            )
            source_status.append((year, 'INDISPONIVEL_SEM_CACHE'))
        continue

    # --------------------------------------------------------------------------
    # Arquivo não existe localmente -> download obrigatório
    # --------------------------------------------------------------------------
    needs_download = not local_exists

    # --------------------------------------------------------------------------
    # Arquivo existe: compara metadados com o último manifesto
    # --------------------------------------------------------------------------
    if local_exists and previous:
        previous_etag = normalize_etag(previous.get('etag'))
        previous_last_modified = previous.get('last_modified')
        previous_length = previous.get('content_length')

        comparable_fields = []

        if metadata.get('etag') and previous_etag:
            comparable_fields.append(
                metadata.get('etag') == previous_etag
            )

        if metadata.get('last_modified') and previous_last_modified:
            comparable_fields.append(
                metadata.get('last_modified') == previous_last_modified
            )

        if metadata.get('content_length') is not None and previous_length is not None:
            comparable_fields.append(
                int(metadata.get('content_length')) == int(previous_length)
            )

        if comparable_fields:
            # Se qualquer metadado relevante mudou, considera atualização.
            metadata_changed = not all(comparable_fields)
            needs_download = metadata_changed

            if not metadata_changed:
                print(
                    f"[CACHE] {year}: ETag/Last-Modified/tamanho "
                    f"compatíveis com o manifesto."
                )
        else:
            # Sem metadados comparáveis, usa SHA-256 local quando possível.
            if zip_exists and previous.get('sha256'):
                local_sha = sha256_file(zip_path)
                if local_sha.lower() == str(previous['sha256']).lower():
                    print(f"[CACHE] {year}: SHA-256 local confere com manifesto.")
                    needs_download = False
                else:
                    print(f"[UPDATE] {year}: SHA-256 local diferente do manifesto.")
                    needs_download = True
            else:
                print(f"[CHECK] {year}: metadados insuficientes; será baixado para conferência.")
                needs_download = True

    elif local_exists and not previous:
        # Arquivo local antigo sem manifesto: baixa uma vez para estabelecer a
        # referência confiável no manifesto.
        print(
            f"[CHECK] {year}: existe cache, mas não há manifesto. "
            f"Baixando para criar referência."
        )
        needs_download = True

    if SIMULAR_FALHA and SIMULAR_FALHA != "rede" and year == SIMULAR_FALHA_ANO:
        needs_download = True  # força o download (com falha) deste ano

    # --------------------------------------------------------------------------
    # Download somente quando necessário
    # --------------------------------------------------------------------------
    if needs_download:
        try:
            changed, new_sha256, final_metadata = download_and_replace_source(
                year,
                url,
                year_dir,
                target_csv,
                zip_path,
                previous if previous else {}
            )

            if changed:
                changed_years.append(year)
                sources_updated = True

            # Grava o manifesto mesmo quando o SHA-256 não mudou (evita
            # re-download eterno quando só o ETag/Last-Modified muda).
            manifest_row = [
                (
                    RUN_ID,
                    int(year),
                    url,
                    final_metadata.get('etag'),
                    final_metadata.get('last_modified'),
                    final_metadata.get('content_length'),
                    new_sha256,
                    datetime.now(timezone.utc)
                )
            ]

            manifest_df = spark.createDataFrame(
                manifest_row,
                schema=(
                    'execution_id string, year int, url string, '
                    'etag string, last_modified string, '
                    'content_length long, sha256 string, downloaded_at timestamp'
                )
            )

            (
                manifest_df.write
                .format('delta')
                .mode('append')
                .saveAsTable(MANIFEST_TABLE)
            )

            source_status.append(
                (year, 'ATUALIZADO' if changed else 'SEM_ALTERACAO_SHA256')
            )

        except Exception as e:
            # Nunca destrói o arquivo antigo se o novo download falhar.
            if local_exists:
                print(
                    f"[WARN] Falha ao atualizar {year}: {e}. "
                    f"Mantendo arquivo local anterior."
                )
                source_status.append((year, 'ERRO_MANTEVE_CACHE'))
            else:
                print(
                    f"[WARN] Falha ao baixar {year}: {e}. "
                    f"Ano será ignorado nesta execução."
                )
                source_status.append((year, 'ERRO_SEM_CACHE'))
                continue

    # --------------------------------------------------------------------------
    # Garantir que o CSV local esteja disponível para o processamento
    # --------------------------------------------------------------------------
    if os.path.exists(target_csv):
        downloaded_paths.append((year, target_csv))
        print(f"[OK] Fonte {year} disponível para o ETL.")


# ------------------------------------------------------------------------------
# Fonte já processada POR ESTE SCHEMA. O download é compartilhado por todas as
# UFs do agravo; se outra UF (ou um experimento) baixou a versão nova, o
# manifesto já está atualizado e o ETag "não mudou" para esta execução. Por isso
# cada schema guarda o SHA-256 da versão que entrou na sua Silver e reprocessa os
# anos em que o SHA atual for diferente.
# ------------------------------------------------------------------------------
PROCESSADO_TABLE = f"{CATALOG}.{SCHEMA}.fonte_processada"


def sha_atual_por_ano():
    manif = get_latest_manifest_by_year()
    return {int(y): (manif.get(int(y)) or {}).get("sha256") for y, _ in downloaded_paths}


if not SOMENTE_GOLD and spark.catalog.tableExists(PROCESSADO_TABLE):
    _proc = {int(r["year"]): r["sha256"] for r in spark.table(PROCESSADO_TABLE).collect()}
    for _ano, _sha in sha_atual_por_ano().items():
        if _sha and _proc.get(_ano) != _sha and _ano not in changed_years:
            print(f"[CHECK] {_ano}: versão da fonte diferente da que este schema processou.")
            changed_years.append(_ano)
            sources_updated = True

if not downloaded_paths:
    raise Exception(
        'Nenhuma fonte do SINAN/OpenDataSUS está disponível para processamento.'
    )

print("\n" + "=" * 90)
print("VERIFICAÇÃO DAS FONTES CONCLUÍDA")
print("=" * 90)
print(f"Anos disponíveis : {[year for year, _ in downloaded_paths]}")
print(f"Anos atualizados  : {changed_years if changed_years else 'Nenhum'}")
print(f"Houve atualização : {'SIM' if sources_updated else 'NÃO'}")
print("Status por ano:")
for year, status in source_status:
    print(f"  {year}: {status}")
print("=" * 90)

# Se nenhuma fonte mudou, normalmente não há necessidade de reprocessar Bronze/Silver/Gold.
# EXCEÇÃO: se o pipeline estiver sendo executado pela primeira vez, ou se alguma
# tabela principal tiver sido apagada, precisamos reconstruir as camadas.
required_tables = [
    BRONZE_TABLE,
    SILVER_TABLE,
    GOLD_WEEKLY,
    GOLD_INDICATORS
]
missing_required_tables = [
    table_name
    for table_name in required_tables
    if not spark.catalog.tableExists(table_name)
]

if SOMENTE_GOLD:
    # Só recalcula a Gold: exige Bronze/Silver prontas e nunca as regrava.
    if not spark.catalog.tableExists(SILVER_TABLE):
        raise Exception(
            f"Modo 'somente_gold' exige a Silver pronta ({SILVER_TABLE}). "
            "Rode uma vez em modo 'completo' antes."
        )
    sources_updated = True
    changed_years = []
    print(
        "[SOMENTE_GOLD] Bronze/Silver preservadas; a Gold será recalculada "
        f"(semanas consecutivas = {SEMANAS_CONSECUTIVAS_ALERTA}, "
        f"anos excluídos do canal = {ANOS_EXCLUIDOS_CANAL or 'nenhum'})."
    )

elif missing_required_tables:
    print(
        "[INIT] Tabelas ausentes detectadas: "
        f"{missing_required_tables}. Reconstrução necessária."
    )
    sources_updated = True
    if not changed_years:
        # Na primeira execução, todos os arquivos locais disponíveis precisam
        # ser carregados na Bronze/Silver.
        changed_years = [year for year, _ in downloaded_paths]

def versao_da_tabela(table_name):
    try:
        props = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]["properties"]
        return (props or {}).get("pipeline.versao")
    except Exception:
        return None


if spark.catalog.tableExists(SILVER_TABLE) and not SOMENTE_GOLD:
    versao_silver = versao_da_tabela(SILVER_TABLE)
    if versao_silver != PIPELINE_VERSION:
        print(
            f"[VERSÃO] Silver gerada pela versão {versao_silver!r}; código atual é "
            f"{PIPELINE_VERSION!r}. Reprocessando todos os anos com as regras novas."
        )
        sources_updated = True
        changed_years = [year for year, _ in downloaded_paths]

if FORCAR_REPROCESSAMENTO and not SOMENTE_GOLD:
    print(
        "[FORÇAR] Widget 'forcar_reprocessamento' = 'sim'. "
        "Reprocessando todos os anos disponíveis, independente de mudança na fonte."
    )
    sources_updated = True
    changed_years = [year for year, _ in downloaded_paths]

# ------------------------------------------------------------------------------
# Simulação de atualização incremental (experimento de custo): age como se só
# SIMULAR_ANO_ALTERADO tivesse mudado na fonte. Representa a atualização semanal
# do arquivo do ano corrente, detectada pelo ETag/SHA-256.
# ------------------------------------------------------------------------------
_anos_disponiveis_local = [year for year, _ in downloaded_paths]
if (
    SIMULAR_ANO_ALTERADO is not None
    and not SOMENTE_GOLD
    and not FORCAR_REPROCESSAMENTO
    and not missing_required_tables
):
    if SIMULAR_ANO_ALTERADO in _anos_disponiveis_local:
        print(f"[SIMULAÇÃO] Tratando {SIMULAR_ANO_ALTERADO} como o único ano alterado na fonte.")
        sources_updated = True
        changed_years = [SIMULAR_ANO_ALTERADO]
    else:
        print(f"[SIMULAÇÃO][WARN] {SIMULAR_ANO_ALTERADO} não está entre os anos disponíveis.")

# Métrica da verificação da fonte (inclui o download, se houve). Registrada
# ANTES da saída antecipada: é o custo diário de "checar e não fazer nada".
_baixados = [y for y, s in source_status if s == 'ATUALIZADO']
registrar_metrica(
    RUN_ID, "verificacao_fonte", time.perf_counter() - t_start_verificacao,
    modo=None if sources_updated else "sem_atualizacao",
    observacao=f"anos verificados={len(SOURCE_YEARS)}; baixados={_baixados or 'nenhum'}",
)

# Se nenhuma fonte mudou, não há necessidade de reprocessar Bronze/Silver/Gold.
# Isso evita custo desnecessário quando o Job é executado diariamente ou de hora
# em hora. O notebook encerra com sucesso e o dashboard continua usando a
# última versão válida das tabelas Delta.
if not sources_updated:
    print(
        "[NO UPDATE] Nenhuma alteração detectada na fonte oficial. "
        "Bronze, Silver e Gold permanecem na última versão processada."
    )
    print(f"[INFO] Execução encerrada: {RUN_ID}")
    try:
        display(
            spark.table(MANIFEST_TABLE)
            .orderBy(F.desc('downloaded_at'))
            .limit(20)
        )
    except Exception:
        pass
    import json as _json_exit
    dbutils.notebook.exit(_json_exit.dumps({
        "status": "NO_UPDATE", "run_id": RUN_ID, "schema": SCHEMA,
        "fontes": {str(a): st for a, st in source_status},
    }))

print(
    f"[UPDATE] Alterações detectadas nos anos: {changed_years}. "
    "Bronze/Silver serão atualizadas somente nesses anos; "
    "a Gold será recalculada integralmente a partir de toda a Silver."
)

# COMMAND ----------

# DBTITLE 1,Iniciar cronômetro da camada Bronze
t_start_bronze = time.perf_counter()

# COMMAND ----------

# DBTITLE 1,10. CAMADA BRONZE
# 10. CAMADA BRONZE
# ==============================================================================
# IMPORTANTE: somente os anos que mudaram são regravados na Bronze.
# O modo de overwrite dinâmico preserva as partições dos demais anos.
# ==============================================================================

# A escrita incremental abaixo usa Delta replaceWhere, que restringe
# explicitamente o overwrite às partições dos anos alterados.

print("\n[BRONZE] Iniciando processamento incremental...")
print(f"[BRONZE] Anos a processar: {changed_years}")


bronze_dfs = []


# Somente os anos alterados entram na escrita incremental da Bronze.
bronze_processing_paths = [
    (year, csv_path)
    for year, csv_path in downloaded_paths
    if year in changed_years
]

for year, csv_path in bronze_processing_paths:

    print(
        f"[BRONZE] Lendo arquivo atualizado {year}..."
    )


    df_raw = (
        spark.read

        .option(
            "header",
            "true"
        )

        .option(
            "sep",
            ","
        )

        .option(
            "inferSchema",
            "false"
        )

        .option(
            "encoding",
            "ISO-8859-1"
        )

        .option(
            "mode",
            "DROPMALFORMED"
        )

        .csv(csv_path)
    )


    # --------------------------------------------------------------------------
    # Padronização dos nomes
    # --------------------------------------------------------------------------

    for column_name in df_raw.columns:

        normalized = (
            column_name
            .strip()
            .upper()
        )

        if column_name != normalized:

            df_raw = df_raw.withColumnRenamed(
                column_name,
                normalized
            )


    # --------------------------------------------------------------------------
    # Metadados de ingestão
    # --------------------------------------------------------------------------

    df_meta = (
        df_raw

        .withColumn(
            "_source_year",
            F.lit(year)
        )

        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )

        .withColumn(
            "_run_id",
            F.lit(RUN_ID)
        )
    )


    bronze_dfs.append(
        df_meta
    )

# COMMAND ----------

# DBTITLE 1,11. UNIÃO DOS ANOS
# ==============================================================================
# 11. UNIÃO DOS ANOS
# ==============================================================================

from functools import reduce


if not bronze_dfs and not SOMENTE_GOLD:

    _sem = [a for a, st in source_status if st == 'INDISPONIVEL_SEM_CACHE']
    if _sem:
        raise Exception(
            f"A fonte do OpenDataSUS recusou a conexão e não há cópia local para os anos {_sem}. "
            f"Isso é falha de rede (servidor do Ministério/AWS ou bloqueio de saída do Databricks), "
            f"não erro de código. Soluções: (1) rode de novo mais tarde; ou (2) baixe os arquivos "
            f"{PREFIXO_ARQUIVO}BRaa.csv.zip no seu computador ({SOURCE_URL_TEMPLATE}) e envie-os "
            f"pelo Catalog Explorer para {RAW_VOLUME_PATH}/<ano>/ — o pipeline extrai o zip sozinho."
        )
    raise Exception(
        "Nenhum arquivo alterado foi carregado para a Bronze. "
        "Verifique changed_years e os arquivos locais."
    )


if bronze_dfs:
    df_bronze_union = reduce(
        lambda df1, df2:
            df1.unionByName(
                df2,
                allowMissingColumns=True
            ),
        bronze_dfs
    )

# COMMAND ----------

# DBTITLE 1,12. SALVAR BRONZE
# ==============================================================================
# 12. SALVAR BRONZE
# ==============================================================================

print(
    "[BRONZE] Gravando somente as partições dos anos alterados..."
)

# ------------------------------------------------------------------------------
# Escrita segura e realmente incremental:
# replaceWhere substitui somente as partições dos anos alterados.
# As partições dos demais anos permanecem intactas.
# ------------------------------------------------------------------------------
bronze_replace_where = "_source_year IN ({})".format(
    ",".join(str(int(y)) for y in changed_years) or "NULL"
)

if not bronze_dfs:
    print("[BRONZE] Modo somente_gold: nada a gravar.")

elif spark.catalog.tableExists(BRONZE_TABLE) and not FORCAR_REPROCESSAMENTO:
    (
        df_bronze_union
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", bronze_replace_where)
        # Novos arquivos do SINAN às vezes trazem colunas extras; sem
        # mergeSchema a escrita falha com erro de schema.
        .option("mergeSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )
else:
    # Primeira carga OU reprocessamento forçado: reescreve a Bronze inteira só com
    # os anos disponíveis agora (no experimento de escalabilidade o número de
    # anos muda entre rodadas e não pode sobrar ano de rodada anterior).
    (
        df_bronze_union
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_source_year")
        .saveAsTable(BRONZE_TABLE)
    )


print(
    "[BRONZE] Concluída."
)

# COMMAND ----------

# DBTITLE 1,13. CARREGAR BRONZE
# ==============================================================================
# 13. CARREGAR BRONZE
# ==============================================================================

df_bronze = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("_source_year").isin([int(y) for y in changed_years]))
)

cols = df_bronze.columns

# COMMAND ----------

# DBTITLE 1,Registrar métrica real da camada Bronze
t_bronze_duration = time.perf_counter() - t_start_bronze
registros_bronze = df_bronze.count()

registrar_metrica(RUN_ID, "bronze", t_bronze_duration, registros_bronze)

# COMMAND ----------

# DBTITLE 1,Iniciar cronômetro da camada Silver
t_start_silver = time.perf_counter()

# COMMAND ----------

# DBTITLE 1,14. IDENTIFICAR COLUNAS
# ==============================================================================
# 14. IDENTIFICAR COLUNAS
# ==============================================================================

# first_existing_column: definida em config_agravo.


uf_col = first_existing_column(
    cols,
    [
        "SG_UF_NOT",
        "SG_UF"
    ]
)


mun_col = first_existing_column(
    cols,
    [
        "ID_MN_RESI",
        "ID_MUNICIP"
    ]
)


date_col = first_existing_column(
    cols,
    [
        "DT_NOTIFIC",
        "DT_SIN_PRI"
    ]
)


print(
    f"[INFO] Coluna UF       : {uf_col}"
)

print(
    f"[INFO] Coluna município: {mun_col}"
)

print(
    f"[INFO] Coluna data     : {date_col}"
)

# COMMAND ----------

# DBTITLE 1,15. RESIDENTES NO BRASIL (SEM FILTRO DE ESTADO)
# ==============================================================================
# 15. RESIDENTES NO BRASIL (TODAS AS UFs – NENHUM FILTRO DE ESTADO)
# ==============================================================================
# Ficam TODOS os registros com residência em qualquer uma das 27 UFs, pelo
# município de residência (ID_MN_RESI) ou, quando ele estiver vazio, pela UF de
# residência (SG_UF). Saem apenas os registros sem residência informada ou com
# residência no exterior, que não têm população de referência no IBGE.
# ==============================================================================

res_mun_col = first_existing_column(cols, ["ID_MN_RESI"])
res_uf_col = first_existing_column(cols, ["SG_UF"])

condition = F.lit(False)

mun_digits = None
if res_mun_col:
    mun_digits = F.regexp_replace(
        F.trim(F.col(res_mun_col).cast("string")), "[^0-9]", ""
    )
    condition = condition | mun_digits.substr(1, 2).isin(*PREFIXOS_UF)

if res_uf_col:
    uf_residencia = F.upper(F.trim(F.col(res_uf_col).cast("string")))
    uf_match = uf_residencia.isin(*PREFIXOS_UF, *SIGLAS_UF)
    if mun_digits is not None:
        # Só usa a UF quando o município de residência não foi informado
        condition = condition | (
            (F.col(res_mun_col).isNull() | (mun_digits == "")) & uf_match
        )
    else:
        condition = condition | uf_match

if not res_mun_col and not res_uf_col:
    raise Exception(
        "Nenhuma coluna de residência (ID_MN_RESI / SG_UF) encontrada na Bronze."
    )

df_residentes = df_bronze.filter(condition)

print(
    f"[SILVER] Filtro de {UF_NOME} por residência aplicado "
    f"({'todas as UFs; exclui só residência ignorada ou no exterior' if ESCOPO_NACIONAL else 'UF única'}) "
    f"(município: {res_mun_col}, UF: {res_uf_col})."
)

# --- Remoção de registros duplicados --------------------------------------------
# Duplicata = linha idêntica em TODAS as colunas originais do SINAN (a coluna
# _source_year entra na chave, então o mesmo registro em arquivos de anos
# diferentes não é fundido). Só os metadados de ingestão são ignorados. A regra
# é conservadora: não tenta adivinhar duplicatas "quase iguais".
colunas_chave_dedup = [
    c for c in df_residentes.columns
    if c not in ("_ingestion_timestamp", "_run_id")
]

n_antes_dedup = df_residentes.count()
df_residentes = df_residentes.dropDuplicates(colunas_chave_dedup)
n_depois_dedup = df_residentes.count()
n_duplicados_removidos = n_antes_dedup - n_depois_dedup

print(
    f"[SILVER] Duplicados exatos removidos: {n_duplicados_removidos:,} "
    f"({(n_duplicados_removidos / n_antes_dedup * 100 if n_antes_dedup else 0):.3f}% "
    f"de {n_antes_dedup:,} registros de {UF_NOME})."
)

# COMMAND ----------

# DBTITLE 1,18. IDENTIFICAÇÃO DAS COLUNAS DA SILVER
# ==============================================================================
# 18. IDENTIFICAÇÃO DAS COLUNAS DA SILVER
# ==============================================================================

cols = df_residentes.columns

date_col = first_existing_column(
    cols,
    ["DT_NOTIFIC", "DT_SIN_PRI"]
)

date_symptom_col = first_existing_column(
    cols,
    ["DT_SIN_PRI", "DT_NOTIFIC"]
)

mun_col = first_existing_column(
    cols,
    ["ID_MN_RESI", "ID_MUNICIP"]
)

uf_col = first_existing_column(
    cols,
    ["SG_UF_NOT", "SG_UF"]
)

class_col = first_existing_column(
    cols,
    ["CLASSI_FIN"]
)

evolution_col = first_existing_column(
    cols,
    ["EVOLUCAO"]
)

week_col = first_existing_column(
    cols,
    ["SEM_PRI", "SEM_NOT"]
)

sex_col = first_existing_column(
    cols,
    ["CS_SEXO", "SEXO"]
)

age_col = first_existing_column(
    cols,
    ["NU_IDADE_N", "NU_IDADE"]
)

age_type_col = first_existing_column(
    cols,
    ["TP_IDADE"]
)

hospital_col = first_existing_column(
    cols,
    ["HOSPITALIZ"]
)

race_col = first_existing_column(
    cols,
    ["CS_RACA"]
)

criterion_col = first_existing_column(
    cols,
    ["CRITERIO"]
)

print("=" * 80)
print("[SILVER] COLUNAS IDENTIFICADAS")
print("=" * 80)
print(f"Data notificação       : {date_col}")
print(f"Data sintomas          : {date_symptom_col}")
print(f"Município residência   : {mun_col}")
print(f"UF                     : {uf_col}")
print(f"Classificação final    : {class_col}")
print(f"Evolução               : {evolution_col}")
print(f"Semana epidemiológica  : {week_col}")
print(f"Sexo                   : {sex_col}")
print(f"Idade                  : {age_col}")
print(f"Tipo da idade          : {age_type_col}")
print(f"Hospitalização         : {hospital_col}")
print(f"Raça/cor               : {race_col}")
print(f"Critério confirmação   : {criterion_col}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,19. CONSTRUÇÃO DA SILVER
# ==============================================================================
# 19. CONSTRUÇÃO DA SILVER
# ==============================================================================

df_silver = (
    df_residentes

    .withColumn(
        "DT_NOTIFIC_Parsed",
        parse_date_flexible(
            df_residentes,
            date_col
        )
    )

    .withColumn(
        "DT_SIN_PRI_Parsed",
        parse_date_flexible(
            df_residentes,
            date_symptom_col
        )
    )

    .withColumn(
        "CLASSI_FIN_Int",
        safe_int(class_col)
        if class_col
        else F.lit(None).cast(IntegerType())
    )

    .withColumn(
        "EVOLUCAO_Int",
        safe_int(evolution_col)
        if evolution_col
        else F.lit(None).cast(IntegerType())
    )

    .withColumn(
        "SEM_PRI_Int",
        safe_int(week_col)
        if week_col
        else F.lit(None).cast(IntegerType())
    )

    # SINAN ID_MN_RESI é código IBGE de 6 dígitos (às vezes vem com 7).
    # CORREÇÃO: antes usava lpad(..., 7) + substring(1, 6), o que transformava
    # "310620" (BH) em "031062" e quebrava o cruzamento com o IBGE.
    # Agora: códigos de 7 dígitos perdem o dígito verificador; os de 6 ficam iguais.
    .withColumn(
        "ID_MN_RESI_CODIGO",
        F.when(
            F.length(
                F.regexp_replace(F.trim(F.col(mun_col).cast("string")), "[^0-9]", "")
            ) >= 6
            if mun_col
            else F.lit(False),
            F.substring(
                F.lpad(
                    F.regexp_replace(
                        F.trim(F.col(mun_col).cast("string")),
                        "[^0-9]",
                        ""
                    ),
                    6,
                    "0"
                ),
                1,
                6
            )
        ).otherwise(F.lit(None).cast("string"))
        if mun_col
        else F.lit(None).cast("string")
    )

    .withColumn(
        "CS_SEXO_PADRAO",
        F.when(
            F.col(sex_col).isNull()
            if sex_col
            else F.lit(True),
            "I"
        )
        .otherwise(
            F.upper(
                F.trim(
                    F.col(sex_col).cast("string")
                )
            )
        )
        if sex_col
        else F.lit("I")
    )

    .withColumn(
        "CS_RACA_PADRAO",
        F.upper(
            F.trim(
                F.col(race_col).cast("string")
            )
        )
        if race_col
        else F.lit("9")
    )

    .withColumn(
        "CRITERIO_Int",
        safe_int(criterion_col)
        if criterion_col
        else F.lit(None).cast(IntegerType())
    )

    .withColumn(
        "HOSPITALIZ_Int",
        safe_int(hospital_col)
        if hospital_col
        else F.lit(None).cast(IntegerType())
    )
)

# COMMAND ----------

# DBTITLE 1,20. IDADE CORRETAMENTE DECODIFICADA
# ==============================================================================
# 20. IDADE CORRETAMENTE DECODIFICADA
#
# A variável NU_IDADE_N do SINAN usa o primeiro dígito para indicar a unidade:
# 1 = hora
# 2 = dia
# 3 = mês
# 4 = ano
#
# Quando TP_IDADE existe, ele é usado como fonte principal.
# ==============================================================================

if age_col:

    idade_raw = safe_int(age_col)

    idade_unit_from_code = (
        F.when(
            F.abs(idade_raw).between(100, 999),
            F.floor(
                F.abs(idade_raw) / F.lit(100)
            )
        )
        .otherwise(
            F.floor(
                F.abs(idade_raw) / F.lit(1000)
            )
        )
        .cast("int")
    )

    idade_value_from_code = (
        F.when(
            (
                F.abs(idade_raw).between(100, 999)
                &
                F.floor(F.abs(idade_raw) / F.lit(100)).isin(1, 2, 3, 4)
            ),
            F.abs(idade_raw) % F.lit(100)
        )
        .when(
            F.abs(idade_raw) >= 1000,
            F.abs(idade_raw) % F.lit(1000)
        )
        .otherwise(
            F.abs(idade_raw)
        )
        .cast("double")
    )

    if age_type_col:

        idade_type = safe_int(age_type_col)

        idade_unit = F.when(
            idade_type.isin(1, 2, 3, 4),
            idade_type
        ).otherwise(
            idade_unit_from_code
        )

    else:

        idade_unit = idade_unit_from_code

    df_silver = (
        df_silver

        .withColumn(
            "idade_tipo",
            idade_unit
        )

        .withColumn(
            "idade_valor",
            idade_value_from_code
        )

        .withColumn(
            "idade_anos",
            F.when(
                F.col("idade_tipo") == 4,
                F.col("idade_valor")
            )
            .when(
                F.col("idade_tipo") == 3,
                F.col("idade_valor") / 12.0
            )
            .when(
                F.col("idade_tipo") == 2,
                F.col("idade_valor") / 365.25
            )
            .when(
                F.col("idade_tipo") == 1,
                F.col("idade_valor") / 8766.0
            )
        )
    )

else:

    df_silver = (
        df_silver
        .withColumn(
            "idade_tipo",
            F.lit(None).cast("int")
        )
        .withColumn(
            "idade_valor",
            F.lit(None).cast("double")
        )
        .withColumn(
            "idade_anos",
            F.lit(None).cast("double")
        )
    )

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,21. VARIÁVEIS TEMPORAIS
# ==============================================================================
# 21. VARIÁVEIS TEMPORAIS
# ==============================================================================

df_silver = (
    df_silver

    .withColumn(
        "ano_notificacao",
        F.year("DT_NOTIFIC_Parsed")
    )

    .withColumn(
        "mes_notificacao",
        F.month("DT_NOTIFIC_Parsed")
    )

    .withColumn(
        "mes_nome",
        F.date_format(
            "DT_NOTIFIC_Parsed",
            "MMMM"
        )
    )

    .withColumn(
        "ano_semana",
        F.coalesce(
            F.year("DT_SIN_PRI_Parsed"),
            F.year("DT_NOTIFIC_Parsed"),
            F.col("_source_year").cast("int")
        )
    )
    .withColumn(
        "SEM_PRI_Int",
        F.coalesce(
            F.col("SEM_PRI_Int"),
            F.when(
                F.col("DT_SIN_PRI_Parsed").isNotNull(),
                F.weekofyear("DT_SIN_PRI_Parsed")
            ),
            F.when(
                F.col("DT_NOTIFIC_Parsed").isNotNull(),
                F.weekofyear("DT_NOTIFIC_Parsed")
            )
        ).cast("int")
    )
)

# COMMAND ----------

# DBTITLE 1,22. INDICADORES DERIVADOS
# ==============================================================================
# 22. INDICADORES DERIVADOS
# ==============================================================================

# Classificação oficial do SINAN:
# 5  = Descartado
# 10 = Dengue
# 11 = Dengue com sinais de alarme
# 12 = Dengue grave
# 13 = Chikungunya
# Para chikungunya e zika os códigos vêm de CFG_AGRAVO (widget "agravo").
# O rótulo de cada código também vem de lá; para dengue é exatamente o mesmo
# texto das versões anteriores (inclusive 8 = Inconclusivo).

EXPR_CLASSIFICACAO = None
for _cod, _rot in CFG_AGRAVO["rotulos"].items():
    _cond = F.col("CLASSI_FIN_Int") == _cod
    EXPR_CLASSIFICACAO = (
        F.when(_cond, _rot) if EXPR_CLASSIFICACAO is None
        else EXPR_CLASSIFICACAO.when(_cond, _rot)
    )
EXPR_CLASSIFICACAO = EXPR_CLASSIFICACAO.otherwise("Não informado")

df_silver = (
    df_silver

    .withColumn(
        "flag_notificado",
        F.lit(1)
    )

    .withColumn(
        "flag_confirmado",
        F.when(
            F.col("CLASSI_FIN_Int").isin(*CFG_AGRAVO["confirmado"]),
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_dengue_classica",
        F.when(
            F.col("CLASSI_FIN_Int") == 10,
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_sinais_alarme",
        F.when(
            F.col("CLASSI_FIN_Int") == 11,
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_dengue_grave",
        F.when(
            F.col("CLASSI_FIN_Int") == 12,
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_descartado",
        F.when(
            F.col("CLASSI_FIN_Int").isin(*CFG_AGRAVO["descartado"]),
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_chikungunya",
        F.when(
            F.col("CLASSI_FIN_Int") == 13,
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_obito",
        F.when(
            F.col("EVOLUCAO_Int") == 2,
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_obito_outras_causas",
        F.when(
            F.col("EVOLUCAO_Int") == 3,
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_obito_investigacao",
        F.when(
            F.col("EVOLUCAO_Int") == 4,
            1
        ).otherwise(0)
    )

    .withColumn(
        "flag_hospitalizado",
        F.when(
            F.col("HOSPITALIZ_Int") == 1,
            1
        )
        .when(
            F.upper(
                F.trim(
                    F.col(hospital_col).cast("string")
                )
            ).isin("SIM", "S"),
            1
        )
        .otherwise(0)
        if hospital_col
        # Arquivo sem a coluna HOSPITALIZ (ex.: zika): a informação NÃO existe,
        # então o indicador fica nulo (e não zero) para não parecer "0 internações".
        else F.lit(None).cast("int")
    )

    .withColumn("classificacao_nome", EXPR_CLASSIFICACAO)

    .withColumn(
        "sexo_nome",
        F.when(
            F.col("CS_SEXO_PADRAO") == "M",
            "Masculino"
        )
        .when(
            F.col("CS_SEXO_PADRAO") == "F",
            "Feminino"
        )
        .otherwise("Ignorado")
    )

    .withColumn(
        "faixa_etaria",
        F.when(
            F.col("idade_anos").isNull(),
            "Ignorada"
        )
        .when(
            F.col("idade_anos") < 1,
            "Menor de 1 ano"
        )
        .when(
            F.col("idade_anos") < 5,
            "01-04 anos"
        )
        .when(
            F.col("idade_anos") < 10,
            "05-09 anos"
        )
        .when(
            F.col("idade_anos") < 20,
            "10-19 anos"
        )
        .when(
            F.col("idade_anos") < 30,
            "20-29 anos"
        )
        .when(
            F.col("idade_anos") < 40,
            "30-39 anos"
        )
        .when(
            F.col("idade_anos") < 50,
            "40-49 anos"
        )
        .when(
            F.col("idade_anos") < 60,
            "50-59 anos"
        )
        .when(
            F.col("idade_anos") < 70,
            "60-69 anos"
        )
        .when(
            F.col("idade_anos") < 80,
            "70-79 anos"
        )
        .otherwise("80 anos ou mais")
    )
)

# COMMAND ----------

# DBTITLE 1,22b. PADRONIZAÇÃO CLÍNICA (sintomas, comorbidades, sinais de alarme)
# ==============================================================================
# 22b. PADRONIZAÇÃO CLÍNICA E DATA DE DISPONIBILIDADE DO REGISTRO
# ==============================================================================
# Os campos clínicos do SINAN vêm como texto "1" (sim) / "2" (não) / outros
# (ignorado). Aqui viram indicadores 1 / 0 / NULL com nome padronizado
# (sint_febre, comorb_diabetes, alarme_sangramento...). NULL = não informado,
# e NÃO é tratado como "não": isso mantém a prevalência correta.
#
# DT_DISPONIVEL = quando o registro passa a existir na base: o maior entre a
# data de digitação (DT_DIGITA) e a de notificação. Se DT_DIGITA não existir
# no arquivo, usa a notificação. É a base do nowcasting (seção 39.5c).
# ==============================================================================

def flag_sim_nao(coluna):
    valor = F.trim(F.col(coluna).cast("string"))
    return F.when(valor == "1", F.lit(1)).when(valor == "2", F.lit(0))


for _nome, _col in SINTOMAS.items():
    if _col in df_silver.columns:
        df_silver = df_silver.withColumn(f"sint_{_nome}", flag_sim_nao(_col))

for _nome, _col in COMORBIDADES.items():
    if _col in df_silver.columns:
        df_silver = df_silver.withColumn(f"comorb_{_nome}", flag_sim_nao(_col))

for _nome, _col in SINAIS_ALARME.items():
    if _col in df_silver.columns:
        df_silver = df_silver.withColumn(f"alarme_{_nome}", flag_sim_nao(_col))

# Gestação: 1-4 = trimestres/idade gestacional ignorada, 5 = não gestante,
# 6 = não se aplica, 9 = ignorado. Só mulheres de 10 a 49 anos entram na análise.
if "CS_GESTANT" in df_silver.columns:
    _gest = F.trim(F.col("CS_GESTANT").cast("string"))
    df_silver = df_silver.withColumn(
        "comorb_gestante",
        F.when(
            (F.col("sexo_nome") == "Feminino")
            & F.col("idade_anos").between(10, 49)
            & _gest.isin("1", "2", "3", "4"),
            F.lit(1)
        ).when(
            (F.col("sexo_nome") == "Feminino")
            & F.col("idade_anos").between(10, 49)
            & (_gest == "5"),
            F.lit(0)
        )
    )

df_silver = (
    df_silver
    .withColumn("DT_DIGITA_Parsed", parse_date_flexible(df_silver, "DT_DIGITA"))
    .withColumn(
        "DT_DISPONIVEL",
        F.greatest(F.col("DT_DIGITA_Parsed"), F.col("DT_NOTIFIC_Parsed"))
    )
    # Marcação de duplicidade feita pelo próprio SINAN (1 = duplicada)
    .withColumn(
        "flag_duplicado_sinan",
        F.when(
            F.trim(F.col("NDUPLIC_N").cast("string")) == "1", F.lit(1)
        ).otherwise(F.lit(0))
        if "NDUPLIC_N" in df_silver.columns
        else F.lit(0)
    )
)

print(
    "[SILVER] Campos clínicos padronizados: "
    f"{sum(1 for c in df_silver.columns if c.startswith(('sint_', 'comorb_', 'alarme_')))} indicadores."
)

# COMMAND ----------

# DBTITLE 1,22c. DECODIFICAÇÃO COMPLETA DA NOTIFICAÇÃO
# ==============================================================================
# 22c. DECODIFICAÇÃO COMPLETA DA FICHA DE NOTIFICAÇÃO (SINAN dengue)
# ==============================================================================
# Aproveita os campos da ficha que ainda não eram usados: critério de
# confirmação, laboratório (IgM, NS1, isolamento viral, RT-PCR, sorotipo),
# local provável de infecção (autóctone ou importado), escolaridade, datas de
# internação/óbito/encerramento/investigação, sinais de gravidade e
# manifestações hemorrágicas, e os tempos entre os eventos.
# Códigos conforme o dicionário de dados do SINAN-dengue; confira em caso de
# dúvida. Valor fora da lista vira "Não informado". Colunas ausentes em algum
# ano ficam nulas, então o código roda com arquivos de anos diferentes.
# ==============================================================================

def _txt(coluna):
    """Texto limpo da coluna, ou NULL se ela não existir no arquivo."""
    if coluna in df_silver.columns:
        return F.trim(F.col(coluna).cast("string"))
    return F.lit(None).cast("string")


def decodificar(coluna, mapa, padrao="Não informado"):
    """Troca o código pelo rótulo. Aceita '1' e '01' como o mesmo código."""
    valor = _txt(coluna)
    valor = F.regexp_replace(valor, r"\.0+$", "")
    valor = F.when(valor.rlike(r"^0[0-9]$"), F.regexp_replace(valor, r"^0", "")).otherwise(valor)
    expr = None
    for cod, rotulo in mapa.items():
        cond = valor == str(cod)
        expr = F.when(cond, rotulo) if expr is None else expr.when(cond, rotulo)
    return expr.otherwise(padrao)


MAPA_RESULTADO_LAB = {"1": "Positivo/Reagente", "2": "Negativo/Não reagente",
                      "3": "Inconclusivo", "4": "Não realizado"}
MAPA_ESCOLARIDADE = {
    "0": "Analfabeto", "1": "1ª a 4ª série incompleta", "2": "4ª série completa",
    "3": "5ª a 8ª série incompleta", "4": "Ensino fundamental completo",
    "5": "Ensino médio incompleto", "6": "Ensino médio completo",
    "7": "Superior incompleto", "8": "Superior completo",
    "9": "Ignorado", "10": "Não se aplica",
}

df_silver = (
    df_silver
    .withColumn("raca_nome", decodificar(
        "CS_RACA_PADRAO",
        {"1": "Branca", "2": "Preta", "3": "Amarela", "4": "Parda",
         "5": "Indígena", "9": "Ignorado"}, "Ignorado"))
    .withColumn("escolaridade_nome", decodificar("CS_ESCOL_N", MAPA_ESCOLARIDADE))
    .withColumn("criterio_nome", decodificar(
        "CRITERIO",
        {"1": "Laboratorial", "2": "Clínico-epidemiológico", "3": "Em investigação"}))
    .withColumn("evolucao_nome", decodificar(
        "EVOLUCAO",
        {"1": "Cura", "2": "Óbito pelo agravo", "3": "Óbito por outras causas",
         "4": "Óbito em investigação", "9": "Ignorado"}))
    .withColumn("hospitalizado_nome", decodificar(
        "HOSPITALIZ", {"1": "Sim", "2": "Não", "9": "Ignorado"}))
    # Local provável de infecção
    .withColumn("autoctone_nome", decodificar(
        "TPAUTOCTO", {"1": "Sim", "2": "Não", "3": "Indeterminado"}))
    .withColumn("uf_infeccao", _txt("COUFINF"))
    .withColumn("municipio_infeccao_codigo", F.substring(_txt("COMUNINF"), 1, 6))
    .withColumn("municipio_notificacao_codigo", F.substring(_txt("ID_MUNICIP"), 1, 6))
    .withColumn("regional_saude_residencia", _txt("ID_RG_RESI"))
    .withColumn("regional_saude_notificacao", _txt("ID_REGIONA"))
    .withColumn("unidade_notificadora_cnes", _txt("ID_UNIDADE"))
    # Laboratório
    .withColumn("lab_igm", decodificar("RESUL_SORO", MAPA_RESULTADO_LAB, "Não informado"))
    .withColumn("lab_ns1", decodificar("RESUL_NS1", MAPA_RESULTADO_LAB, "Não informado"))
    .withColumn("lab_isolamento_viral", decodificar("RESUL_VI_N", MAPA_RESULTADO_LAB, "Não informado"))
    .withColumn("lab_rtpcr", decodificar("RESUL_PCR_", MAPA_RESULTADO_LAB, "Não informado"))
    .withColumn("lab_histopatologia", decodificar("HISTOPA_N", MAPA_RESULTADO_LAB, "Não informado"))
    .withColumn("lab_imunohistoquimica", decodificar("IMUNOH_N", MAPA_RESULTADO_LAB, "Não informado"))
    .withColumn("sorotipo", decodificar(
        "SOROTIPO", {"1": "DENV-1", "2": "DENV-2", "3": "DENV-3", "4": "DENV-4"},
        "Não informado"))
    # Datas adicionais
    .withColumn("DT_INTERNA_Parsed", parse_date_flexible(df_silver, "DT_INTERNA"))
    .withColumn("DT_OBITO_Parsed", parse_date_flexible(df_silver, "DT_OBITO"))
    .withColumn("DT_ENCERRA_Parsed", parse_date_flexible(df_silver, "DT_ENCERRA"))
    .withColumn("DT_INVEST_Parsed", parse_date_flexible(df_silver, "DT_INVEST"))
    .withColumn("DT_ALRM_Parsed", parse_date_flexible(df_silver, "DT_ALRM"))
    .withColumn("DT_GRAV_Parsed", parse_date_flexible(df_silver, "DT_GRAV"))
)

# Teste laboratorial positivo para qualquer método direto ou sorológico
df_silver = df_silver.withColumn(
    "lab_algum_positivo",
    F.when(
        (F.col("lab_igm") == "Positivo/Reagente")
        | (F.col("lab_ns1") == "Positivo/Reagente")
        | (F.col("lab_isolamento_viral") == "Positivo/Reagente")
        | (F.col("lab_rtpcr") == "Positivo/Reagente"),
        1
    ).otherwise(0)
)

# Sinais de gravidade (GRAV_*) e manifestações hemorrágicas
SINAIS_GRAVIDADE = {
    "pulso_fraco": "GRAV_PULSO", "convulsao": "GRAV_CONV", "enchimento_capilar": "GRAV_ENCH",
    "insuf_respiratoria": "GRAV_INSUF", "taquicardia": "GRAV_TAQUI", "extremidades_frias": "GRAV_EXTRE",
    "hipotensao": "GRAV_HIPOT", "hematemese": "GRAV_HEMAT", "melena": "GRAV_MELEN",
    "metrorragia": "GRAV_METRO", "sangramento": "GRAV_SANG", "ast_alt_1000": "GRAV_AST",
    "miocardite": "GRAV_MIOC", "consciencia_alterada": "GRAV_CONSC", "outros_orgaos": "GRAV_ORGAO",
}
MANIFESTACOES_HEMORRAGICAS = {
    "epistaxe": "EPISTAXE", "gengivorragia": "GENGIVO", "metrorragia": "METRO",
    "petequias": "PETEQUIAS", "hematuria": "HEMATURA", "sangramento_outros": "SANGRAM",
    "prova_laco_positiva": "LACO_N", "alguma_manifestacao": "MANI_HEMOR",
}
for _n, _c in SINAIS_GRAVIDADE.items():
    if _c in df_silver.columns:
        df_silver = df_silver.withColumn(f"grav_{_n}", flag_sim_nao(_c))
for _n, _c in MANIFESTACOES_HEMORRAGICAS.items():
    if _c in df_silver.columns:
        df_silver = df_silver.withColumn(f"hemor_{_n}", flag_sim_nao(_c))

# Tempos entre eventos, em dias. Valores negativos ou absurdos viram NULL
# (erro de digitação de data), para não distorcer medianas.
def _dias(fim, ini, maximo=365):
    d = F.datediff(F.col(fim), F.col(ini))
    return F.when((d >= 0) & (d <= maximo), d)


df_silver = (
    df_silver
    .withColumn("dias_sintoma_notificacao", _dias("DT_NOTIFIC_Parsed", "DT_SIN_PRI_Parsed", 90))
    .withColumn("dias_sintoma_internacao", _dias("DT_INTERNA_Parsed", "DT_SIN_PRI_Parsed", 90))
    .withColumn("dias_sintoma_obito", _dias("DT_OBITO_Parsed", "DT_SIN_PRI_Parsed", 365))
    .withColumn("dias_notificacao_encerramento", _dias("DT_ENCERRA_Parsed", "DT_NOTIFIC_Parsed", 365))
    .withColumn("dias_sintoma_alarme", _dias("DT_ALRM_Parsed", "DT_SIN_PRI_Parsed", 30))
    .withColumn("dias_sintoma_gravidade", _dias("DT_GRAV_Parsed", "DT_SIN_PRI_Parsed", 30))
)

print(
    "[SILVER] Decodificação completa: "
    f"{sum(1 for c in df_silver.columns if c.startswith(('lab_','grav_','hemor_','dias_')))} "
    "campos adicionais (laboratório, gravidade, hemorragia e tempos)."
)

# COMMAND ----------

# DBTITLE 1,23. SALVAR SILVER
# ==============================================================================
# 23. SALVAR SILVER
# ==============================================================================

print(
    "[SILVER] Salvando somente as partições dos anos alterados..."
)

# ------------------------------------------------------------------------------
# Verifica se existem anos alterados
# ------------------------------------------------------------------------------

if not changed_years:
    print("[SILVER] Nenhum ano alterado. Nada será gravado.")

else:

    # --------------------------------------------------------------------------
    # Condição usada para substituir somente os anos alterados
    # --------------------------------------------------------------------------

    silver_replace_where = "_source_year IN ({})".format(
        ",".join(str(int(y)) for y in changed_years)
    )

    # --------------------------------------------------------------------------
    # Se a tabela Silver já existe
    # --------------------------------------------------------------------------

    if spark.catalog.tableExists(SILVER_TABLE):

        print(f"[SILVER] Tabela existente: {SILVER_TABLE}")

        # ----------------------------------------------------------------------
        # Obtém o schema atual da tabela Silver
        # ----------------------------------------------------------------------

        silver_schema = spark.table(SILVER_TABLE).schema

        silver_columns = [field.name for field in silver_schema]

        print(
            "[SILVER] Colunas existentes:",
            len(silver_columns)
        )

        # ----------------------------------------------------------------------
        # Verifica se todas as colunas da tabela existem no DataFrame
        # ----------------------------------------------------------------------

        missing_columns = [
            c for c in silver_columns
            if c not in df_silver.columns
        ]

        extra_columns = [
            c for c in df_silver.columns
            if c not in silver_columns
        ]

        anos_reprocessados = {int(y) for y in changed_years}
        if "_source_year" not in silver_columns:
            # Silver de uma versão antiga do notebook (sem a coluna de partição):
            # não dá para substituir por ano; reescreve a tabela inteira.
            print(
                "[SILVER][WARN] A Silver existente não tem _source_year (versão antiga). "
                "Será reescrita por completo com os anos processados agora. Se algum ano "
                "faltar, rode novamente com forcar_reprocessamento = sim."
            )
            reescrita_total = True
        else:
            anos_na_tabela = {
                int(r[0]) for r in
                spark.table(SILVER_TABLE).select("_source_year").distinct().collect()
                if r[0] is not None
            }
            # Forçado = reconstruir com os anos disponíveis agora (também remove
            # anos de uma rodada anterior com ano_inicio_fonte diferente).
            reescrita_total = (
                anos_na_tabela.issubset(anos_reprocessados) or FORCAR_REPROCESSAMENTO
            )

        if reescrita_total:
            # ------------------------------------------------------------------
            # Todos os anos da tabela estão sendo reprocessados (mudança de
            # versão, widget "forçar" ou primeira carga): reescreve a Silver
            # inteira com o schema NOVO. Colunas de versões antigas do
            # notebook (ex.: NU_IDADE_N_Int, ID_MN_RESI_Int) são descartadas.
            # ------------------------------------------------------------------
            if missing_columns or extra_columns:
                print(
                    "[SILVER] Schema mudou. Removidas: "
                    f"{missing_columns or 'nenhuma'} | Novas: {extra_columns or 'nenhuma'}"
                )
            print("[SILVER] Reescrita completa da tabela (todos os anos).")

            (
                df_silver
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("_source_year")
                .saveAsTable(SILVER_TABLE)
            )

        else:
            # ------------------------------------------------------------------
            # Atualização parcial (só alguns anos): mantém o schema da tabela.
            # Colunas antigas que o código não gera mais entram como nulas;
            # colunas novas são adicionadas via mergeSchema.
            # ------------------------------------------------------------------
            if missing_columns:
                print(
                    "[SILVER][WARN] Colunas antigas preenchidas com nulo nos anos "
                    f"reprocessados: {missing_columns}"
                )

            tipos = {f.name: f.dataType for f in silver_schema}
            df_silver_aligned = df_silver.select(
                *[
                    (F.col(c).cast(tipos[c]) if c in df_silver.columns
                     else F.lit(None).cast(tipos[c])).alias(c)
                    for c in silver_columns
                ],
                *extra_columns
            )

            (
                df_silver_aligned
                .write
                .format("delta")
                .mode("overwrite")
                .option("replaceWhere", silver_replace_where)
                .option("mergeSchema", "true")
                .saveAsTable(SILVER_TABLE)
            )

        print(
            "[SILVER] Partições atualizadas:",
            changed_years
        )

    # --------------------------------------------------------------------------
    # Criação inicial da tabela Silver
    # --------------------------------------------------------------------------

    else:

        print(
            "[SILVER] Tabela não existe. Criando tabela Silver..."
        )

        (
            df_silver
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("_source_year")
            .saveAsTable(SILVER_TABLE)
        )

        print(
            "[SILVER] Tabela criada com sucesso."
        )

if changed_years:
    spark.sql(
        f"ALTER TABLE {SILVER_TABLE} "
        f"SET TBLPROPERTIES ('pipeline.versao' = '{PIPELINE_VERSION}')"
    )
    # Registra a versão da fonte (SHA-256) que entrou na Silver deste schema
    _proc_atual = {}
    if spark.catalog.tableExists(PROCESSADO_TABLE) and not FORCAR_REPROCESSAMENTO:
        _proc_atual = {int(r["year"]): r["sha256"] for r in spark.table(PROCESSADO_TABLE).collect()}
    _sha_agora = sha_atual_por_ano()
    for _ano, _sha in _sha_agora.items():
        if _ano in changed_years or _ano not in _proc_atual:
            _proc_atual[_ano] = _sha
    _proc_atual = {a: h for a, h in _proc_atual.items() if a in _sha_agora}
    (
        spark.createDataFrame(
            [(int(a), h, datetime.now(timezone.utc)) for a, h in sorted(_proc_atual.items())],
            "year int, sha256 string, processado_em timestamp"
        )
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(PROCESSADO_TABLE)
    )

print("[SILVER] Concluída.")

# COMMAND ----------

# DBTITLE 1,24. RECARREGAR SILVER
# ==============================================================================
# 24. RECARREGAR SILVER
# ==============================================================================

df_silver_persisted = spark.table(SILVER_TABLE)

registros_silver = df_silver_persisted.count()

print(
    f"[SILVER] Registros: "
    f"{registros_silver:,}"
)

# COMMAND ----------

# DBTITLE 1,Registrar métrica real da camada Silver
t_silver_duration = time.perf_counter() - t_start_silver

registrar_metrica(RUN_ID, "silver", t_silver_duration, registros_silver)

# COMMAND ----------

# DBTITLE 1,25. POPULAÇÃO DO IBGE (notebook 10_populacao_ibge)
# A dimensão de população é compartilhada pelos três agravos e gravada pelo
# notebook 10. Aqui só se confere se ela cobre todos os anos; se faltar algum,
# o notebook 10 é chamado. O cronômetro "ibge" continua medindo esta etapa.
t_start_ibge = time.perf_counter()


def _anos_populacao():
    if not spark.catalog.tableExists(POPULATION_TABLE):
        return set()
    return {int(r[0]) for r in spark.table(POPULATION_TABLE).filter(F.col("populacao").isNotNull())
            .select("ano_populacao").distinct().collect()}


_faltam_pop = sorted(set(POPULATION_YEARS) - _anos_populacao())
_origem_cache_ibge = POPULATION_TABLE
if _faltam_pop:
    print(f"[IBGE] Faltam anos de população {_faltam_pop}: rodando 10_populacao_ibge…")
    _ini_pop_ms = int(time.time() * 1000) - 5000
    try:
        _saida_pop = dbutils.notebook.run(caminho_absoluto("./10_populacao_ibge"), 3 * 3600, {
            "agravo": AGRAVO, "ano_inicio_fonte": str(START_YEAR), "ano_fim_fonte": str(END_YEAR),
            "usar_cache_ibge": "sim" if USAR_CACHE_IBGE else "nao",
        })
        print(f"[IBGE] {_saida_pop}")
        _origem_cache_ibge = "notebook 10_populacao_ibge"
    except Exception as _ex_pop:
        print(f"[IBGE][WARN] O notebook 10 falhou; usando a dimensão existente.\n{erro_detalhado(_ex_pop, _ini_pop_ms)}")
    _faltam_pop = sorted(set(POPULATION_YEARS) - _anos_populacao())
    if _faltam_pop and spark.catalog.tableExists(POPULATION_TABLE):
        # Completa os anos sem população com o ano mais próximo que existe (o mesmo
        # critério do notebook 10), registrando isso em fonte_populacao. Sem isso,
        # a incidência dos anos de histórico ficaria nula e o canal endêmico falharia.
        _dim = spark.table(POPULATION_TABLE)
        _tem = sorted(_anos_populacao())
        if _tem:
            _mapa = [(int(a), int(min(_tem, key=lambda x: (abs(x - a), -x)))) for a in _faltam_pop]
            _novos = (
                _dim.filter(F.col("populacao").isNotNull())
                .join(spark.createDataFrame(_mapa, "_ano_novo int, _ano_origem int"),
                      F.col("ano_populacao") == F.col("_ano_origem"))
                .withColumn("ano_populacao", F.col("_ano_novo"))
                .withColumn("fonte_populacao", F.concat(F.lit("Repetida de "), F.col("_ano_origem").cast("string"),
                                                        F.lit(" (ano sem dado do IBGE)")))
                .drop("_ano_novo", "_ano_origem")
            )
            _linhas = (_dim.filter(~F.col("ano_populacao").isin(_faltam_pop))
                       .unionByName(_novos.select(*_dim.columns)).collect())
            (spark.createDataFrame(_linhas, _dim.schema).write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(POPULATION_TABLE))
            print(f"[IBGE] Anos completados com o ano mais próximo: {', '.join(f'{a}<-{o}' for a, o in _mapa)}")
        _faltam_pop = sorted(set(POPULATION_YEARS) - _anos_populacao())
    if _faltam_pop:
        print(f"[IBGE][WARN] Ainda sem população para {_faltam_pop}: a incidência desses anos fica nula.")
else:
    print(f"[IBGE] População disponível para {POPULATION_YEARS[0]}-{POPULATION_YEARS[-1]} em {POPULATION_TABLE}.")

# COMMAND ----------

# DBTITLE 1,Registrar métrica da dimensão IBGE e iniciar cronômetro da Gold
registrar_metrica(
    RUN_ID, "ibge", time.perf_counter() - t_start_ibge,
    observacao=(f"cache de {_origem_cache_ibge}" if _origem_cache_ibge else "API do IBGE"),
)
t_start_gold = time.perf_counter()

# COMMAND ----------

# MAGIC %run ./base_analitica_agravo

# COMMAND ----------

# DBTITLE 1,27. GOLD SEMANAL
# ==============================================================================
# 27. GOLD SEMANAL (ano e semana epidemiológicos) + incidência Brasil
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_weekly...")

AGG_PADRAO = [
    F.sum("flag_notificado").alias("total_notificacoes"),
    F.sum("flag_caso_provavel").alias("casos_provaveis"),
    F.sum("flag_confirmado").alias("casos_confirmados"),
    F.sum("flag_dengue_classica").alias("dengue_classica"),
    F.sum("flag_sinais_alarme").alias("casos_sinais_alarme"),
    F.sum("flag_dengue_grave").alias("casos_graves"),
    F.sum("flag_hospitalizado").alias("hospitalizados"),
    F.sum("flag_obito").alias("obitos"),
    F.sum("flag_obito_evolucao_agravo").alias("obitos_evolucao_agravo"),
    F.sum("flag_obito_investigacao").alias("obitos_investigacao"),
    F.sum("flag_obito_outras_causas").alias("obitos_outras_causas"),
]

LETALIDADE = F.when(
    F.col("casos_confirmados") > 0,
    F.round(F.col("obitos") / F.col("casos_confirmados") * 100, 2)
).otherwise(F.lit(0.0))

df_gold_weekly = com_incidencia(
    gold_base
    .groupBy("ano_analise", "SEM_PRI_Int")
    .agg(*AGG_PADRAO)
    .withColumn("letalidade_pct", LETALIDADE),
    "ano_analise"
)

# COMMAND ----------

# DBTITLE 1,28. GOLD MENSAL
# ==============================================================================
# 28. GOLD MENSAL + incidência Brasil
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_monthly...")

df_gold_monthly = com_incidencia(
    gold_base
    .groupBy("ano_notificacao", "mes_notificacao", "mes_nome")
    .agg(*AGG_PADRAO)
    .withColumn("letalidade_pct", LETALIDADE),
    "ano_notificacao"
)

# COMMAND ----------

# DBTITLE 1,29. GOLD ANUAL
# ==============================================================================
# 29. GOLD ANUAL + incidência Brasil
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_annual...")

df_gold_annual = com_incidencia(
    gold_base
    .groupBy("ano_analise")
    .agg(
        *AGG_PADRAO,
        F.countDistinct(
            F.when(F.col("flag_confirmado") == 1, F.col("ID_MN_RESI_CODIGO"))
        ).alias("municipios_com_casos")
    )
    .withColumn("letalidade_pct", LETALIDADE),
    "ano_analise"
)

# Versão da fonte usada em cada ano (Last-Modified do arquivo no OpenDataSUS).
# O painel do Ministério muda toda semana: compare só números da mesma data.
_manif_anual = get_latest_manifest_by_year()
_versao_fonte = spark.createDataFrame(
    [(int(a), (m or {}).get("last_modified"), (m or {}).get("sha256"))
     for a, m in _manif_anual.items()],
    "_ano_fonte int, versao_fonte_last_modified string, versao_fonte_sha256 string",
) if _manif_anual else None
if _versao_fonte is not None:
    df_gold_annual = (
        df_gold_annual
        .join(_versao_fonte, F.col("ano_analise") == F.col("_ano_fonte"), "left")
        .drop("_ano_fonte")
    )

# COMMAND ----------

# DBTITLE 1,30. GOLD MUNICIPAL
# ==============================================================================
# 30. GOLD MUNICIPAL (todos os 853 municípios x ano, inclusive os sem casos)
# ==============================================================================
# Municípios sem notificação aparecem com 0 casos e incidência 0 – essencial
# para mapas coropléticos e para não superestimar médias municipais.

print(f"[GOLD] Criando {GOLD_PREFIX}_municipal...")

# anos_analise: calculado em base_analitica_agravo.

casos_mun_ano = (
    gold_base
    .groupBy("ano_analise", "ID_MN_RESI_CODIGO")
    .agg(*AGG_PADRAO)
)

grade_mun_ano = (
    spark.table(POPULATION_TABLE)
    .filter(F.col("ano_populacao").isin(anos_analise))
    .select(
        F.col("ano_populacao").alias("_ano"),
        F.col("municipio_codigo_6").alias("_mun"),
        "municipio_codigo_ibge",
        "municipio_nome",
        "uf",
        F.col("populacao").alias("populacao_referencia"),
        F.col("ano_populacao").alias("ano_populacao_referencia"),
        "fonte_populacao"
    )
)

colunas_contagem = [
    "total_notificacoes", "casos_provaveis", "casos_confirmados",
    "dengue_classica", "casos_sinais_alarme", "casos_graves",
    "hospitalizados", "obitos", "obitos_investigacao", "obitos_outras_causas"
]

df_gold_municipal = (
    grade_mun_ano
    .join(
        casos_mun_ano,
        (F.col("_ano") == F.col("ano_analise"))
        & (F.col("_mun") == F.col("ID_MN_RESI_CODIGO")),
        "full_outer"
    )
    .withColumn("ano_analise", F.coalesce("ano_analise", "_ano"))
    .withColumn("ID_MN_RESI_CODIGO", F.coalesce("ID_MN_RESI_CODIGO", "_mun"))
    .withColumn("uf", F.coalesce(F.col("uf"), F.lit(UF_SIGLA)))
    .drop("_ano", "_mun")
    # sem a coluna HOSPITALIZ no arquivo (ex.: zika), hospitalizados fica nulo
    .fillna(0, subset=[c for c in colunas_contagem if hospital_col or c != "hospitalizados"])
    .withColumn("letalidade_pct", LETALIDADE)
)
df_gold_municipal = com_incidencia(
    df_gold_municipal, "ano_analise", col_pop="populacao_referencia"
)
# compatibilidade com o dashboard antigo
df_gold_municipal = df_gold_municipal.withColumn(
    "incidencia_100mil", F.col("incidencia_confirmados_100mil")
)

# COMMAND ----------

# DBTITLE 1,31. GOLD MUNICIPAL SEMANAL
# ==============================================================================
# 31. GOLD MUNICIPAL SEMANAL
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_municipal_weekly...")

df_gold_municipal_weekly = com_incidencia(
    gold_base
    .groupBy(
        "ano_analise",
        "SEM_PRI_Int",
        "ID_MN_RESI_CODIGO",
        "municipio_codigo_ibge",
        "municipio_nome",
        "uf_municipio",
        "populacao_referencia",
        "ano_populacao_referencia"
    )
    .agg(*AGG_PADRAO),
    "ano_analise",
    col_pop="populacao_referencia"
).withColumn(
    "incidencia_100mil", F.col("incidencia_confirmados_100mil")
)

# COMMAND ----------

# DBTITLE 1,32. GOLD DEMOGRÁFICA
# ==============================================================================
# 32. GOLD DEMOGRÁFICA
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_demographic...")

df_gold_demographic = (
    gold_base
    .groupBy(
        "ano_analise",
        "sexo_nome",
        "faixa_etaria"
    )
    .agg(
        F.sum("flag_notificado")
        .alias("total_notificacoes"),

        F.sum("flag_confirmado")
        .alias("casos_confirmados"),

        F.sum("flag_dengue_grave")
        .alias("casos_graves"),

        F.sum("flag_hospitalizado")
        .alias("hospitalizados"),

        F.sum("flag_obito")
        .alias("obitos")
    )
)

# COMMAND ----------

# DBTITLE 1,32.1 GOLD SEXO - RESUMO GERAL (série completa)
# ==============================================================================
# 32.1. GOLD SEXO - RESUMO GERAL
# ==============================================================================
# Reagrupa df_gold_demographic por sexo, somando todos os anos e faixas
# etárias. Inclui taxa de letalidade e taxa de hospitalização por sexo,
# indicadores usados no artigo.

print(f"[GOLD] Criando {GOLD_PREFIX}_sexo_summary...")

df_gold_sexo_summary = (
    df_gold_demographic
    .groupBy(
        "sexo_nome"
    )
    .agg(
        F.sum("total_notificacoes")
        .alias("total_notificacoes"),

        F.sum("casos_confirmados")
        .alias("casos_confirmados"),

        F.sum("casos_graves")
        .alias("casos_graves"),

        F.sum("hospitalizados")
        .alias("hospitalizados"),

        F.sum("obitos")
        .alias("obitos")
    )
    .withColumn(
        "taxa_letalidade_pct",
        F.round(
            F.col("obitos")
            /
            F.col("casos_confirmados")
            * 100,
            2
        )
    )
    .withColumn(
        "taxa_hospitalizacao_pct",
        F.round(
            F.col("hospitalizados")
            /
            F.col("casos_confirmados")
            * 100,
            2
        )
    )
    .withColumn(
        "taxa_gravidade_pct",
        F.round(
            F.col("casos_graves")
            /
            F.col("casos_confirmados")
            * 100,
            2
        )
    )
    .orderBy(
        F.desc("casos_confirmados")
    )
)

# COMMAND ----------

# DBTITLE 1,32.2 GOLD SEXO - EVOLUÇÃO ANUAL
# ==============================================================================
# 32.2. GOLD SEXO - EVOLUÇÃO ANUAL
# ==============================================================================
# Casos, hospitalizações e óbitos por sexo, ano a ano, para mostrar
# tendência temporal por sexo.

print(f"[GOLD] Criando {GOLD_PREFIX}_sexo_anual...")

df_gold_sexo_anual = (
    df_gold_demographic
    .groupBy(
        "ano_analise",
        "sexo_nome"
    )
    .agg(
        F.sum("total_notificacoes")
        .alias("total_notificacoes"),

        F.sum("casos_confirmados")
        .alias("casos_confirmados"),

        F.sum("casos_graves")
        .alias("casos_graves"),

        F.sum("hospitalizados")
        .alias("hospitalizados"),

        F.sum("obitos")
        .alias("obitos")
    )
    .withColumn(
        "taxa_letalidade_pct",
        F.round(
            F.col("obitos")
            /
            F.col("casos_confirmados")
            * 100,
            2
        )
    )
)

# Incidência por sexo – denominador: população do Brasil por sexo no Censo 2022
# (o IBGE não publica estimativas anuais por sexo para municípios/UF).
# ATENÇÃO (série ampliada): para anos anteriores a 2022 a incidência por sexo é
# APROXIMADA, pois usa a população do Censo 2022 como denominador fixo. Se essa
# tabela entrar no artigo, declare a aproximação ou restrinja-a a 2022 em diante.
if spark.catalog.tableExists(POPULATION_SEXO_TABLE):
    pop_sexo = spark.table(POPULATION_SEXO_TABLE).select(
        "sexo_nome", F.col("populacao").alias("populacao_sexo_censo2022")
    )
    df_gold_sexo_anual = (
        df_gold_sexo_anual
        .join(pop_sexo, "sexo_nome", "left")
        .withColumn(
            "incidencia_confirmados_100mil",
            F.round(F.col("casos_confirmados") / F.col("populacao_sexo_censo2022") * 100000, 2)
        )
    )

df_gold_sexo_anual = df_gold_sexo_anual.orderBy("ano_analise", "sexo_nome")

# COMMAND ----------

# DBTITLE 1,32.3 GOLD SEXO x FAIXA ETÁRIA
# ==============================================================================
# 32.3. GOLD SEXO x FAIXA ETÁRIA
# ==============================================================================
# Cruzamento sexo x faixa etária (somando todos os anos), útil para uma
# tabela ou heatmap.

print(f"[GOLD] Criando {GOLD_PREFIX}_sexo_faixa...")

df_gold_sexo_faixa = (
    df_gold_demographic
    .groupBy(
        "faixa_etaria",
        "sexo_nome"
    )
    .agg(
        F.sum("total_notificacoes")
        .alias("total_notificacoes"),

        F.sum("casos_confirmados")
        .alias("casos_confirmados"),

        F.sum("casos_graves")
        .alias("casos_graves"),

        F.sum("hospitalizados")
        .alias("hospitalizados"),

        F.sum("obitos")
        .alias("obitos")
    )
    .orderBy(
        "faixa_etaria",
        "sexo_nome"
    )
)

# COMMAND ----------

# DBTITLE 1,32.4 GOLD PERFIL CLÍNICO (sintomas) E FATORES DE RISCO
# ==============================================================================
# 32.4. PERFIL CLÍNICO E FATORES DE RISCO (casos confirmados)
# ==============================================================================
# (a) gold_dengue_sintomas: % de casos confirmados com cada sintoma, por ano e
#     faixa etária. Denominador = só quem teve o campo informado (1 ou 2).
# (b) gold_dengue_fatores_risco: para cada comorbidade (e gestação), compara o
#     risco de internação, de forma grave (sinais de alarme ou dengue grave) e
#     de óbito entre expostos e não expostos: risco relativo com IC 95%.
#     É uma associação BRUTA (sem ajuste por idade ou outros fatores) e depende
#     de os campos terem sido preenchidos; declarar isso no artigo.
# ==============================================================================

import math

print("[GOLD] Criando perfil clínico e fatores de risco...")

confirmados_clin = (
    gold_base
    .filter(F.col("flag_confirmado") == 1)
    .withColumn(
        "flag_forma_grave",
        F.when((F.col("flag_sinais_alarme") == 1) | (F.col("flag_dengue_grave") == 1), 1).otherwise(0)
    )
)

# ---- (a) sintomas ---------------------------------------------------------------
sintomas_presentes = [n for n in SINTOMAS if f"sint_{n}" in confirmados_clin.columns]

if sintomas_presentes:
    sint_array = F.array(*[
        F.struct(F.lit(n).alias("sintoma"), F.col(f"sint_{n}").alias("valor"))
        for n in sintomas_presentes
    ])
    sint_long = (
        confirmados_clin
        .withColumn("_s", F.explode(sint_array))
        .select("ano_analise", "faixa_etaria", F.col("_s.sintoma").alias("sintoma"), F.col("_s.valor").alias("valor"))
    )

    def _agg_sintomas(df, chaves, faixa_rotulo=None):
        r = df.groupBy(*chaves).agg(
            F.count("valor").alias("n_informado"),
            F.sum("valor").alias("n_presente"),
        )
        if faixa_rotulo is not None:
            r = r.withColumn("faixa_etaria", F.lit(faixa_rotulo))
        return r.select("ano_analise", "faixa_etaria", "sintoma", "n_informado", "n_presente")

    df_gold_sintomas = (
        _agg_sintomas(sint_long, ["ano_analise", "faixa_etaria", "sintoma"])
        .unionByName(_agg_sintomas(sint_long, ["ano_analise", "sintoma"], "Todas"))
        .withColumn(
            "pct_presente",
            F.when(F.col("n_informado") > 0, F.round(F.col("n_presente") / F.col("n_informado") * 100, 2))
        )
        .orderBy("ano_analise", "sintoma", "faixa_etaria")
    )
else:
    print(f"[GOLD][WARN] Nenhuma coluna de sintoma encontrada; {GOLD_SINTOMAS} ficará vazia.")
    df_gold_sintomas = spark.createDataFrame(
        [], "ano_analise int, faixa_etaria string, sintoma string, n_informado long, n_presente long, pct_presente double"
    )

# ---- (b) fatores de risco -------------------------------------------------------
fatores_presentes = [(n, f"comorb_{n}") for n in list(COMORBIDADES) + ["gestante"]
                     if f"comorb_{n}" in confirmados_clin.columns]
DESFECHOS = [
    ("internacao", "flag_hospitalizado"),
    ("forma_grave", "flag_forma_grave"),
    ("obito", "flag_obito"),
]

schema_risco = (
    "ano string, fator string, desfecho string, n_expostos long, eventos_expostos long, "
    "taxa_expostos_pct double, n_nao_expostos long, eventos_nao_expostos long, "
    "taxa_nao_expostos_pct double, risco_relativo double, ic95_inferior double, ic95_superior double"
)

linhas_risco = []
if fatores_presentes:
    aggs_r = []
    for fator, col_f in fatores_presentes:
        exp = F.col(col_f) == 1
        nexp = F.col(col_f) == 0
        for desf, col_d in DESFECHOS:
            ev = F.col(col_d) == 1
            k = f"{fator}__{desf}"
            aggs_r += [
                F.sum(exp.cast("int")).alias(f"{k}__n1"),
                F.sum((exp & ev).cast("int")).alias(f"{k}__a"),
                F.sum(nexp.cast("int")).alias(f"{k}__n0"),
                F.sum((nexp & ev).cast("int")).alias(f"{k}__c"),
            ]

    resultado_risco = (
        confirmados_clin
        .rollup("ano_analise")  # uma linha por ano + uma linha total (ano nulo)
        .agg(*aggs_r)
        .collect()
    )

    for row in resultado_risco:
        ano_rot = "Todos" if row["ano_analise"] is None else str(int(row["ano_analise"]))
        for fator, _ in fatores_presentes:
            for desf, _ in DESFECHOS:
                k = f"{fator}__{desf}"
                n1, a, n0, c = (int(row[f"{k}__{x}"] or 0) for x in ("n1", "a", "n0", "c"))
                t1 = a / n1 * 100 if n1 else None
                t0 = c / n0 * 100 if n0 else None
                rr = lo = hi = None
                if n1 and n0 and a > 0 and c > 0:
                    rr = (a / n1) / (c / n0)
                    se = math.sqrt(1 / a - 1 / n1 + 1 / c - 1 / n0)
                    lo, hi = rr * math.exp(-1.96 * se), rr * math.exp(1.96 * se)
                linhas_risco.append((
                    ano_rot, fator, desf, n1, a,
                    round(t1, 2) if t1 is not None else None,
                    n0, c,
                    round(t0, 2) if t0 is not None else None,
                    round(rr, 3) if rr is not None else None,
                    round(lo, 3) if lo is not None else None,
                    round(hi, 3) if hi is not None else None,
                ))

df_gold_fatores_risco = spark.createDataFrame(linhas_risco, schema_risco)

# COMMAND ----------

# DBTITLE 1,32.5 GOLD DECODIFICAÇÃO COMPLETA (perfil, laboratório, local de infecção, tempos, gravidade, cubo)
# ==============================================================================
# 32.5 GOLD – O QUE A FICHA DE NOTIFICAÇÃO PERMITE DESCREVER
# ==============================================================================
# Seis tabelas novas, todas por ano (período de análise):
#   gold_dengue_perfil            sexo x faixa etária x raça x escolaridade x
#                                 classificação x critério x evolução x internação
#   gold_dengue_laboratorio       sorotipo e resultado de IgM, NS1, RT-PCR, isolamento
#   gold_dengue_autoctonia        local provável de infecção por município de residência
#   gold_dengue_tempos            mediana e P90 dos tempos entre eventos
#   gold_dengue_gravidade_sinais  prevalência dos sinais de gravidade e hemorragia
#   gold_dengue_cubo_semanal      semana x município x sexo x classificação
#   gold_dengue_demografico_semanal  ano x semana x sexo x faixa etária x raça/cor
#   gold_dengue_clinico_semanal   ano x semana x sintoma/comorbidade/sinal de alarme
#   gold_dengue_hospitalizacao_semanal ano x semana x internação x sexo x faixa x classificação
#   gold_dengue_regional          ano x regional de saúde de residência
#   (casos por município, ano e semana: gold_dengue_municipal_weekly)
# ==============================================================================

if "lab_igm" not in gold_base.columns:
    raise RuntimeError(
        "A Silver atual não tem os campos decodificados da versão 9 (laboratório, "
        "sorotipo, local de infecção). Rode uma vez no modo 'completo' para recriar a "
        "Silver; depois o modo 'somente_gold' volta a funcionar."
    )

print("[GOLD] Criando tabelas de decodificação completa...")

_n_not = F.sum("flag_notificado").alias("notificacoes")
_n_prov = F.sum("flag_caso_provavel").alias("casos_provaveis")
_n_obito = F.sum("flag_obito").alias("obitos")
_n_obito_inv = F.sum("flag_obito_investigacao").alias("obitos_investigacao")
_n_hosp = F.sum("flag_hospitalizado").alias("hospitalizados")

# 1) Perfil sociodemográfico e clínico
df_gold_perfil = (
    gold_base
    .groupBy(
        "ano_analise", "sexo_nome", "faixa_etaria", "raca_nome",
        "escolaridade_nome", "classificacao_nome", "criterio_nome",
        "evolucao_nome", "hospitalizado_nome"
    )
    .agg(_n_not, _n_prov, _n_obito, _n_hosp)
)

# 2) Laboratório e sorotipo
df_gold_laboratorio = (
    gold_base
    .groupBy(
        "ano_analise", "sorotipo", "lab_igm", "lab_ns1", "lab_rtpcr",
        "lab_isolamento_viral", "criterio_nome"
    )
    .agg(_n_not, _n_prov, _n_obito)
)

# 3) Local provável de infecção
df_gold_autoctonia = (
    gold_base
    .withColumn(
        "infeccao_no_municipio_residencia",
        F.when(F.col("municipio_infeccao_codigo").isNull() | (F.col("municipio_infeccao_codigo") == ""), "Não informado")
        .when(F.col("municipio_infeccao_codigo") == F.col("ID_MN_RESI_CODIGO"), "Sim")
        .otherwise("Não")
    )
    .groupBy(
        "ano_analise", F.col("ID_MN_RESI_CODIGO").alias("municipio_codigo"),
        "municipio_nome", "autoctone_nome", "infeccao_no_municipio_residencia"
    )
    .agg(_n_not, _n_prov)
)

# 4) Tempos entre eventos (mediana e P90 por ano e classificação)
_colunas_tempo = [
    ("dias_sintoma_notificacao", "Início dos sintomas até a notificação"),
    ("dias_sintoma_internacao", "Início dos sintomas até a internação"),
    ("dias_sintoma_alarme", "Início dos sintomas até os sinais de alarme"),
    ("dias_sintoma_gravidade", "Início dos sintomas até a gravidade"),
    ("dias_sintoma_obito", "Início dos sintomas até o óbito"),
    ("dias_notificacao_encerramento", "Notificação até o encerramento do caso"),
]
_partes_tempo = []
for _c, _rot in _colunas_tempo:
    _partes_tempo.append(
        gold_base
        .filter(F.col("flag_caso_provavel") == 1)
        .groupBy("ano_analise", "classificacao_nome")
        .agg(
            F.count(F.col(_c)).alias("n_com_data"),
            F.expr(f"percentile_approx({_c}, 0.5)").alias("mediana_dias"),
            F.expr(f"percentile_approx({_c}, 0.9)").alias("p90_dias"),
        )
        .withColumn("intervalo", F.lit(_rot))
        .select("ano_analise", "classificacao_nome", "intervalo", "n_com_data", "mediana_dias", "p90_dias")
    )
df_gold_tempos = _partes_tempo[0]
for _d in _partes_tempo[1:]:
    df_gold_tempos = df_gold_tempos.unionByName(_d)

# 5) Prevalência dos sinais de gravidade e das manifestações hemorrágicas
_sinais = (
    [(c, "Sinal de gravidade", c[len("grav_"):]) for c in gold_base.columns if c.startswith("grav_")]
    + [(c, "Manifestação hemorrágica", c[len("hemor_"):]) for c in gold_base.columns if c.startswith("hemor_")]
)
if _sinais:
    _pares = ", ".join(f"'{c}|{g}|{n}', `{c}`" for c, g, n in _sinais)
    df_gold_gravidade_sinais = (
        gold_base
        .filter(F.col("flag_caso_provavel") == 1)
        .select("ano_analise", F.expr(f"stack({len(_sinais)}, {_pares}) as (chave, valor)"))
        .withColumn("grupo", F.split("chave", r"\|").getItem(1))
        .withColumn("sinal", F.split("chave", r"\|").getItem(2))
        .groupBy("ano_analise", "grupo", "sinal")
        .agg(
            F.count("valor").alias("n_informado"),
            F.sum("valor").alias("n_presente"),
        )
        .withColumn(
            "pct_presente",
            F.when(F.col("n_informado") > 0, F.round(F.col("n_presente") / F.col("n_informado") * 100, 2))
        )
    )
else:
    df_gold_gravidade_sinais = spark.createDataFrame(
        [], "ano_analise int, grupo string, sinal string, n_informado long, n_presente long, pct_presente double"
    )

# 6) Cubo semanal: semana x município x sexo x classificação
df_gold_cubo_semanal = (
    gold_base
    .groupBy(
        "ano_analise", F.col("SEM_PRI_Int").alias("semana"),
        F.col("ID_MN_RESI_CODIGO").alias("municipio_codigo"),
        "sexo_nome", "classificacao_nome"
    )
    .agg(_n_not, _n_prov, _n_obito, _n_obito_inv, _n_hosp)
)

# 6b) Base do painel (dashboard). Uma linha por ano epidemiológico x semana x
#     município de residência x classificação x sexo x faixa etária, com o grupo
#     da classificação para o filtro "confirmado ou não":
#       Confirmado                = classificação final de caso confirmado
#       Provável não confirmado   = provável, mas inconclusivo / em investigação
#       Descartado                = descartado (não entra nos casos prováveis)
# UF de residência: dos 2 primeiros dígitos do município; sem município, da
# coluna SG_UF (código numérico ou sigla).
_mapa_sigla_cod = F.create_map(*[x for s, (c, _) in UFS.items() for x in (F.lit(s), F.lit(str(c)))])
_sg = F.upper(F.trim(F.col("SG_UF").cast("string"))) if "SG_UF" in gold_base.columns else F.lit(None).cast("string")
_UF_RESIDENCIA_PAINEL = F.coalesce(
    F.substring("ID_MN_RESI_CODIGO", 1, 2),
    F.when(_sg.rlike("^[0-9]{2}$"), _sg),
    _mapa_sigla_cod[_sg],
)
df_gold_painel = (
    gold_base
    .withColumn(
        "grupo_classificacao",
        F.when(F.col("flag_confirmado") == 1, F.lit("Confirmado"))
         .when(F.col("flag_caso_provavel") == 1, F.lit("Provável não confirmado"))
         .otherwise(F.lit("Descartado"))
    )
    .withColumn("uf_codigo", _UF_RESIDENCIA_PAINEL)
    .groupBy(
        "ano_analise", F.col("SEM_PRI_Int").alias("semana"),
        "uf_codigo", F.col("ID_MN_RESI_CODIGO").alias("municipio_codigo"),
        "grupo_classificacao", "classificacao_nome", "sexo_nome", "faixa_etaria",
    )
    .agg(
        _n_not, _n_prov, F.sum("flag_confirmado").alias("casos_confirmados"),
        _n_obito, _n_obito_inv, _n_hosp,
    )
)

# 7) Regional de saúde de residência (código da regional de saúde do SINAN)
df_gold_regional = (
    gold_base
    .groupBy("ano_analise", "regional_saude_residencia")
    .agg(
        F.countDistinct("ID_MN_RESI_CODIGO").alias("municipios_com_casos"),
        _n_not, _n_prov, _n_obito, _n_obito_inv, _n_hosp,
    )
)

# 8) Sexo x faixa etária x raça/cor, por ano e semana epidemiológica
df_gold_demografico_semanal = (
    gold_base
    .groupBy(
        "ano_analise", F.col("SEM_PRI_Int").alias("semana"),
        "sexo_nome", "faixa_etaria", "raca_nome"
    )
    .agg(_n_not, _n_prov, _n_obito, _n_obito_inv, _n_hosp)
)

# 9) Sintomas, comorbidades e sinais de alarme por ano e semana epidemiológica.
#    Base = casos prováveis; denominador = quem teve o campo informado.
_itens_clin = (
    [(c, "Sintoma", c[len("sint_"):]) for c in gold_base.columns if c.startswith("sint_")]
    + [(c, "Comorbidade", c[len("comorb_"):]) for c in gold_base.columns if c.startswith("comorb_")]
    + [(c, "Sinal de alarme", c[len("alarme_"):]) for c in gold_base.columns if c.startswith("alarme_")]
)
if _itens_clin:
    _pares_clin = ", ".join(f"'{g}|{n}', `{c}`" for c, g, n in _itens_clin)
    df_gold_clinico_semanal = (
        gold_base
        .filter(F.col("flag_caso_provavel") == 1)
        .select(
            "ano_analise", F.col("SEM_PRI_Int").alias("semana"),
            F.expr(f"stack({len(_itens_clin)}, {_pares_clin}) as (chave, valor)")
        )
        .withColumn("grupo", F.split("chave", r"\|").getItem(0))
        .withColumn("item", F.split("chave", r"\|").getItem(1))
        .groupBy("ano_analise", "semana", "grupo", "item")
        .agg(
            F.count("valor").alias("n_informado"),
            F.sum("valor").alias("n_presente"),
        )
        .withColumn(
            "pct_presente",
            F.when(F.col("n_informado") > 0, F.round(F.col("n_presente") / F.col("n_informado") * 100, 2))
        )
    )
else:
    df_gold_clinico_semanal = spark.createDataFrame(
        [], "ano_analise int, semana int, grupo string, item string, n_informado long, n_presente long, pct_presente double"
    )

# 10) Hospitalização por ano e semana epidemiológica
df_gold_hospitalizacao_semanal = (
    gold_base
    .groupBy(
        "ano_analise", F.col("SEM_PRI_Int").alias("semana"),
        "hospitalizado_nome", "sexo_nome", "faixa_etaria", "classificacao_nome"
    )
    .agg(_n_not, _n_prov, _n_obito)
)

print("[GOLD] Tabelas de decodificação completa criadas.")

# COMMAND ----------

# DBTITLE 1,32.6 GOLD COEFICIENTE DE INCIDÊNCIA
# ==============================================================================
# 32.6 COEFICIENTE DE INCIDÊNCIA (casos prováveis por 100 mil habitantes)
# ==============================================================================
#   coeficiente = casos prováveis no período / população do mesmo local e ano x 100.000
#
# Uma única tabela longa (gold_dengue_incidencia), um nível por vez:
#   UF_ANO, UF_SEMANA, UF_ACUMULADA (soma do ano até a semana),
#   MUNICIPIO_ANO, MUNICIPIO_SEMANA, SEXO_ANO (Censo 2022) e, se existir o arquivo
#   ibge/populacao_faixa_etaria.csv, FAIXA_ETARIA_ANO.
# Cada linha traz a população usada e o intervalo de confiança de 95% (aproximação
# de Byar para contagens de Poisson), para o artigo mostrar a incerteza em
# municípios pequenos, onde poucos casos mudam muito o coeficiente.
# Coeficiente SEMANAL é o da semana, sem anualizar.
# ==============================================================================

print("[GOLD] Calculando coeficientes de incidência...")

_Z = 1.96


def _com_ic(df):
    """Acrescenta coeficiente e IC 95% (Byar) a um df com casos_provaveis e populacao."""
    c = F.col("casos_provaveis").cast("double")
    pop = F.col("populacao").cast("double")
    inf = F.when(c > 0, c * F.pow(1 - 1 / (9 * c) - _Z / (3 * F.sqrt(c)), F.lit(3.0))).otherwise(F.lit(0.0))
    sup = (c + 1) * F.pow(1 - 1 / (9 * (c + 1)) + _Z / (3 * F.sqrt(c + 1)), F.lit(3.0))
    return (
        df
        .withColumn("coef_incidencia_100mil", F.when(pop > 0, F.round(c / pop * 100000, 2)))
        .withColumn("ic95_inf", F.when(pop > 0, F.round(inf / pop * 100000, 2)))
        .withColumn("ic95_sup", F.when(pop > 0, F.round(sup / pop * 100000, 2)))
    )


_COLS_INC = [
    "nivel", "ano_analise", "semana", "codigo", "nome", "casos_provaveis",
    "populacao", "coef_incidencia_100mil", "ic95_inf", "ic95_sup", "base_populacao",
]
_pop_uf = pop_territorio_ano.select(F.col("_ano_pop").alias("ano_analise"), F.col("populacao_territorio").alias("populacao"))

# --- UF por ano, por semana e acumulada -------------------------------------
_uf_sem = (
    gold_base.groupBy("ano_analise", F.col("SEM_PRI_Int").alias("semana"))
    .agg(F.sum("flag_caso_provavel").alias("casos_provaveis"))
    .filter(F.col("semana").isNotNull())
)
_jan = Window.partitionBy("ano_analise").orderBy("semana").rowsBetween(Window.unboundedPreceding, Window.currentRow)

_uf_semana = _com_ic(
    _uf_sem.join(_pop_uf, "ano_analise", "left")
    .withColumn("nivel", F.lit("UF_SEMANA")).withColumn("codigo", F.lit(str(UF_CODIGO_IBGE))).withColumn("nome", F.lit(UF_NOME))
    .withColumn("base_populacao", F.lit("Estimativa/Censo IBGE do ano"))
)
_uf_acum = _com_ic(
    _uf_sem.withColumn("casos_provaveis", F.sum("casos_provaveis").over(_jan))
    .join(_pop_uf, "ano_analise", "left")
    .withColumn("nivel", F.lit("UF_ACUMULADA")).withColumn("codigo", F.lit(str(UF_CODIGO_IBGE))).withColumn("nome", F.lit(UF_NOME))
    .withColumn("base_populacao", F.lit("Estimativa/Censo IBGE do ano"))
)
_uf_ano = _com_ic(
    _uf_sem.groupBy("ano_analise").agg(F.sum("casos_provaveis").alias("casos_provaveis"))
    .join(_pop_uf, "ano_analise", "left")
    .withColumn("nivel", F.lit("UF_ANO")).withColumn("semana", F.lit(None).cast("int"))
    .withColumn("codigo", F.lit(str(UF_CODIGO_IBGE))).withColumn("nome", F.lit(UF_NOME))
    .withColumn("base_populacao", F.lit("Estimativa/Censo IBGE do ano"))
)

# --- Município por ano e por semana -----------------------------------------
_mun_base = ["ano_analise", "ID_MN_RESI_CODIGO", "municipio_nome", "populacao_referencia", "fonte_populacao"]
_mun_ano = _com_ic(
    gold_base.groupBy(*_mun_base)
    .agg(F.sum("flag_caso_provavel").alias("casos_provaveis"))
    .withColumn("nivel", F.lit("MUNICIPIO_ANO")).withColumn("semana", F.lit(None).cast("int"))
    .withColumn("codigo", F.col("ID_MN_RESI_CODIGO")).withColumn("nome", F.col("municipio_nome"))
    .withColumn("populacao", F.col("populacao_referencia")).withColumn("base_populacao", F.col("fonte_populacao"))
)
_mun_sem = _com_ic(
    gold_base.filter(F.col("SEM_PRI_Int").isNotNull())
    .groupBy(*_mun_base, F.col("SEM_PRI_Int").alias("semana"))
    .agg(F.sum("flag_caso_provavel").alias("casos_provaveis"))
    .withColumn("nivel", F.lit("MUNICIPIO_SEMANA"))
    .withColumn("codigo", F.col("ID_MN_RESI_CODIGO")).withColumn("nome", F.col("municipio_nome"))
    .withColumn("populacao", F.col("populacao_referencia")).withColumn("base_populacao", F.col("fonte_populacao"))
)

_partes_inc = [
    d.select(*_COLS_INC) for d in (_uf_ano, _uf_semana, _uf_acum, _mun_ano, _mun_sem)
]

# --- Brasil inteiro: incidência de cada UF por ano e por semana ---------------
# (o nível UF_* acima é o território analisado; no escopo nacional, o Brasil)
if ESCOPO_NACIONAL:
    _nome_uf = spark.createDataFrame(
        [(str(c), n) for c, n in UFS.values()], "codigo string, nome string"
    )
    _pop_estado = (
        spark.table(POPULATION_TABLE)
        .filter(F.col("populacao").isNotNull())
        .groupBy(
            F.substring("municipio_codigo_6", 1, 2).alias("codigo"),
            F.col("ano_populacao").alias("ano_analise"),
        )
        .agg(F.sum("populacao").alias("populacao"))
    )
    _gb_uf = gold_base.withColumn("codigo", F.substring("ID_MN_RESI_CODIGO", 1, 2))
    _est_ano = _com_ic(
        _gb_uf.filter(F.col("codigo").isin(*UF_POR_CODIGO))
        .groupBy("ano_analise", "codigo").agg(F.sum("flag_caso_provavel").alias("casos_provaveis"))
        .join(_pop_estado, ["codigo", "ano_analise"], "left")
        .join(_nome_uf, "codigo", "left")
        .withColumn("nivel", F.lit("ESTADO_ANO")).withColumn("semana", F.lit(None).cast("int"))
        .withColumn("base_populacao", F.lit("Soma dos municípios (IBGE)"))
    )
    _est_sem = _com_ic(
        _gb_uf.filter(F.col("codigo").isin(*UF_POR_CODIGO) & F.col("SEM_PRI_Int").isNotNull())
        .groupBy("ano_analise", "codigo", F.col("SEM_PRI_Int").alias("semana"))
        .agg(F.sum("flag_caso_provavel").alias("casos_provaveis"))
        .join(_pop_estado, ["codigo", "ano_analise"], "left")
        .join(_nome_uf, "codigo", "left")
        .withColumn("nivel", F.lit("ESTADO_SEMANA"))
        .withColumn("base_populacao", F.lit("Soma dos municípios (IBGE)"))
    )
    _partes_inc += [_est_ano.select(*_COLS_INC), _est_sem.select(*_COLS_INC)]

# --- Sexo por ano (Censo 2022) -----------------------------------------------
if spark.catalog.tableExists(POPULATION_SEXO_TABLE):
    _pop_sexo = spark.table(POPULATION_SEXO_TABLE).select("sexo_nome", F.col("populacao").cast("long").alias("populacao"))
    _sexo_ano = _com_ic(
        gold_base.filter(F.col("sexo_nome").isin("Masculino", "Feminino"))
        .groupBy("ano_analise", "sexo_nome").agg(F.sum("flag_caso_provavel").alias("casos_provaveis"))
        .join(_pop_sexo, "sexo_nome", "left")
        .withColumn("nivel", F.lit("SEXO_ANO")).withColumn("semana", F.lit(None).cast("int"))
        .withColumn("codigo", F.col("sexo_nome")).withColumn("nome", F.col("sexo_nome"))
        .withColumn("base_populacao", F.lit("Censo 2022 (fixa; aproximada em outros anos)"))
    )
    _partes_inc.append(_sexo_ano.select(*_COLS_INC))
else:
    print("[INCIDÊNCIA][WARN] Sem população por sexo (tabela dim_populacao_sexo_uf); nível SEXO_ANO não calculado.")

# --- Faixa etária por ano (arquivo opcional) -----------------------------------
# Arquivo: /Volumes/<catalogo>/arboviroses_br/raw/ibge/populacao_faixa_etaria.csv (vale para
# os três agravos; o local antigo <volume do agravo>/ibge/ também é aceito),
# colunas  ano;faixa_etaria;populacao
# Rótulos exatamente: 00-04 anos, 05-09 anos, 10-19 anos, 20-29 anos, 30-39 anos,
# 40-49 anos, 50-59 anos, 60-69 anos, 70-79 anos, 80 anos ou mais.
# Deixe "ano" vazio para usar a mesma população em todos os anos (ex.: Censo 2022).
_arq_faixa = next((c for c in [os.path.join(VOLUME_COMPARTILHADO, "ibge", "populacao_faixa_etaria.csv"),
                                os.path.join(VOLUME_PATH, "ibge", "populacao_faixa_etaria.csv")]
                   if os.path.exists(c)), "")
if os.path.exists(_arq_faixa):
    _linhas_faixa = []
    with open(_arq_faixa, encoding="utf-8-sig") as _f:
        _amostra = _f.read(2048); _f.seek(0)
        _delim = ";" if _amostra.count(";") >= _amostra.count(",") else ","
        for _r in _csv.DictReader(_f, delimiter=_delim):
            _pop_f = to_int_pop(_r.get("populacao"))
            _faixa = (_r.get("faixa_etaria") or "").strip()
            if _pop_f and _faixa:
                _ano_f = to_int_pop(_r.get("ano"))
                _linhas_faixa.append((_ano_f, _faixa, int(_pop_f)))
    if _linhas_faixa:
        _pop_faixa = spark.createDataFrame(_linhas_faixa, "ano_pop int, faixa_pop string, populacao bigint")
        _faixa_ano = (
            gold_base
            .withColumn(
                "faixa_inc",
                F.when(F.col("faixa_etaria").isin("Menor de 1 ano", "01-04 anos"), "00-04 anos")
                .otherwise(F.col("faixa_etaria"))
            )
            .filter(F.col("faixa_inc") != "Ignorada")
            .groupBy("ano_analise", "faixa_inc").agg(F.sum("flag_caso_provavel").alias("casos_provaveis"))
            .join(
                _pop_faixa,
                (F.col("faixa_inc") == _pop_faixa["faixa_pop"])
                & (_pop_faixa["ano_pop"].isNull() | (_pop_faixa["ano_pop"] == F.col("ano_analise"))),
                "left"
            )
            .select("ano_analise", F.col("faixa_inc"), "casos_provaveis", "populacao")
        )
        _faixa_ano = _com_ic(
            _faixa_ano
            .withColumn("nivel", F.lit("FAIXA_ETARIA_ANO")).withColumn("semana", F.lit(None).cast("int"))
            .withColumn("codigo", F.col("faixa_inc")).withColumn("nome", F.col("faixa_inc"))
            .withColumn("base_populacao", F.lit("CSV manual (ibge/populacao_faixa_etaria.csv)"))
        )
        _partes_inc.append(_faixa_ano.select(*_COLS_INC))
        print(f"[INCIDÊNCIA] Faixa etária calculada com {len(_linhas_faixa)} linhas de população.")
else:
    print(
        "[INCIDÊNCIA] Sem ibge/populacao_faixa_etaria.csv: incidência por faixa etária não calculada "
        "(precisa da população por faixa; ver comentário desta seção)."
    )

df_gold_incidencia = _partes_inc[0]
for _d in _partes_inc[1:]:
    df_gold_incidencia = df_gold_incidencia.unionByName(_d)

print("[GOLD] Coeficientes de incidência prontos.")

# COMMAND ----------

# DBTITLE 1,33. GOLD GRAVIDADE
# ==============================================================================
# 33. GOLD GRAVIDADE
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_severity...")

df_gold_severity = (
    gold_base
    .groupBy(
        "ano_analise",
        "classificacao_nome"
    )
    .agg(
        F.count("*")
        .alias("total_registros"),

        F.sum("flag_confirmado")
        .alias("casos_confirmados"),

        F.sum("flag_dengue_grave")
        .alias("casos_graves"),

        F.sum("flag_hospitalizado")
        .alias("hospitalizados"),

        F.sum("flag_obito")
        .alias("obitos")
    )
)

# COMMAND ----------

# DBTITLE 1,34. GOLD HOSPITALIZAÇÃO
# ==============================================================================
# 34. GOLD HOSPITALIZAÇÃO
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_hospitalization...")

df_gold_hospitalization = (
    gold_base
    .groupBy(
        "ano_analise"
    )
    .agg(
        F.sum("flag_confirmado")
        .alias("casos_confirmados"),

        F.sum("flag_hospitalizado")
        .alias("hospitalizados"),

        F.sum("flag_dengue_grave")
        .alias("casos_graves"),

        F.sum("flag_obito")
        .alias("obitos")
    )
    .withColumn(
        "percentual_hospitalizacao",
        F.when(
            F.col("casos_confirmados") > 0,
            F.round(
                F.col("hospitalizados")
                / F.col("casos_confirmados")
                * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )
)

# COMMAND ----------

# DBTITLE 1,37. GOLD INDICADORES GERAIS
# ==============================================================================
# 37. GOLD INDICADORES GERAIS
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_indicators...")

df_gold_indicators = (
    gold_base
    .agg(
        F.sum("flag_notificado")
        .alias("total_notificacoes"),

        F.sum("flag_caso_provavel")
        .alias("total_casos_provaveis"),

        F.sum("flag_confirmado")
        .alias("total_casos_confirmados"),

        F.sum("flag_dengue_classica")
        .alias("total_dengue_classica"),

        F.sum("flag_sinais_alarme")
        .alias("total_casos_sinais_alarme"),

        F.sum("flag_dengue_grave")
        .alias("total_casos_graves"),

        F.sum("flag_hospitalizado")
        .alias("total_hospitalizados"),

        F.sum("flag_obito")
        .alias("total_obitos"),

        F.countDistinct(
            F.when(
                F.col("flag_confirmado") == 1,
                F.col("ID_MN_RESI_CODIGO")
            )
        ).alias(
            "municipios_com_casos"
        )
    )
    .withColumn(
        "letalidade_pct",
        F.when(
            F.col("total_casos_confirmados") > 0,
            F.round(
                F.col("total_obitos")
                /
                F.col("total_casos_confirmados")
                * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )
)

# COMMAND ----------

# DBTITLE 1,38. GOLD QUALIDADE
# ==============================================================================
# 38. GOLD QUALIDADE
# ==============================================================================

print(f"[GOLD] Criando {GOLD_PREFIX}_data_quality...")

df_gold_quality = (
    gold_base
    .groupBy("_source_year")
    .agg(
        F.count("*")
        .alias("total_registros"),

        F.sum(
            F.when(
                F.col("DT_NOTIFIC_Parsed").isNull(),
                1
            ).otherwise(0)
        )
        .alias("datas_invalidas"),

        F.sum(
            F.when(
                F.col("ID_MN_RESI_CODIGO").isNull(),
                1
            ).otherwise(0)
        )
        .alias("municipios_nulos"),

        F.sum(
            F.when(
                F.col("CLASSI_FIN_Int").isNull(),
                1
            ).otherwise(0)
        )
        .alias("classificacao_nula"),

        F.sum(
            F.when(
                F.col("SEM_PRI_Int").isNull(),
                1
            ).otherwise(0)
        )
        .alias("semana_epidemiologica_nula"),

        F.sum(
            F.when(
                F.col("populacao_referencia").isNull(),
                1
            ).otherwise(0)
        )
        .alias("populacao_nao_encontrada")
    )
    .withColumn(
        "pct_data_invalida",
        F.round(
            F.col("datas_invalidas")
            /
            F.col("total_registros")
            * 100,
            2
        )
    )
    .withColumn(
        "pct_municipio_nulo",
        F.round(
            F.col("municipios_nulos")
            /
            F.col("total_registros")
            * 100,
            2
        )
    )
    .withColumn(
        "pct_classificacao_nula",
        F.round(
            F.col("classificacao_nula")
            /
            F.col("total_registros")
            * 100,
            2
        )
    )
    .withColumn(
        "pct_semana_nula",
        F.round(
            F.col("semana_epidemiologica_nula")
            /
            F.col("total_registros")
            * 100,
            2
        )
    )
    .withColumn(
        "pct_populacao_nao_encontrada",
        F.round(
            F.col("populacao_nao_encontrada")
            /
            F.col("total_registros")
            * 100,
            2
        )
    )
    .withColumn(
        "execution_id",
        F.lit(RUN_ID)
    )
    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

# DBTITLE 1,39.3 TCC – SNAPSHOTS E REVISÃO DOS DADOS (backfill)
# ==============================================================================
# 39.3. REVISÃO DOS DADOS AO LONGO DO TEMPO
# ==============================================================================
# Pergunta: quando o OpenDataSUS publica uma atualização, quanto das
# notificações de uma semana já está na base? (ex.: "com 2 semanas de idade,
# o dado tem só 60% do valor final").
#
# Como funciona:
#  1. A cada atualização da fonte, gravamos um SNAPSHOT das contagens
#     semanais (append, nunca sobrescreve). A "assinatura" (SHA-256 dos
#     arquivos) evita gravar o mesmo snapshot duas vezes.
#  2. Na primeira execução, tentamos recuperar snapshots antigos pelo time
#     travel da Delta (versões anteriores da gold_dengue_weekly), se o
#     VACUUM ainda não os apagou.
#  3. Comparamos cada snapshot com o snapshot mais recente da mesma semana,
#     considerando "consolidadas" só as semanas com SEMANAS_CONSOLIDACAO de idade.
#
# IMPORTANTE: o resultado fica melhor quanto mais snapshots existirem.
# Agende o Job (ex.: diário) o quanto antes – cada atualização da fonte vira
# um ponto de observação.
# ==============================================================================

import hashlib as _hashlib

manifesto_atual = get_latest_manifest_by_year()
anos_disponiveis = sorted(int(y) for y, _ in downloaded_paths)
assinatura_fonte = _hashlib.md5(
    "|".join(
        f"{a}:{(manifesto_atual.get(a) or {}).get('sha256', '')}"
        for a in anos_disponiveis
    ).encode()
).hexdigest()

# Data do snapshot = data de publicação mais recente da fonte (Last-Modified
# do OpenDataSUS). Se não houver, usa a data da execução.
from email.utils import parsedate_to_datetime

datas_publicacao = []
for m in manifesto_atual.values():
    try:
        datas_publicacao.append(parsedate_to_datetime(m.get("last_modified")).date())
    except Exception:
        pass
DATA_SNAPSHOT = max(datas_publicacao) if datas_publicacao else HOJE
print(f"[TCC] Data de referência do snapshot: {DATA_SNAPSHOT}")

schema_snapshot = (
    "run_id string, data_snapshot date, ano_epi int, semana_epi int, "
    "total_notificacoes bigint, casos_provaveis bigint, casos_confirmados bigint, "
    "obitos bigint, assinatura_fonte string, origem string"
)


def normalizar_semanal(df, run_id, data_snapshot, assinatura, origem):
    """Converte uma versão (atual ou antiga) da gold semanal para o formato
    do snapshot. Versões antigas tinham SEM_PRI_Int no formato AAAASS."""
    sem = F.col("SEM_PRI_Int").cast("int")
    cols = df.columns
    return (
        df.select(
            F.lit(run_id).alias("run_id"),
            F.lit(data_snapshot).cast("date").alias("data_snapshot"),
            F.when(sem >= 190001, F.floor(sem / 100)).otherwise(F.col("ano_analise")).cast("int").alias("ano_epi"),
            F.when(sem >= 190001, sem % 100).otherwise(sem).cast("int").alias("semana_epi"),
            F.col("total_notificacoes").cast("bigint").alias("total_notificacoes"),
            (F.col("casos_provaveis") if "casos_provaveis" in cols else F.lit(None)).cast("bigint").alias("casos_provaveis"),
            F.col("casos_confirmados").cast("bigint").alias("casos_confirmados"),
            F.col("obitos").cast("bigint").alias("obitos"),
            F.lit(assinatura).alias("assinatura_fonte"),
            F.lit(origem).alias("origem"),
        )
        .filter(F.col("semana_epi").between(1, 53) & F.col("ano_epi").isNotNull())
        # semanas repetidas (versões antigas sem a correção do ano) são somadas
        .groupBy("run_id", "data_snapshot", "ano_epi", "semana_epi", "assinatura_fonte", "origem")
        .agg(*[F.sum(c).alias(c) for c in ["total_notificacoes", "casos_provaveis", "casos_confirmados", "obitos"]])
        .select(*[s.split()[0] for s in schema_snapshot.split(", ")])
    )


# --- 1) Recuperar histórico pelo time travel (somente na primeira vez) -------
if not spark.catalog.tableExists(GOLD_SNAPSHOT) and spark.catalog.tableExists(GOLD_WEEKLY):
    try:
        versoes = (
            spark.sql(f"DESCRIBE HISTORY {GOLD_WEEKLY}")
            .select("version", F.to_date("timestamp").alias("dia"))
            .withColumn(
                "_rn",
                F.row_number().over(Window.partitionBy("dia").orderBy(F.desc("version")))
            )
            .filter("_rn = 1")  # uma versão por dia
            .collect()
        )
        recuperados = []
        for v in versoes:
            try:
                df_v = spark.sql(f"SELECT * FROM {GOLD_WEEKLY} VERSION AS OF {v['version']}")
                # collect() força a leitura agora: se os arquivos já foram
                # apagados pelo VACUUM, o erro acontece aqui e a versão é pulada.
                recuperados += normalizar_semanal(
                    df_v, f"timetravel_v{v['version']}", v["dia"],
                    f"timetravel_v{v['version']}", "delta_time_travel"
                ).collect()
            except Exception as e:
                print(f"[TCC] Versão {v['version']} indisponível (provável VACUUM): {str(e)[:80]}")
        if recuperados:
            spark.createDataFrame(recuperados, schema_snapshot) \
                .write.format("delta").mode("append").saveAsTable(GOLD_SNAPSHOT)
            n_rec = len({r["data_snapshot"] for r in recuperados})
            print(f"[TCC] Snapshots recuperados do time travel: {n_rec} datas")
    except Exception as e:
        print(f"[TCC] Não foi possível ler o histórico da Delta: {e}")

# --- 2) Snapshot desta execução ------------------------------------------------
ja_existe = (
    spark.catalog.tableExists(GOLD_SNAPSHOT)
    and spark.table(GOLD_SNAPSHOT).filter(F.col("assinatura_fonte") == assinatura_fonte).limit(1).count() > 0
)

if ja_existe:
    print("[TCC] Snapshot desta versão da fonte já existe; nada a gravar.")
else:
    (
        normalizar_semanal(df_gold_weekly, RUN_ID, DATA_SNAPSHOT, assinatura_fonte, "pipeline")
        .write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(GOLD_SNAPSHOT)
    )
    print(f"[TCC] Snapshot gravado em {GOLD_SNAPSHOT} (assinatura {assinatura_fonte[:10]}...).")

# --- 3) Revisão: cada snapshot comparado com o valor mais recente --------------
w_semana = Window.partitionBy("ano_epi", "semana_epi")

df_gold_revisao = (
    spark.table(GOLD_SNAPSHOT)
    .join(df_cal_epi, ["ano_epi", "semana_epi"])
    .withColumn("idade_semanas", F.floor(F.datediff("data_snapshot", "fim_semana") / 7).cast("int"))
    .filter(F.col("idade_semanas") >= 0)
    .withColumn("n_snapshots", F.count("*").over(w_semana))
    .withColumn("notificacoes_final", F.max_by("total_notificacoes", "data_snapshot").over(w_semana))
    .withColumn("idade_semanas_final", F.max("idade_semanas").over(w_semana))
    .withColumn(
        "pct_do_final",
        F.when(F.col("notificacoes_final") > 0,
               F.round(F.col("total_notificacoes") / F.col("notificacoes_final") * 100, 2))
    )
)

df_gold_revisao_resumo = (
    df_gold_revisao
    .filter(
        (F.col("idade_semanas_final") >= SEMANAS_CONSOLIDACAO)
        & (F.col("notificacoes_final") > 0)
        & (F.col("idade_semanas") < F.col("idade_semanas_final"))
        & (F.col("idade_semanas") <= 26)
    )
    .groupBy("idade_semanas")
    .agg(
        F.count("*").alias("n_observacoes"),
        F.round(F.percentile_approx("pct_do_final", 0.5), 2).alias("pct_do_final_mediana"),
        F.round(F.percentile_approx("pct_do_final", 0.1), 2).alias("pct_do_final_p10"),
        F.round(F.percentile_approx("pct_do_final", 0.9), 2).alias("pct_do_final_p90"),
    )
    .orderBy("idade_semanas")
)

n_snap = spark.table(GOLD_SNAPSHOT).select("data_snapshot").distinct().count()
print(f"[TCC] Snapshots distintos disponíveis: {n_snap}")
if n_snap < 4:
    print("[TCC] Ainda há poucos snapshots; o resumo de revisão fica mais confiável com o tempo.")

# COMMAND ----------

# DBTITLE 1,39.8 TESTES DE QUALIDADE AUTOMATIZADOS (data quality gate)
# ==============================================================================
# 39.8. TESTES DE QUALIDADE AUTOMATIZADOS
# ==============================================================================
# A cada execução, o pipeline testa a própria saída e registra o resultado em
# gold_dengue_testes_qualidade (por ano da fonte): percentual de registros que
# violam cada regra, o limite aceito e o status (APROVADO, ALERTA ou FALHA).
#   * FALHA  -> problema que invalida análises (ex.: volume do ano caiu mais de
#               5% em relação à execução anterior = download truncado).
#   * ALERTA -> problema de qualidade que deve ser mencionado nos resultados.
# O pipeline NÃO interrompe a execução: o resultado fica registrado e é mostrado
# no final do notebook. (Mude QUALIDADE_INTERROMPE para True para parar na falha.)
# ==============================================================================

QUALIDADE_INTERROMPE = False
COLUNAS_ESSENCIAIS = [
    "DT_NOTIFIC", "DT_SIN_PRI", "SEM_PRI", "ID_MN_RESI",
    "CLASSI_FIN", "EVOLUCAO", "HOSPITALIZ", "CS_SEXO", "NU_IDADE_N",
]
DOMINIO_CLASSIFICACAO = tuple(CFG_AGRAVO["dominio"])

print("[QUALIDADE] Executando testes automatizados...")

_dp = F.col("DT_DIGITA_Parsed") if "DT_DIGITA_Parsed" in gold_base_full.columns else F.lit(None).cast("date")
_dup = F.col("flag_duplicado_sinan") if "flag_duplicado_sinan" in gold_base_full.columns else F.lit(0)

_linhas_q = (
    gold_base_full
    .groupBy("_source_year")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.col("ID_MN_RESI_CODIGO").isNull().cast("int")).alias("mun_residencia_nulo"),
        F.sum(F.col("DT_NOTIFIC_Parsed").isNull().cast("int")).alias("data_notificacao_invalida"),
        F.sum(F.col("SEM_PRI_Int").isNull().cast("int")).alias("semana_sintomas_invalida"),
        F.sum(
            (F.col("CLASSI_FIN_Int").isNotNull() & ~F.col("CLASSI_FIN_Int").isin(*DOMINIO_CLASSIFICACAO)).cast("int")
        ).alias("classificacao_fora_dominio"),
        F.sum((F.col("DT_SIN_PRI_Parsed") > F.col("DT_NOTIFIC_Parsed")).cast("int")).alias("sintomas_apos_notificacao"),
        F.sum(((F.col("idade_anos") < 0) | (F.col("idade_anos") > 120)).cast("int")).alias("idade_implausivel"),
        F.sum((_dp < F.col("DT_NOTIFIC_Parsed")).cast("int")).alias("digitacao_antes_notificacao"),
        F.sum(F.col("municipio_nome").isNull().cast("int")).alias("municipio_fora_dimensao_ibge"),
        F.sum(_dup.cast("int")).alias("duplicado_marcado_sinan"),
        F.sum(F.col("flag_descartado")).alias("n_descartados"),
        F.sum((F.col("flag_obito_evolucao_agravo") - F.col("flag_obito")).cast("int")).alias("obitos_nao_confirmados"),
    )
    .collect()
)

# (coluna, descrição, limite % aceito, gravidade se ultrapassar)
REGRAS_QUALIDADE = [
    ("mun_residencia_nulo", "Município de residência ausente", 1.0, "ALERTA"),
    ("data_notificacao_invalida", "Data de notificação ausente ou inválida", 1.0, "FALHA"),
    ("semana_sintomas_invalida", "Semana de primeiros sintomas ausente ou fora de 1-53", 1.0, "ALERTA"),
    ("classificacao_fora_dominio", f"Classificação final fora do domínio {set(DOMINIO_CLASSIFICACAO)}", 2.0, "ALERTA"),
    ("sintomas_apos_notificacao", "Início dos sintomas posterior à notificação", 1.0, "ALERTA"),
    ("idade_implausivel", "Idade negativa ou acima de 120 anos", 0.5, "ALERTA"),
    ("digitacao_antes_notificacao", "Data de digitação anterior à notificação", 1.0, "ALERTA"),
    ("municipio_fora_dimensao_ibge", "Município de residência fora da dimensão IBGE", 1.0, "FALHA"),
    ("duplicado_marcado_sinan", "Registros marcados como duplicados pelo SINAN (NDUPLIC_N=1)", 1.0, "ALERTA"),
]

linhas_testes = []
volume_atual = {}
for _r in _linhas_q:
    _ano, _n = int(_r["_source_year"]), int(_r["n"])
    volume_atual[_ano] = _n
    for _col, _desc, _lim, _grav in REGRAS_QUALIDADE:
        _viol = int(_r[_col] or 0)
        _pct = round(_viol / _n * 100, 3) if _n else None
        _status = "APROVADO" if (_pct is not None and _pct <= _lim) else _grav
        linhas_testes.append((RUN_ID, _ano, _col, _desc, _n, _viol, _pct, _lim, _status))
    # Sem nenhum descartado no arquivo, "total_notificacoes" é igual a "casos
    # prováveis" e NÃO representa todas as notificações do painel.
    _desc_n = int(_r["n_descartados"] or 0)
    linhas_testes.append((
        RUN_ID, _ano, "descartados_presentes",
        "Arquivo contém notificações descartadas (sem elas, total de notificações = casos prováveis)",
        _n, _desc_n, round(_desc_n / _n * 100, 3) if _n else None, 0.0,
        "APROVADO" if _desc_n > 0 else "ALERTA",
    ))
    # Óbitos pelo agravo em casos não confirmados: ficam fora de "obitos"
    _onc = int(_r["obitos_nao_confirmados"] or 0)
    linhas_testes.append((
        RUN_ID, _ano, "obitos_nao_confirmados",
        "Óbitos pelo agravo (EVOLUCAO 2) em casos não confirmados – fora da contagem de óbitos",
        _n, _onc, round(_onc / _n * 100, 3) if _n else None, 0.0,
        "APROVADO" if _onc == 0 else "ALERTA",
    ))

# Colunas essenciais presentes no arquivo (teste estrutural)
_faltando = [c for c in COLUNAS_ESSENCIAIS if c not in gold_base_full.columns]
linhas_testes.append((
    RUN_ID, 0, "colunas_essenciais", "Colunas essenciais do SINAN presentes: faltando "
    + (", ".join(_faltando) if _faltando else "nenhuma"),
    len(COLUNAS_ESSENCIAIS), len(_faltando),
    round(len(_faltando) / len(COLUNAS_ESSENCIAIS) * 100, 3), 0.0,
    "FALHA" if _faltando else "APROVADO",
))

# Variação de volume em relação à execução anterior (detecta download truncado)
if spark.catalog.tableExists(VOLUME_ANO_TABLE):
    _ultimo_run = (
        spark.table(VOLUME_ANO_TABLE)
        .filter(F.col("run_id") != RUN_ID)
        .orderBy(F.desc("executado_em"))
        .select("run_id").limit(1).collect()
    )
    if _ultimo_run:
        _ant = {
            int(r["ano_fonte"]): int(r["n_registros"])
            for r in spark.table(VOLUME_ANO_TABLE).filter(F.col("run_id") == _ultimo_run[0]["run_id"]).collect()
        }
        for _ano, _n in volume_atual.items():
            if _ano in _ant and _ant[_ano] > 0:
                _queda = round((_ant[_ano] - _n) / _ant[_ano] * 100, 3)
                linhas_testes.append((
                    RUN_ID, _ano, "queda_de_volume",
                    "Queda do número de registros em relação à execução anterior",
                    _n, max(_ant[_ano] - _n, 0), _queda, 5.0,
                    "APROVADO" if _queda <= 5.0 else "FALHA",
                ))

if not SOMENTE_GOLD:
    spark.createDataFrame(
        [(RUN_ID, a, n, datetime.now(timezone.utc)) for a, n in volume_atual.items()],
        "run_id string, ano_fonte int, n_registros long, executado_em timestamp"
    ).write.format("delta").mode("append").saveAsTable(VOLUME_ANO_TABLE)

df_gold_testes_qualidade = spark.createDataFrame(
    linhas_testes,
    "run_id string, ano_fonte int, teste string, descricao string, registros_testados long, "
    "violacoes long, pct_violacao double, limite_pct double, status string"
).orderBy("ano_fonte", "teste")

_n_falhas = sum(1 for l in linhas_testes if l[-1] == "FALHA")
_n_alertas = sum(1 for l in linhas_testes if l[-1] == "ALERTA")
print(f"[QUALIDADE] {len(linhas_testes)} testes: {_n_falhas} FALHA(S), {_n_alertas} ALERTA(S).")
for _l in linhas_testes:
    if _l[-1] != "APROVADO":
        print(f"[QUALIDADE]   {_l[-1]} | ano {_l[1]} | {_l[3]} | {_l[6]}% (limite {_l[7]}%)")
if _n_falhas and QUALIDADE_INTERROMPE:
    raise Exception(f"{_n_falhas} teste(s) de qualidade com FALHA; veja {GOLD_TESTES_QUALIDADE}.")

# Distribuição de CLASSI_FIN por arquivo anual: confere se a codificação
# assumida para o agravo (CFG_AGRAVO) é a que aparece de fato nos dados.
df_gold_dist_classificacao = (
    gold_base_full
    .groupBy("_source_year", "CLASSI_FIN_Int", "classificacao_nome")
    .agg(F.count("*").alias("registros"))
    .withColumn(
        "pct_no_ano",
        F.round(F.col("registros") / F.sum("registros").over(Window.partitionBy("_source_year")) * 100, 3)
    )
    .withColumn("conta_como_confirmado", F.col("CLASSI_FIN_Int").isin(*CFG_AGRAVO["confirmado"]))
    .withColumn("conta_como_provavel", F.col("CLASSI_FIN_Int").isNull() | ~F.col("CLASSI_FIN_Int").isin(*CODIGOS_NAO_PROVAVEIS))
    .withColumn("agravo", F.lit(AGRAVO))
    .orderBy("_source_year", "CLASSI_FIN_Int")
)

# COMMAND ----------

# DBTITLE 1,40. SALVAR TODAS AS TABELAS GOLD
# ==============================================================================
# 40. SALVAR TODAS AS TABELAS GOLD
# ==============================================================================

# A revisão foi calculada com o histórico completo; na saída ficam só os anos
# do período de análise. (As tabelas de vigilância são gravadas pelo 21.)
df_gold_revisao = df_gold_revisao.filter(F.col("ano_epi") >= ANO_INICIO_ANALISE)

gold_objects = [
    (df_gold_weekly, GOLD_WEEKLY),
    (df_gold_annual, GOLD_ANNUAL),
    (df_gold_municipal, GOLD_MUNICIPAL),
    (df_gold_municipal_weekly, GOLD_MUNICIPAL_WEEKLY),
    (df_gold_demographic, GOLD_DEMOGRAPHIC),
    (df_gold_sexo_summary, GOLD_SEXO_SUMMARY),
    (df_gold_sexo_anual, GOLD_SEXO_ANUAL),
    (df_gold_sexo_faixa, GOLD_SEXO_FAIXA),
    (df_gold_severity, GOLD_SEVERITY),
    (df_gold_hospitalization, GOLD_HOSPITALIZATION),
    (df_gold_monthly, GOLD_MONTHLY),
    (df_gold_indicators, GOLD_INDICATORS),
    (df_gold_quality, GOLD_QUALITY),
    (df_gold_revisao, GOLD_REVISAO),
    (df_gold_revisao_resumo, GOLD_REVISAO_RESUMO),
    (df_gold_sintomas, GOLD_SINTOMAS),
    (df_gold_perfil, GOLD_PERFIL),
    (df_gold_laboratorio, GOLD_LABORATORIO),
    (df_gold_autoctonia, GOLD_AUTOCTONIA),
    (df_gold_tempos, GOLD_TEMPOS),
    (df_gold_gravidade_sinais, GOLD_GRAVIDADE_SINAIS),
    (df_gold_cubo_semanal, GOLD_CUBO_SEMANAL),
    (df_gold_painel, GOLD_PAINEL),
    (df_gold_regional, GOLD_REGIONAL),
    (df_gold_incidencia, GOLD_INCIDENCIA),
    (df_gold_demografico_semanal, GOLD_DEMOGRAFICO_SEMANAL),
    (df_gold_clinico_semanal, GOLD_CLINICO_SEMANAL),
    (df_gold_hospitalizacao_semanal, GOLD_HOSPITALIZACAO_SEMANAL),
    (df_gold_fatores_risco, GOLD_FATORES_RISCO),
    (df_gold_testes_qualidade, GOLD_TESTES_QUALIDADE),
    (df_gold_dist_classificacao, GOLD_DIST_CLASSIFICACAO),
]



for dataframe, table_name in gold_objects:

    print(
        f"[GOLD] Salvando {table_name}..."
    )

    (
        dataframe
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(table_name)
    )

# COMMAND ----------

# DBTITLE 1,40b. VIEWS PARA O PAINEL (Databricks AI/BI)
# ==============================================================================
# 40b. VIEWS DO PAINEL
# ==============================================================================
# O painel ao vivo é um dashboard AI/BI do Databricks. Ele lê estas views, que
# ficam sempre atualizadas: quando o pipeline regrava as tabelas Gold, o painel
# mostra os dados novos na próxima abertura ou atualização, sem exportar CSV.
# As views só renomeiam, juntam o nome do município e a população; nenhuma conta
# nova é feita aqui.
# ==============================================================================

_P = f"{CATALOG}.{SCHEMA}"
VIEWS_PAINEL = {
    "vw_painel_cubo": f"""
        SELECT c.ano_analise AS ano, c.semana, CAST(c.municipio_codigo AS STRING) AS municipio_codigo,
               COALESCE(d.municipio_nome, CAST(c.municipio_codigo AS STRING)) AS municipio,
               c.sexo_nome AS sexo, c.classificacao_nome AS classificacao,
               c.notificacoes, c.casos_provaveis, c.obitos,
               c.obitos_investigacao, c.hospitalizados
        FROM {GOLD_CUBO_SEMANAL} c
        LEFT JOIN (
            SELECT municipio_codigo_6, MAX(municipio_nome) AS municipio_nome
            FROM {POPULATION_TABLE} GROUP BY municipio_codigo_6
        ) d ON d.municipio_codigo_6 = CAST(c.municipio_codigo AS STRING)
    """,
    "vw_painel_demografico": f"""
        SELECT ano_analise AS ano, semana, sexo_nome AS sexo, faixa_etaria,
               raca_nome AS raca_cor, casos_provaveis, obitos,
               obitos_investigacao, hospitalizados
        FROM {GOLD_DEMOGRAFICO_SEMANAL}
    """,
    "vw_painel_clinico": f"""
        SELECT ano_analise AS ano, semana, grupo, item, n_informado, n_presente,
               pct_presente
        FROM {GOLD_CLINICO_SEMANAL}
    """,
    "vw_painel_incidencia": f"""
        SELECT nivel, ano_analise AS ano, semana, codigo, nome, casos_provaveis,
               populacao, coef_incidencia_100mil, ic95_inf, ic95_sup, base_populacao
        FROM {GOLD_INCIDENCIA}
    """,
    "vw_painel_anual": f"""
        SELECT ano_analise AS ano, SUM(casos_provaveis) AS casos_provaveis,
               SUM(obitos) AS obitos, SUM(obitos_investigacao) AS obitos_investigacao,
               SUM(hospitalizados) AS hospitalizados
        FROM {GOLD_CUBO_SEMANAL} GROUP BY ano_analise
    """,
}

for _nome_view, _sql_view in (VIEWS_PAINEL.items() if CRIAR_VIEWS_PAINEL else []):
    try:
        spark.sql(f"CREATE OR REPLACE VIEW {_P}.{_nome_view} AS {_sql_view}")
        print(f"[PAINEL] View atualizada: {_P}.{_nome_view}")
    except Exception as e:
        print(f"[PAINEL][WARN] {_nome_view}: {str(e)[:200]}")
if not CRIAR_VIEWS_PAINEL:
    print("[PAINEL] Views vw_painel_* não criadas (widget criar_views_painel = nao; o dashboard não as usa).")

# COMMAND ----------

# DBTITLE 1,Registrar métrica do cálculo da Gold e iniciar cronômetro da manutenção
# gold_calculo = da seção 26 até gravar todas as tabelas Gold e as views.
# Por avaliação preguiçosa do Spark, o trabalho real acontece nas ações
# (collect, count, write), todas dentro deste intervalo.
registrar_metrica(RUN_ID, "gold_calculo", time.perf_counter() - t_start_gold,
                  registros_processados=registros_silver)
t_start_manutencao = time.perf_counter()

# COMMAND ----------

# DBTITLE 1,41. OTIMIZAÇÃO
# ==============================================================================
# 41. OTIMIZAÇÃO
# ==============================================================================

print("[OPTIMIZE] Otimizando tabelas...")

gold_tables = [
    GOLD_WEEKLY,
    GOLD_ANNUAL,
    GOLD_MUNICIPAL,
    GOLD_MUNICIPAL_WEEKLY,
    GOLD_DEMOGRAPHIC,
    GOLD_SEXO_SUMMARY,
    GOLD_SEXO_ANUAL,
    GOLD_SEXO_FAIXA,
    GOLD_SEVERITY,
    GOLD_HOSPITALIZATION,
    GOLD_MONTHLY,
    GOLD_INDICATORS,
    GOLD_QUALITY,
    GOLD_SNAPSHOT,
    GOLD_SINTOMAS,
]

for table_name in gold_tables:

    try:

        spark.sql(
            f"OPTIMIZE {table_name}"
        )

        print(
            f"[OPTIMIZE] OK: {table_name}"
        )

    except Exception as e:

        print(
            f"[WARN] OPTIMIZE não executado "
            f"em {table_name}: {str(e)}"
        )

# COMMAND ----------

# DBTITLE 1,41b. LINHAGEM E RASTREABILIDADE DOS DADOS
# ==============================================================================
# 41b. LINHAGEM: de qual versão da fonte e com quais parâmetros cada tabela saiu
# ==============================================================================
# Para cada tabela Gold gravada nesta execução registra: versão Delta, número de
# linhas, hash da fonte (assinatura dos SHA-256 dos arquivos do SINAN), versão do
# pipeline e parâmetros. Também grava essas informações como propriedades da
# própria tabela (TBLPROPERTIES), visíveis no Catalog Explorer. Assim, qualquer
# número do artigo pode ser rastreado até a versão exata da fonte que o gerou.
# ==============================================================================

import json as _json

_parametros_run = _json.dumps({
    "periodo_analise": [ANO_INICIO_ANALISE, END_YEAR],
    "anos_fonte": [int(a) for a, _ in downloaded_paths],
    "semanas_consecutivas_alerta": SEMANAS_CONSECUTIVAS_ALERTA,
    "anos_excluidos_canal": ANOS_EXCLUIDOS_CANAL,
    "janela_anos_canal": JANELA_ANOS_CANAL,
    "min_anos_historico": MIN_ANOS_HISTORICO,
    "defasagens_informacao": DEFASAGENS_INFORMACAO,
    "limiares_sensibilidade": LIMIARES_SENSIBILIDADE,
    "data_disponibilidade": DATA_DISPONIBILIDADE,
    "remover_duplicados_sinan": REMOVER_DUPLICADOS_SINAN,
    "excluir_chikungunya": EXCLUIR_CHIKUNGUNYA,
    "modo_execucao": "somente_gold" if SOMENTE_GOLD else "completo",
    "agravo": AGRAVO,
    "uf": UF_SIGLA,
    "schema": SCHEMA,
    "experimento": EXPERIMENTO or None,
    "configuracao": CONFIGURACAO or None,
    "notebook_version": NOTEBOOK_VERSION,
}, ensure_ascii=False)

_linhas_linhagem = []
for _df, _tabela in gold_objects:
    try:
        _versao = spark.sql(f"DESCRIBE HISTORY {_tabela} LIMIT 1").collect()[0]["version"]
        _linhas = spark.table(_tabela).count()
        spark.sql(
            f"ALTER TABLE {_tabela} SET TBLPROPERTIES ("
            f"'pipeline.run_id' = '{RUN_ID}', "
            f"'pipeline.versao' = '{PIPELINE_VERSION}', "
            f"'pipeline.assinatura_fonte' = '{assinatura_fonte}')"
        )
        _linhas_linhagem.append((
            RUN_ID, _tabela, int(_linhas), int(_versao), PIPELINE_VERSION,
            assinatura_fonte, _parametros_run, datetime.now(timezone.utc)
        ))
    except Exception as e:
        print(f"[LINHAGEM][WARN] {_tabela}: {str(e)[:120]}")

if _linhas_linhagem:
    (
        spark.createDataFrame(
            _linhas_linhagem,
            "run_id string, tabela string, linhas long, versao_delta long, pipeline_version string, "
            "assinatura_fonte string, parametros string, executado_em timestamp"
        )
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable(GOLD_LINHAGEM)
    )
    print(f"[LINHAGEM] {len(_linhas_linhagem)} tabelas registradas em {GOLD_LINHAGEM}.")

# COMMAND ----------

# DBTITLE 1,42. VALIDAÇÃO
# ==============================================================================
# 42. VALIDAÇÃO
# ==============================================================================

print("\n")
print("=" * 90)
print("VALIDAÇÃO FINAL DA GOLD")
print("=" * 90)

for dataframe, table_name in gold_objects:

    try:

        count = (
            spark.table(table_name)
            .count()
        )

        print(
            f"{table_name:<75} "
            f"{count:>12,} registros"
        )

    except Exception as e:

        print(
            f"{table_name:<75} ERRO: {e}"
        )


print("\n")
print("Indicadores gerais:")
display(
    spark.table(GOLD_INDICATORS)
)

print("\n")
print(f"Casos por sexo ({START_YEAR}-{END_YEAR}):")
display(
    spark.table(GOLD_SEXO_SUMMARY)
)

print("\n")
print("Evolução anual por sexo:")
display(
    spark.table(GOLD_SEXO_ANUAL)
    .orderBy(
        F.desc("ano_analise"),
        "sexo_nome"
    )
)

print("\n")
print("Top 20 municípios por casos confirmados:")
display(
    spark.table(GOLD_MUNICIPAL)
    .orderBy(
        F.desc("casos_confirmados")
    )
    .limit(20)
)

print("\n")
print("Últimas semanas:")
display(
    spark.table(GOLD_WEEKLY)
    .orderBy(
        F.desc("ano_analise"),
        F.desc("SEM_PRI_Int")
    )
    .limit(20)
)

print("\n")
print("=" * 90)
print("PIPELINE CONCLUÍDO COM GOLD DEFINITIVA")
print("=" * 90)
print(f"Execução: {RUN_ID}")
print(f"Tempo total: {time.perf_counter() - RUN_START:.2f} segundos")
print("=" * 90)

# COMMAND ----------

# DBTITLE 1,Registrar métrica da manutenção (OPTIMIZE, linhagem, validação)
registrar_metrica(RUN_ID, "gold_manutencao", time.perf_counter() - t_start_manutencao)

# COMMAND ----------

# DBTITLE 1,Microdados tratados (Parquet) – widget exportar_microdados
# Um registro por notificação, todos os campos decodificados, sem nome nem
# endereço (o SINAN aberto já não traz identificação pessoal). Parquet,
# particionado por ano, em <volume>/exportacoes/<rotulo_execucao>/microdados_parquet.
EXPORT_DIR = os.path.join(VOLUME_PATH, "exportacoes", ROTULO_EXECUCAO)
if True:
    # Microdados tratados (um registro por notificação, todos os campos decodificados,
    # sem nome nem endereço: o SINAN aberto já não traz identificação pessoal).
    # Parquet, particionado por ano. Ligue o widget "exportar_microdados".
    if EXPORTAR_MICRODADOS:
        try:
            _dir_micro = os.path.join(EXPORT_DIR, "microdados_parquet")
            _cols_micro = [
                c for c in gold_base.columns
                if not c.startswith("_")
                and c not in ("NU_IDADE_N",)  # bruto; idade_anos já está decodificada
            ]
            (
                gold_base.select(*_cols_micro)
                .write.mode("overwrite").partitionBy("ano_analise")
                .parquet(_dir_micro)
            )
            print(f"[EXPORT] microdados: {len(_cols_micro)} colunas -> {_dir_micro}")
        except Exception as e:
            print(f"[EXPORT][WARN] microdados: {str(e)[:200]}")
    else:
        print("[EXPORT] Microdados não exportados (widget exportar_microdados = nao).")

# COMMAND ----------

# DBTITLE 1,Contagem de registros por camada
print("Bronze:", spark.table(BRONZE_TABLE).count())
print("Silver:", spark.table(SILVER_TABLE).count())
print("Gold semanal:", spark.table(GOLD_WEEKLY).count())
print("Gold anual:", spark.table(GOLD_ANNUAL).count())
print("Gold municipal:", spark.table(GOLD_MUNICIPAL).count())
print("Gold sexo (resumo):", spark.table(GOLD_SEXO_SUMMARY).count())

# COMMAND ----------

# DBTITLE 1,Métricas reais desta execução (tempo por camada)
# As durações abaixo são reais, medidas com time.perf_counter() durante o
# processamento de cada camada (ver células "Registrar métrica real da
# camada ..." acima) e gravadas na tabela pipeline_metrics.

metricas_desta_execucao = (
    spark.table(METRICS_TABLE)
    .filter(F.col("run_id") == RUN_ID)
    .orderBy("executado_em")
)

display(metricas_desta_execucao)

tempo_total_camadas = (
    metricas_desta_execucao
    .agg(F.sum("duracao_segundos"))
    .collect()[0][0]
) or 0.0

print(f"[MÉTRICA] Execução: {RUN_ID}")
print(f"[MÉTRICA] Soma das camadas medidas: {tempo_total_camadas:.2f}s")
print(f"[MÉTRICA] Tempo Total (RUN_START, inclui verificação de fontes): {time.perf_counter() - RUN_START:.2f}s")

# COMMAND ----------

# DBTITLE 1,Informações do ambiente de execução
import os

# 1. Versão do Spark (Sempre disponível nativamente)
spark_version = spark.version

# 2. Captura segura das tags e metadados do ambiente
try:
    dbr_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
except Exception:
    dbr_version = os.getenv("DATABRICKS_RUNTIME_VERSION", "Databricks Runtime (Standard)")

try:
    node_type = spark.conf.get("spark.databricks.clusterUsageTags.nodeTypeId")
except Exception:
    node_type = "Não aplicável / Serverless"

try:
    num_workers = spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers")
except Exception:
    num_workers = "Dinâmico / Auto-scaling"

print(f"Versão do Spark: {spark_version}")
print(f"Databricks Runtime (DBR): {dbr_version}")
print(f"Tipo de Máquina (Node Type): {node_type}")
print(f"Nº de Workers: {num_workers}")

# COMMAND ----------

# DBTITLE 1,Saída do notebook (lida pelo 12 e pelo 24_eficiencia_da_solucao)
import json as _json_saida

dbutils.notebook.exit(_json_saida.dumps({
    "status": "OK",
    "run_id": RUN_ID,
    "schema": SCHEMA,
    "agravo": AGRAVO,
    "uf": UF_SIGLA,
    "tempo_total_s": round(time.perf_counter() - RUN_START, 2),
    "fontes": {str(a): st for a, st in source_status},
}))