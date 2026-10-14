"""
Ingestão das tabelas de referência (para os joins) no S3 + tabelas no Athena (database datasus_raw).

Organização (igual no S3 e na pasta local data/):
  raw/<tabela>/                  dados prontos para consulta: 1 pasta por tabela do Athena
  landing/referencias/<tabela>/  arquivos originais das fontes, antes da simplificação

Tabelas:
  municipios                   IBGE: nome do município, UF e regiões
  doencas                      DATASUS: CID-10, 1 linha por código de doença (subcat)
  hospitais                    OpenDataSUS: Hospitais e Leitos 2024, 1 linha por hospital (cnes)
  doencas_categorias           DATASUS: CID-10, categoria de cada código de doença
  doencas_grupos               DATASUS: CID-10, grupo de cada código de doença
  doencas_capitulos            DATASUS: CID-10, capítulo de cada código de doença
  internacoes_evitaveis_lista  Lista ICSAP da Portaria SAS/MS nº 221/2008, 1 linha por código evitável

municipios e doencas já chegam prontas e vão direto para raw/. As outras cinco são salvas
em landing/referencias/ e depois simplificadas por simplificar_tabelas.py (chamado no final).

Todas as colunas ficam como texto. Pode rodar de novo: os arquivos e as tabelas são substituídos.

Uso:
  uv run python ingestion/ingest_referencias.py
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import re
import sys
import time
import unicodedata
import urllib.request
import zipfile
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

BUCKET = "datasus-icsap-data-nm2026"
PROFILE = "datasus"
REGION = "us-east-1"
WORKGROUP = "datasus-icsap"
DATABASE = "datasus_raw"
LOCAL_DIR = Path(__file__).resolve().parent.parent / "data"

URL_IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
URL_CID10 = "http://www2.datasus.gov.br/cid10/V2008/downloads/CID10CSV.zip"
URL_LEITOS = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS/Leitos_2024.csv"

# Tabelas que já chegam prontas (vão direto para raw/ e viram tabela no Athena)
READY_TABLES = {"municipios", "doencas"}

CID10_FILES = {
    "doencas_capitulos": "CID-10-CAPITULOS.CSV",
    "doencas_grupos": "CID-10-GRUPOS.CSV",
    "doencas_categorias": "CID-10-CATEGORIAS.CSV",
    "doencas": "CID-10-SUBCATEGORIAS.CSV",
}

# ---------------------------------------------------------------------------
# Lista ICSAP - Portaria SAS/MS nº 221, de 17/04/2008 (anexo)
# Códigos CID-10 sem ponto, no mesmo formato da coluna diag_princ do SIH.
# cid_inicio/cid_fim com 3 caracteres = faixa de categorias; com 4 = subcategorias.
# ---------------------------------------------------------------------------
G1 = "Doenças preveníveis por imunização e condições sensíveis"
G2 = "Gastroenterites infecciosas e complicações"
G4 = "Deficiências nutricionais"
G5 = "Infecções de ouvido, nariz e garganta"
G6 = "Pneumonias bacterianas"
G8 = "Doenças pulmonares"
G9 = "Hipertensão"
G11 = "Insuficiência cardíaca"
G12 = "Doenças cerebrovasculares"
G13 = "Diabetes mellitus"
G15 = "Infecção no rim e trato urinário"
G16 = "Infecção da pele e tecido subcutâneo"
G17 = "Doença inflamatória de órgãos pélvicos femininos"
G18 = "Úlcera gastrointestinal"
G19 = "Doenças relacionadas ao pré-natal e parto"

ICSAP = [
    # (grupo, grupo_nome, subgrupo, diagnostico, cid_inicio, cid_fim)
    ("1", G1, "1.1", "Coqueluche", "A37", "A37"),
    ("1", G1, "1.2", "Difteria", "A36", "A36"),
    ("1", G1, "1.3", "Tétano", "A33", "A35"),
    ("1", G1, "1.4", "Parotidite", "B26", "B26"),
    ("1", G1, "1.5", "Rubéola", "B06", "B06"),
    ("1", G1, "1.6", "Sarampo", "B05", "B05"),
    ("1", G1, "1.7", "Febre amarela", "A95", "A95"),
    ("1", G1, "1.8", "Hepatite B", "B16", "B16"),
    ("1", G1, "1.9", "Meningite por Haemophilus", "G000", "G000"),
    ("1", G1, "1.10", "Meningite tuberculosa", "A170", "A170"),
    ("1", G1, "1.11", "Tuberculose miliar", "A19", "A19"),
    ("1", G1, "1.12", "Tuberculose pulmonar", "A150", "A153"),
    ("1", G1, "1.12", "Tuberculose pulmonar", "A160", "A162"),
    ("1", G1, "1.12", "Tuberculose pulmonar", "A154", "A159"),
    ("1", G1, "1.12", "Tuberculose pulmonar", "A163", "A169"),
    ("1", G1, "1.12", "Tuberculose pulmonar", "A171", "A179"),
    ("1", G1, "1.16", "Outras tuberculoses", "A18", "A18"),
    ("1", G1, "1.17", "Febre reumática", "I00", "I02"),
    ("1", G1, "1.18", "Sífilis", "A51", "A53"),
    ("1", G1, "1.19", "Malária", "B50", "B54"),
    ("1", G1, "1.20", "Ascaridíase", "B77", "B77"),
    ("2", G2, "2.1", "Desidratação", "E86", "E86"),
    ("2", G2, "2.2", "Gastroenterites", "A00", "A09"),
    ("3", "Anemia", "3.1", "Anemia por deficiência de ferro", "D50", "D50"),
    ("4", G4, "4.1", "Kwashiorkor e outras formas de desnutrição proteico-calórica", "E40", "E46"),
    ("4", G4, "4.2", "Outras deficiências nutricionais", "E50", "E64"),
    ("5", G5, "5.1", "Otite média supurativa", "H66", "H66"),
    ("5", G5, "5.2", "Nasofaringite aguda (resfriado comum)", "J00", "J00"),
    ("5", G5, "5.3", "Sinusite aguda", "J01", "J01"),
    ("5", G5, "5.4", "Faringite aguda", "J02", "J02"),
    ("5", G5, "5.5", "Amigdalite aguda", "J03", "J03"),
    ("5", G5, "5.6", "Infecção aguda das vias aéreas superiores", "J06", "J06"),
    ("5", G5, "5.7", "Rinite, nasofaringite e faringite crônicas", "J31", "J31"),
    ("6", G6, "6.1", "Pneumonia pneumocócica", "J13", "J13"),
    ("6", G6, "6.2", "Pneumonia por Haemophilus influenzae", "J14", "J14"),
    ("6", G6, "6.3", "Pneumonia por Streptococcus", "J153", "J154"),
    ("6", G6, "6.4", "Pneumonia bacteriana NE", "J158", "J159"),
    ("6", G6, "6.5", "Pneumonia lobar NE", "J181", "J181"),
    ("7", "Asma", "7.1", "Asma", "J45", "J46"),
    ("8", G8, "8.1", "Bronquite aguda", "J20", "J21"),
    ("8", G8, "8.2", "Bronquite não especificada como aguda ou crônica", "J40", "J40"),
    ("8", G8, "8.3", "Bronquite crônica simples e mucopurulenta", "J41", "J41"),
    ("8", G8, "8.4", "Bronquite crônica não especificada", "J42", "J42"),
    ("8", G8, "8.5", "Enfisema", "J43", "J43"),
    ("8", G8, "8.6", "Bronquiectasia", "J47", "J47"),
    ("8", G8, "8.7", "Outras doenças pulmonares obstrutivas crônicas", "J44", "J44"),
    ("9", G9, "9.1", "Hipertensão essencial", "I10", "I10"),
    ("9", G9, "9.2", "Doença cardíaca hipertensiva", "I11", "I11"),
    ("10", "Angina", "10.1", "Angina pectoris", "I20", "I20"),
    ("11", G11, "11.1", "Insuficiência cardíaca", "I50", "I50"),
    ("11", G11, "11.3", "Edema agudo de pulmão", "J81", "J81"),
    ("12", G12, "12.1", "Doenças cerebrovasculares", "I63", "I67"),
    ("12", G12, "12.1", "Doenças cerebrovasculares", "I69", "I69"),
    ("12", G12, "12.1", "Doenças cerebrovasculares", "G45", "G46"),
    *[row for cat in ("E10", "E11", "E12", "E13", "E14") for row in (
        ("13", G13, "13.1", "Diabetes com coma ou cetoacidose", f"{cat}0", f"{cat}1"),
        ("13", G13, "13.2", "Diabetes com complicações", f"{cat}2", f"{cat}8"),
        ("13", G13, "13.3", "Diabetes sem complicações específicas", f"{cat}9", f"{cat}9"),
    )],
    ("14", "Epilepsias", "14.1", "Epilepsias", "G40", "G41"),
    ("15", G15, "15.1", "Nefrite túbulo-intersticial aguda", "N10", "N10"),
    ("15", G15, "15.2", "Nefrite túbulo-intersticial crônica", "N11", "N11"),
    ("15", G15, "15.3", "Nefrite túbulo-intersticial NE aguda ou crônica", "N12", "N12"),
    ("15", G15, "15.4", "Cistite", "N30", "N30"),
    ("15", G15, "15.5", "Uretrite", "N34", "N34"),
    ("15", G15, "15.6", "Infecção do trato urinário de localização NE", "N390", "N390"),
    ("16", G16, "16.1", "Erisipela", "A46", "A46"),
    ("16", G16, "16.2", "Impetigo", "L01", "L01"),
    ("16", G16, "16.3", "Abscesso cutâneo, furúnculo e carbúnculo", "L02", "L02"),
    ("16", G16, "16.4", "Celulite", "L03", "L03"),
    ("16", G16, "16.5", "Linfadenite aguda", "L04", "L04"),
    ("16", G16, "16.6", "Outras infecções localizadas na pele e tecido subcutâneo", "L08", "L08"),
    ("17", G17, "17.1", "Salpingite e ooforite", "N70", "N70"),
    ("17", G17, "17.2", "Doença inflamatória do útero, exceto o colo", "N71", "N71"),
    ("17", G17, "17.3", "Doença inflamatória do colo do útero", "N72", "N72"),
    ("17", G17, "17.4", "Outras doenças inflamatórias pélvicas femininas", "N73", "N73"),
    ("17", G17, "17.5", "Doenças da glândula de Bartholin", "N75", "N75"),
    ("17", G17, "17.6", "Outras afecções inflamatórias da vagina e da vulva", "N76", "N76"),
    ("18", G18, "18.1", "Úlcera gastrointestinal", "K25", "K28"),
    ("18", G18, "18.1", "Úlcera gastrointestinal", "K920", "K922"),
    ("19", G19, "19.1", "Infecção do trato urinário na gravidez", "O23", "O23"),
    ("19", G19, "19.2", "Sífilis congênita", "A50", "A50"),
    ("19", G19, "19.3", "Síndrome da rubéola congênita", "P350", "P350"),
]


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------
def download(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (datasus-icsap)"})
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                data = resp.read()
            if data[:2] == b"\x1f\x8b":  # resposta compactada (gzip), como a da API do IBGE
                data = gzip.decompress(data)
            return data
        except OSError as err:
            print(f"   falha no download (tentativa {attempt}/3): {err}")
            time.sleep(5 * attempt)
    raise RuntimeError(f"Não foi possível baixar {url}")


def clean_name(name: str) -> str:
    """Nome de coluna seguro para o Athena: minúsculo, sem acento, só letras/números/_."""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9_]", "_", name.strip().lower()).strip("_")


def csv_to_table(text: str) -> pa.Table:
    """CSV (texto) -> tabela Arrow com todas as colunas como string."""
    sample = text[:10000]
    delimiter = ";" if sample.count(";") > sample.count(",") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    header = next(reader)

    names, positions = [], []
    for pos, raw in enumerate(header):
        name = clean_name(raw)
        if not name:
            continue  # coluna vazia (ex.: ";" no fim da linha)
        base, n = name, 2
        while name in names:
            name, n = f"{base}_{n}", n + 1
        names.append(name)
        positions.append(pos)

    columns: dict[str, list] = {n: [] for n in names}
    for row in reader:
        if not any(cell.strip() for cell in row):
            continue
        for name, pos in zip(names, positions):
            value = row[pos].strip() if pos < len(row) else ""
            columns[name].append(value or None)
    return pa.table({n: pa.array(v, type=pa.string()) for n, v in columns.items()})


def dicts_to_table(rows: list[dict]) -> pa.Table:
    names = list(rows[0].keys())
    return pa.table({n: pa.array([r[n] for r in rows], type=pa.string()) for n in names})


def save_and_upload(s3, table: pa.Table, name: str) -> str:
    """Grava o Parquet localmente e no S3. Devolve o caminho (igual nos dois lugares)."""
    layer = "raw" if name in READY_TABLES else "landing/referencias"
    key = f"{layer}/{name}/{name}.parquet"
    local = LOCAL_DIR / key
    local.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, local, compression="snappy")
    s3.upload_file(str(local), BUCKET, key)
    return key


def run_query(athena, sql: str) -> dict:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if info["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if info["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(f"Query falhou: {info['Status'].get('StateChangeReason')}")
    return info


def create_athena_table(athena, name: str, columns: list[str]) -> int:
    cols_sql = ",\n  ".join(f"`{c}` string" for c in columns)
    run_query(athena, f"DROP TABLE IF EXISTS {DATABASE}.{name}")
    run_query(athena, f"""
CREATE EXTERNAL TABLE {DATABASE}.{name} (
  {cols_sql}
)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/raw/{name}/'""")
    info = run_query(athena, f"SELECT count(*) FROM {DATABASE}.{name}")
    rows = athena.get_query_results(QueryExecutionId=info["QueryExecutionId"])["ResultSet"]["Rows"]
    return int(rows[1]["Data"][0]["VarCharValue"])


# ---------------------------------------------------------------------------
# Cada tabela de referência
# ---------------------------------------------------------------------------
def build_municipios() -> pa.Table:
    data = json.loads(download(URL_IBGE))
    rows = []
    for m in data:
        imediata = m.get("regiao-imediata") or {}
        intermediaria = imediata.get("regiao-intermediaria") or {}
        micro = m.get("microrregiao") or {}
        meso = micro.get("mesorregiao") or {}
        uf = intermediaria.get("UF") or meso.get("UF") or {}
        regiao = uf.get("regiao") or {}
        cod7 = str(m["id"])
        rows.append({
            "cod_municipio_ibge7": cod7,
            "cod_municipio": cod7[:6],  # 6 dígitos: mesmo formato do SIH (munic_res / munic_mov)
            "nome_municipio": m.get("nome"),
            "uf_sigla": uf.get("sigla"),
            "uf_nome": uf.get("nome"),
            "regiao_nome": regiao.get("nome"),
            "regiao_intermediaria": intermediaria.get("nome"),
            "regiao_imediata": imediata.get("nome"),
            "mesorregiao": meso.get("nome"),
            "microrregiao": micro.get("nome"),
        })
    return dicts_to_table(rows)


def build_cid10() -> dict[str, pa.Table]:
    zf = zipfile.ZipFile(io.BytesIO(download(URL_CID10)))
    by_name = {Path(n).name.upper(): n for n in zf.namelist()}
    tables = {}
    for table_name, file_name in CID10_FILES.items():
        text = zf.read(by_name[file_name]).decode("latin-1")  # arquivos em ISO-8859-1
        tables[table_name] = csv_to_table(text)
    return tables


def build_estabelecimentos() -> pa.Table:
    raw = download(URL_LEITOS)
    landing = LOCAL_DIR / "landing" / "referencias" / "hospitais" / "Leitos_2024.csv"
    landing.parent.mkdir(parents=True, exist_ok=True)
    landing.write_bytes(raw)
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
    return csv_to_table(text)


def build_icsap() -> pa.Table:
    keys = ["grupo", "grupo_nome", "subgrupo", "diagnostico", "cid_inicio", "cid_fim"]
    return dicts_to_table([dict(zip(keys, row)) for row in ICSAP])


# ---------------------------------------------------------------------------
def main() -> int:
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    s3, athena = session.client("s3"), session.client("athena")

    steps = [
        ("Municípios (IBGE)", lambda: {"municipios": build_municipios()}),
        ("CID-10 (DATASUS)", build_cid10),
        ("Estabelecimentos / Hospitais e Leitos 2024 (OpenDataSUS)",
         lambda: {"hospitais": build_estabelecimentos()}),
        ("Lista ICSAP (Portaria 221/2008)", lambda: {"internacoes_evitaveis_lista": build_icsap()}),
    ]

    summary, failed = [], False
    for label, builder in steps:
        print(f"\n>> {label}: baixando...")
        try:
            for name, table in builder().items():
                key = save_and_upload(s3, table, name)
                if name in READY_TABLES:
                    n = create_athena_table(athena, name, table.column_names)
                    print(f"   {DATABASE}.{name}: {n} linhas, {table.num_columns} colunas")
                    summary.append(f"{DATABASE}.{name}: ok ({n} linhas)")
                else:
                    print(f"   original salvo em {key} ({table.num_rows} linhas)")
                    summary.append(f"{key}: ok ({table.num_rows} linhas)")
        except Exception as err:
            print(f"   ERRO: {err}")
            summary.append(f"{label}: ERRO - {err}")
            failed = True

    print("\n=== RESUMO ===")
    print("\n".join(summary))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
