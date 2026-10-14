"""
Deixa as tabelas de referência prontas para JOIN simples (só com "="), todas no database datasus_raw.

O que muda:
  hospitais                    passa a ter 1 linha por hospital (o mês mais recente de cada CNES)
  doencas_categorias           passa a ter 1 linha por código de doença (subcat)
  doencas_grupos               idem
  doencas_capitulos            idem
  internacoes_evitaveis_lista  idem (só os códigos que são evitáveis)

Assim, todas as tabelas de doença se ligam à tabela internacoes do mesmo jeito:
  ON x.subcat = i.diag_princ

Lê os arquivos originais já baixados em data/landing/referencias/ (não baixa nada de novo),
grava as versões simples em raw/<tabela>/ (local e S3) e recria as tabelas no Athena.
Os arquivos originais continuam em landing/referencias/. Pode rodar de novo sem problema.

Uso:
  uv run python ingestion/simplificar_tabelas.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

BUCKET = "datasus-icsap-data-nm2026"
PROFILE = "datasus"
REGION = "us-east-1"
WORKGROUP = "datasus-icsap"
DATABASE = "datasus_raw"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"


# ---------------------------------------------------------------------------
# Regras (listas de dicionários; sem dependência de AWS)
# ---------------------------------------------------------------------------
def code(value: str | None) -> str:
    """Código CID-10 sem ponto e sem símbolos, igual ao formato do diag_princ. Ex.: 'A00.0' -> 'A000'"""
    return "".join(ch for ch in (value or "").upper() if ch.isalnum())


def build_hospitais(rows: list[dict]) -> list[dict]:
    latest: dict[str, dict] = {}
    for r in rows:
        cnes = r.get("cnes")
        if cnes and (cnes not in latest or (r.get("comp") or "") > (latest[cnes].get("comp") or "")):
            latest[cnes] = r
    return [latest[k] for k in sorted(latest)]


def build_categorias(subcats: list[str], categorias: list[dict]) -> list[dict]:
    by_cat = {code(c["cat"]): c for c in categorias}
    out = []
    for s in subcats:
        c = by_cat.get(s[:3])
        if c:
            out.append({"subcat": s, "cat": s[:3], "categoria": c.get("descricao")})
    return out


def narrowest(cat: str, ranges: list[dict]) -> dict | None:
    """Faixa mais específica que contém a categoria (quando um grupo grande contém grupos menores)."""
    hits = [r for r in ranges if code(r["catinic"]) <= cat <= code(r["catfim"])]
    if not hits:
        return None
    hits.sort(key=lambda r: (code(r["catinic"]), [-ord(ch) for ch in code(r["catfim"])]))
    return hits[-1]


def build_grupos(subcats: list[str], grupos: list[dict]) -> list[dict]:
    out = []
    for s in subcats:
        g = narrowest(s[:3], grupos)
        if g:
            out.append({"subcat": s, "grupo": g.get("descricao"),
                        "grupo_inicio": code(g["catinic"]), "grupo_fim": code(g["catfim"])})
    return out


def build_capitulos(subcats: list[str], capitulos: list[dict]) -> list[dict]:
    out = []
    for s in subcats:
        c = narrowest(s[:3], capitulos)
        if c:
            out.append({"subcat": s, "numcap": c.get("numcap"), "capitulo": c.get("descricao")})
    return out


def build_evitaveis(subcats: list[str], icsap: list[dict]) -> list[dict]:
    out = []
    for s in subcats:
        hits = []
        for r in icsap:
            ini, fim = r["cid_inicio"], r["cid_fim"]
            key = s[:3] if len(ini) == 3 else s
            if ini <= key <= fim:
                hits.append(r)
        if len(hits) > 1:
            raise ValueError(f"Código {s} caiu em mais de uma faixa da lista ICSAP")
        if hits:
            r = hits[0]
            out.append({"subcat": s, "grupo": r["grupo"], "grupo_nome": r["grupo_nome"],
                        "subgrupo": r["subgrupo"], "diagnostico": r["diagnostico"]})
    return out


def check_unique(name: str, rows: list[dict], key: str) -> None:
    keys = [r[key] for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError(f"{name}: a chave {key} está repetida")


# ---------------------------------------------------------------------------
# Leitura, gravação e Athena
# ---------------------------------------------------------------------------
def read_rows(table: str, layer: str = "landing/referencias") -> list[dict]:
    import pyarrow.parquet as pq
    path = DATA_DIR / layer / table / f"{table}.parquet"
    if not path.exists():
        sys.exit(f"Arquivo não encontrado: {path}\nRode antes: uv run python ingestion/ingest_referencias.py")
    return pq.read_table(path).to_pylist()


def publish(s3, athena, table: str, rows: list[dict]) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq
    names = list(rows[0].keys())
    data = pa.table({n: pa.array([r.get(n) for r in rows], type=pa.string()) for n in names})
    key = f"raw/{table}/{table}.parquet"
    local = DATA_DIR / key
    local.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(data, local, compression="snappy")
    s3.upload_file(str(local), BUCKET, key)

    cols_sql = ",\n  ".join(f"`{c}` string" for c in names)
    run_query(athena, f"DROP TABLE IF EXISTS {DATABASE}.{table}")
    run_query(athena, f"""
CREATE EXTERNAL TABLE {DATABASE}.{table} (
  {cols_sql}
)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/raw/{table}/'""")
    qid = run_query(athena, f"SELECT count(*) FROM {DATABASE}.{table}")
    result = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return int(result[1]["Data"][0]["VarCharValue"])


def run_query(athena, sql: str) -> str:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Query falhou: {status.get('StateChangeReason')}")
    return qid


def main() -> int:
    import boto3
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    s3, athena = session.client("s3"), session.client("athena")

    subcats = sorted({code(r["subcat"]) for r in read_rows("doencas", layer="raw")} - {""})
    tables = {
        "hospitais": (build_hospitais(read_rows("hospitais")), "cnes"),
        "doencas_categorias": (build_categorias(subcats, read_rows("doencas_categorias")), "subcat"),
        "doencas_grupos": (build_grupos(subcats, read_rows("doencas_grupos")), "subcat"),
        "doencas_capitulos": (build_capitulos(subcats, read_rows("doencas_capitulos")), "subcat"),
        "internacoes_evitaveis_lista": (build_evitaveis(subcats, read_rows("internacoes_evitaveis_lista")), "subcat"),
    }

    print("=== RESUMO ===")
    failed = False
    for table, (rows, key) in tables.items():
        try:
            check_unique(table, rows, key)
            n = publish(s3, athena, table, rows)
            print(f"{DATABASE}.{table}: ok ({n} linhas, 1 por {key})")
        except Exception as err:
            print(f"{DATABASE}.{table}: ERRO - {err}")
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
