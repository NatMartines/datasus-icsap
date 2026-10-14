"""
Registra os Parquets do SIH/SUS (raw/internacoes/) como tabela no Athena: datasus_raw.internacoes

- Lê o schema dos Parquets locais (pasta data/) para montar a lista de colunas (todas texto)
- Usa "partition projection": o Athena descobre as partições ano/mes/uf pelo caminho no S3,
  sem precisar rodar MSCK REPAIR ou crawler (mais simples e sem custo extra)
- Recria a tabela a cada execução (DROP só remove o metadado, NUNCA os arquivos no S3)
- No final, roda uma contagem por mês para validar

Uso:
  uv run python ingestion/create_raw_table.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import boto3
import pyarrow.parquet as pq

BUCKET = "datasus-icsap-data-nm2026"
PROFILE = "datasus"
REGION = "us-east-1"
WORKGROUP = "datasus-icsap"
DATABASE = "datasus_raw"
TABLE = "internacoes"
LOCAL_RAW = Path(__file__).resolve().parent.parent / "data" / "raw" / TABLE
PARTITION_COLS = {"ano", "mes", "uf"}
UFS = "AC,AL,AM,AP,BA,CE,DF,ES,GO,MA,MG,MS,MT,PA,PB,PE,PI,PR,RJ,RN,RO,RR,RS,SC,SE,SP,TO"


def collect_columns() -> list[str]:
    """União das colunas de todos os Parquets locais, na ordem em que aparecem."""
    files = sorted(LOCAL_RAW.rglob("*.parquet"))
    if not files:
        sys.exit(f"Nenhum Parquet encontrado em {LOCAL_RAW}. Rode a ingestão primeiro.")
    columns: list[str] = []
    for f in files:
        for name in pq.read_schema(f).names:
            if name not in columns and name not in PARTITION_COLS:
                columns.append(name)
    print(f"{len(files)} arquivos lidos, {len(columns)} colunas encontradas")
    return columns


def build_ddl(columns: list[str]) -> str:
    cols_sql = ",\n  ".join(f"`{c}` string" for c in columns)
    location = f"s3://{BUCKET}/raw/{TABLE}/"
    return f"""
CREATE EXTERNAL TABLE {DATABASE}.{TABLE} (
  {cols_sql}
)
PARTITIONED BY (ano int, mes int, uf string)
STORED AS PARQUET
LOCATION '{location}'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.ano.type' = 'integer',
  'projection.ano.range' = '2008,2035',
  'projection.mes.type' = 'integer',
  'projection.mes.range' = '1,12',
  'projection.mes.digits' = '2',
  'projection.uf.type' = 'enum',
  'projection.uf.values' = '{UFS}',
  'storage.location.template' = '{location}ano=${{ano}}/mes=${{mes}}/uf=${{uf}}/'
)"""


def run_query(athena, sql: str) -> dict:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        sys.exit(f"Query falhou ({state}): {info['Status'].get('StateChangeReason')}\n{sql}")
    return info


def main() -> None:
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    athena = session.client("athena")

    columns = collect_columns()

    print(f"Recriando a tabela {DATABASE}.{TABLE} (os dados no S3 não são apagados)...")
    run_query(athena, f"DROP TABLE IF EXISTS {DATABASE}.{TABLE}")
    run_query(athena, build_ddl(columns))
    print("Tabela criada.")

    print("\nValidando: internações por mês")
    sql = (f"SELECT uf, ano, mes, count(*) AS internacoes "
           f"FROM {DATABASE}.{TABLE} GROUP BY uf, ano, mes ORDER BY uf, ano, mes")
    info = run_query(athena, sql)
    rows = athena.get_query_results(QueryExecutionId=info["QueryExecutionId"])["ResultSet"]["Rows"]

    total = 0
    for row in rows[1:]:  # a primeira linha é o cabeçalho
        uf, ano, mes, n = (c.get("VarCharValue") for c in row["Data"])
        total += int(n)
        print(f"  {uf} {ano}-{int(mes):02d}: {int(n):>9,}".replace(",", "."))
    print(f"  TOTAL: {total:,}".replace(",", "."))

    scanned = info["Statistics"].get("DataScannedInBytes", 0)
    print(f"\nDados escaneados pela validação: {scanned / 1024**2:.2f} MB")


if __name__ == "__main__":
    main()
