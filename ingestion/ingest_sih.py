"""
Ingestão do SIH/SUS (AIH Reduzida - RD) do DATASUS para o Amazon S3.

Fluxo para cada arquivo (UF + ano + mês):
  1. Baixa o .dbc original do FTP público do DATASUS
  2. Envia o .dbc para   s3://<bucket>/landing/internacoes/ano=AAAA/mes=MM/uf=UF/
  3. Descompacta (.dbc -> .dbf) e converte para Parquet, com TODAS as colunas como texto
     (a tipagem é responsabilidade da camada de staging do dbt)
  4. Envia o Parquet para s3://<bucket>/raw/internacoes/ano=AAAA/mes=MM/uf=UF/

Idempotente: se o Parquet do mês já existe no S3, o mês é pulado.
Use --force para reprocessar.

Exemplos:
  uv run python ingestion/ingest_sih.py --uf SP --ano 2024 --meses 1
  uv run python ingestion/ingest_sih.py --uf SP --ano 2024
"""

from __future__ import annotations

import argparse
import ftplib
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import datasus_dbc
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dbfread import DBF

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/dissemin/publicos/SIHSUS/200801_/Dados"
DEFAULT_BUCKET = "datasus-icsap-data-nm2026"
DEFAULT_PROFILE = "datasus"
DEFAULT_REGION = "us-east-1"
LOCAL_DIR = Path(__file__).resolve().parent.parent / "data"

log = logging.getLogger("ingest_sih")


def file_name(uf: str, ano: int, mes: int) -> str:
    """Nome do arquivo no padrão do DATASUS. Ex.: RDSP2401"""
    return f"RD{uf}{ano % 100:02d}{mes:02d}"


def s3_key(layer: str, uf: str, ano: int, mes: int, ext: str) -> str:
    """Caminho no S3 com partições no estilo Hive (ano=/mes=/uf=)."""
    return f"{layer}/internacoes/ano={ano}/mes={mes:02d}/uf={uf}/{file_name(uf, ano, mes)}.{ext}"


def s3_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def download_dbc(name: str, dest: Path, retries: int = 3) -> bool:
    """Baixa o .dbc do FTP. Retorna False se o arquivo não existir no servidor."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, retries + 1):
        try:
            with ftplib.FTP(FTP_HOST, timeout=180) as ftp:
                ftp.login()
                ftp.cwd(FTP_DIR)
                with open(dest, "wb") as fh:
                    ftp.retrbinary(f"RETR {name}.dbc", fh.write)
            return True
        except ftplib.error_perm as err:
            if str(err).startswith("550"):
                log.warning("%s.dbc não existe no FTP do DATASUS (ainda não publicado?)", name)
                dest.unlink(missing_ok=True)
                return False
            raise
        except (ftplib.error_temp, OSError, EOFError) as err:
            log.warning("Falha no download de %s (tentativa %d/%d): %s", name, attempt, retries, err)
            time.sleep(5 * attempt)
    raise RuntimeError(f"Não foi possível baixar {name}.dbc após {retries} tentativas")


def dbc_to_parquet(dbc_path: Path, parquet_path: Path) -> int:
    """Converte .dbc -> .dbf -> Parquet (colunas em texto, nomes em minúsculas)."""
    dbf_path = dbc_path.with_suffix(".dbf")
    datasus_dbc.decompress(str(dbc_path), str(dbf_path))

    try:
        dbf = DBF(str(dbf_path), raw=True)  # raw=True: valores chegam como bytes, sem conversão de tipo
        fields = dbf.field_names
        columns: dict[str, list] = {f: [] for f in fields}

        for record in dbf:
            for field in fields:
                value = record[field]
                if value is None:
                    columns[field].append(None)
                else:
                    text = value.decode("latin-1").strip()
                    columns[field].append(text or None)

        n_rows = len(columns[fields[0]]) if fields else 0
        ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

        arrays = {f.lower(): pa.array(columns[f], type=pa.string()) for f in fields}
        arrays["_arquivo_origem"] = pa.array([dbc_path.name] * n_rows, type=pa.string())
        arrays["_ingerido_em"] = pa.array([ingested_at] * n_rows, type=pa.string())

        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(arrays), parquet_path, compression="snappy")
        return n_rows
    finally:
        dbf_path.unlink(missing_ok=True)


def process_month(s3, bucket: str, uf: str, ano: int, mes: int, force: bool) -> str:
    name = file_name(uf, ano, mes)
    raw_key = s3_key("raw", uf, ano, mes, "parquet")
    landing_key = s3_key("landing", uf, ano, mes, "dbc")

    if not force and s3_exists(s3, bucket, raw_key):
        log.info("%s: já existe no S3, pulando", name)
        return "pulado"

    local_dbc = LOCAL_DIR / landing_key
    local_parquet = LOCAL_DIR / raw_key

    log.info("%s: baixando do FTP do DATASUS...", name)
    if not download_dbc(name, local_dbc):
        return "indisponível"

    log.info("%s: enviando .dbc para s3://%s/%s", name, bucket, landing_key)
    s3.upload_file(str(local_dbc), bucket, landing_key)

    log.info("%s: convertendo para Parquet...", name)
    n_rows = dbc_to_parquet(local_dbc, local_parquet)

    log.info("%s: enviando Parquet (%d linhas) para s3://%s/%s", name, n_rows, bucket, raw_key)
    s3.upload_file(str(local_parquet), bucket, raw_key)
    return f"ok ({n_rows} linhas)"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingestão SIH/SUS (RD) do DATASUS para o S3")
    parser.add_argument("--uf", required=True, help="Sigla da UF, ex.: SP")
    parser.add_argument("--ano", required=True, type=int, help="Ano com 4 dígitos, ex.: 2024")
    parser.add_argument("--meses", type=int, nargs="+", default=list(range(1, 13)),
                        help="Meses a processar (padrão: 1 a 12)")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help="Perfil do AWS CLI")
    parser.add_argument("--force", action="store_true", help="Reprocessa mesmo se já existir no S3")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s",
                        datefmt="%H:%M:%S")
    args = parse_args()
    uf = args.uf.upper()

    session = boto3.Session(profile_name=args.profile, region_name=DEFAULT_REGION)
    s3 = session.client("s3")

    results = {}
    for mes in sorted(set(args.meses)):
        try:
            results[mes] = process_month(s3, args.bucket, uf, args.ano, mes, args.force)
        except Exception as err:  # continua os outros meses e reporta no final
            log.error("%s: ERRO - %s", file_name(uf, args.ano, mes), err)
            results[mes] = f"erro: {err}"

    print("\n=== RESUMO ===")
    for mes, status in results.items():
        print(f"{file_name(uf, args.ano, mes)}: {status}")

    return 1 if any(str(s).startswith("erro") for s in results.values()) else 0


if __name__ == "__main__":
    sys.exit(main())