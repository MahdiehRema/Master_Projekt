%pip install pvd_modules 
%pip install cx_Oracle

import oracledb
import pandas as pd
import data_bro as bro
import pvd_modules as pvd
import polars as pl
from datetime import datetime,date, timedelta
import hashlib
from sqlalchemy.dialects import oracle
from zoneinfo import ZoneInfo

today = date.today() 
vault_path="vtdwh/cdip_out"
conn = bro.db.build_engine("vtdwh/cdip_out")
now = datetime.now(ZoneInfo("Europe/Berlin"))
columns_to_hash = [
    "PVID",
    "EMAIL",
    "STATUS",
    "TRASSENABSTANDSKATEGORIE_OWN",
    "P1_TEL_TYP",
    "P1_TEL_PING_ERGEBNIS",
    "P1_TEL_PING_DATUM",
    "MASTER_MARKETABLE",
    "FLAG_BESTANDSKUNDE",
    "FLAG_BLACKLIST",
    "ENTFL_ADDRESS",
    "FLAG_PUBLIC",
]
final_column_order = [
    'PVID', 'EMAIL', 'STATUS', 'TRASSENABSTANDSKATEGORIE_OWN',
    'P1_TEL_TYP', 'P1_TEL_PING_ERGEBNIS', 'P1_TEL_PING_DATUM',
    'MASTER_MARKETABLE', 'FLAG_BESTANDSKUNDE', 'FLAG_BLACKLIST',
    'ENTFL_ADDRESS', 'FLAG_PUBLIC',
     'GÜLTIG_AB', 'GÜLTIG_BIS','HASH_KEY']

df_data= pvd.util.read_origin_per_polars("pvd_determined")

init_historisierung = df_data.select([
        "PVID",
        "EMAIL",
        "STATUS",
        "TRASSENABSTANDSKATEGORIE_OWN",
        "P1_TEL_TYP",
        "P1_TEL_PING_ERGEBNIS",
        "P1_TEL_PING_DATUM",
        "MASTER_MARKETABLE",
        "FLAG_BESTANDSKUNDE",
        "FLAG_BLACKLIST",
        "ENTFL_ADDRESS",
        "FLAG_PUBLIC",
    ])

def row_hash_sha256(row):
    values = [str(v) if v is not None else 'NULL' for v in row.values()]
    concat_str = "|DM_NULL_SEPERATOR|".join(values)
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

del df_data

init_historisierung = init_historisierung.with_columns([pl.lit(now).alias("GÜLTIG_AB"),
                                              pl.lit(None, dtype=pl.Date).alias("GÜLTIG_BIS"),
                                              pl.struct(columns_to_hash).map_elements(row_hash_sha256).alias("HASH_KEY")])
init_historisierung = init_historisierung.select(final_column_order)
def write_polars_in_chunks(df: pl.DataFrame, table_name: str):
    for i in range(0, df.height, chunk_size):
        chunk = df.slice(i, chunk_size)
        chunk.write_database(
            table_name, connection=pvd.util.engine_oracle(), if_table_exists="append"
        )
write_polars_in_chunks(init_historisierung, "PVD_HISTORISIERUNG")

