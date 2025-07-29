from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo



chunk_size = 100_000
infer_schema_length = 1000
heute = date.today()
now = datetime.now(ZoneInfo("Europe/Berlin"))
before_one_min = now - timedelta(minutes=1)
STR_NULL = "-777"

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

columns_to_compare = ["HASH_KEY"]

final_column_order = [
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
    "GÜLTIG_AB",
    "GÜLTIG_BIS",
    "HASH_KEY",
]
