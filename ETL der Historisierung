import hashlib
import sqlalchemy
import polars as pl
from .config import (
    chunk_size,
    infer_schema_length,
    now,
    before_one_min,
    columns_to_hash,
    final_column_order,
)
from ..util import read_origin_per_polars
from ..util import engine_oracle


def _row_hash_sha256(row):
    values = [str(v) if v is not None else "NULL" for v in row.values()]
    concat_str = "|DM_NULL_SEPERATOR|".join(values)
    return hashlib.sha256(concat_str.encode("utf-8")).hexdigest()


def _write_polars_in_chunks(df: pl.DataFrame, table_name: str):
    for i in range(0, df.height, chunk_size):
        chunk = df.slice(i, chunk_size)
        chunk.write_database(
            table_name, connection=engine_oracle(), if_table_exists="append"
        )


def extract_ht_old() -> pl.DataFrame:
    """Die Historisierungs-Tabelle wird aus der Oracle-Datenbank stückweise eingelesen.

    :param df: None
    :returns: df_HT als pl.Dataframe
    """
    query_HT = "select * from PVD_HISTORISIERUNG where GÜLTIG_BIS is null"
    df_HT = pl.read_database(
        query_HT,
        connection=engine_oracle(),
        batch_size=chunk_size,
        infer_schema_length=infer_schema_length,
    )
    df_HT = df_HT.with_columns(
        [
            pl.col("P1_TEL_PING_DATUM")
            .cast(pl.Datetime("ns"))
            .alias("P1_TEL_PING_DATUM"),
            pl.col("GÜLTIG_AB").cast(pl.Datetime("ns")).dt.date().alias("GÜLTIG_AB"),
            pl.col("GÜLTIG_BIS").cast(pl.Datetime("ns")).dt.date().alias("GÜLTIG_BIS"),
            pl.col("P1_TEL_PING_ERGEBNIS").cast(pl.Float64, strict=False),
        ]
    )
    return df_HT


def extract_pvd() -> pl.DataFrame:
    """Importiert PVD-Daten und extrahiert die relevanten Spalten für Vergleich.

    :param df: None
    :returns: df_PVD als pl.Dataframe
    """
    df_data = read_origin_per_polars("pvd_determined")
    df_PVD = (df_data.select(columns_to_hash))

    df_PVD = df_PVD.with_columns(
        pl.struct(columns_to_hash).map_elements(_row_hash_sha256).alias("HASH_KEY")
    )
    del df_data
    df_PVD = df_PVD.with_columns(
        [
            pl.col("P1_TEL_PING_DATUM").cast(pl.Datetime("ns")),
            pl.col("P1_TEL_PING_ERGEBNIS").cast(pl.Float64, strict=False),
        ]
    )

    return df_PVD


def insert_chaneged_pvd(df_PVD: pl.DataFrame, df_HT: pl.DataFrame) -> pl.DataFrame:
    """wird geänderte Datensätze aus PVD in Oracle geschrieben.

    :Param df: df_PVD als pl.DataFrame, df_HT als pl.DataFrame
    :return: None
    """

    df_new_pvids = df_PVD.join(df_HT, on="HASH_KEY", how="anti")
    df_new_pvids = df_new_pvids.with_columns(
        pl.lit(now).alias("GÜLTIG_AB"), pl.lit(None).alias("GÜLTIG_BIS")
    )
    df_new_pvids = df_new_pvids.select(final_column_order)
    _write_polars_in_chunks(df_new_pvids, "PVD_HISTORISIERUNG")
    return None


def update_chaneged_ht():
    """geänderte Datensätze in der Historisierung-Tabelle werden aktualisiert,
    indem das Feld ‚Gültig bis‘ auf das now gesetzt wird.

    :param: None
    :returns: None
    """
    with engine_oracle().begin() as conn:
        conn.execute(
            sqlalchemy.text(
                """
                UPDATE PVD_HISTORISIERUNG
                SET GÜLTIG_BIS = :before_one_min
                WHERE 
                    GÜLTIG_BIS IS NULL 
                    AND HASH_KEY IN (
                        SELECT t.HASH_KEY
                        FROM PVD_HISTORISIERUNG t
                        JOIN (
                            SELECT PVID, MAX(GÜLTIG_AB) AS max_gueltig_ab
                            FROM PVD_HISTORISIERUNG
                            GROUP BY PVID
                        ) latest
                          ON t.PVID = latest.PVID
                         AND t.GÜLTIG_AB < latest.max_gueltig_ab
                )
                """
            ),
            {"before_one_min": before_one_min},
        )
    return None


def update_gelöschte_pvid(df_HT: pl.DataFrame, df_PVD: pl.DataFrame) -> None:
    """
    Findet abgelaufene PVIDs und schließt sie in der Datenbank.

    :param df_PVD: Polars DataFrame mit aktuellen PVIDs
    :param df_HT: Polars DataFrame mit historisierten PVIDs
    :returns: None
    """
    with engine_oracle().begin() as conn:
        conn.execute(sqlalchemy.text(f"""TRUNCATE TABLE TEMP_PVD_HASH_KEY"""))

    zuschlissende_hash_key = df_HT.join(df_PVD, on="PVID", how="anti").select(
        "HASH_KEY"
    )
    zuschlissende_hash_key.write_database(
        "cdip_out.temp_pvd_hash_key",
        connection=engine_oracle(),
        if_table_exists="append",
    )
    with engine_oracle().begin() as conn:
        conn.execute(
            sqlalchemy.text(
                """
                UPDATE PVD_HISTORISIERUNG ph
                SET GÜLTIG_BIS = :before_one_min
                WHERE HASH_KEY IN (
                    SELECT HASH_KEY
                    FROM TEMP_PVD_HASH_KEY tmp
                    WHERE tmp.HASH_KEY = ph.HASH_KEY
                )
                """
            ),
            {"before_one_min": before_one_min},
        )

    return None


def löschroutine_ht():
    """Die Historisierungstabelle wird bereinigt,
    indem Datensätze, die älter als ein Jahr sind, gelöscht werden.

    :param: None
    :returns: None
    """
    lösch_query = """ DELETE FROM PVD_HISTORISIERUNG 
               WHERE GÜLTIG_BIS <= SYSDATE - 365 """
    with engine_oracle().begin() as conn:
        conn.execute(sqlalchemy.text(lösch_query))

    return None
