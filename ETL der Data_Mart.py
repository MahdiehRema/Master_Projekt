import polars as pl
import sqlalchemy
from ..util import engine_oracle
from .config import (
    STR_NULL,
    chunk_size,
    infer_schema_length,
    before_one_min,
)


def _write_polars_in_chunks(df: pl.DataFrame, table_name: str):
    for i in range(0, df.height, chunk_size):
        chunk = df.slice(i, chunk_size)
        chunk.write_database(
            table_name, connection=engine_oracle(), if_table_exists="append"
        )


def extract_ht_new() -> pl.DataFrame:
    """extract alle Daten aus aktualisierte Historisierung, die GÜLTIG_BIS NULL erfüllen.
    :param: None
    :returns: df_HT_new als pl.Dataframe
    """
    query_ht_new = "SELECT * FROM PVD_HISTORISIERUNG WHERE TRUNC(GÜLTIG_AB) = TRUNC(SYSDATE) AND GÜLTIG_BIS IS NULL"
    df_HT_new = pl.read_database(
        query_ht_new,
        connection=engine_oracle(),
        batch_size=chunk_size,
        infer_schema_length=infer_schema_length,
    )
    return df_HT_new


def etl_dim_kunde(df_HT_new: pl.DataFrame) -> pl.DataFrame:
    """Die Tabelle dim_kunde wird aus der Oracle-Datenbank eingelesen und aktualisiert,
    indem sie mit der Historisierung_Tabelle verglichen wird. Wenn eine neue PVID vorkommt,
    wird sie in dim_kunde eingefügt. Bei einer Änderung der E-Mail-Adresse wird der entsprechende Eintrag         in dim_kunde aktualisiert.

    :param: None
    :returns: None
    """
    with engine_oracle().begin() as conn:
        conn.execute(sqlalchemy.text(f"""TRUNCATE TABLE TEMP_PVD_DIM_KUNDE"""))

    kunde_query = "SELECT * FROM CDIP_OUT.PVD_DIM_KUNDE pdk"
    dim_kunde = pl.read_database(
        query=kunde_query, connection=engine_oracle(), batch_size=chunk_size
    )
    df_HT_kunde = df_HT_new.select([pl.col("PVID"), pl.col("EMAIL")])
    del df_HT_new
    dim_kunde_neue_PVID = df_HT_kunde.join(dim_kunde, on="PVID", how="anti")
    if not dim_kunde_neue_PVID.is_empty():
        _write_polars_in_chunks(dim_kunde_neue_PVID, "PVD_dim_kunde")
    dim_kunde = dim_kunde.rename({"EMAIL": "EMAIL_alt"})
    df_diff = df_HT_kunde.join(dim_kunde, on="PVID", how="inner")
    email_cols = ["EMAIL", "EMAIL_alt"]
    for column in email_cols:
        df_diff = df_diff.with_columns(
            pl.when(pl.col(column).is_null())
            .then(pl.lit(STR_NULL))
            .otherwise(pl.col(column))
            .alias(column)
        )
    df_diff = df_diff.filter(pl.col("EMAIL") != pl.col("EMAIL_alt"))
    df_diff = df_diff.select("PVID", "EMAIL")
    new_email = ["EMAIL"]
    for column in new_email:
        df_diff = df_diff.with_columns(
            pl.when(pl.col(column) == STR_NULL)
            .then(None)
            .otherwise(pl.col(column))
            .alias(column)
        )
    _write_polars_in_chunks(df_diff, "temp_pvd_dim_kunde")
    diff_query = """ UPDATE PVD_DIM_KUNDE dk
              SET dk.EMAIL = (
              SELECT tdk.EMAIL
              FROM temp_pvd_dim_kunde tdk
              WHERE tdk.PVID = dk.PVID)
              WHERE EXISTS (
              SELECT 1
              FROM temp_pvd_dim_kunde tdk
              WHERE tdk.PVID = dk.PVID)"""
    with engine_oracle().begin() as conn:
        conn.execute(
            sqlalchemy.text(diff_query),
        )
    return None


def extract_dims() -> pl.DataFrame:
    """Alle DIMS-Tabellen werden eingelesen, um eine aktualisierte Faktentabelle zu erstellen.
    :param: None
    :returns: dim_ping als pl.Dataframe & dim_tel als pl.Dataframe & dim_entfadresse als pl.Dataframe & dim_trassen als pl.Dataframe
    """
    query_ping = "SELECT * FROM CDIP_OUT.PVD_DIM_PING pdp "
    dim_ping = pl.read_database(query=query_ping, connection=engine_oracle())

    query_tel = "SELECT * FROM CDIP_OUT.PVD_DIM_TEL pdt "
    dim_tel = pl.read_database(query=query_tel, connection=engine_oracle())

    query_entfadresse = "SELECT * FROM CDIP_OUT.PVD_DIM_ENTFADRESSE pde "
    dim_entfadresse = pl.read_database(
        query=query_entfadresse, connection=engine_oracle()
    )

    query_trassen = "SELECT * FROM CDIP_OUT.PVD_DIM_TRASSEN pdt "
    dim_trassen = pl.read_database(query=query_trassen, connection=engine_oracle())

    return dim_ping, dim_tel, dim_entfadresse, dim_trassen


def update_fact_ht(
    dim_ping: pl.DataFrame,
    dim_tel: pl.DataFrame,
    dim_entfadresse: pl.DataFrame,
    dim_trassen: pl.DataFrame,
    df_HT_new: pl.DataFrame,
) -> pl.DataFrame:
    """aktualisierte Historisierung_Tabelle wird mit alle dims_Tabellen gejoint, damit aktualisierte Fact_Tabelle aufgebaut werden.
    :param: dim_ping als pl.Dataframe, dim_tel als pl.Dataframe, dim_entfadresse als pl.Dataframe, dim_trassen als pl.Dataframe
    :returns: fact_df als pl.dataframe
    """
    df_HT_new = df_HT_new.join(dim_entfadresse, on="ENTFL_ADDRESS", how="left")
    df_HT_new = df_HT_new.join(dim_tel, on="P1_TEL_TYP", how="left")
    df_HT_new = df_HT_new.join(
        dim_trassen, on="TRASSENABSTANDSKATEGORIE_OWN", how="left"
    )
    df_HT_new = df_HT_new.with_columns(
        [pl.col("P1_TEL_PING_DATUM").cast(pl.Datetime("ns"))]
    )

    dim_ping = dim_ping.with_columns(
        [pl.col("P1_TEL_PING_DATUM").cast(pl.Datetime("ns"))]
    )

    df_HT_new = df_HT_new.join(
        dim_ping, on=["P1_TEL_PING_ERGEBNIS", "P1_TEL_PING_DATUM"], how="left"
    )
    fact_df = df_HT_new[
        "HASH_KEY",
        "PVID",
        "STATUS",
        "GÜLTIG_AB",
        "GÜLTIG_BIS",
        "ENTFADRESSE_ID",
        "TEL_ID",
        "TRASSEN_ID",
        "PING_ID",
        "MASTER_MARKETABLE",
        "FLAG_BESTANDSKUNDE",
        "FLAG_BLACKLIST",
        "FLAG_PUBLIC",
    ]
    del df_HT_new
    fact_df = fact_df.with_columns(
        [
            pl.col("GÜLTIG_AB")
            .dt.strftime("%Y%m%d")
            .cast(pl.Int32)
            .alias("GÜLTIG_AB_ID"),
            pl.col("GÜLTIG_BIS")
            .cast(pl.Datetime("ms"))
            .dt.strftime("%Y%m%d")
            .cast(pl.Int32)
            .alias("GÜLTIG_BIS_ID"),
        ]
    )

    fact_df = fact_df.with_columns(
        [
            pl.col("TEL_ID").cast(pl.Float64),
            pl.col("TRASSEN_ID").cast(pl.Float64),
            pl.col("PING_ID").cast(pl.Float64),
            pl.col("GÜLTIG_BIS_ID").cast(pl.Float64),
        ]
    )
    return fact_df


def insert_chaneged_in_fact(fact_df: pl.DataFrame):
    """aktualisierte Fact tabelle wird in Oracle gespeichert.
    :param: fact_df als pl.Dataframe
    :returns: None
    """
    _write_polars_in_chunks(fact_df, "PVD_FACT_HISTORISIERUNG")
    return None


def update_chaneged_in_fact():
    """Geänderte Datensätze in der Historisierung-Tabelle werden in der Facttabelle aktualisiert,
    indem das Feld ‚Gültig bis‘ auf das now gesetzt wird.

    :param: None
    :returns: None
    """
    with engine_oracle().begin() as conn:
        conn.execute(
            sqlalchemy.text(
                """
                UPDATE PVD_FACT_HISTORISIERUNG
                SET GÜLTIG_BIS = :before_one_min
                WHERE 
                    GÜLTIG_BIS IS NULL 
                    AND HASH_KEY IN (
                        SELECT t.HASH_KEY
                        FROM PVD_FACT_HISTORISIERUNG t
                        JOIN (
                            SELECT PVID, MAX(GÜLTIG_AB) AS max_gueltig_ab
                            FROM PVD_FACT_HISTORISIERUNG
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


def update_gelöschte_pvid_in_fact():
    """schließt abgelaufene PVIDs in der Data Mart.

    :param : None
    :returns: None
    """
    with engine_oracle().begin() as conn:
        conn.execute(
            sqlalchemy.text(
                """
                UPDATE PVD_FACT_HISTORISIERUNG ph
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


def set_gültig_bis_id():
    """generiert GÜLTIG_BIs_ID für schlissende Hash_keys.
    :Param: None
    :Return: None"""
    update_query = """UPDATE PVD_FACT_HISTORISIERUNG 
                    SET GÜLTIG_BIS_ID = TO_NUMBER(TO_CHAR(GÜLTIG_BIS, 'YYYYMMDD'))
                    WHERE TRUNC(GÜLTIG_BIS) = TRUNC(SYSDATE)"""
    with engine_oracle().begin() as conn:
        conn.execute(sqlalchemy.text(update_query))
    return None


def löschroutine_fact():
    """Die Facttabelle wird bereinigt,
    indem Datensätze, die älter als ein Jahr sind, gelöscht werden.

    :param: None
    :returns: None
    """
    lösch_query = """ DELETE FROM PVD_FACT_HISTORISIERUNG
           WHERE GÜLTIG_BIS <= SYSDATE - 365 """
    with engine_oracle().begin() as conn:
        conn.execute(sqlalchemy.text(lösch_query))

    return None
