%pip install pvd_modules 
%pip install cx_Oracle
import oracledb
import pandas as pd
import data_bro as bro
import pvd_modules as pvd
import polars as pl
from datetime import date, timedelta
import hashlib


today = date.today() 
vault_path="vh/daab"

query_HT= "select * from PVD_HISTORISIERUNG where GÜLTIG_BIS is null"
df_HT = pl.read_database(query_HT, connection=conn.connect())

def create_dim_col(df: pl.DataFrame, col: str, dim_name: str):
   
    dim = df.select(col).unique()
    dim = dim.with_row_index(name=f"{dim_name}_id", offset=1)
    dim_pd = dim.to_pandas()
    bro.db.push_database(
        df=dim_pd,
        table=f"dim_{dim_name}",  
        if_exists="replace",
        conn="vwh/daab"               
    )
    return dim_pd

def create_dim_cols(df: pl.DataFrame, cols: list, dim_name: str):
        dim = df.select(cols).unique()
        dim = dim.with_row_index(name=f"{dim_name}_id", offset=1)
        dim_pd = dim.to_pandas()
        bro.db.push_database(
        df=dim_pd,
        table=f"dim_{dim_name}",  
        if_exists="replace",
        conn="vh/daab"               
    )
        return dim

def create_dim_date(start_date:str, end_date:str):
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    dim_date = pd.DataFrame()
    dim_date["datum"] = dates
    dim_date["datum_id"] = dim_date["datum"].dt.strftime("%Y%m%d").astype(int)
    dim_date["jahr"] = dim_date["datum"].dt.year
    dim_date["monat"] = dim_date["datum"].dt.month
    dim_date["tag"] = dim_date["datum"].dt.day
    dim_date["wochentag"] = dim_date["datum"].dt.day_name()
    dim_date["ist_wochenende"] = dim_date["wochentag"].isin(["Samstag", "Sonntag"])
    dim_date["ist_wochenende"] = dim_date["ist_wochenende"].replace({True: "Ja", False: "Nein"})
    bro.db.push_database(df=dim_date, table="dim_date", if_exists="replace",conn="vwh/dab")
    return dim_date

dim_date=create_dim_date("2025-01-01","2045-12-31")
dim_date=pl.from_pandas(dim_date)
dim_date = dim_date.with_columns(pl.col("datum").cast(pl.Date))

dim_kunde= create_dim_cols(df_HT, ["PVID","EMAIL"], "kunde")
dim_kunde=pl.from_pandas(dim_kunde)

dim_ping= create_dim_cols(df_HT, ["P1_TEL_PING_ERGEBNIS","P1_TEL_PING_DATUM"], "ping")
dim_ping=pl.from_pandas(dim_ping)

dim_tel= create_dim_col(df_HT, "P1_TEL_TYP", "tel")
dim_tel=pl.from_pandas(dim_tel)

dim_entfadresse= create_dim_col(df_HT,"ENTFL_ADDRESS", "entfadresse")
dim_entfadresse=pl.from_pandas(dim_entfadresse)

dim_trassen= create_dim_col(df_HT, "TRASSENABSTANDSKATEGORIE_OWN", "trassen")
dim_trassen=pl.from_pandas(dim_trassen)

df1 = df_HT.join(dim_entfadresse, on="ENTFL_ADDRESS", how="left")
df2 = df1.join(dim_tel, on="P1_TEL_TYP", how="left")
df3 = df2.join(dim_trassen, on="TRASSENABSTANDSKATEGORIE_OWN", how="left")
dim_ping = dim_ping.with_columns(pl.col("P1_TEL_PING_DATUM").cast(pl.Date))
df4= df3.join(dim_ping, on=["P1_TEL_PING_ERGEBNIS", "P1_TEL_PING_DATUM"], how="left")

fact_df=df4["HASH_KEY", "PVID","STATUS","GÜLTIG_AB", "GÜLTIG_BIS",
        "ENTFADRESSE_ID", "TEL_ID", "TRASSEN_ID", "PING_ID",
        "MASTER_MARKETABLE", "FLAG_BESTANDSKUNDE", "FLAG_BLACKLIST","FLAG_PUBLIC"]

fact_df = fact_df.with_columns([pl.col("GÜLTIG_AB").dt.strftime("%Y%m%d").cast(pl.Int32).alias("GÜLTIG_AB_ID")])
fact_df = fact_df.with_columns([pl.when(pl.col("GÜLTIG_BIS").is_not_null())
                               .then(pl.col("GÜLTIG_BIS").cast(pl.Datetime("ns")).dt.strftime("%Y%m%d").cast(pl.Int32))
                               .otherwise(None)
                               .alias("GÜLTIG_BIS_ID")])


fact_df=fact_df.to_pandas()
bro.db.push_database(df=fact_df, table="FACT_HT",if_exists="replace",conn="vwh/dab",)





cursor.execute("ALTER TABLE Dim_Kunde ADD CONSTRAINT pk_dim_kunde PRIMARY KEY (PVID);
cursor.execute("ALTER TABLE DIM_TEL  ADD CONSTRAINT pk_dim_tel PRIMARY KEY (TeL_ID);")
cursor.execute("ALTER TABLE Dim_TRASSEN ADD CONSTRAINT pk_dim_trassen PRIMARY KEY (TRASSEN_ID);")
cursor.execute("ALTER TABLE DIM_ENTFADRESSE ADD CONSTRAINT pk_dim_entfaddress PRIMARY KEY (ENTFADRESSE_ID);")
cursor.execute("ALTER TABLE Dim_Ping ADD CONSTRAINT pk_dim_ping PRIMARY KEY (Ping_ID);")
cursor.execute("ALTER TABLE Dim_Date ADD CONSTRAINT pk_dim_date PRIMARY KEY (Datum_ID);")
cursor.execute("ALTER TABLE FACT_HT  ADD CONSTRAINT pk_fact_ht PRIMARY KEY (Hash_Key);)
cursor.execute("ALTER TABLE FACT_HT  ENABLE CONSTRAINT FK_PVD_KUNDE;)
cursor.execute("ALTER TABLE FACT_HT  ADD CONSTRAINT fk_pvd_tel FOREIGN KEY (TeL_ID) REFERENCES Dim_Tel(TeL_ID); ")
cursor.execute("ALTER TABLE FACT_HT  ADD CONSTRAINT fk_pvd_entfaddress FOREIGN KEY (ENTFADRESSE_ID) REFERENCES DIM_ENTFADRESSE(ENTFADRESSE_ID); ")
cursor.execute("ALTER TABLE FACT_HT  ADD CONSTRAINT fk_pvd_ping FOREIGN KEY (Ping_ID) REFERENCES Dim_Ping(Ping_ID); ")")
cursor.execute("ALTER TABLE FACT_HT  ADD CONSTRAINT fk_pvd_trassen FOREIGN KEY (Trassen_ID) REFERENCES Dim_Trassen(Trassen_ID);")

