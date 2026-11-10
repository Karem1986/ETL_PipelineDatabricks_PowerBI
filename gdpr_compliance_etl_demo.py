# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # AVG-compliance: van Postgres naar Power BI for demo purposes.
# MAGIC
# MAGIC Nagebouwde versie van een project dat ik bij CGI deed. Alle data hier is nep en gegenereerd,
# MAGIC want de echte data en code zijn vertrouwelijk.
# MAGIC
# MAGIC **Doel:** een interactief Power BI-rapport dat handmatige AVG (Algemene verordening gegevensberscherming) controles vervangt. Het compliance team kan per AVG gebied filteren.
# MAGIC In deze demo drie gebieden:
# MAGIC 1. **Bewaartermijnen:** worden persoonsgegevens op tijd verwijderd?
# MAGIC 2. **Verzoeken van betrokkenen:** worden correctie en verwijderverzoeken binnen 1 maand afgehandeld?
# MAGIC 3. **Datalekken:** worden meldplichtige datalekken binnen 72 uur gemeld?
# MAGIC
# MAGIC **Architectuur:** Postgres → Azure Data Factory → Bronze (Delta) → Silver (schoon, SCD Type 2) → Gold (dbt, sterschema) → Power BI
# MAGIC
# MAGIC **Hoofdprobleem:** het laden was traag. Oplossing in deze demo: incrementeel laden in plaats van elke dag alles opnieuw.

# COMMAND ----------

from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.window import Window

N_RECORDS = 1_000_000    # persoonsgegevens
N_VERZOEKEN = 20_000
N_LEKKEN = 300

DAG1 = "2026-11-09"       # eerste volledige load
DAG2 = "2026-11-10"       # tweede load
PEILDATUM = DAG2          # "vandaag" in deze demo

# systeem_id, naam, afdeling, bewaartermijn_jaren
SYSTEMEN_DAG1 = [
    (1, "Klantportaal", "Klantcontact", 2),
    (2, "Klantenservice", "Klantcontact", 3),
    (3, "Archief", "Informatiebeheer", 7),
    (4, "Facturatie", "Financien", 7),
    (5, "HR-systeem", "HR", 2),
    (6, "Registratie", "Registratie", 5),
    (7, "Inkoop", "Financien", 7),
    (8, "Webformulieren", "Communicatie", 1),
]
# Op dag 2: Archief krijgt een bewaartermijn van 10 jaar, Klantenservice gaat naar een andere afdeling,
# en er komt een nieuw systeem bij, de chatbot met een bewaartermijn van 1 jaar. Dit wordt verwerkt met SCD Type 2.
SYSTEMEN_DAG2 = [
    (1, "Klantportaal", "Klantcontact", 2),
    (2, "Klantenservice", "Service en Support", 3),
    (3, "Archief", "Informatiebeheer", 10),
    (4, "Facturatie", "Financien", 7),
    (5, "HR-systeem", "HR", 2),
    (6, "Registratie", "Registratie", 5),
    (7, "Inkoop", "Financien", 7),
    (8, "Webformulieren", "Communicatie", 1),
    (9, "Chatbot", "Klantcontact", 1),
]
SYSTEMEN_SCHEMA = "systeem_id INT, naam STRING, afdeling STRING, bewaartermijn_jaren INT"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. bron (de Postgres-database)
# MAGIC In het project haalde Azure Data Factory de tabellen uit Postgres. Hier maken we nepdata in dezelfde vorm.

# COMMAND ----------

def willekeurige_datum(van, tot, seed):
    dagen = (date.fromisoformat(tot) - date.fromisoformat(van)).days
    return F.expr(f"date_add(DATE'{van}', CAST(rand({seed}) * {dagen} AS INT))")

def kies(waarden, seed):
    lijst = ", ".join(f"'{w}'" for w in waarden)
    return F.expr(f"element_at(array({lijst}), CAST(rand({seed}) * {len(waarden)} AS INT) + 1)")


CATEGORIEEN = ["contactgegevens", "BSN", "financiele gegevens", "adresgegevens"]
termijn = "CASE systeem_id " + " ".join(f"WHEN {s} THEN {t}" for s, _, _, t in SYSTEMEN_DAG1) + " ELSE 5 END"

# Records met persoonsgegevens op dag 1
records_dag1 = (spark.range(1, N_RECORDS + 1).withColumnRenamed("id", "record_id")
                .withColumn("_s", F.rand(1))
                # 0,1% verwijst naar een systeem dat niet bestaat: dat moet silver eruit halen
                .withColumn("systeem_id", F.when(F.col("_s") < 0.001, F.lit(99))
                                           .otherwise((F.col("_s") * 8).cast("int") + 1))
                .withColumn("categorie", kies(CATEGORIEEN, 2))
                .withColumn("aangemaakt_op", willekeurige_datum("2012-01-01", "2026-11-08", 3))
                .withColumn("_deadline", F.expr(f"add_months(aangemaakt_op, 12 * ({termijn}))"))
                .withColumn("_r", F.rand(4))
                # De meeste verlopen records zijn verwijderd, sommige te laat, sommige helemaal niet
                .withColumn("verwijderd_op",
                            F.when((F.col("_deadline") < F.lit(DAG1).cast("date")) & (F.col("_r") < 0.8),
                                   F.least(F.expr("date_add(_deadline, CAST(rand(5) * 45 - 35 AS INT))"),
                                           F.lit("2026-11-08").cast("date"))))
                .withColumn("laatst_gewijzigd", F.coalesce("verwijderd_op", "aangemaakt_op").cast("timestamp"))
                .drop("_s", "_deadline", "_r"))
records_dag1.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bron_records_dag1")

# Dag 2: er komen 5.000 nieuwe records bij
dag1 = spark.table("bron_records_dag1")
gewijzigd = (dag1.filter(F.col("verwijderd_op").isNull() & (F.col("record_id") % 100 == 7))
             .withColumn("verwijderd_op", F.lit(DAG2).cast("date"))
             .withColumn("laatst_gewijzigd", F.lit(f"{DAG2} 08:00:00").cast("timestamp")))
nieuw = (spark.range(N_RECORDS + 1, N_RECORDS + 5001).withColumnRenamed("id", "record_id")
         .withColumn("systeem_id", (F.rand(6) * 9).cast("int") + 1)
         .withColumn("categorie", kies(CATEGORIEEN, 7))
         .withColumn("aangemaakt_op", F.lit(DAG2).cast("date"))
         .withColumn("verwijderd_op", F.lit(None).cast("date"))
         .withColumn("laatst_gewijzigd", F.lit(f"{DAG2} 09:00:00").cast("timestamp")))
records_dag2 = (dag1.join(gewijzigd.select("record_id"), "record_id", "left_anti")
                .unionByName(gewijzigd)
                .unionByName(nieuw))
records_dag2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bron_records_dag2")

# Verzoeken van betrokkenen
verzoeken = (spark.range(1, N_VERZOEKEN + 1).withColumnRenamed("id", "verzoek_id")
             .withColumn("type", kies(["inzage", "correctie", "verwijdering"], 20))
             .withColumn("ontvangen_op", willekeurige_datum("2020-01-01", "2026-11-09", 21))
             .withColumn("_r", F.rand(22))
             .withColumn("afgehandeld_op",
                         F.when(F.col("_r") < 0.97,
                                F.least(F.expr("date_add(ontvangen_op, CAST(rand(23) * 38 AS INT))"),
                                        F.lit(DAG2).cast("date"))))
             .drop("_r"))
verzoeken.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bron_verzoeken")

# Datalekken
lekken = (spark.range(1, N_LEKKEN + 1).withColumnRenamed("id", "lek_id")
          .withColumn("ontdekt_op", F.expr("CAST(date_add(DATE'2020-01-01', CAST(rand(30) * 2500 AS INT)) AS TIMESTAMP)"
                                           " + make_interval(0, 0, 0, 0, CAST(rand(31) * 24 AS INT), 0, 0)"))
          .withColumn("meldplichtig", F.rand(32) < 0.6)
          .withColumn("_r", F.rand(33))
          .withColumn("gemeld_op",
                      F.when(F.col("meldplichtig") & (F.col("_r") < 0.97),
                             F.expr("ontdekt_op + make_interval(0, 0, 0, 0, CAST(rand(34) * 90 AS INT), 0, 0)")))
          .drop("_r"))
lekken.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bron_datalekken")

for naam in ["bron_records_dag1", "bron_records_dag2", "bron_verzoeken", "bron_datalekken"]:
    print(naam, spark.table(naam).count(), "rijen")

# COMMAND ----------

print(spark.table("bron_verzoeken").columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: eerste volledige load (dag 1)
# MAGIC Bronze slaat de data op zoals hij binnenkomt, zonder iets te veranderen. Plus een kolom wanneer het geladen is.

# COMMAND ----------

def laad_bronze(bron, doel, geladen_op):
    (spark.table(bron).withColumn("_geladen_op", F.lit(geladen_op).cast("timestamp"))
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(doel))


laad_bronze("bron_records_dag1", "bronze_records", f"{DAG1} 06:00:00")
laad_bronze("bron_verzoeken", "bronze_verzoeken", f"{DAG2} 06:00:00")
laad_bronze("bron_datalekken", "bronze_datalekken", f"{DAG2} 06:00:00")
spark.createDataFrame(SYSTEMEN_DAG1, SYSTEMEN_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_systemen_dag1")
spark.createDataFrame(SYSTEMEN_DAG2, SYSTEMEN_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_systemen_dag2")

print("bronze_records na dag 1:", spark.table("bronze_records").count(), "rijen")

# COMMAND ----------

# Checking the data

df = spark.table("bronze_records").toPandas()
print(df.head(5))



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Het probleem: elke dag alles opnieuw laden
# MAGIC De recordtabel heeft meer dan een miljoen rijen. Elke dag de hele tabel uit Postgres halen kost veel tijd,
# MAGIC terwijl er maar een paar duizend rijen veranderd zijn.

# COMMAND ----------

volledig = spark.table("bron_records_dag2")
print("Volledige herlaad op dag 2:", volledig.count(), "rijen uit Postgres halen")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. De oplossing: incrementeel laden met een watermark
# MAGIC 1. Kijk wat de laatste `laatst_gewijzigd` in bronze is (de watermark).
# MAGIC 2. Haal alleen rijen op die daarna gewijzigd zijn.
# MAGIC 3. Voeg ze toe met `MERGE`: bestaande rijen bijwerken, nieuwe rijen invoegen.
# MAGIC
# MAGIC Met een echte Postgres-bron laat je het filter in de database zelf draaien, zodat alleen de wijzigingen over het netwerk gaan:
# MAGIC ```python
# MAGIC spark.read.format("jdbc").option("query", f"SELECT * FROM records WHERE laatst_gewijzigd > '{watermark}'")
# MAGIC ```
# MAGIC In Azure Data Factory werkt het hetzelfde: een Lookup haalt de watermark op en de Copy-activiteit gebruikt die in de query.
# MAGIC Een index op `laatst_gewijzigd` in Postgres maakt dat filter snel.

# COMMAND ----------

watermark = spark.table("bronze_records").agg(F.max("laatst_gewijzigd")).first()[0]
print("Watermark:", watermark)

wijzigingen = (spark.table("bron_records_dag2")
               .filter(F.col("laatst_gewijzigd") > F.lit(watermark))
               .withColumn("_geladen_op", F.lit(f"{DAG2} 06:00:00").cast("timestamp")))
print("Incrementeel op dag 2:", wijzigingen.count(), "rijen uit Postgres halen")

wijzigingen.createOrReplaceTempView("wijzigingen")
spark.sql("""
    MERGE INTO bronze_records AS doel
    USING wijzigingen AS bron
    ON doel.record_id = bron.record_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# Controle: geeft incrementeel laden precies hetzelfde resultaat als alles opnieuw laden?
bronze = spark.table("bronze_records").drop("_geladen_op")
verschil = bronze.exceptAll(volledig).count() + volledig.exceptAll(bronze).count()
print("Rijen in bronze:", bronze.count(), "| in de bron:", volledig.count(), "| verschillen:", verschil)
assert verschil == 0, "Incrementeel laden wijkt af van een volledige herlaad"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: SCD Type 2 voor systemen
# MAGIC De bewaartermijn hoort bij een systeem en kan veranderen. Om te controleren of een record op tijd is verwijderd,
# MAGIC moet je de regel gebruiken die gold toen het record werd aangemaakt. Daarom bewaren we de geschiedenis.
# MAGIC
# MAGIC - `hash_diff`: een vingerafdruk van de kolommen die we volgen. Andere hash betekent dat er iets veranderd is.
# MAGIC - Veranderd: de oude rij afsluiten (`geldig_tot`, `is_actueel = false`) en een nieuwe rij invoegen.
# MAGIC
# MAGIC (In dbt kan dit ook met een snapshot. Hier laat ik de logica zelf zien.)

# COMMAND ----------

def met_hash(df):
    return df.withColumn("hash_diff", F.sha2(F.concat_ws("|", F.col("naam"), F.col("afdeling"),
                                                         F.col("bewaartermijn_jaren").cast("string")), 256))


# Eerste load: alles is geldig vanaf het begin
(met_hash(spark.table("bronze_systemen_dag1"))
 .withColumn("geldig_vanaf", F.lit("1900-01-01").cast("date"))
 .withColumn("geldig_tot", F.lit("9999-12-31").cast("date"))
 .withColumn("is_actueel", F.lit(True))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_dim_systeem"))

# Dag 2, stap 1: gewijzigde rijen afsluiten en helemaal nieuwe systemen invoegen
met_hash(spark.table("bronze_systemen_dag2")).createOrReplaceTempView("systemen_nieuw")
spark.sql(f"""
    MERGE INTO silver_dim_systeem AS doel
    USING systemen_nieuw AS bron
    ON doel.systeem_id = bron.systeem_id AND doel.is_actueel = true
    WHEN MATCHED AND doel.hash_diff <> bron.hash_diff THEN
      UPDATE SET geldig_tot = DATE'{DAG2}', is_actueel = false
    WHEN NOT MATCHED THEN
      INSERT (systeem_id, naam, afdeling, bewaartermijn_jaren, hash_diff, geldig_vanaf, geldig_tot, is_actueel)
      VALUES (bron.systeem_id, bron.naam, bron.afdeling, bron.bewaartermijn_jaren, bron.hash_diff,
              DATE'{DAG2}', DATE'9999-12-31', true)
""")

# Dag 2, stap 2: de nieuwe versie invoegen voor systemen die nu geen actuele rij meer hebben
spark.sql(f"""
    INSERT INTO silver_dim_systeem
    SELECT n.systeem_id, n.naam, n.afdeling, n.bewaartermijn_jaren, n.hash_diff,
           DATE'{DAG2}', DATE'9999-12-31', true
    FROM systemen_nieuw n
    LEFT JOIN silver_dim_systeem d
      ON d.systeem_id = n.systeem_id AND d.is_actueel = true
    WHERE d.systeem_id IS NULL
""")

display(spark.table("silver_dim_systeem").orderBy("systeem_id", "geldig_vanaf"))

# COMMAND ----------

# Controles op de SCD Type 2-tabel
dim = spark.table("silver_dim_systeem")

# Precies één actuele rij per systeem
assert dim.filter("is_actueel").groupBy("systeem_id").count().filter("count > 1").count() == 0, "Meer dan één actuele rij"

# Geen gaten of overlap: geldig_tot is gelijk aan geldig_vanaf van de volgende versie
volgorde = Window.partitionBy("systeem_id").orderBy("geldig_vanaf")
gaten = (dim.withColumn("volgende_vanaf", F.lead("geldig_vanaf").over(volgorde))
         .filter(F.col("volgende_vanaf").isNotNull() & (F.col("geldig_tot") != F.col("volgende_vanaf"))))
assert gaten.count() == 0, "Gaten of overlap in de geschiedenis"

# Geen twee opeenvolgende versies met dezelfde hash (dan is er niets echt veranderd)
dubbel = (dim.withColumn("vorige_hash", F.lag("hash_diff").over(volgorde))
          .filter(F.col("vorige_hash") == F.col("hash_diff")))
assert dubbel.count() == 0, "Versie zonder echte wijziging"
print("SCD Type 2-controles geslaagd")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver: records, verzoeken en datalekken controleren
# MAGIC Rijen die niet kloppen gaan naar een quarantainetabel, zodat ze niet stilletjes verdwijnen.

# COMMAND ----------

bekende_systemen = spark.table("silver_dim_systeem").select("systeem_id").distinct()
records = spark.table("bronze_records").drop("_geladen_op")

goed = (records.join(bekende_systemen, "systeem_id", "left_semi")
        .filter(F.col("verwijderd_op").isNull() | (F.col("verwijderd_op") >= F.col("aangemaakt_op"))))
fout = records.join(goed.select("record_id"), "record_id", "left_anti")

goed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_records")
fout.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_records_quarantaine")

(spark.table("bronze_verzoeken").drop("_geladen_op")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_verzoeken"))
(spark.table("bronze_datalekken").drop("_geladen_op")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_datalekken"))

sr = spark.table("silver_records")
print("Records in silver:", sr.count(), "| in quarantaine:", spark.table("silver_records_quarantaine").count())
assert sr.count() == sr.select("record_id").distinct().count(), "Dubbele record_id's"
assert sr.filter(F.col("aangemaakt_op").isNull()).count() == 0, "Records zonder aanmaakdatum"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold: sterschema voor Power BI
# MAGIC In het project waren dit dbt-modellen, bijvoorbeeld `models/gold/fct_avg_controles.sql`,
# MAGIC met tests zoals `unique`, `not_null` en `accepted_values` op de status. Hier staat dezelfde SQL direct in het notebook.
# MAGIC
# MAGIC - `gold_dim_systeem`: dimensie met een surrogaatsleutel per versie
# MAGIC - `gold_fct_avg_controles`: één rij per controle, met AVG-gebied, deadline en status
# MAGIC - `gold_avg_per_maand`: percentage op tijd per AVG-gebied per maand

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE gold_dim_systeem AS
    SELECT xxhash64(systeem_id, geldig_vanaf) AS systeem_sk,
           systeem_id, naam, afdeling, bewaartermijn_jaren, geldig_vanaf, geldig_tot, is_actueel
    FROM silver_dim_systeem
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE gold_fct_avg_controles AS
    WITH bewaartermijnen AS (
        -- de bewaartermijn die gold toen het record werd aangemaakt (point-in-time join op SCD Type 2)
        SELECT 'Bewaartermijnen' AS avg_gebied,
               CAST(r.record_id AS STRING) AS item_id,
               d.systeem_sk,
               r.categorie AS soort,
               CAST(add_months(r.aangemaakt_op, 12 * d.bewaartermijn_jaren) AS TIMESTAMP) AS deadline,
               CAST(r.verwijderd_op AS TIMESTAMP) AS afgehandeld_op
        FROM silver_records r
        JOIN gold_dim_systeem d
          ON r.systeem_id = d.systeem_id
         AND r.aangemaakt_op >= d.geldig_vanaf
         AND r.aangemaakt_op < d.geldig_tot
    ),
    verzoeken AS (
        SELECT 'Verzoeken van betrokkenen', CAST(verzoek_id AS STRING), CAST(NULL AS BIGINT), type,
               CAST(add_months(ontvangen_op, 1) AS TIMESTAMP), CAST(afgehandeld_op AS TIMESTAMP)
        FROM silver_verzoeken
    ),
    datalekken AS (
        SELECT 'Datalekken', CAST(lek_id AS STRING), CAST(NULL AS BIGINT), 'meldplichtig',
               ontdekt_op + INTERVAL 72 HOURS, gemeld_op
        FROM silver_datalekken
        WHERE meldplichtig
    ),
    alles AS (
        SELECT * FROM bewaartermijnen
        UNION ALL SELECT * FROM verzoeken
        UNION ALL SELECT * FROM datalekken
    )
    SELECT *,
           CASE
               WHEN afgehandeld_op IS NOT NULL AND afgehandeld_op <= deadline THEN 'Op tijd'
               WHEN afgehandeld_op IS NOT NULL THEN 'Te laat'
               WHEN deadline < TIMESTAMP'{PEILDATUM}' THEN 'Verlopen, niet afgehandeld'
               ELSE 'Nog binnen termijn'
           END AS status
    FROM alles
""")

spark.sql("""
    CREATE OR REPLACE TABLE gold_avg_per_maand AS
    SELECT avg_gebied, jaar, maand, aantal_te_beoordelen, aantal_op_tijd,
           round(100 * aantal_op_tijd / NULLIF(aantal_te_beoordelen, 0), 1) AS pct_op_tijd
    FROM (
        SELECT avg_gebied, year(deadline) AS jaar, month(deadline) AS maand,
               COUNT(*) AS aantal_te_beoordelen,
               SUM(CASE WHEN status = 'Op tijd' THEN 1 ELSE 0 END) AS aantal_op_tijd
        FROM gold_fct_avg_controles
        WHERE status <> 'Nog binnen termijn'
        GROUP BY avg_gebied, year(deadline), month(deadline)
    ) t
""")

# Overzicht per AVG-gebied: hoeveel controles hebben welke status?
display(spark.sql("""
    SELECT avg_gebied, status, COUNT(*) AS aantal
    FROM gold_fct_avg_controles
    GROUP BY avg_gebied, status
    ORDER BY avg_gebied, status
"""))

# COMMAND ----------

# Laatste 12 maanden per AVG-gebied
display(spark.sql(f"""
    SELECT * FROM gold_avg_per_maand
    WHERE make_date(jaar, maand, 1) >= add_months(DATE'{PEILDATUM}', -12)
      AND make_date(jaar, maand, 1) <= DATE'{PEILDATUM}'
    ORDER BY avg_gebied, jaar, maand
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Naar Power BI
# MAGIC - Power BI maakt verbinding met de gold-tabellen, bijvoorbeeld via een Databricks SQL warehouse (Import of DirectQuery).
# MAGIC - Sterschema: `gold_fct_avg_controles` in het midden, `gold_dim_systeem` en een datumtabel eromheen.
# MAGIC - Een slicer op `avg_gebied`, zodat het compliance team per AVG-gebied apart kan filteren.
# MAGIC - Percentage als DAX-measure, zodat het klopt bij elk filter:
# MAGIC ```
# MAGIC % op tijd =
# MAGIC DIVIDE(
# MAGIC     CALCULATE(COUNTROWS(fct_avg_controles), fct_avg_controles[status] = "Op tijd"),
# MAGIC     CALCULATE(COUNTROWS(fct_avg_controles), fct_avg_controles[status] <> "Nog binnen termijn")
# MAGIC )
# MAGIC ```