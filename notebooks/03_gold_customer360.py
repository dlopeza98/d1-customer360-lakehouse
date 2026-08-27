# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — Customer 360 e Information Mart
# MAGIC
# MAGIC Publica la capa de consumo: fuente única para el modelo semántico de
# MAGIC Power BI y para el Genie space (RF-015, RF-016).
# MAGIC
# MAGIC El núcleo de identidad va **separado** de los atributos de dominio. Un
# MAGIC área consumidora amplía la vista agregando un satélite, sin reprocesar el
# MAGIC matching ni tocar el hub.

# COMMAND ----------
dbutils.widgets.text("catalogo", "d1_customer360")
catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------
from pyspark.sql import functions as F

golden = spark.table(f"{catalogo}.vault.sat_golden_record")
tx = (spark.table(f"{catalogo}.silver.identidad_tipificada")
      .filter("tipo_identidad = 'valida'"))

# Cada registro crudo trazado a su cliente único: es el puente entre las
# transacciones y el Golden Record.
enlaces = spark.table(f"{catalogo}.vault.lnk_same_as")

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_cliente — el núcleo de identidad

# COMMAND ----------
(golden
    .select(
        "cliente_unico_id", "cedula", "nombre", "correo",
        "identidades_crudas", "regla_resolucion", "omnicanal",
        "consent_fact_electronica", "consent_tratamiento_datos", "consent_notificaciones",
        # Linaje: de qué fuente vino cada atributo superviviente (RF-012).
        "cedula_origen", "nombre_origen", "correo_origen")
    .withColumn("_publicado_en", F.current_timestamp())
    .write.mode("overwrite").saveAsTable(f"{catalogo}.gold.dim_cliente"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## fct_identificacion — la vista de cobertura
# MAGIC
# MAGIC Mide qué porcentaje de las transacciones por tienda y canal quedan
# MAGIC asociadas a una identidad. Es la línea base del 2% que la propuesta se
# MAGIC compromete a medir y publicar.

# COMMAND ----------
(spark.table(f"{catalogo}.silver.agregado_anonimo")
    .withColumn("tasa_identificacion",
                F.when(F.col("transacciones") > 0,
                       F.col("identificadas") / F.col("transacciones")).otherwise(F.lit(0.0)))
    .write.mode("overwrite").saveAsTable(f"{catalogo}.gold.fct_identificacion"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## ft_cliente — Feature Store
# MAGIC
# MAGIC 8 features deterministas con el cliente único como llave primaria
# MAGIC (RF-020). **Ninguna es un modelo**: los modelos son Fase 2.
# MAGIC
# MAGIC Corrección punto-en-tiempo: ninguna feature puede ver datos posteriores
# MAGIC a la fecha de corte. Sin eso, un modelo de Fase 2 entrenaría con el
# MAGIC futuro y su desempeño en producción no se parecería al de laboratorio.

# COMMAND ----------
corte = F.current_date()

# Mapear cada transacción a su cliente único vía el grafo de enlaces.
registro_a_cliente = (enlaces
    .select(F.col("id_a").alias("registro_id"), "regla_resolucion")
    .unionByName(enlaces.select(F.col("id_b").alias("registro_id"), "regla_resolucion"))
    .dropDuplicates(["registro_id"]))

tx_cliente = (tx.join(registro_a_cliente, "registro_id", "left")
              .join(golden.select("cliente_unico_id", "cedula"),
                    tx["cedula_norm"] == golden["cedula"], "inner")
              .filter(F.col("fecha") < corte))

features = (tx_cliente
    .groupBy("cliente_unico_id")
    .agg(
        F.datediff(corte, F.max("fecha")).alias("recencia_dias"),
        F.count("*").alias("frecuencia"),
        F.coalesce(F.sum("monto"), F.lit(0.0)).alias("monto_total"),
        F.coalesce(F.avg("monto"), F.lit(0.0)).alias("ticket_promedio"),
        F.datediff(corte, F.min("fecha")).alias("antiguedad_dias"),
        F.first("canal").alias("canal_preferido"),
        F.countDistinct("tienda").alias("tiendas_distintas"),
        F.avg(F.when(F.col("tipo_identidad") == "valida", 1.0).otherwise(0.0))
         .alias("tasa_identificacion"))
    .withColumn("_fecha_corte", corte)
    # Cada feature tiene una sola definición vigente y trazable (RF-033).
    .withColumn("_version_definicion", F.lit("v1")))

features.write.mode("overwrite").saveAsTable(f"{catalogo}.gold.ft_cliente")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Métricas del tablero
# MAGIC
# MAGIC Las mismas tres que usa la validación de paridad entre nubes (RF-038).
# MAGIC Publicarlas como tabla las hace comparables entre Azure y GCP sin
# MAGIC recalcular nada.

# COMMAND ----------
n_clientes = golden.count()
crudas = golden.agg(F.sum("identidades_crudas")).collect()[0][0] or 0
cobertura = spark.table(f"{catalogo}.gold.fct_identificacion").agg(
    F.sum("identificadas").alias("i"), F.sum("transacciones").alias("t")).collect()[0]

metricas = [
    ("clientes_unicos", float(n_clientes)),
    ("identidades_crudas", float(crudas)),
    ("tasa_consolidacion", float(crudas / n_clientes) if n_clientes else 0.0),
    ("tasa_identificacion_global",
     float(cobertura["i"] / cobertura["t"]) if cobertura["t"] else 0.0),
    ("clientes_omnicanal", float(golden.filter("omnicanal = 1").count())),
    ("clientes_contactables",
     float(golden.filter("consent_notificaciones = 1").count())),
]

(spark.createDataFrame(metricas, "metrica STRING, valor DOUBLE")
    .withColumn("_calculado_en", F.current_timestamp())
    .write.mode("overwrite").saveAsTable(f"{catalogo}.gold.metricas_certificadas"))

# Distribución por regla: el tercer criterio de paridad.
(golden.groupBy("regla_resolucion").count().withColumnRenamed("count", "clientes")
    .write.mode("overwrite").saveAsTable(f"{catalogo}.gold.metricas_por_regla"))

# COMMAND ----------
for m, v in metricas:
    print(f"  {m:<28} {v:>14,.4f}")

print("\nGold publicado: dim_cliente · fct_identificacion · ft_cliente")
print("                metricas_certificadas · metricas_por_regla")

dbutils.notebook.exit(f"OK · {n_clientes} clientes en Gold")
