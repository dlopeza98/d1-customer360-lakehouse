# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Silver — normalización y tipificación de identidad
# MAGIC
# MAGIC Unifica las 2 fuentes en un solo esquema de identidad y clasifica cada
# MAGIC registro en tres categorías (RF-005):
# MAGIC
# MAGIC | Tipo | Qué es | A dónde va |
# MAGIC |---|---|---|
# MAGIC | `valida` | Cédula plausible | Al matching |
# MAGIC | `no_identificado` | Valor centinela de caja | Se conserva, se agrega |
# MAGIC | `invalida` | Formato imposible | Cuarentena con motivo |
# MAGIC
# MAGIC **Ningún registro se descarta.** Tipificar antes de resolver reduce el
# MAGIC volumen que entra al matching en un factor cercano a 50.

# COMMAND ----------
dbutils.widgets.text("catalogo", "d1_customer360")
catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------
from pyspark.sql import functions as F

# Los centinelas son datos, no código: un valor nuevo se absorbe agregando
# una fila a esta tabla (gap G2 de la propuesta).
centinelas = [r.valor for r in spark.table(f"{catalogo}.bronze.catalogo_centinelas").collect()]
print(f"Valores centinela vigentes: {len(centinelas)} → {centinelas}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Unificar las dos fuentes en un esquema común de identidad

# COMMAND ----------
fact = (spark.table(f"{catalogo}.bronze.fact_electronica")
    .select(
        "registro_id", "cedula", "nombre", "correo", "telefono", "fecha_nac",
        "tienda", "canal", "fecha", "monto",
        "consent_fact_electronica", "consent_tratamiento_datos", "consent_notificaciones",
        F.lit("facturacion_electronica").alias("fuente")))

# E-commerce llega en 3 tablas: se aplanan al mismo esquema.
# Se toma el correo y el teléfono marcados como principales.
correo_principal = (spark.table(f"{catalogo}.bronze.ecom_correos")
    .filter("principal = 1").select("cliente_ecom_id", "correo"))
telefono_principal = (spark.table(f"{catalogo}.bronze.ecom_telefonos")
    .filter("principal = 1").select("cliente_ecom_id", "telefono"))

ecom = (spark.table(f"{catalogo}.bronze.ecom_clientes")
    .join(correo_principal, "cliente_ecom_id", "left")
    .join(telefono_principal, "cliente_ecom_id", "left")
    .select(
        F.col("cliente_ecom_id").alias("registro_id"),
        "cedula", "nombre", "correo", "telefono", "fecha_nac",
        F.lit(None).cast("string").alias("tienda"),
        F.lit("ecommerce").alias("canal"),
        F.col("fecha_registro").alias("fecha"),
        F.lit(None).cast("double").alias("monto"),
        "consent_fact_electronica", "consent_tratamiento_datos", "consent_notificaciones",
        F.lit("ecommerce").alias("fuente")))

crudo = fact.unionByName(ecom)
print(f"Registros unificados: {crudo.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Normalizar los 5 atributos de identidad (RF-007)

# COMMAND ----------
normalizado = (crudo
    # Solo dígitos, y se quitan ceros a la izquierda: dos representaciones de
    # la misma cédula tienen que colapsar antes de comparar.
    .withColumn("cedula_norm",
                F.regexp_replace(F.col("cedula").cast("string"), r"[^0-9]", ""))
    .withColumn("cedula_norm",
                F.when(F.col("cedula_norm").rlike(r"^0+\d"),
                       F.regexp_replace("cedula_norm", r"^0+", ""))
                 .otherwise(F.col("cedula_norm")))
    .withColumn("nombre_norm",
                F.upper(F.trim(F.regexp_replace(F.coalesce("nombre", F.lit("")), r"\s+", " "))))
    .withColumn("correo_norm", F.lower(F.trim("correo")))
    .withColumn("telefono_norm",
                F.regexp_replace(F.col("telefono").cast("string"), r"[^0-9]", "")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tipificar
# MAGIC
# MAGIC El orden importa: primero centinela, después validez. Un centinela como
# MAGIC `1234567890` tiene longitud válida — si se evaluara la longitud primero,
# MAGIC entraría al matching y contaminaría el Golden Record.

# COMMAND ----------
tipificado = normalizado.withColumn(
    "tipo_identidad",
    F.when(F.col("cedula_norm").isNull() | (F.col("cedula_norm") == ""), F.lit("invalida"))
     .when(F.col("cedula").isin(centinelas), F.lit("no_identificado"))
     .when(F.col("cedula_norm").isin(centinelas), F.lit("no_identificado"))
     .when(F.length("cedula_norm").between(6, 10), F.lit("valida"))
     .otherwise(F.lit("invalida")))

# Indicadores de calidad por atributo (RF-007)
tipificado = (tipificado
    .withColumn("q_cedula", (F.col("tipo_identidad") == "valida").cast("int"))
    .withColumn("q_nombre", (F.length("nombre_norm") > 3).cast("int"))
    .withColumn("q_correo", F.col("correo_norm").rlike(r"^[^@]+@[^@]+\.[a-z]{2,}$").cast("int"))
    .withColumn("q_telefono", (F.length("telefono_norm") == 10).cast("int"))
    .withColumn("q_fecha_nac", F.col("fecha_nac").isNotNull().cast("int")))

tipificado.write.mode("overwrite").saveAsTable(f"{catalogo}.silver.identidad_tipificada")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cuarentena — se conservan, no se eliminan (RF-008)

# COMMAND ----------
(tipificado.filter("tipo_identidad = 'invalida'")
    .withColumn("motivo_rechazo",
        F.when(F.col("cedula_norm").isNull() | (F.col("cedula_norm") == ""), "cedula_vacia")
         .when(F.length("cedula_norm") < 6, "cedula_muy_corta")
         .when(F.length("cedula_norm") > 10, "cedula_muy_larga")
         .otherwise("formato_invalido"))
    .write.mode("overwrite").saveAsTable(f"{catalogo}.silver.cuarentena"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agregados de transacciones anónimas
# MAGIC
# MAGIC De lo no identificado se conserva solo el agregado por día, tienda y
# MAGIC canal. Es la fuente del indicador de **tasa de identificación** — la
# MAGIC línea base del 2% que la propuesta se compromete a medir.

# COMMAND ----------
(tipificado
    .groupBy("fecha", "tienda", "canal")
    .agg(F.sum(F.when(F.col("tipo_identidad") == "valida", 1).otherwise(0)).alias("identificadas"),
         F.count("*").alias("transacciones"),
         F.sum("monto").alias("monto_total"))
    .write.mode("overwrite").saveAsTable(f"{catalogo}.silver.agregado_anonimo"))

# COMMAND ----------
resumen = (tipificado.groupBy("fuente", "tipo_identidad").count()
           .orderBy("fuente", "tipo_identidad"))
resumen.show(truncate=False)

total = tipificado.count()
validas = tipificado.filter("tipo_identidad = 'valida'").count()
print(f"\nReducción antes del matching: {total:,} → {validas:,} "
      f"({validas / total * 100:.1f}% entra a resolución de identidad)")

dbutils.notebook.exit(f"OK · {validas} identidades validas de {total}")
