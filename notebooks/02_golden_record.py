# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Golden Record
# MAGIC
# MAGIC Une las dos fuentes y produce un registro único por persona.
# MAGIC
# MAGIC Tres decisiones que sostienen el demo de portabilidad:
# MAGIC
# MAGIC 1. **Las reglas viven fuera del código**, en `jobs/reglas_identidad.json`.
# MAGIC    Cambiar el matching no es cambiar el notebook.
# MAGIC 2. **La llave de cliente es un hash determinista**, no un
# MAGIC    autoincremental. Por eso la misma persona obtiene la misma llave en
# MAGIC    Azure y en GCP — que es lo que permite comparar las dos corridas.
# MAGIC 3. **Cero referencias de nube.** Todo se direcciona por
# MAGIC    `catálogo.esquema.tabla`.

# COMMAND ----------
dbutils.widgets.text("catalogo", "d1_customer360")
dbutils.widgets.text("ruta_reglas", "../jobs/reglas_identidad.json")

catalogo = dbutils.widgets.get("catalogo")
ruta_reglas = dbutils.widgets.get("ruta_reglas")

# COMMAND ----------
import hashlib
import json

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

with open(ruta_reglas) as fh:
    catalogo_reglas = json.load(fh)

reglas = sorted([r for r in catalogo_reglas["reglas"] if r["activa"]],
                key=lambda r: r["rank"])

print(f"Catálogo {catalogo_reglas['version']} — {len(reglas)} reglas activas:")
for r in reglas:
    print(f"  {r['id']}  {r['descripcion']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Normalizar y tipificar
# MAGIC
# MAGIC Ningún registro se descarta. Los que no tienen identidad usable se
# MAGIC apartan con su motivo, porque siguen siendo venta real.

# COMMAND ----------
centinelas = [r.valor for r in spark.table(f"{catalogo}.bronze.centinelas").collect()]

crudo = (spark.table(f"{catalogo}.bronze.facturacion")
         .unionByName(spark.table(f"{catalogo}.bronze.ecommerce")))

normalizado = (crudo
    # Solo dígitos y sin ceros a la izquierda: dos escrituras de la misma
    # cédula tienen que colapsar antes de compararlas.
    .withColumn("cedula_norm", F.regexp_replace(F.col("cedula"), r"[^0-9]", ""))
    .withColumn("cedula_norm", F.regexp_replace("cedula_norm", r"^0+(?=\d)", ""))
    .withColumn("nombre_norm",
                F.upper(F.trim(F.regexp_replace(F.coalesce("nombre", F.lit("")), r"\s+", " "))))
    .withColumn("correo_norm", F.lower(F.trim("correo")))
    .withColumn("telefono_norm", F.regexp_replace(F.coalesce("telefono", F.lit("")), r"[^0-9]", "")))

# El orden importa: primero centinela, después longitud. Un centinela puede
# tener longitud válida y colarse al matching si se evalúa al revés.
tipificado = normalizado.withColumn(
    "tipo_identidad",
    F.when(F.col("cedula").isin(centinelas), F.lit("no_identificado"))
     .when(F.length("cedula_norm").between(6, 10), F.lit("valida"))
     .otherwise(F.lit("invalida")))

validas = tipificado.filter("tipo_identidad = 'valida'").cache()

resumen = tipificado.groupBy("tipo_identidad").count().collect()
for fila in sorted(resumen, key=lambda f: -f["count"]):
    print(f"  {fila['tipo_identidad']:<18} {fila['count']:>8,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Emparejar
# MAGIC
# MAGIC Solo se comparan registros que comparten cédula, correo o teléfono. Con
# MAGIC volumen real, comparar todos contra todos es inviable: 25 M de
# MAGIC identidades son 3·10¹⁴ pares.

# COMMAND ----------
COLUMNAS = ["cedula_norm", "nombre_norm", "correo_norm", "telefono_norm"]

izq = validas.select(F.col("registro_id").alias("id_a"),
                     *[F.col(c).alias(f"{c}_a") for c in COLUMNAS])
der = validas.select(F.col("registro_id").alias("id_b"),
                     *[F.col(c).alias(f"{c}_b") for c in COLUMNAS])

candidatos = None
for llave in COLUMNAS:
    par = (izq.join(der, izq[f"{llave}_a"] == der[f"{llave}_b"], "inner")
              .filter(F.col(f"{llave}_a").isNotNull() & (F.col(f"{llave}_a") != ""))
              .filter(F.col("id_a") < F.col("id_b")))     # evita el par espejo
    candidatos = par if candidatos is None else candidatos.unionByName(par)

candidatos = candidatos.dropDuplicates(["id_a", "id_b"]).cache()
print(f"Pares candidatos: {candidatos.count():,}")

# COMMAND ----------
# Cada regla marca los pares que resuelve, dejando registrado cuál fue.
# Así toda coincidencia queda trazada hasta la regla que la produjo.
resueltos = None
for regla in reglas:
    marcados = (candidatos.filter(regla["condicion"])
                .withColumn("regla", F.lit(regla["id"]))
                .withColumn("rank_regla", F.lit(regla["rank"])))
    resueltos = marcados if resueltos is None else resueltos.unionByName(marcados)

# Si dos reglas resuelven el mismo par, gana la de menor rank.
w = Window.partitionBy("id_a", "id_b").orderBy("rank_regla")
enlaces = (resueltos.withColumn("_rn", F.row_number().over(w))
           .filter("_rn = 1").select("id_a", "id_b", "regla").cache())

print(f"Enlaces confirmados: {enlaces.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agrupar y consolidar
# MAGIC
# MAGIC Si A≈B y B≈C, los tres son la misma persona. Se propaga el menor id del
# MAGIC grupo hasta que deja de cambiar.

# COMMAND ----------
aristas = (enlaces.select(F.col("id_a").alias("src"), F.col("id_b").alias("dst"))
    .unionByName(enlaces.select(F.col("id_b").alias("src"), F.col("id_a").alias("dst")))
    .unionByName(validas.select(F.col("registro_id").alias("src"),
                                F.col("registro_id").alias("dst"))))

grupos = aristas.groupBy("src").agg(F.min("dst").alias("grupo"))

for i in range(8):
    nuevo = (aristas
        .join(grupos.selectExpr("src as dst", "grupo as grupo_vecino"), "dst")
        .groupBy("src").agg(F.min("grupo_vecino").alias("grupo")))
    cambios = (nuevo.alias("n").join(grupos.alias("g"), "src")
               .filter(F.col("n.grupo") != F.col("g.grupo")).count())
    grupos = nuevo.cache()
    print(f"  iteración {i + 1}: {cambios:,} cambios")
    if cambios == 0:
        break

grupos = grupos.withColumnRenamed("src", "registro_id")

# COMMAND ----------
@F.udf(StringType())
def llave_cliente(grupo):
    """Determinista. Mismo grupo, misma llave — en cualquier nube."""
    return hashlib.sha256(f"d1:{grupo}".encode()).hexdigest()[:16]


regla_por_grupo = (enlaces
    .join(grupos.withColumnRenamed("registro_id", "id_a"), "id_a")
    .groupBy("grupo").agg(F.min("regla").alias("regla")))

golden = (validas.join(grupos, "registro_id")
    .groupBy("grupo")
    .agg(
        F.min("cedula_norm").alias("cedula"),
        F.max("nombre_norm").alias("nombre"),
        F.max("correo_norm").alias("correo"),
        F.count("*").alias("identidades_crudas"),
        F.countDistinct("canal").alias("canales"),
        F.sum("monto").alias("monto_total"),
        # El consentimiento sobrevive si CUALQUIER canal lo otorgó: es un
        # permiso concedido, no un atributo a promediar.
        F.max("consent_contacto").alias("consent_contacto"))
    .join(regla_por_grupo, "grupo", "left")
    .withColumn("regla", F.coalesce("regla", F.lit("sin_par")))
    .withColumn("cliente_unico_id", llave_cliente(F.col("grupo")))
    .withColumn("omnicanal", (F.col("canales") > 1).cast("int"))
    .drop("grupo"))

golden.write.mode("overwrite").saveAsTable(f"{catalogo}.gold.golden_record")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Métricas
# MAGIC
# MAGIC Estas tres son las que se comparan entre Azure y GCP para demostrar que
# MAGIC la migración no cambió el resultado.

# COMMAND ----------
n = golden.count()
crudas = golden.agg(F.sum("identidades_crudas")).collect()[0][0]
n_omni = golden.filter("omnicanal = 1").count()
contactables = golden.filter("consent_contacto = 1").count()

metricas = [
    ("clientes_unicos", float(n)),
    ("identidades_crudas", float(crudas)),
    ("tasa_consolidacion", round(crudas / n, 6)),
    ("clientes_omnicanal", float(n_omni)),
    ("clientes_contactables", float(contactables)),
]

(spark.createDataFrame(metricas, "metrica STRING, valor DOUBLE")
    .write.mode("overwrite").saveAsTable(f"{catalogo}.gold.metricas"))

(golden.groupBy("regla").count()
    .write.mode("overwrite").saveAsTable(f"{catalogo}.gold.metricas_por_regla"))

# COMMAND ----------
print()
for m, v in metricas:
    print(f"  {m:<24} {v:>12,.4f}")
print(f"\n  {n_omni:,} clientes compran en los dos canales.")
print("  Hoy D1 no puede reconocerlos como la misma persona.")

dbutils.notebook.exit(f"OK · {n} clientes unicos · consolidacion x{crudas / n:.4f}")
