# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Vault — Identity Graph y Golden Record
# MAGIC
# MAGIC Resuelve qué registros de las dos fuentes corresponden a la misma persona
# MAGIC y consolida un registro único (RF-010, RF-011, RF-012).
# MAGIC
# MAGIC Tres decisiones de diseño que hay que poder defender:
# MAGIC
# MAGIC 1. **Bloqueo antes de comparar.** Con 25 M de identidades, comparar todos
# MAGIC    contra todos son 3·10¹⁴ pares. Solo se comparan registros que comparten
# MAGIC    alguna llave de bloqueo (RNF-003).
# MAGIC 2. **Las reglas viven fuera del código**, en `../jobs/reglas_identidad.json`.
# MAGIC    Desactivar una y reejecutar no requiere desplegar (RF-013, RNF-011).
# MAGIC 3. **La llave de cliente es un hash determinista**, no un autoincremental.
# MAGIC    Por eso sobrevive a un reproceso y a un cambio de nube — que es lo que
# MAGIC    hace posible la validación de paridad (RF-038).

# COMMAND ----------
dbutils.widgets.text("catalogo", "d1_customer360")
dbutils.widgets.text("ruta_reglas", "")

catalogo = dbutils.widgets.get("catalogo")
ruta_reglas = dbutils.widgets.get("ruta_reglas")

# COMMAND ----------
import hashlib
import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cargar el catálogo de reglas

# COMMAND ----------
if not ruta_reglas:
    # Junto al notebook cuando corre desde el bundle desplegado.
    ruta_reglas = str(Path(__file__).parent.parent / "jobs" / "reglas_identidad.json") \
        if "__file__" in dir() else "../jobs/reglas_identidad.json"

with open(ruta_reglas) as fh:
    catalogo_reglas = json.load(fh)

reglas = [r for r in catalogo_reglas["reglas"] if r["activa"]]
reglas.sort(key=lambda r: r["rank"])

print(f"Catálogo {catalogo_reglas['version']} — {len(reglas)} reglas activas "
      f"de {len(catalogo_reglas['reglas'])}\n")
for r in reglas:
    print(f"  {r['id']}  rank {r['rank']}  umbral {r['umbral']:<5}  {r['descripcion']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Universo: solo identidades válidas

# COMMAND ----------
validas = (spark.table(f"{catalogo}.silver.identidad_tipificada")
           .filter("tipo_identidad = 'valida'")
           .select("registro_id", "fuente", "canal", "cedula_norm", "nombre_norm",
                   "correo_norm", "telefono_norm", "fecha_nac",
                   "consent_fact_electronica", "consent_tratamiento_datos",
                   "consent_notificaciones", "q_cedula", "q_nombre", "q_correo"))

print(f"Identidades válidas a resolver: {validas.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloqueo (RNF-003)
# MAGIC
# MAGIC Tres llaves. Un par de registros se compara solo si coincide en al menos
# MAGIC una. Un mismo par puede aparecer por varias llaves: se deduplica después.

# COMMAND ----------
bloqueado = (validas
    .withColumn("bk_cedula", F.col("cedula_norm"))
    .withColumn("bk_correo", F.col("correo_norm"))
    # Fonético del primer nombre + últimos 4 del teléfono: atrapa cédulas mal
    # digitadas cuando el nombre y el teléfono sí coinciden.
    .withColumn("bk_nombre_tel",
                F.when(F.col("telefono_norm").isNotNull() & (F.col("nombre_norm") != ""),
                       F.concat_ws("|",
                                   F.soundex(F.split("nombre_norm", " ")[0]),
                                   F.substring(F.col("telefono_norm"), -4, 4)))))


def pares_por_bloque(llave):
    izq = bloqueado.select(
        F.col("registro_id").alias("id_a"), F.col(llave).alias("bk"),
        *[F.col(c).alias(f"{c}_a") for c in
          ["cedula_norm", "nombre_norm", "correo_norm", "telefono_norm", "fecha_nac"]])
    der = bloqueado.select(
        F.col("registro_id").alias("id_b"), F.col(llave).alias("bk"),
        *[F.col(c).alias(f"{c}_b") for c in
          ["cedula_norm", "nombre_norm", "correo_norm", "telefono_norm", "fecha_nac"]])
    return (izq.join(der, on="bk", how="inner")
               .filter(F.col("bk").isNotNull() & (F.col("bk") != ""))
               .filter(F.col("id_a") < F.col("id_b"))      # evita el par espejo
               .drop("bk"))


candidatos = None
for llave in ["bk_cedula", "bk_correo", "bk_nombre_tel"]:
    p = pares_por_bloque(llave)
    candidatos = p if candidatos is None else candidatos.unionByName(p)

candidatos = candidatos.dropDuplicates(["id_a", "id_b"]).cache()
n_cand = candidatos.count()
n_val = validas.count()
print(f"Pares candidatos: {n_cand:,}")
print(f"Comparación exhaustiva habría sido: {n_val * (n_val - 1) // 2:,} pares")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Motor de reglas Match · Merge · Survivor
# MAGIC
# MAGIC Cada regla marca los pares que resuelve, dejando registrado **su id y su
# MAGIC umbral**. Toda coincidencia queda trazada hasta la regla que la produjo,
# MAGIC que es lo que permite auditarla después.

# COMMAND ----------
resueltos = None
for regla in reglas:
    marcados = (candidatos
        .filter(regla["condicion"])
        .withColumn("regla_resolucion", F.lit(regla["id"]))
        .withColumn("umbral_aplicado", F.lit(regla["umbral"]))
        .withColumn("rank_regla", F.lit(regla["rank"])))
    resueltos = marcados if resueltos is None else resueltos.unionByName(marcados)

# Precedencia: si dos reglas resuelven el mismo par, gana la de menor rank.
w = Window.partitionBy("id_a", "id_b").orderBy("rank_regla")
enlaces = (resueltos
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1").drop("_rn")
    .select("id_a", "id_b", "regla_resolucion", "umbral_aplicado"))

enlaces.write.mode("overwrite").saveAsTable(f"{catalogo}.vault.lnk_same_as")
print(f"Enlaces same-as confirmados: {enlaces.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Componentes conectados
# MAGIC
# MAGIC Si A≈B y B≈C, entonces A, B y C son la misma persona. Se propaga el
# MAGIC menor id del componente hasta que deja de cambiar.
# MAGIC
# MAGIC Con el volumen real esto se resuelve con GraphFrames; aquí se hace por
# MAGIC iteración explícita para que el mecanismo sea legible en el demo.

# COMMAND ----------
MAX_ITER = 10

# Arista en ambos sentidos + auto-enlace, para que todo registro tenga grupo.
aristas = (enlaces.select(F.col("id_a").alias("src"), F.col("id_b").alias("dst"))
           .unionByName(enlaces.select(F.col("id_b").alias("src"), F.col("id_a").alias("dst")))
           .unionByName(validas.select(F.col("registro_id").alias("src"),
                                       F.col("registro_id").alias("dst"))))

grupos = aristas.groupBy("src").agg(F.min("dst").alias("grupo"))

for i in range(MAX_ITER):
    nuevo = (aristas
        .join(grupos.withColumnRenamed("src", "dst").withColumnRenamed("grupo", "grupo_vecino"),
              on="dst", how="inner")
        .groupBy("src").agg(F.min("grupo_vecino").alias("grupo")))

    cambios = (nuevo.alias("n").join(grupos.alias("g"), "src")
               .filter(F.col("n.grupo") != F.col("g.grupo")).count())
    grupos = nuevo.cache()
    print(f"  iteración {i + 1}: {cambios:,} registros cambiaron de componente")
    if cambios == 0:
        break

grupos = grupos.withColumnRenamed("src", "registro_id")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Golden Record — supervivencia de atributos
# MAGIC
# MAGIC Sobrevive el atributo de mayor calidad. Ante empate, el de e-commerce,
# MAGIC porque ahí el cliente lo digitó él mismo en lugar de un cajero.

# COMMAND ----------
@F.udf(StringType())
def llave_cliente(grupo):
    """Determinista: mismo grupo → misma llave, en cualquier nube y corrida."""
    return hashlib.sha256(f"d1:{grupo}".encode()).hexdigest()[:16]


enriquecido = (validas.join(grupos, "registro_id", "inner")
    .withColumn("prioridad_fuente", F.when(F.col("fuente") == "ecommerce", 0).otherwise(1)))

w_ced = Window.partitionBy("grupo").orderBy(F.desc("q_cedula"), "prioridad_fuente", "registro_id")
w_nom = Window.partitionBy("grupo").orderBy(F.desc("q_nombre"), "prioridad_fuente", "registro_id")
w_cor = Window.partitionBy("grupo").orderBy(F.desc("q_correo"), "prioridad_fuente", "registro_id")

sobreviviente = (enriquecido
    .withColumn("cedula_sv", F.first("cedula_norm").over(w_ced))
    .withColumn("cedula_sv_fuente", F.first("fuente").over(w_ced))
    .withColumn("nombre_sv", F.first("nombre_norm").over(w_nom))
    .withColumn("nombre_sv_fuente", F.first("fuente").over(w_nom))
    .withColumn("correo_sv", F.first("correo_norm").over(w_cor))
    .withColumn("correo_sv_fuente", F.first("fuente").over(w_cor)))

# La regla que se reporta por cliente es la de menor rank de su componente:
# determinista, no depende del orden de las filas.
regla_por_grupo = (enlaces
    .join(grupos.withColumnRenamed("registro_id", "id_a"), "id_a")
    .groupBy("grupo").agg(F.min("regla_resolucion").alias("regla_resolucion")))

golden = (sobreviviente
    .groupBy("grupo")
    .agg(
        F.first("cedula_sv").alias("cedula"),
        F.first("cedula_sv_fuente").alias("cedula_origen"),
        F.first("nombre_sv").alias("nombre"),
        F.first("nombre_sv_fuente").alias("nombre_origen"),
        F.first("correo_sv").alias("correo"),
        F.first("correo_sv_fuente").alias("correo_origen"),
        F.countDistinct("registro_id").alias("identidades_crudas"),
        F.collect_set("canal").alias("canales"),
        # Consentimiento por canal: sobrevive el "sí" de cualquier fuente,
        # porque es un permiso otorgado, no un atributo a promediar (RF-014).
        F.max("consent_fact_electronica").alias("consent_fact_electronica"),
        F.max("consent_tratamiento_datos").alias("consent_tratamiento_datos"),
        F.max("consent_notificaciones").alias("consent_notificaciones"))
    .join(regla_por_grupo, "grupo", "left")
    .withColumn("regla_resolucion", F.coalesce("regla_resolucion", F.lit("R00")))
    .withColumn("cliente_unico_id", llave_cliente(F.col("grupo")))
    .withColumn("omnicanal", (F.size("canales") > 1).cast("int"))
    .drop("grupo"))

golden.write.mode("overwrite").saveAsTable(f"{catalogo}.vault.sat_golden_record")

# COMMAND ----------
n_golden = golden.count()
crudas = golden.agg(F.sum("identidades_crudas")).collect()[0][0]
omni = golden.filter("omnicanal = 1").count()

print(f"Golden Record          : {n_golden:,} clientes únicos")
print(f"Identidades crudas     : {crudas:,}")
print(f"Tasa de consolidación  : x{crudas / n_golden:.4f}")
print(f"Clientes omnicanal     : {omni:,} ({omni / n_golden * 100:.1f}%)")
print("   ^ estos son los que hoy D1 no puede reconocer como la misma persona")

dbutils.notebook.exit(f"OK · {n_golden} clientes unicos · consolidacion x{crudas / n_golden:.4f}")
