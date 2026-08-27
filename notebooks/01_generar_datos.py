# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Datos de origen
# MAGIC
# MAGIC Genera las dos fuentes de cliente de D1 con el problema que hay que
# MAGIC resolver: **la misma persona aparece distinta en cada canal.**
# MAGIC
# MAGIC | Fuente | Cómo se captura | Qué sale mal |
# MAGIC |---|---|---|
# MAGIC | Facturación electrónica | Cédula en caja | Digitación con errores, valores centinela cuando nadie se identifica |
# MAGIC | E-commerce | Registro del cliente | Correo distinto al que usa en tienda |
# MAGIC
# MAGIC **Lo importante para este demo:** ni una línea de este notebook sabe en
# MAGIC qué nube corre. No hay rutas físicas de almacenamiento — solo nombres
# MAGIC de tres niveles de Unity Catalog, que es quien resuelve dónde vive
# MAGIC realmente cada tabla.

# COMMAND ----------
dbutils.widgets.text("catalogo", "d1_customer360")
dbutils.widgets.text("n_personas", "5000")

catalogo = dbutils.widgets.get("catalogo")
n_personas = int(dbutils.widgets.get("n_personas"))

# COMMAND ----------
import random
from datetime import date, timedelta
from pyspark.sql import functions as F

# Semilla fija: dos corridas producen exactamente lo mismo. Es lo que permite
# comparar el resultado de Azure contra el de GCP y saber que la diferencia,
# si aparece, viene de la plataforma y no del azar.
rng = random.Random(1531)

NOMBRES = ["MARIA", "JOSE", "LUIS", "ANA", "CARLOS", "SOFIA", "JUAN", "LAURA",
           "ANDRES", "PAULA", "DIEGO", "CAMILA", "JORGE", "VALENTINA"]
APELLIDOS = ["GOMEZ", "RODRIGUEZ", "MARTINEZ", "LOPEZ", "GARCIA", "PEREZ",
             "SANCHEZ", "RAMIREZ", "TORRES", "CASTRO"]
CENTINELAS = ["222222222222", "999999999", "0", "1"]
HOY = date(2026, 8, 27)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Las personas reales
# MAGIC
# MAGIC Primero se generan personas verdaderas; después cada canal las observa
# MAGIC de forma imperfecta. Así el dataset tiene una respuesta correcta conocida.

# COMMAND ----------
personas = []
for i in range(n_personas):
    nombre = f"{rng.choice(NOMBRES)} {rng.choice(APELLIDOS)}"
    personas.append({
        "cedula": str(rng.randint(10_000_000, 1_299_999_999)),
        "nombre": nombre,
        "correo": f"{nombre.split()[0].lower()}.{nombre.split()[1].lower()}{rng.randint(1, 999)}@mail.com",
        "telefono": f"3{rng.randint(10, 25)}{rng.randint(1000000, 9999999)}",
        # ~40% compra en los dos canales: son los que el matching debe unir.
        "omnicanal": rng.random() < 0.40,
    })

omni = sum(p["omnicanal"] for p in personas)
print(f"Personas reales: {len(personas):,}  ·  en ambos canales: {omni:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fuente 1 · Facturación electrónica
# MAGIC
# MAGIC Canal de mayor volumen. Se conoce la cédula pero no hay permiso de
# MAGIC contacto. Aquí entran los errores de caja.

# COMMAND ----------
def ensuciar(cedula, r):
    """Los errores reales de digitación en caja."""
    caso = r.random()
    if caso < 0.70:
        return cedula                                     # bien digitada
    if caso < 0.88 and len(cedula) > 2:                   # dígitos transpuestos
        p = r.randint(0, len(cedula) - 2)
        return cedula[:p] + cedula[p + 1] + cedula[p] + cedula[p + 2:]
    return cedula.zfill(11)                               # ceros a la izquierda


facturacion = []
for idx, p in enumerate(personas):
    r = random.Random(1531 + idx)
    for _ in range(r.randint(1, 5)):
        # 1 de cada 6 compras queda sin identificar.
        anonima = r.random() < 0.17
        facturacion.append({
            "registro_id": f"FE-{len(facturacion):08d}",
            "cedula": r.choice(CENTINELAS) if anonima else ensuciar(p["cedula"], r),
            "nombre": None if anonima else p["nombre"],
            "correo": None,
            "telefono": None,
            "canal": "facturacion_electronica",
            "fecha": HOY - timedelta(days=r.randint(0, 540)),
            "monto": round(r.uniform(3500, 180000), 2),
            "consent_contacto": 0,      # este canal NO da permiso de contacto
        })

print(f"Transacciones de facturación: {len(facturacion):,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fuente 2 · E-commerce
# MAGIC
# MAGIC El cliente se registró y aceptó el tratamiento de sus datos. La cédula
# MAGIC viene limpia, pero el correo suele ser otro.

# COMMAND ----------
ecommerce = []
for idx, p in enumerate(personas):
    if not p["omnicanal"]:
        continue
    r = random.Random(1531 * 7 + idx)
    ecommerce.append({
        "registro_id": f"EC-{len(ecommerce):08d}",
        "cedula": p["cedula"],
        "nombre": p["nombre"],
        # Menos de la mitad usa el mismo correo: por eso el matching no puede
        # depender solo del correo.
        "correo": p["correo"] if r.random() < 0.45
                  else f"{p['nombre'].split()[0].lower()}{r.randint(1, 9999)}@otromail.com",
        "telefono": p["telefono"],
        "canal": "ecommerce",
        "fecha": HOY - timedelta(days=r.randint(30, 700)),
        "monto": round(r.uniform(20000, 350000), 2),
        "consent_contacto": 1,
    })

print(f"Registros de e-commerce: {len(ecommerce):,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Escritura a Bronze
# MAGIC
# MAGIC `saveAsTable` sobre un nombre de tres niveles. Unity Catalog resuelve
# MAGIC dónde vive físicamente esa tabla — en Azure apunta a ADLS, en GCP a
# MAGIC Cloud Storage. **El notebook nunca lo sabe.**

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo}")
for zona in ["bronze", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{zona}")

esquema = """
    registro_id STRING, cedula STRING, nombre STRING, correo STRING,
    telefono STRING, canal STRING, fecha DATE, monto DOUBLE,
    consent_contacto INT
"""

(spark.createDataFrame(facturacion, esquema)
    .withColumn("_ingestado_en", F.current_timestamp())
    .write.mode("overwrite").saveAsTable(f"{catalogo}.bronze.facturacion"))

(spark.createDataFrame(ecommerce, esquema)
    .withColumn("_ingestado_en", F.current_timestamp())
    .write.mode("overwrite").saveAsTable(f"{catalogo}.bronze.ecommerce"))

# Los valores centinela son configuración, no código: si aparece uno nuevo se
# agrega una fila y se reejecuta, sin desplegar.
(spark.createDataFrame([(c,) for c in CENTINELAS], "valor STRING")
    .write.mode("overwrite").saveAsTable(f"{catalogo}.bronze.centinelas"))

# COMMAND ----------
for t in ["facturacion", "ecommerce", "centinelas"]:
    print(f"  bronze.{t:<14} {spark.table(f'{catalogo}.bronze.{t}').count():>8,}")

dbutils.notebook.exit(f"OK · {len(facturacion)} facturacion · {len(ecommerce)} ecommerce")
