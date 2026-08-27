# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generación de datos sintéticos
# MAGIC
# MAGIC Reproduce las **2 fuentes en alcance** del POC de D1, con sus patologías
# MAGIC reales — que son las que justifican todo el resto del pipeline:
# MAGIC
# MAGIC | Fuente | Tablas | Patología que reproduce |
# MAGIC |---|---|---|
# MAGIC | Facturación electrónica | `fact_electronica` | Cédula en caja, **sin consentimiento de contacto**. Valores centinela cuando el comprador no se identifica. Cédulas mal digitadas. |
# MAGIC | E-commerce | `ecom_clientes`, `ecom_correos`, `ecom_telefonos` | Cliente registrado **con** consentimiento. Correos distintos por canal. |
# MAGIC
# MAGIC El mismo humano aparece en ambas fuentes con datos que no coinciden
# MAGIC exactamente. Ese es el problema que el Golden Record resuelve.
# MAGIC
# MAGIC **Semilla fija**: dos corridas producen exactamente lo mismo. Es requisito
# MAGIC para poder validar paridad entre nubes después (RF-038).

# COMMAND ----------
dbutils.widgets.text("catalogo", "d1_customer360")
dbutils.widgets.text("n_personas", "20000")
dbutils.widgets.text("semilla", "1531")

catalogo = dbutils.widgets.get("catalogo")
n_personas = int(dbutils.widgets.get("n_personas"))
semilla = int(dbutils.widgets.get("semilla"))

# COMMAND ----------
import random
from datetime import date, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructField, StructType, IntegerType, DateType, DoubleType,
)

rng = random.Random(semilla)

NOMBRES = ["MARIA", "JOSE", "LUIS", "ANA", "CARLOS", "SOFIA", "JUAN", "LAURA",
           "ANDRES", "PAULA", "DIEGO", "CAMILA", "JORGE", "VALENTINA", "MIGUEL",
           "DANIELA", "SANTIAGO", "ISABELLA", "SEBASTIAN", "MARIANA"]
APELLIDOS = ["GOMEZ", "RODRIGUEZ", "MARTINEZ", "LOPEZ", "GARCIA", "PEREZ",
             "SANCHEZ", "RAMIREZ", "TORRES", "FLOREZ", "CASTRO", "RUIZ",
             "MORENO", "JIMENEZ", "VARGAS", "ROJAS"]

# Valores centinela: lo que el cajero teclea cuando el comprador no se identifica.
# El catálogo completo NO está confirmado por el cliente — es el gap G2 de la
# propuesta. Por eso vive aquí como dato y no como condición en el código.
CENTINELAS = ["222222222222", "999999999", "111111111", "0", "1", "1234567890"]

TIENDAS = [f"T{n:04d}" for n in range(1, 121)]
HOY = date(2026, 8, 27)

# COMMAND ----------
# MAGIC %md
# MAGIC ## El universo de personas reales
# MAGIC
# MAGIC Primero se generan personas *verdaderas*. Después cada fuente las observa
# MAGIC de forma imperfecta. Así el dataset tiene una respuesta correcta conocida
# MAGIC contra la cual medir el matching.

# COMMAND ----------
def cedula_valida(r):
    return str(r.randint(10_000_000, 1_299_999_999))


personas = []
for i in range(n_personas):
    nombre = f"{rng.choice(NOMBRES)} {rng.choice(APELLIDOS)}"
    ced = cedula_valida(rng)
    personas.append({
        "persona_id": i,                      # verdad de referencia, NO se ingesta
        "cedula": ced,
        "nombre": nombre,
        "correo": f"{nombre.split()[0].lower()}.{nombre.split()[1].lower()}{rng.randint(1, 999)}@mail.com",
        "telefono": f"3{rng.randint(10, 25)}{rng.randint(1000000, 9999999)}",
        "fecha_nac": date(rng.randint(1955, 2006), rng.randint(1, 12), rng.randint(1, 28)),
        # ~35% de las personas compran en ambos canales: son las que el
        # matching tiene que reconciliar.
        "omnicanal": rng.random() < 0.35,
        "solo_ecommerce": rng.random() < 0.15,
    })

print(f"Personas reales generadas: {len(personas):,}")
print(f"  omnicanal (aparecen en las 2 fuentes): {sum(p['omnicanal'] for p in personas):,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fuente 1 · Facturación electrónica
# MAGIC
# MAGIC Canal de mayor volumen. Se conoce la cédula pero **no hay permiso de
# MAGIC contacto**. Aquí es donde entran los valores centinela y los errores de
# MAGIC digitación en caja.

# COMMAND ----------
def ensuciar_cedula(ced, r):
    """Reproduce los errores de digitación en caja."""
    modo = r.random()
    if modo < 0.55:
        return ced                                       # bien digitada
    if modo < 0.75:                                      # dígitos transpuestos
        if len(ced) < 2:
            return ced
        p = r.randint(0, len(ced) - 2)
        return ced[:p] + ced[p + 1] + ced[p] + ced[p + 2:]
    if modo < 0.88:
        return ced.zfill(11)                             # ceros a la izquierda
    if modo < 0.96:
        return ced[:-1]                                  # falta un dígito
    return ced + str(r.randint(0, 9))                    # sobra un dígito


filas_fact = []
registro = 0
for p in personas:
    if p["solo_ecommerce"]:
        continue
    for _ in range(rng.randint(1, 9)):                   # varias compras
        registro += 1
        r_local = random.Random(semilla + registro)

        # 1 de cada 6 transacciones queda sin identificar: valor centinela.
        if r_local.random() < 0.17:
            ced_obs = r_local.choice(CENTINELAS)
            nom_obs = None
        else:
            ced_obs = ensuciar_cedula(p["cedula"], r_local)
            nom_obs = p["nombre"] if r_local.random() < 0.9 else p["nombre"].split()[0]

        filas_fact.append({
            "registro_id": f"FE-{registro:09d}",
            "cedula": ced_obs,
            "nombre": nom_obs,
            # El correo en facturación electrónica NO está confirmado que exista:
            # es el gap G1 de la propuesta. Se simula presente solo a veces.
            "correo": p["correo"] if r_local.random() < 0.22 else None,
            "telefono": None,
            "fecha_nac": None,
            "tienda": r_local.choice(TIENDAS),
            "canal": "facturacion_electronica",
            "fecha": HOY - timedelta(days=r_local.randint(0, 720)),
            "monto": round(r_local.uniform(3500, 180000), 2),
            # Este canal entrega factura, pero NO da permiso de contacto.
            "consent_fact_electronica": 1,
            "consent_tratamiento_datos": 0,
            "consent_notificaciones": 0,
        })

print(f"Transacciones de facturación electrónica: {len(filas_fact):,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fuente 2 · E-commerce
# MAGIC
# MAGIC Tres tablas, como en el origen real: `clientes`, `correos` y `telefonos`.
# MAGIC Aquí el cliente sí se registró y aceptó el tratamiento de sus datos.

# COMMAND ----------
ecom_clientes, ecom_correos, ecom_telefonos = [], [], []
for p in personas:
    if not (p["omnicanal"] or p["solo_ecommerce"]):
        continue
    r_local = random.Random(semilla * 7 + p["persona_id"])
    cid = f"EC-{p['persona_id']:08d}"

    ecom_clientes.append({
        "cliente_ecom_id": cid,
        "cedula": p["cedula"],                           # aquí sí viene limpia
        "nombre": p["nombre"],
        "fecha_nac": p["fecha_nac"],
        "fecha_registro": HOY - timedelta(days=r_local.randint(30, 900)),
        "consent_fact_electronica": 0,
        "consent_tratamiento_datos": 1,
        "consent_notificaciones": 1 if r_local.random() < 0.42 else 0,
    })

    # El mismo humano usa un correo distinto en el canal digital: por eso el
    # matching no puede depender solo del correo.
    correo_ecom = (p["correo"] if r_local.random() < 0.45
                   else f"{p['nombre'].split()[0].lower()}{r_local.randint(1, 9999)}@otromail.com")
    ecom_correos.append({"cliente_ecom_id": cid, "correo": correo_ecom, "principal": 1})
    if r_local.random() < 0.3:
        ecom_correos.append({"cliente_ecom_id": cid, "correo": p["correo"], "principal": 0})

    ecom_telefonos.append({"cliente_ecom_id": cid, "telefono": p["telefono"], "principal": 1})

print(f"Clientes e-commerce: {len(ecom_clientes):,}")
print(f"Correos: {len(ecom_correos):,}  ·  Teléfonos: {len(ecom_telefonos):,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Escritura a Bronze

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo}")
for zona in ["bronze", "silver", "vault", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{zona}")

esquema_fact = StructType([
    StructField("registro_id", StringType()),
    StructField("cedula", StringType()),
    StructField("nombre", StringType()),
    StructField("correo", StringType()),
    StructField("telefono", StringType()),
    StructField("fecha_nac", DateType()),
    StructField("tienda", StringType()),
    StructField("canal", StringType()),
    StructField("fecha", DateType()),
    StructField("monto", DoubleType()),
    StructField("consent_fact_electronica", IntegerType()),
    StructField("consent_tratamiento_datos", IntegerType()),
    StructField("consent_notificaciones", IntegerType()),
])

(spark.createDataFrame(filas_fact, esquema_fact)
    .withColumn("_ingestado_en", F.current_timestamp())
    .write.mode("overwrite")
    .saveAsTable(f"{catalogo}.bronze.fact_electronica"))

(spark.createDataFrame(ecom_clientes)
    .withColumn("_ingestado_en", F.current_timestamp())
    .write.mode("overwrite").saveAsTable(f"{catalogo}.bronze.ecom_clientes"))

(spark.createDataFrame(ecom_correos)
    .write.mode("overwrite").saveAsTable(f"{catalogo}.bronze.ecom_correos"))

(spark.createDataFrame(ecom_telefonos)
    .write.mode("overwrite").saveAsTable(f"{catalogo}.bronze.ecom_telefonos"))

# El catálogo de centinelas es CONFIGURACIÓN, no código (gap G2):
# si aparece uno nuevo, se agrega una fila y se reejecuta. Sin desplegar.
(spark.createDataFrame([(c, "valor por defecto en caja") for c in CENTINELAS],
                       "valor STRING, motivo STRING")
    .write.mode("overwrite").saveAsTable(f"{catalogo}.bronze.catalogo_centinelas"))

# COMMAND ----------
print("Bronze poblado:")
for t in ["fact_electronica", "ecom_clientes", "ecom_correos",
          "ecom_telefonos", "catalogo_centinelas"]:
    n = spark.table(f"{catalogo}.bronze.{t}").count()
    print(f"  {t:<22} {n:>10,}")

dbutils.notebook.exit(f"OK · {len(filas_fact)} transacciones · {len(ecom_clientes)} clientes ecom")
