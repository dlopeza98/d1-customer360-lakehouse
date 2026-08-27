# D1 · Customer 360 — el mismo código en Azure y en GCP

Demo de portabilidad entre nubes. Un pipeline de resolución de identidad que
se despliega en **Azure Databricks** o en **GCP Databricks** cambiando una
palabra.

Proyecto 1531. Datos sintéticos.

---

## La idea en una frase

> En una migración de nube, la pregunta no es cuánto tardamos en copiar.
> Es **qué parte del sistema sabe en qué nube está**.

En este repo esa parte son **2 líneas**, y están las dos en `databricks.yml`.
Todo lo demás — los notebooks, el job, las reglas de identidad — no sabe
dónde corre.

Compruébalo:

```bash
bash scripts/verificar_portabilidad.sh
```

---

## Estructura

```
notebooks/     01 genera las 2 fuentes · 02 produce el Golden Record
jobs/          la definición del job en JSON + las reglas de identidad
scripts/       el generador y el verificador de portabilidad
databricks.yml los dos targets. Aquí, y solo aquí, se nombran las nubes.
```

`resources/` se genera desde `jobs/` y está en `.gitignore`.

---

## Cómo se mueve de Azure a GCP

**A mano:**

```bash
databricks bundle deploy -t azure
databricks bundle deploy -t gcp
```

**Por pipeline** — promoción por rama:

```
develop ──push──> target azure ──> Azure Databricks
   │
   └── merge ──> main ──push──> target gcp ──> GCP Databricks
```

Mover el POC entre nubes es un merge.

---

## Por qué funciona

Tres decisiones, y sin ellas el demo no se sostiene:

**1. Nada se direcciona por ruta física.** Los notebooks usan
`catálogo.esquema.tabla`. Unity Catalog resuelve dónde vive esa tabla — ADLS
Gen2 en Azure, Cloud Storage en GCP. El código nunca lo sabe.

**2. Las reglas de identidad viven fuera del código**, en
`jobs/reglas_identidad.json`. Activar o desactivar una regla y reejecutar no
requiere desplegar nada. El archivo viaja a GCP como un archivo más.

**3. La llave de cliente es un hash determinista**, no un autoincremental. La
misma persona obtiene la misma llave en las dos nubes — que es exactamente lo
que permite comparar las dos corridas y demostrar que la migración no cambió
el resultado.

---

## Lo que cambia de verdad al migrar

| Azure | GCP |
|---|---|
| ADLS Gen2 | Cloud Storage |
| Access Connector (identidad administrada) | Service Account **generada por Databricks** |
| Key Vault | Secret Manager |
| NAT Gateway + IP pública | Cloud NAT + IP reservada |
| Unity Catalog · Delta Lake · Spark | **Idéntico** |

Un detalle que atrapa a quien porta este tipo de infraestructura: en Azure
**tú** creas la identidad y le pasas su id a Databricks; en GCP **Databricks**
genera la service account y tú le asignas los roles IAM. La flecha apunta al
revés.

---

## Correr en local

```bash
python scripts/build_resources.py     # jobs/*.json -> resources/*.yml
bash scripts/verificar_portabilidad.sh
databricks bundle validate -t azure
```

Sin workspace configurado, `bundle validate` resuelve todo el YAML y falla al
autenticarse. Es lo esperado.

**Windows:** `python` en el PATH suele resolver al stub del Microsoft Store.
Invoca el intérprete real por ruta completa si los scripts no arrancan.

### Por qué los jobs están en JSON

Databricks Asset Bundles solo acepta YAML en `include`:

```
Error: Files in the 'include' configuration section must be YAML files.
```

Pero la definición nativa de un job es JSON — es lo que devuelve
`databricks jobs get`. Manteniéndolos en JSON, lo versionado es exactamente el
contrato de la API. `scripts/build_resources.py` hace el puente, y de paso
valida: dependencias que no existen, clusters no declarados, notebooks
faltantes y campos que la API no reconoce cortan el build.

---

## Secrets para GitHub Actions

`DATABRICKS_HOST_AZURE` · `DATABRICKS_HOST_GCP` · `DATABRICKS_CLIENT_ID` ·
`DATABRICKS_CLIENT_SECRET`

Sin ellos, CI valida igual lo que no necesita workspace, y Deploy se omite con
un mensaje que dice qué falta.
