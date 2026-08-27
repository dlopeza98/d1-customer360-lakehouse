# D1 · Customer 360 & Golden Record

Lakehouse de resolución de identidad sobre Databricks, con **promoción entre
nubes por rama**: el mismo código se despliega en Azure Databricks o en GCP
Databricks cambiando el target del bundle.

Proyecto 1531. Los datos son sintéticos.

---

## Cómo está organizado

```
notebooks/     Lo que corre. Cuatro etapas, ninguna sabe en qué nube está.
jobs/          La constitución de cada job en JSON, forma nativa de la Jobs API.
resources/     GENERADO desde jobs/. Está en .gitignore.
scripts/       El generador y los validadores que corren en CI.
databricks.yml El bundle. Aquí — y solo aquí — se nombran las dos nubes.
```

### Por qué los jobs están en JSON y no en el YAML del bundle

Databricks Asset Bundles solo acepta YAML en su sección `include`:

```
Error: Files in the 'include' configuration section must be YAML files.
```

Pero la definición nativa de un job es JSON: es lo que devuelve
`databricks jobs get` y lo que consume la Jobs API. Manteniendo los jobs en
JSON, **lo versionado es exactamente el contrato de la API** — se puede
exportar un job existente del workspace y versionarlo sin traducirlo a mano.

`scripts/build_resources.py` es el puente: envuelve cada JSON en la estructura
`resources.jobs.<clave>` que el bundle espera. Corre en CI antes de cada
deploy. La fuente de verdad son los JSON; `resources/` es artefacto.

---

## El pipeline

| Etapa | Notebook | Qué hace |
|---|---|---|
| **00** | `00_generar_datos_sinteticos.py` | Reproduce las 2 fuentes de D1 con sus patologías: cédulas mal digitadas en caja, valores centinela, correos distintos por canal, y ~35% de personas comprando en ambos canales |
| **01** | `01_silver_identidad.py` | Normaliza los 5 atributos y tipifica en válida / no identificado / inválida. Ningún registro se descarta |
| **02** | `02_vault_golden_record.py` | Bloqueo, motor de 12 reglas, componentes conectados, Golden Record con llave estable |
| **03** | `03_gold_customer360.py` | Customer 360, cobertura, 8 features punto-en-tiempo y las métricas certificadas |

El job `99_pipeline_completo` los encadena. Es el que se reejecuta en GCP para
producir el Golden Record que se compara contra el de Azure.

### Las tres decisiones que hay que poder defender

1. **Bloqueo antes de comparar.** Con 25 M de identidades, comparar todos
   contra todos son 3·10¹⁴ pares. Solo se comparan registros que comparten
   alguna llave de bloqueo.
2. **Las reglas viven fuera del código**, en `jobs/reglas_identidad.json`.
   Desactivar una y reejecutar no requiere desplegar.
3. **La llave de cliente es un hash determinista**, no un autoincremental. Por
   eso sobrevive a un reproceso y a un cambio de nube — que es lo que hace
   posible comparar los dos entornos.

---

## Promoción

```
develop  ──push──>  target dev  ──>  Azure Databricks
   │
   └── merge ──>  main  ──push──>  target gcp  ──>  GCP Databricks
```

Promover el POC de Azure a GCP no es un proyecto de migración: es un merge de
`develop` a `main`. El comando es idéntico, solo cambia el target:

```bash
databricks bundle deploy -t dev
databricks bundle deploy -t gcp
```

En un proyecto normal estos targets serían `dev` / `qa` / `prd`. Aquí
representan las dos nubes, porque el POC tiene un único ambiente por nube.

---

## Correr en local

```bash
python scripts/build_resources.py     # jobs/*.json -> resources/*.yml
python scripts/validar_reglas.py      # valida el catálogo de identidad
bash scripts/verificar_portabilidad.sh
databricks bundle validate -t dev
```

Sin un workspace configurado, `bundle validate` resuelve todo el YAML y falla
al autenticarse. Eso es lo esperado.

### Windows

`python` en el PATH suele resolver al stub del Microsoft Store. Invoca el
intérprete real por ruta completa si los scripts no arrancan.

---

## Lo que el CI verifica

`scripts/build_resources.py --check` no solo convierte: **valida**. Y los
validadores están probados en negativo — cada uno de estos casos corta el
build:

| Caso | Quién lo atrapa |
|---|---|
| Job que depende de una tarea que no existe | `build_resources` |
| Job que usa un `job_cluster_key` no declarado | `build_resources` |
| Notebook referenciado que no está en el repo | `build_resources` |
| Campo que la Jobs API no reconoce | `build_resources` |
| Dos reglas con el mismo `rank` | `validar_reglas` |
| Condición que usa una columna inexistente | `validar_reglas` |
| Umbral fuera de `[0,1]` | `validar_reglas` |
| Una ruta `abfss://` dentro de un notebook | `verificar_portabilidad` |

El caso del `rank` duplicado merece explicación: si dos reglas comparten rank,
cuál gana depende del orden de las filas, y Spark no lo garantiza. El Golden
Record dejaría de ser reproducible entre corridas — y sin reproducibilidad, la
comparación entre nubes no significa nada.

---

## Secrets que espera GitHub Actions

| Secret | Para qué |
|---|---|
| `DATABRICKS_HOST_AZURE` | URL del workspace de Azure |
| `DATABRICKS_HOST_GCP` | URL del workspace de GCP |
| `DATABRICKS_CLIENT_ID` | Service principal |
| `DATABRICKS_CLIENT_SECRET` | Service principal |

Los hosts no se versionan: en `databricks.yml` van como placeholders.
