#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# RNF-017 · Portabilidad entre nubes, como test ejecutable.
#
#   "El código y el modelo de datos no deben depender de servicios propietarios
#    de Azure, de modo que la migración a GCP Databricks se resuelva recreando
#    la configuración de plataforma y no reescribiendo lógica."
#
# La tesis NO es "nada sabe en qué nube está" — eso sería falso y cualquier
# ingeniero lo detectaría. La tesis es que ese conocimiento está CONFINADO al
# bloque `targets` de databricks.yml, y AUSENTE de todo lo que se migra:
# notebooks, definiciones de jobs y catálogo de reglas (RF-037).
#
# Si alguien escribe abfss:// dentro de un notebook, este script falla y el
# build se cae. La portabilidad deja de ser una intención.
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/.."

PATRONES='abfss://|wasbs://|gs://|s3://|\.dfs\.core\.windows\.net|blob\.core\.windows\.net|storage\.googleapis\.com|azuredatabricks\.net|gcp\.databricks\.com|AZURE_|GOOGLE_APPLICATION'

LOGICA="notebooks jobs"

echo
echo "== RNF-017 · Portabilidad entre nubes ==============================="
echo
echo "1) LA LÓGICA — notebooks, definiciones de jobs y catálogo de reglas"
echo "   Es lo que RF-037 define como migrable. No puede saber de nubes."
echo

if grep -rInE "$PATRONES" $LOGICA 2>/dev/null; then
  echo
  echo "   FALLA: cada línea de arriba habría que reescribirla en GCP."
  exit 1
fi

archivos=$(find $LOGICA -type f \( -name '*.py' -o -name '*.json' \) | wc -l)
lineas=$(find $LOGICA -type f \( -name '*.py' -o -name '*.json' \) -exec cat {} + | wc -l)
printf "   OK: 0 referencias de nube en %s archivos / %s líneas.\n" \
  "$(echo "$archivos")" "$(echo "$lineas")"

echo
echo "2) LA CONFIGURACIÓN — databricks.yml"
echo "   Aquí SÍ se nombran las dos nubes. Es el propósito del archivo."
echo
grep -nE "$PATRONES" databricks.yml | grep -v '^\s*[0-9]*:\s*#' | sed 's/^/     /'
n_config=$(grep -cE "$PATRONES" databricks.yml)
echo
printf "   %s líneas conocen la nube. Son declaraciones, no lógica.\n" "$n_config"

echo
echo "   Migrar = cambiar el target. No = reescribir la lógica."
echo "====================================================================="
echo
