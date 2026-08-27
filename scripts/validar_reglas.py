#!/usr/bin/env python3
"""
Valida el catálogo de reglas de identidad (jobs/reglas_identidad.json).

Las reglas están fuera del código a propósito (RF-013, RNF-011): se activan y
desactivan sin desplegar. Esa flexibilidad tiene un costo — un archivo mal
formado tumba el job de matching en tiempo de ejecución, no de compilación.

Este validador cubre ese hueco. Corre en CI, antes de cualquier despliegue.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

RUTA = Path(__file__).resolve().parent.parent / "jobs" / "reglas_identidad.json"

CAMPOS_OBLIGATORIOS = {"id", "rank", "activa", "descripcion", "condicion", "umbral"}

# Columnas que el DataFrame de pares candidatos expone al motor de reglas.
# Una condición que use otra cosa falla en ejecución.
COLUMNAS_DISPONIBLES = {
    f"{atributo}_{lado}"
    for atributo in ["cedula_norm", "nombre_norm", "correo_norm",
                     "telefono_norm", "fecha_nac"]
    for lado in ["a", "b"]
}

# Funciones Spark SQL permitidas en las condiciones.
FUNCIONES_PERMITIDAS = {
    "levenshtein", "soundex", "split", "length", "lower", "upper", "trim",
    "substring", "concat", "coalesce", "regexp_replace", "abs", "size",
}

PALABRAS_SQL = {
    "and", "or", "not", "is", "null", "in", "like", "rlike", "between",
    "true", "false", "case", "when", "then", "else", "end",
}


def fallar(mensajes: list[str]) -> int:
    print("\nCatálogo de reglas INVÁLIDO:\n", file=sys.stderr)
    for m in mensajes:
        print(f"  - {m}", file=sys.stderr)
    print(file=sys.stderr)
    return 1


def main() -> int:
    if not RUTA.exists():
        return fallar([f"No existe {RUTA}"])

    try:
        catalogo = json.loads(RUTA.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return fallar([f"JSON inválido: {exc}"])

    errores: list[str] = []
    avisos: list[str] = []

    if "reglas" not in catalogo:
        return fallar(["Falta la lista 'reglas'."])

    reglas = catalogo["reglas"]
    vistos_id: set[str] = set()
    vistos_rank: dict[int, str] = {}

    for regla in reglas:
        rid = regla.get("id", "<sin id>")

        faltantes = CAMPOS_OBLIGATORIOS - set(regla)
        if faltantes:
            errores.append(f"{rid}: faltan campos {sorted(faltantes)}")
            continue

        if not re.fullmatch(r"R\d{2}", regla["id"]):
            errores.append(f"{rid}: el id debe tener la forma R01, R02, ...")

        if regla["id"] in vistos_id:
            errores.append(f"{rid}: id duplicado.")
        vistos_id.add(regla["id"])

        # El rank define la precedencia cuando dos reglas resuelven el mismo
        # par. Si dos comparten rank, el resultado depende del orden de las
        # filas — y eso rompe la paridad entre nubes (RF-038).
        rank = regla["rank"]
        if rank in vistos_rank:
            errores.append(
                f"{rid}: rank {rank} duplicado con {vistos_rank[rank]}. "
                f"La precedencia quedaría indeterminada y el Golden Record "
                f"dejaría de ser reproducible entre corridas.")
        vistos_rank[rank] = regla["id"]

        if not isinstance(regla["activa"], bool):
            errores.append(f"{rid}: 'activa' debe ser true o false.")

        umbral = regla["umbral"]
        if not isinstance(umbral, (int, float)) or not 0.0 <= umbral <= 1.0:
            errores.append(f"{rid}: 'umbral' debe estar entre 0.0 y 1.0 (es {umbral!r}).")

        # Toda columna referenciada tiene que existir en el DataFrame de pares.
        condicion = regla["condicion"]
        identificadores = set(re.findall(r"\b[a-z_][a-z0-9_]*\b", condicion.lower()))
        candidatas = {
            i for i in identificadores
            if i not in PALABRAS_SQL
            and i not in FUNCIONES_PERMITIDAS
            and (i.endswith("_a") or i.endswith("_b"))
        }
        desconocidas = candidatas - COLUMNAS_DISPONIBLES
        if desconocidas:
            errores.append(
                f"{rid}: la condición usa columnas que no existen: "
                f"{sorted(desconocidas)}")

        funciones = set(re.findall(r"\b([a-z_]+)\s*\(", condicion.lower()))
        no_permitidas = funciones - FUNCIONES_PERMITIDAS
        if no_permitidas:
            errores.append(
                f"{rid}: funciones no permitidas: {sorted(no_permitidas)}")

        if condicion.count("(") != condicion.count(")"):
            errores.append(f"{rid}: paréntesis desbalanceados en la condición.")

        if not regla["activa"] and "motivo_desactivacion" not in regla:
            avisos.append(
                f"{rid}: está desactivada sin 'motivo_desactivacion'. "
                f"Sin el motivo, nadie sabe si volver a activarla.")

    activas = [r for r in reglas if r.get("activa")]
    if not activas:
        errores.append("No hay ninguna regla activa: el matching no resolvería nada.")

    if errores:
        return fallar(errores)

    print(f"Catálogo {catalogo.get('version', '?')} — vigente desde "
          f"{catalogo.get('vigente_desde', '?')}\n")
    print(f"  {len(reglas)} reglas · {len(activas)} activas · "
          f"{len(reglas) - len(activas)} desactivadas\n")

    for r in sorted(reglas, key=lambda r: r["rank"]):
        estado = "activa " if r["activa"] else "APAGADA"
        print(f"  {r['id']}  rank {r['rank']:<2}  {estado}  "
              f"umbral {r['umbral']:<5}  {r['descripcion']}")

    if avisos:
        print("\nAvisos:")
        for a in avisos:
            print(f"  - {a}")

    print("\nCatálogo válido.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
