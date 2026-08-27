#!/usr/bin/env python3
"""
Convierte jobs/*.json en resources/*.yml para que el bundle los consuma.

¿Por qué existe este paso?

Databricks Asset Bundles solo acepta YAML en su seccion `include`:

    Error: Files in the 'include' configuration section must be YAML files.

Pero la definicion nativa de un job — la que devuelve `databricks jobs get` y
la que consume la Jobs API — es JSON. Mantener los jobs en JSON significa que
lo versionado es exactamente el contrato de la API: se puede exportar un job
existente del workspace y versionarlo sin traducirlo a mano, y se puede
comparar contra lo desplegado sin ambiguedad de formato.

Este script es el puente. Toma cada `jobs/<nombre>.json` con la forma de la
Jobs API y lo envuelve en la estructura que el bundle espera:

    { "name": ..., "tasks": [...] }
              |
              v
    resources:
      jobs:
        <nombre>:
          name: ...
          tasks: [...]

`resources/` es artefacto generado y esta en .gitignore. La fuente de verdad
son los JSON.

Uso:
    python scripts/build_resources.py            # genera
    python scripts/build_resources.py --check    # falla si algo esta mal
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DIR_JOBS = RAIZ / "jobs"
DIR_RESOURCES = RAIZ / "resources"

# Archivos de jobs/ que NO son definiciones de job.
NO_SON_JOBS = {"reglas_identidad.json"}

# Campos que un job de la Jobs API puede traer y el bundle acepta.
CAMPOS_VALIDOS = {
    "name", "description", "tags", "tasks", "job_clusters", "schedule",
    "email_notifications", "webhook_notifications", "notification_settings",
    "timeout_seconds", "max_concurrent_runs", "parameters", "queue",
    "run_as", "health", "trigger", "continuous", "environments", "edit_mode",
    "deployment", "git_source", "access_control_list", "budget_policy_id",
    "performance_target",
}


class ErrorDeJob(Exception):
    pass


def escapar(valor: str) -> str:
    """Cita una cadena para YAML sin romper las referencias ${var.x}."""
    return json.dumps(valor, ensure_ascii=False)


def a_yaml(dato, sangria: int = 0) -> list[str]:
    """Serializa a YAML en bloque. Deliberadamente pequeno: no queremos una
    dependencia de PyYAML solo para esto, y el subconjunto que necesitamos
    (dicts, listas, escalares) cabe aqui."""
    pad = "  " * sangria
    lineas: list[str] = []

    if isinstance(dato, dict):
        for clave, valor in dato.items():
            if isinstance(valor, (dict, list)) and valor:
                lineas.append(f"{pad}{clave}:")
                lineas.extend(a_yaml(valor, sangria + 1))
            elif isinstance(valor, (dict, list)):
                lineas.append(f"{pad}{clave}: {{}}" if isinstance(valor, dict) else f"{pad}{clave}: []")
            else:
                lineas.append(f"{pad}{clave}: {escalar(valor)}")

    elif isinstance(dato, list):
        for item in dato:
            if isinstance(item, dict):
                sub = a_yaml(item, sangria + 1)
                # El primer campo va en la misma linea que el guion.
                primera = sub[0].lstrip()
                lineas.append(f"{pad}- {primera}")
                lineas.extend(sub[1:])
            else:
                lineas.append(f"{pad}- {escalar(item)}")

    return lineas


def escalar(valor) -> str:
    if valor is None:
        return "null"
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, (int, float)):
        return str(valor)
    return escapar(str(valor))


def validar(nombre: str, definicion: dict) -> None:
    if not isinstance(definicion, dict):
        raise ErrorDeJob(f"{nombre}: la raiz debe ser un objeto JSON.")

    if "name" not in definicion:
        raise ErrorDeJob(f"{nombre}: falta el campo obligatorio 'name'.")

    tareas = definicion.get("tasks")
    if not tareas:
        raise ErrorDeJob(f"{nombre}: un job necesita al menos una tarea en 'tasks'.")

    claves_tarea = {t.get("task_key") for t in tareas}
    if None in claves_tarea:
        raise ErrorDeJob(f"{nombre}: hay tareas sin 'task_key'.")

    # Toda dependencia debe apuntar a una tarea que exista en el mismo job.
    for tarea in tareas:
        for dep in tarea.get("depends_on", []):
            objetivo = dep.get("task_key")
            if objetivo not in claves_tarea:
                raise ErrorDeJob(
                    f"{nombre}: la tarea '{tarea['task_key']}' depende de "
                    f"'{objetivo}', que no existe en este job.")

    # Todo job_cluster_key referenciado debe estar declarado.
    declarados = {c.get("job_cluster_key") for c in definicion.get("job_clusters", [])}
    for tarea in tareas:
        usado = tarea.get("job_cluster_key")
        if usado and usado not in declarados:
            raise ErrorDeJob(
                f"{nombre}: la tarea '{tarea['task_key']}' usa el cluster "
                f"'{usado}', que no esta declarado en 'job_clusters'.")

    # Los notebooks referenciados tienen que existir en el repo.
    for tarea in tareas:
        nb = tarea.get("notebook_task", {}).get("notebook_path")
        if nb and not nb.startswith("${"):
            destino = (DIR_JOBS / nb).resolve()
            if not destino.exists():
                raise ErrorDeJob(
                    f"{nombre}: la tarea '{tarea['task_key']}' apunta a "
                    f"'{nb}', que no existe en el repo.")

    desconocidos = set(definicion) - CAMPOS_VALIDOS - {"_comentario"}
    if desconocidos:
        raise ErrorDeJob(
            f"{nombre}: campos no reconocidos por la Jobs API: "
            f"{', '.join(sorted(desconocidos))}")


def construir() -> tuple[int, list[str]]:
    DIR_RESOURCES.mkdir(exist_ok=True)

    # Limpiar lo generado antes, para que un job borrado no sobreviva.
    for viejo in DIR_RESOURCES.glob("*.yml"):
        viejo.unlink()

    archivos = sorted(p for p in DIR_JOBS.glob("*.json") if p.name not in NO_SON_JOBS)
    if not archivos:
        raise ErrorDeJob("No hay definiciones de job en jobs/.")

    errores: list[str] = []
    generados = 0

    for archivo in archivos:
        clave = archivo.stem
        try:
            definicion = json.loads(archivo.read_text(encoding="utf-8"))
            definicion.pop("_comentario", None)
            validar(archivo.name, definicion)
        except json.JSONDecodeError as exc:
            errores.append(f"{archivo.name}: JSON invalido — {exc}")
            continue
        except ErrorDeJob as exc:
            errores.append(str(exc))
            continue

        cuerpo = a_yaml({"resources": {"jobs": {clave: definicion}}})
        cabecera = [
            "# ARCHIVO GENERADO — no editar a mano.",
            f"# Fuente: jobs/{archivo.name}",
            "# Regenerar con: python scripts/build_resources.py",
            "",
        ]
        (DIR_RESOURCES / f"{clave}.yml").write_text(
            "\n".join(cabecera + cuerpo) + "\n", encoding="utf-8")
        generados += 1
        print(f"  jobs/{archivo.name}  ->  resources/{clave}.yml")

    return generados, errores


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="Solo validar; sale con 1 si hay errores.")
    args = ap.parse_args()

    print("Construyendo recursos del bundle desde jobs/*.json\n")
    try:
        generados, errores = construir()
    except ErrorDeJob as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1

    if errores:
        print("\nDefiniciones de job invalidas:\n", file=sys.stderr)
        for e in errores:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print(f"\n{generados} job(s) listos para el bundle.")
    if args.check:
        print("Validacion OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
