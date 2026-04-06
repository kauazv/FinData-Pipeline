import argparse
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Stage:
    name: str
    script: str
    enabled: bool = True


def build_stages(skip_ingestion: bool, skip_analysis: bool) -> list[Stage]:
    return [
        Stage("ingestion", "ingestion/collect_market_data.py", enabled=not skip_ingestion),
        Stage("silver_processing", "processing/process_market_data.py"),
        Stage("validation", "validation/validate_market_data.py"),
        Stage("gold_processing", "processing/create_gold_dataset.py"),
        Stage("warehouse_load", "warehouse/load_to_duckdb.py"),
        Stage("analytics_query", "warehouse/query_analysis.py", enabled=not skip_analysis),
    ]


def run_stage(project_root: Path, stage: Stage) -> tuple[int, float]:
    script_path = project_root / stage.script
    if not script_path.exists():
        raise FileNotFoundError(f"Script nao encontrado: {script_path}")

    started = time.perf_counter()
    completed = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        check=False,
    )
    elapsed = time.perf_counter() - started
    return completed.returncode, elapsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Executa o pipeline financeiro fim a fim.")
    parser.add_argument("--skip-ingestion", action="store_true", help="Pula etapa de ingestao.")
    parser.add_argument("--skip-analysis", action="store_true", help="Pula consulta analitica final.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    project_root = Path(__file__).resolve().parents[1]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("pipelines.run_pipeline")

    logger.info("Pipeline iniciado | run_id=%s", run_id)
    logger.info("Workspace: %s", project_root)

    stages = build_stages(skip_ingestion=args.skip_ingestion, skip_analysis=args.skip_analysis)
    enabled_stages = [stage for stage in stages if stage.enabled]
    logger.info("Etapas habilitadas: %s", [stage.name for stage in enabled_stages])

    total_started = time.perf_counter()

    for stage in stages:
        if not stage.enabled:
            logger.info("Etapa ignorada: %s", stage.name)
            continue

        logger.info("Executando etapa: %s", stage.name)
        try:
            return_code, elapsed = run_stage(project_root, stage)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Falha inesperada na etapa %s: %s", stage.name, exc)
            return 1

        if return_code != 0:
            logger.error(
                "Etapa %s falhou | exit_code=%d | duracao=%.2fs",
                stage.name,
                return_code,
                elapsed,
            )
            return return_code

        logger.info("Etapa %s concluida | duracao=%.2fs", stage.name, elapsed)

    total_elapsed = time.perf_counter() - total_started
    logger.info("Pipeline concluido com sucesso | run_id=%s | duracao_total=%.2fs", run_id, total_elapsed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
