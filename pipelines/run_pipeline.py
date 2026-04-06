import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv


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


def write_run_report(
    report_dir: Path,
    run_id: str,
    status: str,
    started_at: str,
    finished_at: str,
    total_duration_seconds: float,
    stages: list[dict],
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"run_{run_id}.json"
    payload = {
        "run_id": run_id,
        "status": status,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "total_duration_seconds": round(total_duration_seconds, 3),
        "stages": stages,
    }
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return report_path


def main() -> int:
    load_dotenv()
    args = parse_args()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    project_root = Path(__file__).resolve().parents[1]
    report_dir = Path(os.getenv("RUN_REPORT_PATH", "storage/runs"))
    run_started_at = datetime.now(timezone.utc).isoformat()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("pipelines.run_pipeline")

    logger.info("Pipeline iniciado | run_id=%s", run_id)
    logger.info("Workspace: %s", project_root)

    stages = build_stages(skip_ingestion=args.skip_ingestion, skip_analysis=args.skip_analysis)
    enabled_stages = [stage for stage in stages if stage.enabled]
    logger.info("Etapas habilitadas: %s", [stage.name for stage in enabled_stages])

    total_started = time.perf_counter()
    stage_results: list[dict] = []

    for stage in stages:
        if not stage.enabled:
            logger.info("Etapa ignorada: %s", stage.name)
            stage_results.append(
                {
                    "name": stage.name,
                    "script": stage.script,
                    "status": "skipped",
                    "exit_code": None,
                    "duration_seconds": 0.0,
                }
            )
            continue

        logger.info("Executando etapa: %s", stage.name)
        try:
            return_code, elapsed = run_stage(project_root, stage)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Falha inesperada na etapa %s: %s", stage.name, exc)
            elapsed = 0.0
            stage_results.append(
                {
                    "name": stage.name,
                    "script": stage.script,
                    "status": "failed_exception",
                    "exit_code": 1,
                    "duration_seconds": round(elapsed, 3),
                }
            )
            total_elapsed = time.perf_counter() - total_started
            report_path = write_run_report(
                report_dir=project_root / report_dir,
                run_id=run_id,
                status="failed",
                started_at=run_started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
                total_duration_seconds=total_elapsed,
                stages=stage_results,
            )
            logger.error("Run report salvo em: %s", report_path)
            return 1

        if return_code != 0:
            logger.error(
                "Etapa %s falhou | exit_code=%d | duracao=%.2fs",
                stage.name,
                return_code,
                elapsed,
            )
            stage_results.append(
                {
                    "name": stage.name,
                    "script": stage.script,
                    "status": "failed",
                    "exit_code": return_code,
                    "duration_seconds": round(elapsed, 3),
                }
            )
            total_elapsed = time.perf_counter() - total_started
            report_path = write_run_report(
                report_dir=project_root / report_dir,
                run_id=run_id,
                status="failed",
                started_at=run_started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
                total_duration_seconds=total_elapsed,
                stages=stage_results,
            )
            logger.error("Run report salvo em: %s", report_path)
            return return_code

        stage_results.append(
            {
                "name": stage.name,
                "script": stage.script,
                "status": "success",
                "exit_code": return_code,
                "duration_seconds": round(elapsed, 3),
            }
        )
        logger.info("Etapa %s concluida | duracao=%.2fs", stage.name, elapsed)

    total_elapsed = time.perf_counter() - total_started
    logger.info("Pipeline concluido com sucesso | run_id=%s | duracao_total=%.2fs", run_id, total_elapsed)
    report_path = write_run_report(
        report_dir=project_root / report_dir,
        run_id=run_id,
        status="success",
        started_at=run_started_at,
        finished_at=datetime.now(timezone.utc).isoformat(),
        total_duration_seconds=total_elapsed,
        stages=stage_results,
    )
    logger.info("Run report salvo em: %s", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
