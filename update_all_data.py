import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import logging
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("MasterETL")

DATA_FILES = [
    "data/eu/bienes_agregado.csv",
    "data/eu/comercio_socios.csv",
    "data/us/bienes_agregado.csv",
    "data/us/comercio_socios.csv",
    "data/uk/bienes_agregado.csv",
    "data/uk/comercio_socios.csv",
    "data/jp/bienes_agregado.csv",
    "data/jp/comercio_socios.csv",
    "data/ca/bienes_agregado.csv",
    "data/ca/comercio_socios.csv",
    "data/exchange_rates.csv",
]


@dataclass
class ETLJob:
    script: str
    description: str
    optional: bool = False


class ETLOrchestrator:
    """ETL orchestrator with support for parallel execution.

    Manages the execution of multiple ETL jobs with configurable concurrency,
    tracks results and timings, and provides summary reporting.
    """

    def __init__(self, force: bool = False, max_concurrent: int = 4) -> None:
        """Initialize the ETL Orchestrator.

        Args:
            force: If True, force update by ignoring cache.
            max_concurrent: Maximum number of concurrent ETL jobs to run.
        """
        self.force = force
        self.max_concurrent = max_concurrent
        self.results: Dict[str, bool] = {}
        self.timings: Dict[str, float] = {}

    async def run_etl(self, job: ETLJob) -> bool:
        """Execute a single ETL job asynchronously.

        Args:
            job: ETLJob object containing script path, description, and options.

        Returns:
            True if the job completed successfully or is optional and failed,
            False if a critical job failed.
        """
        script_path = Path(job.script)
        if not script_path.exists():
            if job.optional:
                logger.info(f"⊘ {job.description}: SKIPPED (script not found)")
                return True
            else:
                logger.error(f"✗ {job.description}: Script not found: {job.script}")
                return False

        logger.info(f"{'=' * 80}")
        logger.info(f"▶ {job.description}")
        logger.info(f"{'=' * 80}")

        start_time = asyncio.get_event_loop().time()
        cmd = ["python3", "-u", str(script_path)]
        if self.force:
            cmd.append("--force")

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            if process.stdout:
                async for line in process.stdout:
                    print(line.decode().rstrip())

            returncode = await process.wait()

            elapsed = asyncio.get_event_loop().time() - start_time
            self.timings[job.description] = elapsed

            if returncode == 0:
                logger.info(f"✓ {job.description} completed in {elapsed:.1f}s")
                return True
            else:
                if job.optional:
                    logger.warning(f"⚠ {job.description} failed (optional, continuing)")
                    return True
                else:
                    logger.error(f"✗ {job.description} failed (exit code {returncode})")
                    return False

        except Exception as e:
            elapsed = asyncio.get_event_loop().time() - start_time
            self.timings[job.description] = elapsed
            logger.exception(f"✗ {job.description} error: {e}")
            return job.optional

    async def run_parallel(self, jobs: List[ETLJob]) -> Dict[str, bool]:
        """Execute multiple ETL jobs in parallel with concurrency limit.

        Args:
            jobs: List of ETLJob objects to execute in parallel.

        Returns:
            Dictionary mapping job descriptions to their success status (True/False).
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def run_with_semaphore(job: ETLJob):
            async with semaphore:
                return job.description, await self.run_etl(job)

        tasks = [run_with_semaphore(job) for job in jobs]
        results = await asyncio.gather(*tasks)

        return dict(results)

    async def run_sequential(self, jobs: List[ETLJob]) -> Dict[str, bool]:
        """Execute ETL jobs sequentially (for handling dependencies).

        Args:
            jobs: List of ETLJob objects to execute sequentially.

        Returns:
            Dictionary mapping job descriptions to their success status (True/False).
            Stops on first critical failure.
        """
        results = {}

        for job in jobs:
            success = await self.run_etl(job)
            results[job.description] = success

            if not success and not job.optional:
                logger.error(f"Critical ETL failed: {job.description}, stopping")
                break

        return results

    def print_summary(self) -> None:
        """Print execution summary with timing and status information.

        Displays total jobs, successful/failed counts, timing breakdown,
        and details of any failed jobs.
        """
        logger.info(f"\n{'=' * 80}")
        logger.info("EXECUTION SUMMARY")
        logger.info(f"{'=' * 80}")

        total = len(self.results)
        success = sum(1 for v in self.results.values() if v)
        failed = total - success

        logger.info(f"Total jobs: {total}")
        logger.info(f"Successful: {success}")
        logger.info(f"Failed: {failed}")

        if self.timings:
            total_time = sum(self.timings.values())
            logger.info(f"Total time: {total_time:.1f}s ({total_time / 60:.1f}min)")

            logger.info("\nTiming breakdown:")
            for job, elapsed in sorted(
                self.timings.items(), key=lambda x: x[1], reverse=True
            ):
                logger.info(f"  {elapsed:6.1f}s - {job}")

        if failed > 0:
            logger.info("\nFailed jobs:")
            for job, success in self.results.items():
                if not success:
                    logger.info(f"  ✗ {job}")

        logger.info(f"{'=' * 80}\n")


async def run_etl_pipeline(args: argparse.Namespace) -> bool:
    """Execute the main ETL pipeline with optimized parallel execution.

    Args:
        args: Parsed command-line arguments containing force, eu_only, non_eu,
            max_concurrent, and skip_integration flags.

    Returns:
        True if all critical jobs completed successfully, False otherwise.
    """
    orchestrator = ETLOrchestrator(force=args.force, max_concurrent=args.max_concurrent)

    phase1_jobs = []

    # Eurostat (EU)
    if not args.non_eu:
        phase1_jobs.append(
            ETLJob(
                script="etl/etl_data.py",
                description="Eurostat (DE, ES, FR, IT)",
            )
        )

    if not args.eu_only:
        phase1_jobs.append(
            ETLJob(
                script="etl/etl_currency.py",
                description="ECB Exchange Rates",
            )
        )

        phase1_jobs.extend(
            [
                ETLJob(
                    script="etl/etl_us.py",
                    description="US Census Bureau",
                    optional=True,
                ),
                ETLJob(
                    script="etl/etl_uk.py",
                    description="UK HMRC",
                    optional=True,
                ),
                ETLJob(
                    script="etl/etl_japan.py",
                    description="Japan e-Stat",
                    optional=True,
                ),
                ETLJob(
                    script="etl/etl_canada.py",
                    description="Canada StatCan",
                    optional=True,
                ),
            ]
        )

    if phase1_jobs:
        logger.info(f"\n{'#' * 80}")
        logger.info(
            f"# PHASE 1: Downloading data sources ({len(phase1_jobs)} jobs in parallel)"
        )
        logger.info(f"# Max concurrent: {orchestrator.max_concurrent}")
        logger.info(f"{'#' * 80}\n")

        phase1_results = await orchestrator.run_parallel(phase1_jobs)
        orchestrator.results.update(phase1_results)

        critical_failed = any(
            not success
            for job, success in phase1_results.items()
            if not any(j.optional for j in phase1_jobs if j.description == job)
        )

        if critical_failed:
            logger.error("Critical ETL failed in Phase 1, aborting")
            return False

    orchestrator.print_summary()
    print_file_summary()
    all_success = all(orchestrator.results.values())
    return all_success


def print_file_summary() -> None:
    """Print summary of generated data files with sizes.

    Checks for existence of expected data files in the data/ directory
    and reports their sizes or missing status.
    """
    logger.info("Generated files:")

    for file_path_str in DATA_FILES:
        file_path = Path(file_path_str)
        if file_path.exists():
            size_mb = file_path.stat().st_size / 1024 / 1024
            logger.info(f"  ✓ {file_path_str}: {size_mb:.2f} MB")
        else:
            logger.info(f"  ✗ {file_path_str}: NOT FOUND")


def clean_cache_files(args: argparse.Namespace) -> None:
    """Clean cache files if --force flag is used.

    Args:
        args: Parsed command-line arguments. Only deletes files if args.force is True.
    """
    if not args.force:
        return

    logger.info("\nCleaning cache files...")

    for file_name in DATA_FILES:
        file_path = Path(file_name)
        if file_path.exists():
            file_path.unlink()
            logger.info(f"  Deleted: {file_name}")

    logger.info("")


def main() -> None:
    """Main entry point for the ETL master script.

    Parses command-line arguments, cleans cache if needed, executes the ETL pipeline,
    and reports final status. Exits with code 0 on success, 1 on partial failure,
    or 130 on keyboard interrupt.
    """
    parser = argparse.ArgumentParser(
        description="Update Widget Trade Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 update_all_data.py                    # Update all
  python3 update_all_data.py --force            # Force update
  python3 update_all_data.py --eu-only          # Only Eurostat
  python3 update_all_data.py --max-concurrent 6 # More parallelism
        """,
    )

    parser.add_argument(
        "--force", action="store_true", help="Force update by deleting cache"
    )
    parser.add_argument(
        "--eu-only", action="store_true", help="Only update Eurostat data"
    )
    parser.add_argument(
        "--non-eu", action="store_true", help="Only update non-EU data (US, UK, JP, CA)"
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=4,
        help="Maximum concurrent ETL jobs (default: 4)",
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("ETL MASTER - WIDGET TRADE BALANCE")
    logger.info("=" * 80)

    clean_cache_files(args)
    start_time = datetime.now()

    try:
        success = asyncio.run(run_etl_pipeline(args))
    except KeyboardInterrupt:
        logger.warning("\n⚠ Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

    elapsed = (datetime.now() - start_time).total_seconds()

    logger.info("=" * 80)
    if success:
        logger.info(f"✓ UPDATE COMPLETE ({elapsed:.1f}s / {elapsed / 60:.1f}min)")
        logger.info("\nRun widget with: streamlit run widget_meteoconomics.py")
        sys.exit(0)
    else:
        logger.info(f"⚠ UPDATE PARTIAL ({elapsed:.1f}s / {elapsed / 60:.1f}min)")
        logger.info("Some ETLs failed. Check logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
