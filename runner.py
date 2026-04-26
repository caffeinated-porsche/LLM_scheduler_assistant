import argparse
import asyncio
import logging
import random
from pathlib import Path

import driver
import generator


async def run_stream(rate, build_fn, rng, experiment_id, namespace, core_v1, batch_v1, state):
    while state["submitted"] < state["total"]:
        await asyncio.sleep(rng.expovariate(rate))
        if state["submitted"] >= state["total"]:
            break
        job = build_fn(rng)
        state["submitted"] += 1
        try:
            await driver.submit_job(job, experiment_id, namespace, core_v1, batch_v1)
        except Exception as e:
            logging.warning("submit failed for %s: %s", job["job_id"], e)
        if state["submitted"] % 50 == 0:
            logging.info("submitted %d / %d",
                         state["submitted"], state["total"])


async def main(args):
    log_dir = Path("logs") / args.experiment_id
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "run.log"),
            logging.StreamHandler(),
        ],
    )
    logging.info("starting experiment=%s total_jobs=%d batch_rate=%.4f service_rate=%.4f",
                 args.experiment_id, args.total_jobs, args.batch_rate, args.service_rate)

    core_v1, batch_v1 = driver.init()
    rng_svc = random.Random(args.seed)
    rng_batch = random.Random(args.seed + 1)
    state = {"submitted": 0, "total": args.total_jobs}

    streams = []
    if args.service_rate > 0:
        streams.append(run_stream(args.service_rate, generator.build_service_job, rng_svc,
                                  args.experiment_id, args.namespace, core_v1, batch_v1, state))
    if args.batch_rate > 0:
        streams.append(run_stream(args.batch_rate, generator.build_batch_job, rng_batch,
                                  args.experiment_id, args.namespace, core_v1, batch_v1, state))
    await asyncio.gather(*streams)

    logging.info("submissions complete, draining cluster")
    await driver.wait_for_drain(core_v1, batch_v1, args.experiment_id, args.namespace)
    logging.info("collecting outcomes")
    driver.collect_outcomes(core_v1, args.experiment_id, args.namespace,
                            str(log_dir / "outcomes.jsonl"))
    logging.info("done: %d jobs submitted", state["submitted"])


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--total-jobs", type=int, default=50)
    p.add_argument("--batch-rate", type=float, required=True)
    p.add_argument("--service-rate", type=float, required=True)
    p.add_argument("--namespace", default="default")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    asyncio.run(main(args))
