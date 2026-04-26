import logging
import math
import random
import uuid
from datetime import datetime, timezone

from coolname import generate_slug

log = logging.getLogger(__name__)


def make_job_id(workload_type, rng):
    prefix = "svc" if workload_type == "service" else "batch"
    slug = generate_slug(2)
    suffix = uuid.UUID(int=rng.getrandbits(128)).hex[:6]
    return f"{prefix}-{slug}-{suffix}"


def build_service_job(rng):
    cpu = max(1, int(rng.lognormvariate(1.92, 0.793)
              * rng.uniform(1.5, 3.0) * 10))
    mem = rng.choice((64, 128, 256))
    duration = max(1.0, rng.lognormvariate(math.log(5.0) - 0.18, 0.6))
    return {
        "job_id":             make_job_id("service", rng),
        "workload_type":      "service",
        "cpu_request_millis": cpu,
        "mem_request_mib":    mem,
        "duration_sec":       round(duration, 3),
        "generated_at":       datetime.now(timezone.utc).isoformat(),
    }


def build_batch_job(rng):
    cpu_units = rng.lognormvariate(4.08, 0.301)
    cpu = int(max(40, min(8000, cpu_units * 10)))
    mem_gb = rng.lognormvariate(-5.05, 0.464)
    mem = int(max(4, min(512, mem_gb * 1024)))
    duration = max(1.0, min(120.0, rng.lognormvariate(3.46, 1.73)))
    parallelism = max(1, min(20, int(rng.lognormvariate(1.5, 0.8))))
    return {
        "job_id":             make_job_id("batch", rng),
        "workload_type":      "batch",
        "cpu_request_millis": cpu,
        "mem_request_mib":    mem,
        "duration_sec":       round(duration, 3),
        "parallelism":        parallelism,
        "generated_at":       datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    rng = random.Random(42)
    print("--- 5 service jobs ---")
    for _ in range(5):
        print(build_service_job(rng))
    print("\n--- 5 batch jobs ---")
    for _ in range(5):
        print(build_batch_job(rng))
