import asyncio
import argparse
import logging
from datetime import datetime, timezone
from kubernetes import client, config
import time
import json
from pathlib import Path

# TODO: import job generator once implemented
# from generator import generate_jobs

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--namespace", type=str, default="default")
    p.add_argument("--experiment-id", type=str, required=True)
    return p.parse_args()


def build_pod_spec(job, experiment_id, namespace):
    labels = {"experiment": experiment_id, "job-id": job["job_id"]}
    annotations = {
        "declared-duration-sec": str(job["duration_sec"]),
        "submit-time": job["submit_time"],
    }
    resources = client.V1ResourceRequirements(
        requests={
            "cpu": f"{job['cpu_request_millis']}m",
            "memory": f"{job['mem_request_mib']}Mi",
        },
        limits={
            "cpu": f"{job['cpu_request_millis']}m",
            "memory": f"{job['mem_request_mib']}Mi",
        },
    )
    container = client.V1Container(
        name="worker",
        image="busybox:1.36",
        command=["sleep"],
        args=[str(int(job["duration_sec"]))],  # k8s args must be strings
        resources=resources,
    )
    return client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=job["job_id"],
            namespace=namespace,
            labels=labels,
            annotations=annotations,
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[container],
        ),
    )


def build_job_spec(job, experiment_id, namespace):
    labels = {"experiment": experiment_id, "job-id": job["job_id"]}
    annotations = {
        "declared-duration-sec": str(job["duration_sec"]),
        "submit-time": job["submit_time"],
    }
    resources = client.V1ResourceRequirements(
        requests={
            "cpu": f"{job['cpu_request_millis']}m",
            "memory": f"{job['mem_request_mib']}Mi",
        },
        limits={
            "cpu": f"{job['cpu_request_millis']}m",
            "memory": f"{job['mem_request_mib']}Mi",
        },
    )
    container = client.V1Container(
        name="worker",
        image="busybox:1.36",
        command=["sleep"],
        args=[str(int(job["duration_sec"]))],
        resources=resources,
    )
    pod_template = client.V1PodTemplateSpec(
        # namespace not set on pod template
        metadata=client.V1ObjectMeta(labels=labels, annotations=annotations),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[container],
        ),
    )
    job_spec = client.V1JobSpec(
        parallelism=job["parallelism"],
        completions=job["parallelism"],
        backoff_limit=0,
        ttl_seconds_after_finished=3600,
        template=pod_template,
    )
    return client.V1Job(
        metadata=client.V1ObjectMeta(
            name=job["job_id"],
            namespace=namespace,
            labels=labels,
            annotations=annotations,
        ),
        spec=job_spec,
    )


def init(namespace="default"):
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    batch_v1 = client.BatchV1Api()
    return core_v1, batch_v1


async def submit_job(job, experiment_id, namespace, core_v1, batch_v1):
    # stamped here so spec and log agree
    job["submit_time"] = datetime.now(timezone.utc).isoformat()
    try:
        if job["workload_type"] == "service":
            pod = build_pod_spec(job, experiment_id, namespace)
            core_v1.create_namespaced_pod(namespace=namespace, body=pod)
        else:
            job_obj = build_job_spec(job, experiment_id, namespace)
            batch_v1.create_namespaced_job(namespace=namespace, body=job_obj)
    except Exception as e:
        log.warning("error submitting job %s: %s",
                    job.get("job_id", "?"), e)


async def wait_for_drain(core_v1, batch_v1, experiment_id, namespace, timeout=3600):
    """Block until all pods for this experiment are terminal."""
    start = time.time()
    while time.time() - start < timeout:
        pods = core_v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"experiment={experiment_id}",
        ).items
        active = [p for p in pods if p.status.phase in ("Pending", "Running")]
        if not active:
            return
        logging.info("draining: %d pods still active", len(active))
        await asyncio.sleep(10)
    logging.warning("drain timeout reached, %d pods still active", len(active))


def collect_outcomes(core_v1, experiment_id, namespace, output_path):
    """Dump per-pod outcomes as JSONL."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pods = core_v1.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"experiment={experiment_id}",
    ).items
    with open(output_path, "w") as f:
        for pod in pods:
            record = {
                "pod_name": pod.metadata.name,
                "labels": pod.metadata.labels,
                "annotations": pod.metadata.annotations or {},
                "node_name": pod.spec.node_name,
                "phase": pod.status.phase,
                "creation_timestamp": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None,
                "start_time": pod.status.start_time.isoformat() if pod.status.start_time else None,
                "scheduled_at": None,
                "finished_at": None,
            }
            for cond in (pod.status.conditions or []):
                if cond.type == "PodScheduled" and cond.status == "True":
                    record["scheduled_at"] = cond.last_transition_time.isoformat()
            if pod.status.container_statuses:
                cs = pod.status.container_statuses[0]
                if cs.state and cs.state.terminated and cs.state.terminated.finished_at:
                    record["finished_at"] = cs.state.terminated.finished_at.isoformat()
            f.write(json.dumps(record) + "\n")
    logging.info("wrote %d pod records to %s", len(pods), output_path)
