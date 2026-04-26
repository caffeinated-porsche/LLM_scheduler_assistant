import base64
import json
import logging
from flask import Flask, request, jsonify
from kubernetes import client, config

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Load in-cluster config (since this runs inside a Pod)
try:
    config.load_incluster_config()
    v1 = client.CoreV1Api()
except Exception as e:
    logging.warning(f"Could not load in-cluster config: {e}")
    v1 = None


def get_active_policy_overrides():
    """Fetches the latest spec overrides from the ConfigMap."""
    if not v1: return {}
    try:
        cm = v1.read_namespaced_config_map("llm-scheduler-config", "default")
        overrides_str = cm.data.get("spec_overrides", "{}")
        return json.loads(overrides_str)
    except Exception as e:
        logging.error(f"Error reading ConfigMap: {e}")
        return {}


@app.route('/mutate', methods=['POST'])
def mutate():
    admission_review = request.json
    req = admission_review.get('request', {})

    uid = req.get('uid')
    namespace = req.get('namespace')

    logging.info(f"Received mutation request for UID: {uid} in namespace: {namespace}")

    # 1. Get the current policy dictates from your LLM loop
    overrides = get_active_policy_overrides()

    patch = []

    # 2. Translate the overrides into a JSON Patch
    if "priorityClassName" in overrides:
        patch.append({
            "op": "add",
            "path": "/spec/priorityClassName",
            "value": overrides["priorityClassName"]
        })

    if "affinity" in overrides:
        # Check if affinity already exists, if not add it, otherwise replace
        # For simplicity in this prototype, we'll assume we are adding it
        patch.append({
            "op": "add",
            "path": "/spec/affinity",
            "value": overrides["affinity"]
        })

    if "topologySpreadConstraints" in overrides:
        patch.append({
            "op": "add",
            "path": "/spec/topologySpreadConstraints",
            "value": overrides["topologySpreadConstraints"]
        })

    # 3. Format the response
    patch_b64 = base64.b64encode(json.dumps(patch).encode()).decode() if patch else ""

    response = {
        "uid": uid,
        "allowed": True,
        "patchType": "JSONPatch" if patch else None,
        "patch": patch_b64 if patch else None
    }

    # Remove null fields to keep K8s happy
    response = {k: v for k, v in response.items() if v is not None}

    return jsonify({
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "response": response
    })


if __name__ == '__main__':
    # Must run on HTTPS. These certs will be mounted via the bash script.
    app.run(host='0.0.0.0', port=4443, ssl_context=('/tls/tls.crt', '/tls/tls.key'))