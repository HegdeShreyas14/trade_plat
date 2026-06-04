# Sandbox Infrastructure

This directory contains the secure runtime scaffold for evaluating contestant exchange engines.

## Build The Engine Image

Build from the repository root so the Dockerfile can copy `matching-engine/`:

```bash
docker build -f matching-engine/Dockerfile -t contestant-submission:latest .
```

The image uses a multi-stage build. The final runtime runs as UID/GID `10001`, exposes only port `8080`, and contains only the compiled engine binary plus the minimal Alpine runtime.

## Deploy The Sandbox

```bash
kubectl apply -k infra/k8s
```

The Kubernetes manifests provide:

- `restricted` Pod Security admission labels on the `evaluation-sandbox` namespace.
- Non-root execution with `allowPrivilegeEscalation: false`.
- `readOnlyRootFilesystem: true`.
- All Linux capabilities dropped.
- Service account token mounting disabled.
- ClusterIP WebSocket service on port `8080`.
- Default-deny ingress/egress for the contestant pod, with ingress allowed only from pods labeled `tier=telemetry-bot-fleet` and egress allowed only to pods labeled `app=kafka-broker` on port `9092`.

## CPU Pinning Note

The engine currently pins internal hot threads to CPU IDs `1`, `2`, and `3`. Kubernetes CPU limits alone do not guarantee stable physical-core assignment. For deterministic pinning in production, run nodes with the Kubernetes CPU Manager `static` policy and schedule this pod with whole-core CPU requests/limits.

## Run A Full Benchmark

Install the orchestrator dependency:

```bash
pip install -r infra/requirements.txt
```

Build the contestant image, deploy the sandbox, run the bot fleet, print bot logs, and clean up:

```bash
python3 infra/orchestrator.py \
  --sub-id local-test \
  --build-bot-image \
  --duration 30
```

Useful flags:

- `--skip-build` reuses existing local images.
- `--exchange-image contestant-submission:tag` overrides the engine image.
- `--bot-image iicpc-traffic-generator:tag` overrides the traffic generator image.
- `--keep-resources` leaves the deployment and bot pod behind for debugging.

The orchestrator creates or patches the namespace, service, network policy, and contestant deployment through the Kubernetes Python API. It then launches a single ephemeral bot pod labeled `tier=telemetry-bot-fleet`, which is the only label allowed through the exchange ingress policy.
