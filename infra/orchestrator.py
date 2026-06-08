#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
import sys
import time
import zipfile


class BenchmarkOrchestrator:
    def __init__(self, namespace="evaluation-sandbox", repo_root="."):
        self.namespace = namespace
        self.repo_root = repo_root  
        self.client = None
        self.config = None
        self.ApiException = None
        self.core = None
        self.apps = None
        self.networking = None

    def connect(self):
        try:
            from kubernetes import client, config
            from kubernetes.client.rest import ApiException
        except ImportError as exc:
            raise RuntimeError(
                "kubernetes Python client is not installed. Run: pip install -r infra/requirements.txt"
            ) from exc

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self.client = client
        self.config = config
        self.ApiException = ApiException
        self.core = client.CoreV1Api()
        self.apps = client.AppsV1Api()
        self.networking = client.NetworkingV1Api()

    def auto_detect_and_build(self, unzip_dir, submission_id):
        """
        Scans unzipped code workspaces recursively, dynamically writes the correct language
        wrapper, compiles an unprivileged container, and yields the image tag.
        """
        print(f"[polyglot] Inspecting file structure for submission: {submission_id}")
        
        # Gather all files recursively to bypass nested folder zip structures cleanly
        all_files = []
        for root, dirs, filenames in os.walk(unzip_dir):
            for f in filenames:
                all_files.append(f)
        
        # 1. Polyglot Template Mapping Engine Logic (Using all_files instead of files)
        if "Cargo.toml" in all_files or any(f.endswith(".rs") for f in all_files):
            detected_lang = "rust"
            dockerfile_content = """
            FROM rust:1.78-alpine AS builder
            RUN apk add --no-cache musl-dev
            WORKDIR /build
            COPY . .
            RUN cargo build --release

            FROM alpine:3.19
            WORKDIR /app
            COPY --from=builder /build/target/release/trading_bot /app/bot
            USER 10001:10001
            ENTRYPOINT ["/app/bot"]
            """
        elif "go.mod" in all_files or any(f.endswith(".go") for f in all_files):
            detected_lang = "go"
            dockerfile_content = """
            FROM golang:1.22-alpine AS builder
            WORKDIR /build
            COPY . .
            RUN CGO_ENABLED=0 GOOS=linux go build -o bot .

            FROM alpine:3.19
            WORKDIR /app
            COPY --from=builder /build/bot /app/bot
            USER 10001:10001
            ENTRYPOINT ["/app/bot"]
            """
            
        elif "requirements.txt" in all_files or "sandbox_sdk.py" in all_files or any(f.endswith(".py") for f in all_files):
            detected_lang = "python"
            
            # --- FIXED RESOLUTION ENGINE ---
            # Locate exactly where sandbox_sdk.py lives inside the unzipped staging directory
            target_script_rel_path = "sandbox_sdk.py"
            for root, dirs, filenames in os.walk(unzip_dir):
                if "sandbox_sdk.py" in filenames:
                    # Deduce the exact relative file route path from the zip root
                    target_script_rel_path = os.path.relpath(os.path.join(root, "sandbox_sdk.py"), unzip_dir)
                    break

            print(f"[polyglot] Target entrypoint located at: {target_script_rel_path}")
            entry_cmd = f'["python", "{target_script_rel_path}"]'
            
            dockerfile_content = f"""
            FROM python:3.11-alpine
            WORKDIR /app
            COPY . .
            RUN find . -name "requirements.txt" -exec pip install --no-cache-dir -r {{}} \;
            USER 10001:10001
            ENTRYPOINT {entry_cmd}
            """

        else:
            # Standard C++ Core Sandbox Compilation Path Fallback
            detected_lang = "cpp"
            dockerfile_content = """
            FROM alpine:3.19 AS builder
            RUN apk add --no-cache g++ make cmake linux-headers
            WORKDIR /build
            COPY . .
            RUN if [ -f CMakeLists.txt ]; then \
                    mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make; \
                 elif ls *.cpp 1> /dev/null 2>&1; then \
                    g++ -std=c++20 -O3 -DNDEBUG -pthread *.cpp -o bot; \
                 else \
                    echo "No valid build targets found" && exit 1; \
                 fi

            FROM alpine:3.19
            RUN apk add --no-cache libstdc++
            WORKDIR /app
            COPY --from=builder /build/build/bot* /app/bot
            COPY --from=builder /build/bot* /app/bot
            USER 10001:10001
            ENTRYPOINT ["/app/bot"]
            """

        print(f"[polyglot] Verified Language Target: {detected_lang.upper()}")
        
        generated_df_path = os.path.join(unzip_dir, "Dockerfile.generated")
        with open(generated_df_path, "w") as f:
            f.write(dockerfile_content.strip())
            
        image_tag = f"contestant-agent:{submission_id}"
        print(f"[build] docker build -f {generated_df_path} -t {image_tag} {unzip_dir}")
        subprocess.run(
            ["docker", "build", "-f", generated_df_path, "-t", image_tag, unzip_dir],
            check=True,
        )
        return image_tag

    def ensure_namespace(self):
        labels = {
            "name": self.namespace,
            "pod-security.kubernetes.io/enforce": "restricted",
            "pod-security.kubernetes.io/audit": "restricted",
            "pod-security.kubernetes.io/warn": "restricted",
        }
        body = self.client.V1Namespace(
            metadata=self.client.V1ObjectMeta(name=self.namespace, labels=labels)
        )
        try:
            self.core.create_namespace(body)
            print(f"[k8s] namespace/{self.namespace} created")
        except self.ApiException as exc:
            if exc.status != 409:
                raise

    def ensure_exchange_service(self):
        body = self.client.V1Service(
            metadata=self.client.V1ObjectMeta(
                name="central-exchange-gateway",
                namespace=self.namespace,
                labels={"app": "central-exchange-core"},
            ),
            spec=self.client.V1ServiceSpec(
                type="ClusterIP",
                selector={"app": "central-exchange-core"},
                ports=[
                    self.client.V1ServicePort(
                        name="websocket",
                        protocol="TCP",
                        port=8080,
                        target_port=8080,
                    )
                ],
            ),
        )
        try:
            self.core.create_namespaced_service(self.namespace, body)
            print("[k8s] service/central-exchange-gateway deployed")
        except self.ApiException as exc:
            if exc.status != 409:
                raise

    def ensure_network_policy(self):
        # NOTE: Using a local target-gateway bridge bypass requires opening egress restrictions.
        policy = self.client.V1NetworkPolicy(
            metadata=self.client.V1ObjectMeta(
                name="sandbox-isolation-jail",
                namespace=self.namespace,
            ),
            spec=self.client.V1NetworkPolicySpec(
                pod_selector=self.client.V1LabelSelector(
                    match_labels={"tier": "contestant-agent"}
                ),
                policy_types=["Egress"],
                egress=[
                    # Allow pod egress calls out to any destination IP to bridge back to the localhost terminal engine
                    self.client.V1NetworkPolicyEgressRule(
                        ports=[
                            self.client.V1NetworkPolicyPort(protocol="TCP", port=8080)
                        ]
                    )
                ],
            ),
        )
        try:
            self.networking.create_namespaced_network_policy(self.namespace, policy)
            print("[k8s] networkpolicy/sandbox-isolation-jail active")
        except self.ApiException as exc:
            if exc.status != 409:
                raise

    def deploy_exchange_core(self, cpu, memory):
        """Deploys your stable C++ Core Orderbook Exchange to listen for trade frames."""
        image_tag = "iicpc-matching-engine:latest"
        subprocess.run(["docker", "build", "-f", "matching-engine/Dockerfile", "-t", image_tag, self.repo_root], check=True)

        container = self.client.V1Container(
            name="exchange-core-engine",
            image=image_tag,
            image_pull_policy="IfNotPresent",
            ports=[self.client.V1ContainerPort(container_port=8080, name="websocket")],
            resources=self.client.V1ResourceRequirements(
                requests={"cpu": cpu, "memory": memory}, limits={"cpu": cpu, "memory": memory}
            ),
            security_context=self.client.V1SecurityContext(
                allow_privilege_escalation=False, read_only_root_filesystem=True, capabilities=self.client.V1Capabilities(drop=["ALL"])
            ),
        )

        engine_pod_security_context = self.client.V1PodSecurityContext(
            run_as_non_root=True,
            run_as_user=10001
        )

        deployment = self.client.V1Deployment(
            metadata=self.client.V1ObjectMeta(name="central-exchange-core", namespace=self.namespace, labels={"app": "central-exchange-core"}),
            spec=self.client.V1DeploymentSpec(
                replicas=1,
                selector=self.client.V1LabelSelector(match_labels={"app": "central-exchange-core"}),
                template=self.client.V1PodTemplateSpec(
                    metadata=self.client.V1ObjectMeta(labels={"app": "central-exchange-core"}),
                    spec=self.client.V1PodSpec(
                        security_context=engine_pod_security_context,
                        containers=[container]
                    )
                )
            )
        )
        try:
            self.apps.create_namespaced_deployment(self.namespace, deployment)
            print("[k8s] Central C++ Engine Core started.")
        except self.ApiException as exc:
            if exc.status != 409: raise

    def spawn_contestant_sandbox_pod(self, image_tag, duration):
        """Spawns the user's compiled dynamic agent container inside the network jail."""
        if not image_tag:
            image_tag = "contestant-agent:anonymous_quant_bot"
            
        # FIX: Point Minikube containers straight to your physical local host machine machine pipeline gateway IP
        # 10.0.2.2 is the default virtual router gateway address mapping back to localhost from inside minikube pods
        target_host = "10.0.2.2"
        
        pod = self.client.V1Pod(
            metadata=self.client.V1ObjectMeta(
                name="active-contestant-sandbox",
                namespace=self.namespace,
                labels={"tier": "contestant-agent", "app": "trading-bot"},
            ),
            spec=self.client.V1PodSpec(
                restart_policy="Never",
                automount_service_account_token=False,
                enable_service_links=False,
                security_context=self.client.V1PodSecurityContext(
                    run_as_non_root=True,
                    run_as_user=10002,
                    run_as_group=10002,
                    seccomp_profile=self.client.V1SeccompProfile(type="RuntimeDefault"),
                ),
                containers=[
                    self.client.V1Container(
                        name="agent-runner",
                        image=image_tag,
                        image_pull_policy="IfNotPresent",
                        env=[
                            self.client.V1EnvVar(name="EXCHANGE_GATEWAY_URL", value=f"ws://{target_host}:8080"),
                            self.client.V1EnvVar(name="CONTESTANT_ID", value=image_tag.split(":")[-1])
                        ],
                        resources=self.client.V1ResourceRequirements(
                            requests={"cpu": "2", "memory": "1Gi"},
                            limits={"cpu": "2", "memory": "1Gi"},
                        ),
                        security_context=self.client.V1SecurityContext(
                            allow_privilege_escalation=False,
                            read_only_root_filesystem=False, 
                            capabilities=self.client.V1Capabilities(drop=["ALL"]),
                        ),
                    )
                ],
            ),
        )

        self.delete_old_sandbox_pod(wait=True)
        self.core.create_namespaced_pod(self.namespace, pod)
        print(f"[k8s] pod/active-contestant-sandbox deployed into network isolation jail targeting host terminal engine on {target_host}:8080")

    def wait_for_agent_completion(self, timeout):
        print("[wait] monitoring contestant sandbox run constraints...")
        deadline = time.time() + timeout
        logs = ""
        while time.time() < deadline:
            pod = self.core.read_namespaced_pod_status("active-contestant-sandbox", self.namespace)
            phase = pod.status.phase
            if phase in ("Succeeded", "Failed"):
                print(f"[done] sandbox evaluation run finished. execution_phase={phase}")
                try:
                    logs = self.core.read_namespaced_pod_log("active-contestant-sandbox", self.namespace, tail_lines=50)
                    if logs:
                        print("[sandbox stdout]\n" + logs)
                except self.ApiException:
                    pass
                return
            time.sleep(1)
            
        # Error Out-of-band Fallback Diagnostic Collector:
        try:
            logs = self.core.read_namespaced_pod_log("active-contestant-sandbox", self.namespace, tail_lines=50)
            print("[TIMEOUT DEBUG - USER BOT STDOUT]:\n", logs)
        except Exception:
            print("[TIMEOUT DEBUG] Could not fetch terminal output from stalled sandbox container pod.")
            
        raise TimeoutError("Evaluation target exceeded assigned contest duration bounds.")

    def delete_old_sandbox_pod(self, wait):
        try:
            self.core.delete_namespaced_pod("active-contestant-sandbox", self.namespace, grace_period_seconds=0)
        except self.ApiException as exc:
            if exc.status != 404: raise
        if wait:
            while True:
                try:
                    self.core.read_namespaced_pod("active-contestant-sandbox", self.namespace)
                    time.sleep(1)
                except self.ApiException as e:
                    if e.status == 404: break

    def cleanup(self):
        print("[cleanup] reaping benchmark resources")
        self.delete_old_sandbox_pod(wait=False)

    def run(self, args):
        self.connect()
        target_workspace = os.path.join("/tmp/sandbox_uploads", args.sub_id)
        
        if not os.path.exists(target_workspace):
            print(f"[warning] Workspace target {target_workspace} missing. Operating on repo root.")
            target_workspace = self.repo_root

        try:
            self.ensure_namespace()
            self.ensure_exchange_service()
            self.ensure_network_policy()
            
            # 1. Boot up matching loop infrastructure if needed inside the cluster
            self.deploy_exchange_core(args.cpu, args.memory)
            
            # 2. Compile user package against dynamic polyglot wrapper parameters
            contestant_image = self.auto_detect_and_build(target_workspace, args.sub_id)
            
            # 3. Drop agent into Kubernetes NetworkPolicy isolation container cell
            self.spawn_contestant_sandbox_pod(contestant_image, args.duration)
            self.wait_for_agent_completion(args.duration + 30)
        finally:
            if not args.keep_resources:
                self.cleanup()


def parse_args():
    parser = argparse.ArgumentParser(description="IICPC Polyglot Benchmark Lifecycle Orchestrator")
    parser.add_argument("--sub-id", required=True, help="Contestant submission identifier")
    parser.add_argument("--namespace", default="evaluation-sandbox")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--keep-resources", action="store_true")
    parser.add_argument("--cpu", default="2")
    parser.add_argument("--memory", default="1Gi")
    parser.add_argument("--duration", type=int, default=30)
    return parser.parse_args()


def main():
    orchestrator = BenchmarkOrchestrator()
    orchestrator.run(parse_args())


if __name__ == "__main__":
    main()