#!/usr/bin/env python3
import argparse
import subprocess
import sys
import time


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
            config.load_kubeconfig()

        self.client = client
        self.config = config
        self.ApiException = ApiException
        self.core = client.CoreV1Api()
        self.apps = client.AppsV1Api()
        self.networking = client.NetworkingV1Api()

    def build_image(self, image_tag, dockerfile):
        print(f"[build] docker build -f {dockerfile} -t {image_tag} {self.repo_root}")
        subprocess.run(
            ["docker", "build", "-f", dockerfile, "-t", image_tag, self.repo_root],
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
            self.core.patch_namespace(self.namespace, body)
            print(f"[k8s] namespace/{self.namespace} patched")

    def ensure_exchange_service(self):
        body = self.client.V1Service(
            metadata=self.client.V1ObjectMeta(
                name="contestant-exchange",
                namespace=self.namespace,
                labels={"app": "contestant-exchange"},
            ),
            spec=self.client.V1ServiceSpec(
                type="ClusterIP",
                selector={"app": "contestant-exchange"},
                ports=[
                    self.client.V1ServicePort(
                        name="websocket",
                        protocol="TCP",
                        port=8080,
                        target_port="websocket",
                    )
                ],
            ),
        )
        try:
            self.core.create_namespaced_service(self.namespace, body)
            print("[k8s] service/contestant-exchange created")
        except self.ApiException as exc:
            if exc.status != 409:
                raise
            self.core.patch_namespaced_service("contestant-exchange", self.namespace, body)
            print("[k8s] service/contestant-exchange patched")

    def ensure_network_policy(self):
        policy = self.client.V1NetworkPolicy(
            metadata=self.client.V1ObjectMeta(
                name="contestant-exchange-isolation",
                namespace=self.namespace,
            ),
            spec=self.client.V1NetworkPolicySpec(
                pod_selector=self.client.V1LabelSelector(
                    match_labels={"app": "contestant-exchange"}
                ),
                policy_types=["Ingress", "Egress"],
                ingress=[
                    self.client.V1NetworkPolicyIngressRule(
                        _from=[
                            self.client.V1NetworkPolicyPeer(
                                pod_selector=self.client.V1LabelSelector(
                                    match_labels={"tier": "telemetry-bot-fleet"}
                                )
                            )
                        ],
                        ports=[
                            self.client.V1NetworkPolicyPort(protocol="TCP", port=8080)
                        ],
                    )
                ],
                egress=[
                    self.client.V1NetworkPolicyEgressRule(
                        to=[
                            self.client.V1NetworkPolicyPeer(
                                pod_selector=self.client.V1LabelSelector(
                                    match_labels={"app": "kafka-broker"}
                                )
                            )
                        ],
                        ports=[
                            self.client.V1NetworkPolicyPort(protocol="TCP", port=9092)
                        ],
                    )
                ],
            ),
        )
        try:
            self.networking.create_namespaced_network_policy(self.namespace, policy)
            print("[k8s] networkpolicy/contestant-exchange-isolation created")
        except self.ApiException as exc:
            if exc.status != 409:
                raise
            self.networking.patch_namespaced_network_policy(
                "contestant-exchange-isolation", self.namespace, policy
            )
            print("[k8s] networkpolicy/contestant-exchange-isolation patched")

    def deploy_exchange(self, image_tag, cpu, memory):
        container = self.client.V1Container(
            name="exchange-engine",
            image=image_tag,
            image_pull_policy="IfNotPresent",
            ports=[
                self.client.V1ContainerPort(
                    container_port=8080,
                    name="websocket",
                    protocol="TCP",
                )
            ],
            resources=self.client.V1ResourceRequirements(
                requests={"cpu": cpu, "memory": memory},
                limits={"cpu": cpu, "memory": memory},
            ),
            security_context=self.client.V1SecurityContext(
                allow_privilege_escalation=False,
                read_only_root_filesystem=True,
                capabilities=self.client.V1Capabilities(drop=["ALL"]),
            ),
            liveness_probe=self.client.V1Probe(
                tcp_socket=self.client.V1TCPSocketAction(port="websocket"),
                initial_delay_seconds=5,
                period_seconds=10,
            ),
            readiness_probe=self.client.V1Probe(
                tcp_socket=self.client.V1TCPSocketAction(port="websocket"),
                initial_delay_seconds=2,
                period_seconds=5,
            ),
        )
        pod_spec = self.client.V1PodSpec(
            automount_service_account_token=False,
            enable_service_links=False,
            share_process_namespace=False,
            security_context=self.client.V1PodSecurityContext(
                run_as_non_root=True,
                run_as_user=10001,
                run_as_group=10001,
                fs_group=10001,
                seccomp_profile=self.client.V1SeccompProfile(type="RuntimeDefault"),
            ),
            containers=[container],
        )
        deployment = self.client.V1Deployment(
            metadata=self.client.V1ObjectMeta(
                name="contestant-exchange",
                namespace=self.namespace,
                labels={"app": "contestant-exchange", "tier": "contestant-code"},
            ),
            spec=self.client.V1DeploymentSpec(
                replicas=1,
                selector=self.client.V1LabelSelector(
                    match_labels={"app": "contestant-exchange"}
                ),
                template=self.client.V1PodTemplateSpec(
                    metadata=self.client.V1ObjectMeta(
                        labels={"app": "contestant-exchange", "tier": "contestant-code"}
                    ),
                    spec=pod_spec,
                ),
            ),
        )

        self.delete_deployment(wait=False)
        try:
            self.apps.create_namespaced_deployment(self.namespace, deployment)
            print(f"[k8s] deployment/contestant-exchange created with {image_tag}")
        except self.ApiException as exc:
            if exc.status != 409:
                raise
            self.apps.replace_namespaced_deployment(
                "contestant-exchange", self.namespace, deployment
            )
            print(f"[k8s] deployment/contestant-exchange replaced with {image_tag}")

    def wait_for_exchange_ready(self, timeout):
        print("[wait] exchange readiness")
        deadline = time.time() + timeout
        while time.time() < deadline:
            dep = self.apps.read_namespaced_deployment_status(
                "contestant-exchange", self.namespace
            )
            if dep.status.ready_replicas and dep.status.ready_replicas >= 1:
                print("[ready] contestant WebSocket gateway is online")
                return
            time.sleep(1)
        raise TimeoutError("contestant exchange did not become ready")

    def spawn_bot_fleet(self, image_tag, mm, noise, momentum, duration):
        target_host = f"contestant-exchange.{self.namespace}.svc.cluster.local"
        pod = self.client.V1Pod(
            metadata=self.client.V1ObjectMeta(
                name="ephemeral-bot-fleet",
                namespace=self.namespace,
                labels={"tier": "telemetry-bot-fleet", "app": "bot-fleet"},
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
                        name="load-generator",
                        image=image_tag,
                        image_pull_policy="IfNotPresent",
                        args=[
                            "--host", target_host,
                            "--port", "8080",
                            "--mm", str(mm),
                            "--noise", str(noise),
                            "--momentum", str(momentum),
                            "--duration", str(duration),
                        ],
                        resources=self.client.V1ResourceRequirements(
                            requests={"cpu": "2", "memory": "1Gi"},
                            limits={"cpu": "2", "memory": "1Gi"},
                        ),
                        security_context=self.client.V1SecurityContext(
                            allow_privilege_escalation=False,
                            read_only_root_filesystem=True,
                            capabilities=self.client.V1Capabilities(drop=["ALL"]),
                        ),
                    )
                ],
            ),
        )

        self.delete_bot_pod(wait=True)
        self.core.create_namespaced_pod(self.namespace, pod)
        print(
            f"[k8s] pod/ephemeral-bot-fleet launched against {target_host} "
            f"({mm} MM | {noise} Noise | {momentum} Momentum)"
        )

    def wait_for_bot_completion(self, timeout):
        print("[wait] bot fleet completion")
        deadline = time.time() + timeout
        while time.time() < deadline:
            pod = self.core.read_namespaced_pod_status("ephemeral-bot-fleet", self.namespace)
            phase = pod.status.phase
            if phase in ("Succeeded", "Failed"):
                print(f"[done] bot fleet phase={phase}")
                try:
                    logs = self.core.read_namespaced_pod_log(
                        "ephemeral-bot-fleet", self.namespace, tail_lines=80
                    )
                    if logs:
                        print("[bot logs]\n" + logs)
                except self.ApiException:
                    pass
                if phase == "Failed":
                    raise RuntimeError("bot fleet pod failed")
                return
            time.sleep(1)
        raise TimeoutError("bot fleet did not complete before timeout")

    def cleanup(self):
        print("[cleanup] reaping benchmark resources")
        self.delete_bot_pod(wait=False)
        self.delete_deployment(wait=False)

    def delete_bot_pod(self, wait):
        try:
            self.core.delete_namespaced_pod(
                "ephemeral-bot-fleet",
                self.namespace,
                grace_period_seconds=0,
            )
            print("[cleanup] pod/ephemeral-bot-fleet deleted")
        except self.ApiException as exc:
            if exc.status != 404:
                raise
        if wait:
            self.wait_for_pod_deleted("ephemeral-bot-fleet", 30)

    def delete_deployment(self, wait):
        try:
            self.apps.delete_namespaced_deployment(
                "contestant-exchange",
                self.namespace,
                grace_period_seconds=0,
            )
            print("[cleanup] deployment/contestant-exchange deleted")
        except self.ApiException as exc:
            if exc.status != 404:
                raise
        if wait:
            self.wait_for_deployment_deleted("contestant-exchange", 30)

    def wait_for_pod_deleted(self, name, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.core.read_namespaced_pod(name, self.namespace)
            except self.ApiException as exc:
                if exc.status == 404:
                    return
                raise
            time.sleep(1)

    def wait_for_deployment_deleted(self, name, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.apps.read_namespaced_deployment(name, self.namespace)
            except self.ApiException as exc:
                if exc.status == 404:
                    return
                raise
            time.sleep(1)

    def run(self, args):
        self.connect()

        exchange_image = args.exchange_image
        bot_image = args.bot_image
        if not args.skip_build:
            exchange_image = self.build_image(exchange_image, "matching-engine/Dockerfile")
            if args.build_bot_image:
                bot_image = self.build_image(bot_image, "bot-fleet/Dockerfile")

        try:
            self.ensure_namespace()
            self.ensure_exchange_service()
            self.ensure_network_policy()
            self.deploy_exchange(exchange_image, args.cpu, args.memory)
            self.wait_for_exchange_ready(args.exchange_timeout)
            self.spawn_bot_fleet(bot_image, args.mm, args.noise, args.momentum, args.duration)
            self.wait_for_bot_completion(args.duration + args.bot_timeout_buffer)
        finally:
            if not args.keep_resources:
                self.cleanup()


def parse_args():
    parser = argparse.ArgumentParser(description="IICPC benchmark lifecycle orchestrator")
    parser.add_argument("--sub-id", required=True, help="Contestant submission identifier")
    parser.add_argument("--namespace", default="evaluation-sandbox")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--exchange-image", default=None)
    parser.add_argument("--bot-image", default="iicpc-traffic-generator:latest")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--build-bot-image", action="store_true")
    parser.add_argument("--keep-resources", action="store_true")
    parser.add_argument("--cpu", default="4")
    parser.add_argument("--memory", default="2Gi")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--mm", type=int, default=850)
    parser.add_argument("--noise", type=int, default=1700)
    parser.add_argument("--momentum", type=int, default=4250)
    parser.add_argument("--exchange-timeout", type=int, default=60)
    parser.add_argument("--bot-timeout-buffer", type=int, default=180)
    args = parser.parse_args()

    if args.exchange_image is None:
        args.exchange_image = f"contestant-submission:{args.sub_id}"
    return args


def main():
    args = parse_args()
    orchestrator = BenchmarkOrchestrator(namespace=args.namespace, repo_root=args.repo_root)
    try:
        orchestrator.run(args)
    except Exception as exc:
        print(f"[fatal] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
