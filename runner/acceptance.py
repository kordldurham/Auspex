"""Bounded, unattended acceptance tests for the existing production scanner.

This controller reads public app diagnostics and dispatches the existing scanner
workflow. It does not collect retailer pages or write application records.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from typing import Any
import zipfile

import httpx

from runner.worker import SITE

REPO = "kordldurham/Auspex"
REPO_ID = 1454612846
OWNER_ID = 66217662
WORKFLOW = "auspex-scanner.yml"
WORKFLOW_PATH = ".github/workflows/" + WORKFLOW
API_PREFIX = "/repos/" + REPO + "/"
TERMINAL = {"completed", "partial"}
COUNTERS = ("total", "completed", "listingsChecked", "observedListings", "verifiedOffers",
            "listingsDiscovered", "listingsPersisted", "rejectedListings", "retailersAttempted", "retailersSucceeded")
RAW_COUNTERS = ("collected", "collectionFailures", "persistenceAcks", "rawEvidenceAcknowledged",
                "rawProductEvidenceAcknowledged", "rawFailureEvidenceAcknowledged")
SCOPE_FIELDS = ("selectedListings", "selectedRetailers", "catalogListings", "catalogRetailers")


class AcceptanceError(RuntimeError):
    """Only finite, non-sensitive reason codes belong in this exception."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise AcceptanceError(code)


def integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_json(raw: bytes) -> Any:
    def invalid(_: str) -> None:
        raise ValueError("Nonfinite JSON")
    return json.loads(raw, parse_constant=invalid)


def safe_job(job: dict[str, Any]) -> dict[str, Any]:
    return {**{key: job[key] for key in COUNTERS if integer(job.get(key))},
            **{key: job[key] for key in ("id", "status", "version", "currentVersion")
               if isinstance(job.get(key), str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", job[key])},
            "scope": {"kind": "full" if job.get("scope", {}).get("kind") == "full" else "other",
                      **{key: job["scope"][key] for key in SCOPE_FIELDS if integer(job.get("scope", {}).get(key))}}}


def safe_proof(proof: dict[str, Any]) -> dict[str, Any]:
    return {"status": proof.get("status") if proof.get("status") in {"completed", "interrupted", "failed", "idle", "busy"} else "unknown",
            "jobId": proof.get("jobId") if isinstance(proof.get("jobId"), str) and re.fullmatch(r"[a-f0-9-]{36}", proof["jobId"]) else None,
            "releaseAcknowledged": proof.get("releaseAcknowledged") is True,
            **{key: proof[key] for key in (*RAW_COUNTERS, "batches", "elapsedSeconds") if integer(proof.get(key))},
            "job": safe_job(proof.get("job") or {})}


def full_scope(job: dict[str, Any], config: dict[str, Any]) -> None:
    scope = job.get("scope") or {}
    require(scope.get("kind") == "full", "not-full-catalog-scope")
    require(job.get("total") == config["expectedListings"], "unexpected-listing-scope")
    require(scope.get("selectedListings") == scope.get("catalogListings") == config["expectedListings"], "selected-catalog-mismatch")
    require(scope.get("selectedRetailers") == scope.get("catalogRetailers") == config["expectedRetailers"], "unexpected-retailer-scope")


def validate_proof(proof: dict[str, Any], config: dict[str, Any], completed: bool) -> dict[str, Any]:
    require(isinstance(proof, dict) and isinstance(proof.get("job"), dict), "missing-scanner-proof")
    job = proof["job"]
    full_scope(job, config)
    require(proof.get("releaseAcknowledged") is True, "scanner-release-unacknowledged")
    require(all(integer(proof.get(key)) for key in RAW_COUNTERS), "invalid-raw-proof-counters")
    require(all(integer(job.get(key)) for key in COUNTERS), "invalid-job-proof-counters")
    require(isinstance(proof.get("jobId"), str) and bool(re.fullmatch(r"[a-f0-9-]{36}", proof["jobId"])) and proof.get("jobId") == job.get("id"), "proof-job-identity-mismatch")
    require(proof["rawProductEvidenceAcknowledged"] + proof["rawFailureEvidenceAcknowledged"] == proof["rawEvidenceAcknowledged"], "raw-proof-total-mismatch")
    require(proof["collected"] + proof["collectionFailures"] == proof["rawEvidenceAcknowledged"], "collection-acknowledgement-mismatch")
    require(0 <= job["verifiedOffers"] <= job["listingsPersisted"] <= job["listingsDiscovered"] <= job["total"], "matching-counters-out-of-range")
    require(job["listingsDiscovered"] == job["listingsPersisted"] + job["rejectedListings"], "discovery-matching-count-mismatch")
    if completed:
        require(job["verifiedOffers"] > 0 and job["listingsPersisted"] > 0, "no-positive-matched-eligible-evidence")
        require(proof.get("status") == "completed" and job.get("status") in TERMINAL, "scanner-not-completed")
        require(job["completed"] == job["total"], "incomplete-terminal-coverage")
        require(job["retailersAttempted"] == config["expectedRetailers"] and job["retailersSucceeded"] > 0, "retailer-attempt-coverage-missing")
    else:
        require(proof.get("status") == "interrupted" and proof.get("batches") == 1, "deliberate-interruption-not-proven")
        require(proof["rawEvidenceAcknowledged"] >= 32 and 0 < job["completed"] <= 32 < job["total"], "interruption-checkpoint-not-proven")
    return job


def correlate_runs(runs: list[dict[str, Any]], before: set[int], head: str, not_before: float) -> int | None:
    candidates = []
    for run in runs:
        if run.get("id") in before or run.get("event") != "workflow_dispatch" or run.get("head_branch") != "main" or run.get("head_sha") != head:
            continue
        try:
            created = dt.datetime.fromisoformat(run["created_at"].replace("Z", "+00:00")).timestamp()
        except (KeyError, TypeError, ValueError):
            continue
        if created >= math.floor(not_before) - 2:
            candidates.append(run["id"])
    require(len(candidates) <= 1, "ambiguous-dispatched-run")
    return candidates[0] if candidates else None


def task_checkpoints(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for row in report.get("listings", []):
        if row.get("status") in {"success", "failed", "rejected", "skipped"}:
            key = hashlib.sha256(str(row.get("id")).encode()).hexdigest()
            result[key] = {"status": row["status"], "checkedAt": row.get("checkedAt"), "attempts": row.get("attempts")}
    return result


class Services:
    def __init__(self, token: str, client: httpx.Client | None = None):
        require(bool(token), "github-token-unavailable")
        self.token = token
        self.client = client or httpx.Client(follow_redirects=False, timeout=25, headers={"User-Agent": "AuspexAcceptance/1.0"})

    def request(self, method: str, url: str, *, github: bool = False, payload: Any = None, binary: bool = False) -> Any:
        parsed = httpx.URL(url)
        require(parsed.scheme == "https" and parsed.port in (None, 443) and not parsed.username and not parsed.password and not parsed.fragment, "unsafe-request-destination")
        headers = {}
        if github:
            require(parsed.host == "api.github.com" and parsed.path.startswith(API_PREFIX) and "/../" not in parsed.path, "unsafe-github-token-destination")
            headers = {"Authorization": "Bearer " + self.token, "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        attempts = 3 if method == "GET" else 1  # A dispatch may succeed even if its response is lost.
        for attempt in range(attempts):
            try:
                started = time.monotonic()
                with self.client.stream(method, url, json=payload, headers=headers, follow_redirects=False) as response:
                    if response.status_code == 302 and binary and github:
                        destination = httpx.URL(response.headers.get("location", ""))
                        host = destination.host or ""
                        require(destination.scheme == "https" and destination.port in (None, 443) and not destination.username and not destination.password and not destination.fragment and
                                (host.endswith(".blob.core.windows.net") or host.endswith(".actions.githubusercontent.com") or host.endswith(".githubusercontent.com")), "unsafe-artifact-redirect")
                        # Signed artifact URLs are never logged; authorization is not forwarded.
                        return self.request("GET", str(destination), binary=True)
                    if response.status_code == 204:
                        return None
                    if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                        try:
                            delay = min(30, max(1, int(response.headers.get("retry-after", "2"))))
                        except ValueError:
                            delay = 2
                        time.sleep(delay)
                        continue
                    require(response.status_code == 200, "github-http-" + str(response.status_code) if github else "public-http-" + str(response.status_code))
                    body = bytearray()
                    for part in response.iter_bytes():
                        body.extend(part)
                        require(time.monotonic() - started <= 45, "response-absolute-deadline")
                        require(len(body) <= 10_000_000, "response-too-large")
                    return bytes(body) if binary else parse_json(bytes(body))
            except httpx.TransportError:
                if attempt + 1 == attempts:
                    raise AcceptanceError("request-transport-failed") from None
                time.sleep(attempt + 1)
        raise AcceptanceError("request-exhausted")

    def github(self, path: str, method: str = "GET", payload: Any = None, binary: bool = False) -> Any:
        require(path.startswith(API_PREFIX), "unexpected-github-api-path")
        return self.request(method, "https://api.github.com" + path, github=True, payload=payload, binary=binary)

    def public(self, path: str) -> dict[str, Any]:
        require(path in {"/api/scans/status", "/api/scans/report", "/api/catalog/coverage", "/api/version"}, "unexpected-public-api-path")
        return self.request("GET", SITE + path)

    def runs(self) -> list[dict[str, Any]]:
        return self.github(API_PREFIX + "actions/workflows/" + WORKFLOW + "/runs?event=workflow_dispatch&branch=main&per_page=100")["workflow_runs"]

    def run(self, run_id: int) -> dict[str, Any]:
        run = self.github(API_PREFIX + "actions/runs/" + str(run_id))
        require(run.get("path") == WORKFLOW_PATH and run.get("head_branch") == "main" and
                run.get("head_repository", {}).get("id") == REPO_ID and
                run.get("head_repository", {}).get("owner", {}).get("id") == OWNER_ID, "unexpected-scanner-workflow-identity")
        return run

    def diagnostics(self, run_id: int) -> list[dict[str, Any]]:
        data = self.github(API_PREFIX + "actions/runs/" + str(run_id) + "/jobs?per_page=100")
        known = {"Install external reader", "Verify reader", "Claim queued work and read permitted sources", "Preserve non-secret execution proof"}
        return [{"id": row.get("id"), "status": row.get("status"), "conclusion": row.get("conclusion"),
                 "steps": [{"name": step["name"], "status": step.get("status"), "conclusion": step.get("conclusion")}
                           for step in row.get("steps", []) if step.get("name") in known]} for row in data.get("jobs", [])]

    def proof(self, run: dict[str, Any]) -> dict[str, Any]:
        run_id = run["id"]
        artifacts = self.github(API_PREFIX + "actions/runs/" + str(run_id) + "/artifacts?per_page=100")["artifacts"]
        name = "auspex-reader-proof-" + str(run_id) + "-" + str(run.get("run_attempt", 1))
        candidates = [a for a in artifacts if a.get("name") == name and not a.get("expired")]
        require(len(candidates) == 1 and candidates[0].get("size_in_bytes", 10_000_001) <= 1_000_000, "scanner-proof-artifact-unavailable")
        raw = self.github(API_PREFIX + "actions/artifacts/" + str(candidates[0]["id"]) + "/zip", binary=True)
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                members = archive.infolist()
                require(len(members) == 1 and members[0].filename == "runner-proof.json" and members[0].file_size <= 250_000, "unexpected-proof-zip-content")
                proof = parse_json(archive.read(members[0]))
        except (zipfile.BadZipFile, ValueError, KeyError):
            raise AcceptanceError("invalid-proof-artifact") from None
        return proof


class Controller:
    def __init__(self, services: Services, config: dict[str, Any], output: Path, *, clock=time.monotonic, sleep=time.sleep):
        self.services, self.config, self.output = services, config, output
        self.clock, self.sleep = clock, sleep
        self.deadline = self.clock() + config.get("totalTimeoutSeconds", 13_500)
        self.report = {"schemaVersion": 1, "requestId": config["requestId"], "startedAt": stamp(), "status": "running", "phases": [], "completedFullJobs": [], "interruptionVerified": False, "sameJobResumed": False}
        output.mkdir(parents=True, exist_ok=True)
        self.save()

    def save(self) -> None:
        temporary = self.output / "acceptance-report.tmp"
        temporary.write_text(json.dumps(self.report, indent=2))
        temporary.replace(self.output / "acceptance-report.json")

    def bounded_sleep(self, seconds: int) -> None:
        require(self.clock() + seconds < self.deadline, "controller-total-deadline")
        self.sleep(seconds)

    def snapshot(self, expected_job: str, completed: bool) -> dict[str, Any]:
        state = self.services.public("/api/scans/status")
        report = self.services.public("/api/scans/report")
        coverage = self.services.public("/api/catalog/coverage")
        version = self.services.public("/api/version")
        job = state.get("job") or {}
        require(job.get("id") == report.get("job", {}).get("id") == expected_job, "public-active-job-changed")
        full_scope(job, self.config)
        require(state.get("version") == version.get("version") == job.get("currentVersion") == self.config["expectedDataVersion"], "production-data-version-mismatch")
        require(coverage.get("trackedListings") == self.config["expectedListings"] and coverage.get("enabledRetailers") == self.config["expectedRetailers"], "persistent-catalog-scope-mismatch")
        require(integer(coverage.get("totalListings")) and coverage["totalListings"] >= self.config["minimumCatalogListings"], "persistent-catalog-listing-loss")
        require(integer(coverage.get("eligibleOffers")) and coverage["eligibleOffers"] > 0, "no-persistent-eligible-prices")
        require(not coverage.get("catalogContinuityWarning"), "catalog-continuity-warning")
        if completed:
            require(job.get("status") in TERMINAL and job.get("completed") == job.get("total"), "public-job-incomplete")
        else:
            require(job.get("status") == "queued" and job.get("pendingListings") == job.get("total") - job.get("completed", 0) > 0 and not state.get("runner", {}).get("available"), "interrupted-job-not-queued")
        result = {"observedAt": stamp(), "dataVersion": version["version"], "job": safe_job(job),
                  "coverage": {k: coverage[k] for k in ("registeredRetailers", "enabledRetailers", "referenceRetailers", "trackedListings", "referenceListings", "totalListings", "reconciledKits", "canonicalProducts", "eligibleOffers", "sourceFailures") if integer(coverage.get(k))},
                  "taskStatusCounts": dict(collections.Counter(row.get("status", "unknown") for row in report.get("listings", []))),
                  "terminalTaskCheckpoints": task_checkpoints(report)}
        return result

    def wait_run(self, run_id: int) -> dict[str, Any]:
        phase_deadline = min(self.deadline, self.clock() + self.config.get("phaseTimeoutSeconds", 4500))
        while self.clock() < phase_deadline:
            run = self.services.run(run_id)
            if run.get("status") == "completed":
                return run
            self.bounded_sleep(self.config.get("pollSeconds", 20))
        raise AcceptanceError("scanner-run-deadline")

    def dispatch(self, force: bool, stop: int) -> int:
        before = {run["id"] for run in self.services.runs()}
        head = self.services.github(API_PREFIX + "branches/main")["commit"]["sha"]
        dispatched_at = time.time()
        self.services.github(API_PREFIX + "actions/workflows/" + WORKFLOW + "/dispatches", method="POST", payload={"ref": "main", "inputs": {"force_full": force, "stop_after_batches": str(stop) if stop else ""}})
        dispatch_deadline = min(self.deadline, self.clock() + 180)
        while self.clock() < dispatch_deadline:
            selected = correlate_runs(self.services.runs(), before, head, dispatched_at)
            if selected:
                self.report["lastDispatchedRunId"] = selected
                self.save()
                return selected
            self.bounded_sleep(5)
        raise AcceptanceError("dispatched-run-not-observed")

    def phase(self, name: str, run_id: int, *, completed: bool, expected_job: str | None = None) -> dict[str, Any]:
        phase = {"name": name, "runId": run_id, "startedAt": stamp(), "status": "waiting"}
        self.report["phases"].append(phase)
        self.save()
        print(json.dumps({"event": "acceptance-phase", "name": name, "runId": run_id}), flush=True)
        try:
            run = self.wait_run(run_id)
            phase["workflowConclusion"] = run.get("conclusion")
            phase["workflowDiagnostics"] = self.services.diagnostics(run_id)
            proof = self.services.proof(run)
            phase["proof"] = safe_proof(proof)
            self.save()
            require(run.get("conclusion") == "success", "scanner-workflow-" + (run.get("conclusion") if run.get("conclusion") in {"failure", "cancelled", "timed_out", "skipped"} else "unsuccessful"))
            job = validate_proof(proof, self.config, completed)
            if expected_job:
                require(proof["jobId"] == expected_job, "resume-or-initial-job-changed")
            if name in {"initial-full", "third-full"}:
                require(proof["rawEvidenceAcknowledged"] >= job["total"], "full-raw-persistence-coverage-missing")
            snapshot = self.snapshot(proof["jobId"], completed)
            require(snapshot["job"]["completed"] == job["completed"], "public-proof-checkpoint-mismatch")
            phase.update({"status": "verified", "finishedAt": stamp(), "publicSnapshot": snapshot})
            if completed:
                require(proof["jobId"] not in self.report["completedFullJobs"], "full-job-counted-twice")
                self.report["completedFullJobs"].append(proof["jobId"])
        except Exception:
            phase["status"] = "failed"
            self.save()
            raise
        self.save()
        return phase

    def run(self) -> dict[str, Any]:
        try:
            initial = self.phase("initial-full", self.config["initialRunId"], completed=True, expected_job=self.config["initialJobId"])
            interrupted = self.phase("deliberate-interruption", self.dispatch(True, 1), completed=False)
            interrupted_id = interrupted["proof"]["jobId"]
            require(interrupted_id != initial["proof"]["jobId"], "interruption-did-not-start-new-job")
            self.report["interruptionVerified"] = True
            self.save()
            resumed = self.phase("resume-same-job", self.dispatch(False, 0), completed=True, expected_job=interrupted_id)
            retained = interrupted["publicSnapshot"]["terminalTaskCheckpoints"]
            final = resumed["publicSnapshot"]["terminalTaskCheckpoints"]
            require(bool(retained) and all(final.get(key) == value for key, value in retained.items()), "completed-checkpoints-not-preserved")
            require(resumed["proof"]["rawEvidenceAcknowledged"] + interrupted["proof"]["rawEvidenceAcknowledged"] >= self.config["expectedListings"], "resumed-raw-persistence-coverage-missing")
            self.report["sameJobResumed"] = True
            self.report["retainedTerminalTasks"] = len(retained)
            self.save()
            third = self.phase("third-full", self.dispatch(True, 0), completed=True)
            require(third["proof"]["jobId"] != interrupted_id and len(self.report["completedFullJobs"]) == 3, "three-distinct-full-jobs-not-proven")
            self.report.update(status="passed", finishedAt=stamp())
        except Exception as error:
            self.report.update(status="failed", finishedAt=stamp(), reason=str(error) if isinstance(error, AcceptanceError) else "unexpected-controller-error")
            self.save()
            raise
        self.save()
        return self.report


def read_config(path: Path) -> dict[str, Any]:
    config = parse_json(path.read_bytes())
    require(isinstance(config, dict) and config.get("schemaVersion") == 1 and isinstance(config.get("enabled"), bool), "invalid-acceptance-config")
    require(isinstance(config.get("requestId"), str) and bool(re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", config["requestId"])), "invalid-request-id")
    for key in ("initialRunId", "expectedListings", "expectedRetailers", "minimumCatalogListings"):
        require(integer(config.get(key)) and config[key] > 0, "invalid-acceptance-count-config")
    require(isinstance(config.get("initialJobId"), str) and bool(re.fullmatch(r"[a-f0-9-]{36}", config["initialJobId"])), "invalid-initial-job-id")
    require(isinstance(config.get("expectedDataVersion"), str) and bool(re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", config["expectedDataVersion"])), "invalid-data-version-config")
    for key, low, high, default in (("pollSeconds", 5, 60, 20), ("phaseTimeoutSeconds", 60, 4500, 4500), ("totalTimeoutSeconds", 60, 13_500, 13_500)):
        value = config.get(key, default)
        require(integer(value) and low <= value <= high, "invalid-deadline-config")
    return config


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("runner/acceptance-config.json"))
    parser.add_argument("--output", type=Path, default=Path("acceptance-proof"))
    args = parser.parse_args()
    try:
        config = read_config(args.config)
        if not config["enabled"]:
            print(json.dumps({"event": "acceptance-disabled"}))
            return 0
        require(os.environ.get("GITHUB_REPOSITORY") == REPO, "unexpected-controller-repository")
        report = Controller(Services(os.environ.get("GITHUB_TOKEN", "")), config, args.output).run()
        print(json.dumps({"event": "acceptance-finished", "status": report["status"], "completedFullJobs": len(report["completedFullJobs"]), "interruptionVerified": report["interruptionVerified"], "sameJobResumed": report["sameJobResumed"]}))
        return 0
    except Exception as error:
        print(json.dumps({"event": "acceptance-failed", "reason": str(error) if isinstance(error, AcceptanceError) else "unexpected-controller-error"}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
