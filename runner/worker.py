"""Authenticated, checkpointed external reader; matching stays on the server.

Every successful observe acknowledgement is the persistence boundary. The reader
never marks a product compatible, available, or eligible itself. Collection
failures and malformed JSON are evidence too and are submitted separately.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import ipaddress
import json
import math
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

SITE = "https://auspex-mini-prices-k7m2.devkorddurham.chatgpt.site"
AUDIENCE = SITE + "/scanner"
MAX_BODY = 2_000_000
MAX_OBSERVE_BODY = 3_000_000
RETRYABLE = {408, 425, 429, 500, 502, 503, 504}


class ReaderError(RuntimeError):
    """A deliberately non-secret operational diagnostic."""


class RuntimeBudgetExpired(ReaderError):
    pass


def strict_json(raw: bytes) -> Any:
    def invalid_constant(_value: str) -> None:
        raise ValueError("Non-finite JSON constant")
    value = json.loads(raw, parse_constant=invalid_constant)
    stack = [value]
    while stack:
        part = stack.pop()
        if isinstance(part, float) and not math.isfinite(part):
            raise ValueError("JSON number exceeds finite supported range")
        if isinstance(part, dict):
            stack.extend(part.values())
        elif isinstance(part, list):
            stack.extend(part)
    return value


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(event: str, **values: Any) -> None:
    # Callers supply only proof counters/IDs, never tokens or merchant bodies.
    print(json.dumps({"event": event, "at": utc(), **values}), flush=True)


def retry_delay(attempt: int, retry_after: str | None = None) -> float:
    if retry_after:
        try:
            seconds = float(retry_after)
        except ValueError:
            try:
                seconds = parsedate_to_datetime(retry_after).timestamp() - time.time()
            except (TypeError, ValueError, OverflowError):
                seconds = 0
        if seconds > 0:
            return min(60.0, seconds)
    return min(30.0, 2 ** attempt + random.random())


def safe_merchant_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("A registered source must use a public HTTPS URL")
    if parsed.hostname.lower() == "localhost":
        raise ValueError("Local source URLs are not allowed")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        pass
    else:
        if not address.is_global:
            raise ValueError("Private-address sources are not allowed")
    return f"{parsed.scheme}://{parsed.netloc}"


class Auth:
    """OIDC minted per job, refreshed before expiry; static bearer is optional.

    JWT claims are NOT trusted here for authorization. exp is used only for
    caching. The Auspex backend verifies signature, issuer, audience and repo.
    """
    def __init__(self, client: httpx.AsyncClient, env: dict[str, str] | None = None):
        self.client = client
        self.env = dict(os.environ if env is None else env)
        self.value: str | None = None
        self.expires = 0.0
        self.lock = asyncio.Lock()

    async def token(self, force: bool = False) -> str:
        bearer = self.env.get("AUSPEX_RUNNER_BEARER")
        if bearer:
            return bearer
        async with self.lock:
            if not force and self.value and self.expires - time.time() > 60:
                return self.value
            url = self.env.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
            credential = self.env.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
            parsed = urlsplit(url)
            if (parsed.scheme != "https" or not parsed.hostname or
                    not (parsed.hostname == "pipelines.actions.githubusercontent.com" or
                         parsed.hostname.endswith(".actions.githubusercontent.com")) or
                    parsed.username or parsed.password or not credential):
                raise ReaderError("GitHub OIDC is unavailable; no external reader credential is configured")
            query = [(k, v) for k, v in parse_qsl(parsed.query) if k != "audience"]
            query.append(("audience", AUDIENCE))
            token_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ""))
            async with asyncio.timeout(20):
                response = await self.client.get(token_url, headers={"Authorization": "Bearer " + credential})
                if response.status_code != 200:
                    raise ReaderError("GitHub OIDC request failed with HTTP " + str(response.status_code))
                value = response.json().get("value")
            if not isinstance(value, str) or len(value.split(".")) != 3:
                raise ReaderError("GitHub returned an invalid OIDC token response")
            try:
                encoded = value.split(".")[1]
                expiry = float(json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))["exp"])
            except (KeyError, ValueError, TypeError):
                raise ReaderError("OIDC token has no usable expiry") from None
            self.value, self.expires = value, expiry
            return value


class Api:
    def __init__(self, client: httpx.AsyncClient, auth: Auth, runner_id: str):
        self.client, self.auth, self.runner_id = client, auth, runner_id
        self.deadline: float | None = None

    async def call(self, action: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if action not in {"status", "claim", "tasks", "observe", "heartbeat", "release"}:
            raise ValueError("Unknown runner action")
        path = "/api/runner/" + action
        url = SITE + path
        if payload is None and action == "tasks":
            url += "?" + urlencode({"runnerId": self.runner_id})
        if payload is not None:
            payload = {**payload, "runnerId": self.runner_id}
        attempts = 1 if action == "release" else 4
        for attempt in range(attempts):
            remaining = 15.0 if action == "release" else (self.deadline - time.monotonic() if self.deadline else 95.0)
            if remaining <= 0:
                raise RuntimeBudgetExpired("Reader execution budget expired; checkpoint remains resumable")
            try:
                async with asyncio.timeout(min(95.0, remaining)):
                    token = await self.auth.token(force=attempt > 0)
                    response = await self.client.request(
                        "GET" if payload is None else "POST", url,
                        headers={"Authorization": "Bearer " + token}, json=payload)
                    if response.status_code == 401 and attempt + 1 < attempts:
                        continue
                    if response.status_code in RETRYABLE and attempt + 1 < attempts:
                        await asyncio.sleep(min(retry_delay(attempt, response.headers.get("Retry-After")), max(0.0, self.deadline - time.monotonic()) if self.deadline else 60.0))
                        continue
                    if response.status_code != 200 and response.status_code != 202:
                        raise ReaderError("Runner API " + action + " returned HTTP " + str(response.status_code))
                    data = response.json()
                    if not isinstance(data, dict):
                        raise ReaderError("Runner API returned an invalid object")
                    return data
            except (httpx.TransportError, TimeoutError):
                if attempt + 1 >= attempts:
                    if self.deadline and time.monotonic() >= self.deadline and action != "release":
                        raise RuntimeBudgetExpired("Reader execution budget expired; checkpoint remains resumable") from None
                    raise ReaderError("Runner API " + action + " request timed out or failed") from None
                await asyncio.sleep(min(retry_delay(attempt), max(0.0, self.deadline - time.monotonic()) if self.deadline else 30.0))
        raise ReaderError("Runner API request exhausted retries")


@dataclass
class Collection:
    observation: dict[str, Any]
    shop_check: dict[str, Any] | None


class Collector:
    def __init__(self, client: httpx.AsyncClient, concurrency: int = 8,
                 timeout: float = 12, attempts: int = 3, shop_interval: float = 1):
        self.client = client
        self.global_limit = asyncio.Semaphore(concurrency)
        self.timeout, self.attempts, self.shop_interval = timeout, attempts, shop_interval
        self.shop_locks: dict[str, asyncio.Lock] = {}
        self.next_request: dict[str, float] = {}
        self.currencies: dict[str, tuple[float, dict[str, Any]]] = {}

    async def fetch(self, url: str, shop: str) -> dict[str, Any]:
        evidence: dict[str, Any] = {}
        for attempt in range(self.attempts):
            wait = self.next_request.get(shop, 0) - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self.next_request[shop] = time.monotonic() + self.shop_interval
            started = time.monotonic()
            body = bytearray()
            try:
                async with self.global_limit:
                    async with asyncio.timeout(self.timeout):
                        async with self.client.stream("GET", url) as response:
                            async for part in response.aiter_bytes():
                                body.extend(part)
                                if len(body) > MAX_BODY:
                                    raise ValueError("Source response exceeded the collection size limit")
                            # JSON parsing stays inside the absolute deadline window.
                            raw = bytes(body)
                            evidence = {"httpStatus": response.status_code,
                                        "responseSha256": hashlib.sha256(raw).hexdigest(),
                                        "responseBytes": len(raw), "attempts": attempt + 1,
                                        "elapsedMs": round((time.monotonic() - started) * 1000)}
                            if response.status_code == 200:
                                try:
                                    product = strict_json(raw)
                                except (ValueError, UnicodeDecodeError, RecursionError):
                                    evidence.update(status="json-error", error="Source returned malformed JSON",
                                                    rawResponseText=raw[:65536].decode("utf-8", errors="replace"),
                                                    rawResponseTruncated=len(raw) > 65536)
                                else:
                                    return {**evidence, "status": "collected", "value": product}
                            else:
                                evidence.update(status="http-error", error="Source HTTP " + str(response.status_code))
                                # Never follow a changed merchant destination or retry access denials.
                                if response.status_code not in RETRYABLE:
                                    return evidence
                            after = response.headers.get("Retry-After")
                if attempt + 1 < self.attempts:
                    await asyncio.sleep(retry_delay(attempt, after))
            except (httpx.TransportError, TimeoutError, ValueError) as error:
                evidence = {"status": "timeout" if isinstance(error, TimeoutError) else "collection-error",
                            "error": "Source collection deadline exceeded" if isinstance(error, TimeoutError)
                            else "Source response too large" if isinstance(error, ValueError)
                            else "Source network request failed", "attempts": attempt + 1,
                            "capturedBytes": len(body), "elapsedMs": round((time.monotonic() - started) * 1000)}
                if attempt + 1 < self.attempts:
                    await asyncio.sleep(retry_delay(attempt))
        return evidence

    async def collect(self, source: dict[str, Any]) -> Collection:
        identity = str(source.get("id", "invalid-task")) if isinstance(source, dict) else "invalid-task"
        base = {"id": identity, "fetchedAt": utc()}
        try:
            shop = str(source["shopId"])
            if safe_merchant_url(source["url"]) != safe_merchant_url(source["currencyUrl"]):
                raise ValueError("Product and currency metadata must use the same registered shop origin")
            async with self.shop_locks.setdefault(shop, asyncio.Lock()):
                cached = self.currencies.get(shop)
                if not cached or time.monotonic() - cached[0] > 240:
                    metadata = await self.fetch(source["currencyUrl"], shop)
                    if metadata.get("status") != "collected":
                        return Collection({**base, "collection": {**metadata, "stage": "currency"},
                                           "error": "Retailer currency could not be read", "status": metadata.get("httpStatus", 503)}, None)
                    value = metadata["value"]
                    check = {"id": shop, "currency": value.get("currency") if isinstance(value, dict) else None,
                             "checkedAt": utc(), "responseSha256": metadata["responseSha256"]}
                    self.currencies[shop] = (time.monotonic(), check)
                else:
                    check = cached[1]
                if check["currency"] != "USD":
                    return Collection({**base, "collection": {"status": "currency-unverified", "stage": "currency"},
                                       "error": "Retailer currency is not verified USD", "status": 400}, check)
                result = await self.fetch(source["url"], shop)
                observation = {"id": identity, "fetchedAt": utc(),
                               "collection": {k: v for k, v in result.items() if k != "value"}}
                if result.get("status") == "collected":
                    observation["product"] = result["value"]
                else:
                    observation.update(error=result.get("error", "Source collection failed"),
                                       status=result.get("httpStatus", 503))
                return Collection(observation, check)
        except (KeyError, TypeError, ValueError):
            return Collection({**base, "collection": {"status": "invalid-task"},
                               "error": "Registered source task has invalid collection coordinates", "status": 400}, None)


def observation_chunks(results: list[Collection], limit: int = MAX_OBSERVE_BODY) -> list[list[Collection]]:
    chunks: list[list[Collection]] = []
    current: list[Collection] = []
    size = 1024
    for result in results:
        encoded = len(json.dumps(result.observation, ensure_ascii=True).encode()) + 512
        if current and size + encoded > limit:
            chunks.append(current)
            current, size = [], 1024
        if encoded > limit:
            # This can occur after JSON escaping; retain failure evidence, never send a misleading product.
            result = Collection({"id": result.observation["id"], "fetchedAt": result.observation["fetchedAt"],
                                 "collection": {"status": "payload-too-large"},
                                 "error": "Raw observation cannot fit a persistence request", "status": 400}, result.shop_check)
            encoded = 1024
        current.append(result)
        size += encoded
    if current:
        chunks.append(current)
    return chunks


class Worker:
    def __init__(self, api: Api, collector: Collector, runtime: float = 3600,
                 stop_after_batches: int | None = None, force_full: bool = False):
        self.api, self.collector = api, collector
        self.runtime, self.stop_after_batches, self.force_full = runtime, stop_after_batches, force_full
        self.stop = asyncio.Event()
        self.job: dict[str, Any] | None = None
        self.batches = 0
        self.proof = {"collected": 0, "collectionFailures": 0, "persistenceAcks": 0}

    async def heartbeat(self) -> None:
        while not self.stop.is_set():
            await asyncio.sleep(25)
            if self.job:
                await self.api.call("heartbeat", {"jobId": self.job["id"]})

    async def run(self) -> dict[str, Any]:
        started = time.monotonic()
        self.api.deadline = started + self.runtime
        claim = await self.api.call("claim", {"forceFull": self.force_full})
        if not claim.get("claimed") or not claim.get("job"):
            outcome = {"status": "busy" if claim.get("busy") else "idle", "batches": 0}
            emit("reader-exit", **outcome)
            return outcome
        self.job = claim["job"]
        returned_id = (claim.get("runner") or {}).get("id")
        if returned_id and returned_id != self.api.runner_id:
            raise ReaderError("Claim returned a different runner identity")
        emit("reader-claimed", jobId=self.job["id"], runnerId=self.api.runner_id,
             scope=self.job.get("scope"), forceFull=self.force_full)
        heart = asyncio.create_task(self.heartbeat())
        reason = "interrupted"
        release_ack = False
        try:
            while not self.stop.is_set() and time.monotonic() - started < self.runtime:
                if heart.done():
                    heart.result()  # Lost authentication/heartbeat must stop writes.
                tasks = await self.api.call("tasks")
                self.job = tasks.get("job") or self.job
                if self.job.get("status") == "cancelled":
                    reason = "cancelled"
                    break
                sources = tasks.get("sources") or []
                if self.job.get("status") == "timed-out":
                    emit("reader-job-deadline", jobId=self.job["id"], checkpointPreserved=True)
                    break
                completed, total = self.job.get("completed", 0), self.job.get("total", 0)
                if not sources:
                    if self.job.get("status") == "failed" and completed < total:
                        break
                    if total and completed >= total:
                        reason = "completed"
                        break
                    await asyncio.sleep(3)
                    continue
                async with asyncio.timeout(max(.001, started + self.runtime - time.monotonic())):
                    results = await asyncio.gather(*(self.collector.collect(source) for source in sources))
                for chunk in observation_chunks(results):
                    payload = {"jobId": self.job["id"], "observations": [r.observation for r in chunk],
                               "shopChecks": list({r.shop_check["id"]: r.shop_check for r in chunk if r.shop_check}.values())}
                    ack = await self.api.call("observe", payload)
                    self.job = ack.get("job") or self.job
                    self.proof["persistenceAcks"] += 1
                    self.proof["collected"] += sum(r.observation.get("collection", {}).get("status") == "collected" for r in chunk)
                    self.proof["collectionFailures"] += sum(r.observation.get("collection", {}).get("status") != "collected" for r in chunk)
                self.batches += 1
                emit("reader-checkpoint", jobId=self.job["id"], batch=self.batches,
                     completed=self.job.get("completed"), total=self.job.get("total"),
                     eligible=self.job.get("verifiedOffers"), **self.proof)
                if self.stop_after_batches and self.batches >= self.stop_after_batches:
                    break
            if self.job and self.job.get("total") and self.job.get("completed", 0) >= self.job["total"]:
                reason = "completed"
        except (RuntimeBudgetExpired, TimeoutError):
            reason = "interrupted"
            emit("reader-runtime-budget", jobId=self.job["id"], checkpointPreserved=True)
        finally:
            self.stop.set()
            heart.cancel()
            await asyncio.gather(heart, return_exceptions=True)
            if self.job:
                try:
                    await self.api.call("release", {"jobId": self.job["id"], "reason": reason})
                    release_ack = True
                except ReaderError as error:
                    emit("reader-release-unconfirmed", jobId=self.job["id"], error=str(error), leaseMustExpire=True)
        outcome = {"status": reason, "jobId": self.job["id"], "runnerId": self.api.runner_id,
                   "batches": self.batches, "elapsedSeconds": round(time.monotonic() - started),
                   "releaseAcknowledged": release_ack,
                   "job": {key: self.job.get(key) for key in ["id", "status", "scope", "total", "completed", "listingsChecked", "observedListings", "verifiedOffers", "rawCollected", "rawPersisted", "matchedListings", "rejectedListings", "retailersAttempted", "retailersSucceeded", "lastCheckpoint"]}, **self.proof}
        emit("reader-exit", status=reason, jobId=self.job["id"], batches=self.batches,
             elapsedSeconds=outcome["elapsedSeconds"], **self.proof)
        return outcome


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force-full", action="store_true", help="Explicitly enqueue a full acceptance run")
    parser.add_argument("--stop-after-batches", type=int)
    parser.add_argument("--max-runtime-seconds", type=int, default=3600)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--run-config", type=Path)
    parser.add_argument("--proof-file", type=Path)
    args = parser.parse_args(argv)
    if args.run_config:
        config = json.loads(args.run_config.read_text())
        if config.get("schemaVersion") != 1 or not isinstance(config.get("forceFull", False), bool):
            parser.error("Invalid run configuration")
        args.force_full = args.force_full or config.get("forceFull", False)
        if args.stop_after_batches is None:
            args.stop_after_batches = config.get("stopAfterBatches")
    if args.stop_after_batches == 0 and not isinstance(args.stop_after_batches, bool):
        args.stop_after_batches = None
    if (not 1 <= args.max_runtime_seconds <= 3600 or not 1 <= args.concurrency <= 16 or
            args.stop_after_batches is not None and (isinstance(args.stop_after_batches, bool) or
            not isinstance(args.stop_after_batches, int) or args.stop_after_batches < 1)):
        parser.error("Runtime must be1–3600 seconds, concurrency1–16, interruption batches positive")
    return args


async def async_main(args: argparse.Namespace) -> int:
    limits = httpx.Limits(max_connections=max(args.concurrency + 4, 12), max_keepalive_connections=12)
    async with httpx.AsyncClient(follow_redirects=False, timeout=20, limits=limits,
                                 headers={"User-Agent": "AuspexMarketReader/1.0"}) as client:
        runner_id = "gh-" + os.environ.get("GITHUB_RUN_ID", "portable") + "-" + uuid.uuid4().hex
        api = Api(client, Auth(client), runner_id)
        worker = Worker(api, Collector(client, args.concurrency), args.max_runtime_seconds,
                        args.stop_after_batches, args.force_full)
        loop = asyncio.get_running_loop()
        for name in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(name, worker.stop.set)
            except NotImplementedError:
                pass
        try:
            outcome = await worker.run()
        except Exception as error:
            outcome = {"status": "failed", "runnerId": runner_id,
                       "jobId": worker.job.get("id") if worker.job else None,
                       "errorType": type(error).__name__,
                       "error": str(error) if isinstance(error, ReaderError) else "Unexpected reader failure",
                       "batches": worker.batches, **worker.proof}
            if args.proof_file:
                args.proof_file.parent.mkdir(parents=True, exist_ok=True)
                args.proof_file.write_text(json.dumps(outcome, indent=2))
            raise
        if args.proof_file:
            args.proof_file.parent.mkdir(parents=True, exist_ok=True)
            args.proof_file.write_text(json.dumps(outcome, indent=2))
        return 0 if outcome.get("releaseAcknowledged", True) else 1


def main(argv: list[str] | None = None) -> int:
    try:
        return asyncio.run(async_main(arguments(argv)))
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        # No repr(error): HTTP/credential clients can include sensitive headers.
        emit("reader-failed", errorType=type(error).__name__, error=str(error) if isinstance(error, ReaderError) else "Unexpected reader failure")
        return 1
