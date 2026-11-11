import asyncio
import base64
import json
import time
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from runner.worker import AUDIENCE, SITE, Api, Auth, Collection, Collector, Worker, arguments, observation_chunks, retry_delay


def token(expiry):
    payload = base64.urlsafe_b64encode(json.dumps({'exp': expiry}).encode()).decode().rstrip('=')
    return 'header.' + payload + '.signature'


class ReaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_oidc_audience_refresh_and_token_destination(self):
        calls = []
        minted = [token(time.time() + 30), token(time.time() + 3600)]
        async def handle(request):
            calls.append(request)
            if request.url.host == 'pipelines.actions.githubusercontent.com':
                self.assertEqual(request.url.params['audience'], AUDIENCE)
                return httpx.Response(200, json={'value': minted.pop(0)})
            self.assertEqual(str(request.url).split('/api/')[0], SITE)
            body = json.loads(request.content)
            self.assertEqual(body['runnerId'], 'runner-test')
            return httpx.Response(200, json={'ok': True})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle), follow_redirects=False) as client:
            auth = Auth(client, {'ACTIONS_ID_TOKEN_REQUEST_URL': 'https://pipelines.actions.githubusercontent.com/token?api-version=2', 'ACTIONS_ID_TOKEN_REQUEST_TOKEN': 'unit-test-only'})
            api = Api(client, auth, 'runner-test')
            await api.call('heartbeat', {'jobId': 'job'})
            await api.call('release', {'jobId': 'job', 'reason': 'interrupted'})
        self.assertEqual(sum(r.url.host == 'pipelines.actions.githubusercontent.com' for r in calls), 2)
        self.assertNotEqual(calls[1].headers['Authorization'], calls[3].headers['Authorization'])

    async def test_api_does_not_follow_redirect_or_send_auth_to_another_host(self):
        calls = []
        async def handle(request):
            calls.append(request)
            return httpx.Response(302, headers={'Location': 'https://other.example/steal'})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle), follow_redirects=False) as client:
            api = Api(client, Auth(client, {'AUSPEX_RUNNER_BEARER': 'unit-test-only'}), 'runner-test')
            with self.assertRaises(RuntimeError):
                await api.call('claim', {})
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].url.host, 'auspex-mini-prices-k7m2.devkorddurham.chatgpt.site')

    async def test_collection_keeps_identity_mismatch_raw_for_server_matching(self):
        async def handle(request):
            self.assertNotIn('authorization', request.headers)
            return httpx.Response(200, json={'currency': 'USD'} if request.url.path == '/meta.json' else {'id': 'DIFFERENT', 'title': 'Actual source title'})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            collector = Collector(client, attempts=1, shop_interval=0)
            result = await collector.collect({'id': 'registered', 'shopId': 'shop', 'url': 'https://merchant.example/products/kit.js', 'currencyUrl': 'https://merchant.example/meta.json'})
        self.assertEqual(result.observation['collection']['status'], 'collected')
        self.assertEqual(result.observation['product']['id'], 'DIFFERENT')
        self.assertEqual(result.shop_check['currency'], 'USD')

    async def test_body_deadline_is_absolute(self):
        async def handle(_request):
            await asyncio.sleep(1)
            return httpx.Response(200, json={})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            collector = Collector(client, timeout=.01, attempts=1, shop_interval=0)
            started = time.monotonic()
            result = await collector.fetch('https://merchant.example/slow', 'shop')
        self.assertEqual(result['status'], 'timeout')
        self.assertLess(time.monotonic() - started, .5)

    async def test_malformed_json_retains_raw_failure_evidence(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, text='{bad json'))) as client:
            result = await Collector(client, attempts=1, shop_interval=0).fetch('https://merchant.example/product', 'shop')
        self.assertEqual(result['status'], 'json-error')
        self.assertEqual(result['rawResponseText'], '{bad json')
        self.assertEqual(len(result['responseSha256']), 64)

    async def test_nonfinite_json_is_preserved_as_parse_failure_instead_of_crashing_batch(self):
        for body in [b'{"price": NaN}', b'{"price": 1e9999}']:
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, content=body))) as client:
                result = await Collector(client, attempts=1, shop_interval=0).fetch('https://merchant.example/product', 'shop')
            self.assertEqual(result['status'], 'json-error')
            self.assertIn('price', result['rawResponseText'])

    async def test_access_denial_is_not_retried(self):
        calls = 0
        async def handle(_request):
            nonlocal calls
            calls += 1
            return httpx.Response(403)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            result = await Collector(client, attempts=3, shop_interval=0).fetch('https://merchant.example/product', 'shop')
        self.assertEqual(calls, 1)
        self.assertEqual(result['httpStatus'], 403)

    async def test_retry_after_is_honored_and_bounded(self):
        calls = 0
        async def handle(_request):
            nonlocal calls
            calls += 1
            return httpx.Response(429, headers={'Retry-After': '2'}) if calls == 1 else httpx.Response(200, json={})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            with patch('runner.worker.asyncio.sleep', new_callable=AsyncMock) as sleep:
                result = await Collector(client, attempts=2, shop_interval=0).fetch('https://merchant.example/product', 'shop')
                sleep.assert_any_await(2.0)
        self.assertEqual(result['status'], 'collected')
        self.assertEqual(result['attempts'], 2)
        self.assertEqual(retry_delay(0, '99999'), 60)

    async def test_one_shop_has_only_one_active_request(self):
        active = 0
        peak = 0
        async def handle(request):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(.005)
            active -= 1
            return httpx.Response(200, json={'currency': 'USD'} if request.url.path == '/meta.json' else {'id': request.url.path})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            collector = Collector(client, attempts=1, shop_interval=0)
            await asyncio.gather(*(collector.collect({'id': str(i), 'shopId': 'same-shop', 'url': f'https://merchant.example/p/{i}', 'currencyUrl': 'https://merchant.example/meta.json'}) for i in range(4)))
        self.assertEqual(peak, 1)

    async def test_interruption_releases_then_resumes_same_job_without_repeating_completed_tasks(self):
        api = FakeApi()
        first = await Worker(api, FakeCollector(), stop_after_batches=1).run()
        self.assertEqual(first['status'], 'interrupted')
        self.assertEqual(first['rawEvidenceAcknowledged'], 1)
        self.assertEqual(first['rawProductEvidenceAcknowledged'], 1)
        self.assertEqual(api.done, ['a'])
        second = await Worker(api, FakeCollector()).run()
        self.assertEqual(second['jobId'], first['jobId'])
        self.assertEqual(second['status'], 'completed')
        self.assertEqual(api.done, ['a', 'b'])
        self.assertEqual(api.releases, ['interrupted', 'completed'])
        self.assertNotIn('cancel', api.actions)

    async def test_runtime_budget_releases_without_cancelling_or_losing_pending_work(self):
        class SlowCollector:
            async def collect(self, _source):
                await asyncio.sleep(1)
        api = FakeApi()
        result = await Worker(api, SlowCollector(), runtime=.01).run()
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(api.done, [])
        self.assertEqual(api.releases, ['interrupted'])

    async def test_idle_poll_never_forces_or_creates_job(self):
        api = FakeApi(idle=True)
        result = await Worker(api, FakeCollector()).run()
        self.assertEqual(result['status'], 'idle')
        self.assertEqual(api.actions, ['claim'])
        self.assertFalse(api.claim_payload['forceFull'])

    def test_raw_persistence_proof_is_distinct_from_matched_catalog_counts(self):
        worker = Worker(FakeApi(), FakeCollector())
        results = [Collection({'id': 'a', 'collection': {'status': 'collected'}}, None),
                   Collection({'id': 'b', 'collection': {'status': 'collected'}}, None),
                   Collection({'id': 'c', 'collection': {'status': 'http-error'}}, None)]
        worker.acknowledge_raw({'rawPersisted': {'collected': 2, 'collectionErrors': 1},
                                'job': {'listingsDiscovered': 2, 'listingsPersisted': 1, 'rejectedListings': 1}}, results)
        self.assertEqual(worker.proof['rawEvidenceAcknowledged'], 3)
        self.assertEqual(worker.proof['rawProductEvidenceAcknowledged'], 2)
        self.assertEqual(worker.proof['rawFailureEvidenceAcknowledged'], 1)
        self.assertEqual(worker.proof['persistenceAcks'], 1)
        with self.assertRaises(RuntimeError):
            worker.acknowledge_raw({'rawPersisted': {'collected': 1, 'collectionErrors': 0}}, results)

    def test_push_run_config_and_scheduled_default_are_distinct(self):
        self.assertFalse(arguments([]).force_full)
        self.assertIsNone(arguments([]).stop_after_batches)
        self.assertIsNone(arguments(['--stop-after-batches', '0']).stop_after_batches)
        self.assertTrue(arguments(['--force-full', '--stop-after-batches', '1']).force_full)
        with self.assertRaises(SystemExit):
            arguments(['--max-runtime-seconds', '3601'])

    def test_observe_payloads_are_chunked_without_changing_raw_products(self):
        results = [Collection({'id': str(i), 'product': {'description': 'x' * 1800}}, None) for i in range(3)]
        chunks = observation_chunks(results, limit=3500)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0][0].observation, results[0].observation)


class FakeCollector:
    async def collect(self, source):
        return Collection({'id': source['id'], 'product': {'id': source['id']}, 'fetchedAt': '2026-11-11T00:00:00Z', 'collection': {'status': 'collected'}}, {'id': 'shop', 'currency': 'USD', 'checkedAt': '2026-11-11T00:00:00Z'})


class FakeApi:
    def __init__(self, idle=False):
        self.runner_id = 'runner-test'
        self.done = []
        self.releases = []
        self.actions = []
        self.idle = idle

    def job(self):
        return {'id': 'same-job', 'status': 'completed' if len(self.done) == 2 else 'running', 'completed': len(self.done), 'total': 2, 'scope': {'kind': 'full'}}

    async def call(self, action, payload=None):
        self.actions.append(action)
        if action == 'claim':
            self.claim_payload = payload
            return {'claimed': not self.idle, 'busy': False, 'job': None if self.idle else self.job(), 'runner': {'id': self.runner_id}}
        if action == 'tasks':
            pending = [x for x in ['a', 'b'] if x not in self.done]
            return {'job': self.job(), 'sources': [{'id': pending[0]}] if pending else []}
        if action == 'observe':
            # Raw collection persistence is the server contract; matching is not
            # implemented in this reader and cannot abort another source here.
            for item in payload['observations']:
                if item['id'] not in self.done:
                    self.done.append(item['id'])
            return {'job': self.job(), 'rawPersisted': {'collected': len(payload['observations']), 'collectionErrors': 0}}
        if action == 'release':
            self.releases.append(payload['reason'])
            return {'job': self.job()}
        return {'ok': True}


if __name__ == '__main__':
    unittest.main()
