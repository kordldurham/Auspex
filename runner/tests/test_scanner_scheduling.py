import asyncio
import json
import unittest
from unittest.mock import patch

import httpx
from runner.worker import Collector, bbts_product, is_bbts_source, manual_bbts_enabled


def source(shop='shop', name='one'):
    return {'id': shop+'-'+name, 'shopId': shop, 'url': 'https://'+shop+'.test/products/'+name+'.js', 'currencyUrl': 'https://'+shop+'.test/meta.json'}


def bbts():
    return {'id': 'bbts-123', 'shopId': 'bbts', 'url': 'https://www.bigbadtoystore.com/Product/VariationDetails/123', 'sourceProductId': '456', 'handle': 'registered-kit'}


def markup(stock='InStock'):
    return '<script type="application/ld+json">'+json.dumps({'@type':'Product','productID':'456','name':'Combat Patrol: Test','manufacturer':{'name':'Games Workshop'},'offers':[{'sku':'123','price':'125.00','priceCurrency':'USD','availability':'https://schema.org/'+stock}]})+'</script>'


class SchedulingTests(unittest.IsolatedAsyncioTestCase):
    async def test_currency_singleflight_product_overlap_and_retailer_bound(self):
        active = peak = metadata = 0
        starts = []
        async def handle(request):
            nonlocal active, peak, metadata
            if request.url.path == '/meta.json':
                metadata += 1
                await asyncio.sleep(.01)
                return httpx.Response(200,json={'currency':'USD'})
            active += 1
            peak = max(peak,active)
            starts.append(asyncio.get_running_loop().time())
            await asyncio.sleep(.03)
            active -= 1
            return httpx.Response(200,json={'id':'observed'})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            c = Collector(client,shop_interval=.005,shop_concurrency=2)
            results = await asyncio.gather(*(c.collect(source(name=str(i))) for i in range(6)))
        self.assertEqual(metadata,1)
        self.assertEqual(peak,2)
        self.assertTrue(all(b-a >= .004 for a,b in zip(starts,starts[1:])))
        self.assertTrue(all('product' in r.observation for r in results))

    async def test_breaker_suspends_then_recovers_and_success_resets(self):
        calls = 0
        async def handle(request):
            nonlocal calls
            calls += 1
            return httpx.Response(503 if calls <= 3 else 200,json={'ok':True})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            c = Collector(client,attempts=1,shop_interval=0)
            for _ in range(3):
                await c.fetch('https://bad.test/p','bad')
            blocked = await c.fetch('https://bad.test/p','bad')
            self.assertEqual(blocked['status'],'circuit-open')
            self.assertEqual(calls,3)
            self.assertLessEqual(float(blocked['retryAfter']),120)
            c.suspended_until['bad'] = 0
            self.assertEqual((await c.fetch('https://bad.test/p','bad'))['status'],'collected')
            self.assertEqual(c.failures['bad'],0)

    async def test_slow_retailer_does_not_take_healthy_retailer_slots(self):
        slow_started,release = asyncio.Event(),asyncio.Event()
        async def handle(request):
            if request.url.host == 'slow.test':
                slow_started.set()
                await release.wait()
            return httpx.Response(200,json={'ok':True})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            c = Collector(client,concurrency=2,shop_concurrency=1,shop_interval=0)
            pending = [asyncio.create_task(c.fetch('https://slow.test/p','slow')) for _ in range(4)]
            await slow_started.wait()
            healthy = await asyncio.wait_for(c.fetch('https://healthy.test/p','healthy'),.3)
            self.assertEqual(healthy['status'],'collected')
            release.set()
            await asyncio.gather(*pending)

    async def test_bbts_default_exclusion_and_explicit_single_attempt(self):
        calls = []
        async def handle(request):
            calls.append(request)
            return httpx.Response(200,text=markup())
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            self.assertTrue(is_bbts_source({**bbts(),'shopId':'unexpected'}))
            result = await Collector(client).collect(bbts())
            self.assertEqual(result.observation['collection']['status'],'manual-required')
            self.assertEqual(calls,[])
            c = Collector(client,include_bbts=True,shop_interval=0)
            original = json.dumps(bbts(),sort_keys=True)
            result = await c.collect(bbts())
            await c.collect(bbts())
            self.assertEqual(len(calls),1)
            self.assertEqual(result.observation['id'],'bbts-123')
            self.assertEqual(result.observation['product']['id'],'456')
            self.assertEqual(result.observation['product']['price'],12500)
            self.assertEqual(original,json.dumps(bbts(),sort_keys=True))

    async def test_bbts_access_denied_has_no_retries_or_redirects(self):
        calls = 0
        async def handle(request):
            nonlocal calls
            calls += 1
            return httpx.Response(403,text='Access denied')
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            c = Collector(client,include_bbts=True,attempts=3)
            result = await c.collect(bbts())
            await c.collect(bbts())
        self.assertEqual(calls,1)
        self.assertIn('403',result.observation['error'])
        self.assertNotIn('product',result.observation)

    def test_bbts_stock_and_exact_variation(self):
        for stock,available in [('InStock',True),('PreOrder',False),('SoldOut',False)]:
            self.assertEqual(bbts_product(markup(stock),'123')['variants'][0]['available'],available)
        with self.assertRaises(ValueError):
            bbts_product(markup(),'999')

    def test_explicit_opt_in_only_for_manual_events(self):
        for event in ['schedule','push','pull_request']:
            self.assertFalse(manual_bbts_enabled(True,event))
        for event in ['', 'workflow_dispatch']:
            self.assertTrue(manual_bbts_enabled(True,event))
            self.assertFalse(manual_bbts_enabled(False,event))

    async def test_failed_retailer_leaves_healthy_collection_and_source_identity_intact(self):
        async def handle(request):
            if request.url.host == 'bad.test':
                return httpx.Response(503)
            return httpx.Response(200,json={'currency':'USD'} if request.url.path=='/meta.json' else {'id':'saved-listing'})
        original = source('healthy')
        snapshot = json.dumps(original,sort_keys=True)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            c = Collector(client,attempts=1,shop_interval=0)
            for _ in range(3):
                await c.fetch('https://bad.test/p','bad')
            failed,healthy = await asyncio.gather(c.collect(source('bad')),c.collect(original))
        self.assertIn('error',failed.observation)
        self.assertEqual(healthy.observation['id'],original['id'])
        self.assertEqual(healthy.observation['product']['id'],'saved-listing')
        self.assertEqual(json.dumps(original,sort_keys=True),snapshot)
