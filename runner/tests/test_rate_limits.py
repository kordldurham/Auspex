import unittest,asyncio,httpx
from unittest.mock import AsyncMock
from runner.worker import Collector,retry_after_seconds
class RateLimits(unittest.IsolatedAsyncioTestCase):
 async def test_429_is_checkpointed_without_premature_retry(self):
  calls=[]
  async def handler(r):calls.append(r);return httpx.Response(429,headers={'Retry-After':'120'},json={'error':'rate limit'})
  async with httpx.AsyncClient(transport=httpx.MockTransport(handler))as client:
   result=await Collector(client,shop_interval=0).fetch('https://shop.test/product.js','shop')
  self.assertEqual(len(calls),1);self.assertEqual(result['retryAfter'],'120');self.assertEqual(result['httpStatus'],429)
 async def test_currency_failure_isolated_and_reused_until_cooldown(self):
  async with httpx.AsyncClient()as client:
   c=Collector(client,shop_interval=0);c.fetch=AsyncMock(return_value={'status':'http-error','httpStatus':503,'error':'Unavailable'})
   source={'id':'one','shopId':'shop','url':'https://shop.test/products/one.js','currencyUrl':'https://shop.test/meta.json'}
   first=await c.collect(source);second=await c.collect({**source,'id':'two'});self.assertEqual(c.fetch.await_count,1);self.assertTrue(second.observation['collection']['reusedCurrencyFailure']);self.assertIn('error',first.observation)
 def test_long_retry_after_is_not_shortened(self):self.assertEqual(retry_after_seconds('120'),120)
