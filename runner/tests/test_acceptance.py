import copy
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import zipfile

import httpx

from runner.acceptance import (API_PREFIX, REPO_ID, OWNER_ID, WORKFLOW_PATH, AcceptanceError,
                               Controller, Services, correlate_runs, read_config, safe_proof,
                               task_checkpoints, validate_proof)

CONFIG = {'schemaVersion': 1, 'enabled': True, 'requestId': 'unit-test', 'initialRunId': 1,
          'initialJobId': '11111111-1111-1111-1111-111111111111', 'expectedListings': 3242,
          'expectedRetailers': 36, 'minimumCatalogListings': 3302,
          'expectedDataVersion': 'unit-v1', 'pollSeconds': 5, 'phaseTimeoutSeconds': 60,
          'totalTimeoutSeconds': 600}
JOB_IDS = [CONFIG['initialJobId'], '22222222-2222-2222-2222-222222222222',
           '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333']


def proof(run_id=1, *, completed=True, raw=None):
    n = 3242 if completed else 32
    raw = n if raw is None else raw
    return {'status': 'completed' if completed else 'interrupted', 'jobId': JOB_IDS[run_id-1],
            'releaseAcknowledged': True, 'batches': 102 if completed else 1, 'elapsedSeconds': 10,
            'collected': raw, 'collectionFailures': 0, 'persistenceAcks': 1,
            'rawEvidenceAcknowledged': raw, 'rawProductEvidenceAcknowledged': raw,
            'rawFailureEvidenceAcknowledged': 0,
            'job': {'id': JOB_IDS[run_id-1], 'status': 'partial' if completed else 'running',
                    'total': 3242, 'completed': n, 'listingsChecked': n, 'observedListings': n,
                    'verifiedOffers': 20, 'listingsDiscovered': n, 'listingsPersisted': n,
                    'rejectedListings': 0, 'retailersAttempted': 36, 'retailersSucceeded': 35,
                    'scope': {'kind': 'full', 'selectedListings': 3242, 'selectedRetailers': 36,
                              'catalogListings': 3242, 'catalogRetailers': 36}}}


class AcceptanceTests(unittest.TestCase):
    def test_full_partial_with_individual_failures_is_honest_completed_execution(self):
        p = proof()
        p['job'].update(listingsPersisted=3200, observedListings=3200, listingsDiscovered=3210, rejectedListings=10)
        self.assertEqual(validate_proof(p, CONFIG, True)['status'], 'partial')
        p['job']['completed'] = 3241
        with self.assertRaisesRegex(AcceptanceError, 'incomplete-terminal'):
            validate_proof(p, CONFIG, True)

    def test_research_empty_or_incomplete_evidence_cannot_pass(self):
        for mutation in [lambda p:p['job'].update(verifiedOffers=0),
                         lambda p:p['job'].update(listingsPersisted=0),
                         lambda p:p.update(releaseAcknowledged=False),
                         lambda p:p['job']['scope'].update(kind='targeted'),
                         lambda p:p['job'].update(retailersAttempted=35),
                         lambda p:p.update(rawEvidenceAcknowledged=True)]:
            p=proof();mutation(p)
            with self.assertRaises(AcceptanceError): validate_proof(p,CONFIG,True)

    def test_interruption_can_have_no_eligible_offer_but_requires_raw_checkpoint(self):
        p=proof(2,completed=False);p['job']['verifiedOffers']=0
        self.assertEqual(validate_proof(p,CONFIG,False)['completed'],32)
        p['rawEvidenceAcknowledged']=0
        with self.assertRaises(AcceptanceError):validate_proof(p,CONFIG,False)

    def test_run_correlation_excludes_push_schedule_old_and_wrong_head(self):
        def run(n,event='workflow_dispatch',head='head'):
            return {'id':n,'event':event,'head_branch':'main','head_sha':head,'created_at':'2026-11-12T00:00:00Z'}
        since=dt.datetime(2026,11,12,tzinfo=dt.timezone.utc).timestamp()
        runs=[run(1),run(2,'push'),run(3,'schedule'),run(4,head='wrong'),run(5)]
        self.assertEqual(correlate_runs(runs,{1},'head',since),5)
        with self.assertRaisesRegex(AcceptanceError,'ambiguous'):
            correlate_runs(runs+[run(6)],{1},'head',since)

    def test_token_only_reaches_exact_repo_on_standard_https_port(self):
        calls=[]
        def handle(req):
            calls.append(req)
            return httpx.Response(200,json={'ok':True})
        with httpx.Client(transport=httpx.MockTransport(handle)) as client:
            service=Services('DO-NOT-LOG-TOKEN',client)
            for target in ['https://other.example'+API_PREFIX+'actions/runs',
                           'https://api.github.com:444'+API_PREFIX+'actions/runs',
                           'https://api.github.com/repos/other/repo/actions/runs']:
                with self.assertRaises(AcceptanceError):service.request('GET',target,github=True)
            service.github(API_PREFIX+'actions/runs')
            service.public('/api/scans/status')
        self.assertEqual(len(calls),2)
        self.assertEqual(calls[0].headers['authorization'],'Bearer DO-NOT-LOG-TOKEN')
        self.assertNotIn('authorization',calls[1].headers)

    def test_artifact_redirect_strips_github_authorization(self):
        calls=[]
        def handle(req):
            calls.append(req)
            if req.url.host=='api.github.com':return httpx.Response(302,headers={'location':'https://results.blob.core.windows.net/proof?signature=DO-NOT-LOG'})
            return httpx.Response(200,content=b'zip')
        with httpx.Client(transport=httpx.MockTransport(handle)) as client:
            self.assertEqual(Services('TOKEN',client).github(API_PREFIX+'actions/artifacts/1/zip',binary=True),b'zip')
        self.assertIn('authorization',calls[0].headers)
        self.assertNotIn('authorization',calls[1].headers)

    def test_artifact_redirect_rejects_untrusted_host_and_port(self):
        for url in ['https://attacker.example/secret','https://results.blob.core.windows.net:444/proof']:
            calls=[]
            def handle(req):calls.append(req);return httpx.Response(302,headers={'location':url})
            with httpx.Client(transport=httpx.MockTransport(handle)) as client:
                with self.assertRaisesRegex(AcceptanceError,'unsafe-artifact'):
                    Services('TOKEN',client).github(API_PREFIX+'actions/artifacts/1/zip',binary=True)
            self.assertEqual(len(calls),1)

    def test_dispatch_is_never_retried_after_uncertain_transport_result(self):
        calls=[]
        def handle(req):calls.append(req);raise httpx.ReadTimeout('sensitive-body-must-not-leak')
        with httpx.Client(transport=httpx.MockTransport(handle)) as client:
            with self.assertRaisesRegex(AcceptanceError,'request-transport-failed'):
                Services('TOKEN',client).github(API_PREFIX+'actions/workflows/auspex-scanner.yml/dispatches',method='POST',payload={})
        self.assertEqual(len(calls),1)

    def test_artifact_zip_is_read_in_memory_and_requires_exact_member(self):
        for filename,success in [('runner-proof.json',True),('../runner-proof.json',False)]:
            stream=io.BytesIO()
            with zipfile.ZipFile(stream,'w') as archive:archive.writestr(filename,json.dumps(proof()))
            with patch.object(Services,'github',side_effect=[{'artifacts':[{'id':1,'name':'auspex-reader-proof-1-1','expired':False,'size_in_bytes':100}]},stream.getvalue()]):
                service=Services('TOKEN')
                if success:self.assertEqual(service.proof({'id':1})['jobId'],JOB_IDS[0])
                else:
                    with self.assertRaises(AcceptanceError):service.proof({'id':1})

    def test_safe_proof_omits_errors_urls_tokens_and_arbitrary_fields(self):
        p=proof();p.update(error='DO-NOT-LOG',token='SECRET',url='https://signed.example/?secret=SECRET')
        p['job'].update(diagnostics=[{'secret':'SECRET'}])
        self.assertNotIn('SECRET',json.dumps(safe_proof(p)))
        self.assertNotIn('DO-NOT-LOG',json.dumps(safe_proof(p)))

    def test_dispatch_payload_uses_blank_for_no_interruption(self):
        service=FakeServices()
        with tempfile.TemporaryDirectory() as folder:
            control=Controller(service,CONFIG,Path(folder))
            self.assertEqual(control.dispatch(False,0),2)
            self.assertEqual(service.dispatches[-1]['inputs'],{'force_full':False,'stop_after_batches':''})
            self.assertEqual(control.dispatch(True,1),3)
            self.assertEqual(service.dispatches[-1]['inputs'],{'force_full':True,'stop_after_batches':'1'})

    def test_controller_proves_three_distinct_full_jobs_and_same_job_resume(self):
        service=FakeServices()
        with tempfile.TemporaryDirectory() as folder:
            control=Controller(service,CONFIG,Path(folder))
            with patch.object(control,'dispatch',side_effect=[2,3,4]):result=control.run()
            self.assertEqual(result['status'],'passed')
            self.assertEqual(len(result['completedFullJobs']),3)
            self.assertTrue(result['sameJobResumed'])
            self.assertEqual(result['retainedTerminalTasks'],32)
            self.assertNotIn('DO-NOT-LOG',Path(folder,'acceptance-report.json').read_text())

    def test_changed_or_repeated_completed_task_fails_resume_acceptance(self):
        service=FakeServices();service.repeat_completed=True
        with tempfile.TemporaryDirectory() as folder:
            control=Controller(service,CONFIG,Path(folder))
            with patch.object(control,'dispatch',side_effect=[2,3,4]):
                with self.assertRaisesRegex(AcceptanceError,'completed-checkpoints-not-preserved'):control.run()
            result=json.loads(Path(folder,'acceptance-report.json').read_text())
            self.assertEqual(result['status'],'failed')
            self.assertFalse(result['sameJobResumed'])

    def test_unexpected_active_job_never_joins_another_jobs_report(self):
        service=FakeServices();service.wrong_active=True
        with tempfile.TemporaryDirectory() as folder:
            control=Controller(service,CONFIG,Path(folder))
            with self.assertRaisesRegex(AcceptanceError,'public-active-job-changed'):control.run()
            self.assertEqual(control.report['completedFullJobs'],[])

    def test_request_config_is_bounded(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder,'config.json');path.write_text(json.dumps(CONFIG))
            self.assertEqual(read_config(path)['requestId'],'unit-test')
            bad={**CONFIG,'phaseTimeoutSeconds':4501};path.write_text(json.dumps(bad))
            with self.assertRaises(AcceptanceError):read_config(path)


class FakeServices:
    def __init__(self):
        self.current=1;self.dispatches=[];self.runs_list=[];self.repeat_completed=False;self.wrong_active=False
    def run(self,n):
        self.current=n
        return {'id':n,'status':'completed','conclusion':'success','path':WORKFLOW_PATH,'head_branch':'main','head_repository':{'id':REPO_ID,'owner':{'id':OWNER_ID}}}
    def proof(self,run):return proof(run['id'],completed=run['id']!=2,raw=3210 if run['id']==3 else None)
    def diagnostics(self,n):return [{'id':n,'conclusion':'success','steps':[]}]
    def runs(self):return self.runs_list[:]
    def github(self,path,method='GET',payload=None,**kwargs):
        if path.endswith('branches/main'):return {'commit':{'sha':'expectedhead'}}
        if method=='POST':
            self.dispatches.append(payload)
            n=len(self.dispatches)+1
            self.runs_list.append({'id':n,'event':'workflow_dispatch','head_branch':'main','head_sha':'expectedhead','created_at':dt.datetime.now(dt.timezone.utc).isoformat()})
            return None
        raise AssertionError('Unexpected fake GitHub call')
    def public(self,path):
        p=self.proof({'id':self.current});job=copy.deepcopy(p['job'])
        job['currentVersion']='unit-v1';job['pendingListings']=job['total']-job['completed']
        if self.current==2:job['status']='queued'
        if self.wrong_active:job['id']='99999999-9999-9999-9999-999999999999'
        coverage={'trackedListings':3242,'enabledRetailers':36,'totalListings':3302,'eligibleOffers':100,'reconciledKits':406}
        if path.endswith('/version'):return {'version':'unit-v1'}
        if path.endswith('/coverage'):return coverage
        if path.endswith('/status'):return {'job':job,'version':'unit-v1','runner':{'available':False}}
        if path.endswith('/report'):
            tasks=[{'id':str(i),'status':'success','checkedAt':'2026-11-12T00:00:00Z','attempts':1} for i in range(job['completed'])]
            if self.repeat_completed and self.current==3:tasks[0]['attempts']=2
            return {'job':job,'listings':tasks}
        raise AssertionError('Unexpected fake public call')


if __name__=='__main__':unittest.main()
