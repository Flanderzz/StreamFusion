"""Exercise fork result selection and required-check reporting with fake GitHub responses."""

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('fork_ci_status', ROOT / '.github/scripts/fork-ci-status.py')
STATUS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STATUS)
PR = {
    'number': 42, 'state': 'open',
    'head': {'sha': 'abc123', 'ref': 'feature', 'repo': {'fork': True, 'full_name': 'contributor/StreamFusion'}},
    'base': {'repo': {'full_name': 'datafusion-contrib/StreamFusion'}},
}
RUN = {'id': 10, 'head_sha': 'abc123', 'head_branch': 'feature', 'event': 'push',
       'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/contributor/StreamFusion/actions/runs/10'}


class ForkCiStatusTest(unittest.TestCase):
    def result(self, runs, workflow='ci.yml', pr=None):
        with patch.object(STATUS, 'api', return_value={'workflow_runs': runs}) as api:
            result = STATUS.payload(pr or PR, workflow)
        return result, api

    def test_exact_head_and_branch_are_required(self):
        for change in ({'head_sha': 'old'}, {'head_branch': 'other'}, {'event': 'pull_request'}):
            with self.subTest(change=change):
                result, _ = self.result([{**RUN, **change}])
                self.assertEqual('action_required', result['conclusion'])
        result, _ = self.result([{**RUN, 'head_sha': 'old'}, RUN])
        self.assertEqual('success', result['conclusion'])
        self.assertEqual(RUN['html_url'], result['details_url'])

    def test_failure_cancel_and_skips_cannot_pass(self):
        for conclusion in ('failure', 'cancelled', 'skipped', 'neutral', 'timed_out', 'startup_failure', None):
            with self.subTest(conclusion=conclusion):
                result, _ = self.result([{**RUN, 'conclusion': conclusion}])
                self.assertEqual('failure', result['conclusion'])

    def test_active_runs_remain_pending(self):
        for state in ('queued', 'waiting', 'in_progress'):
            result, _ = self.result([{**RUN, 'status': state, 'conclusion': None}])
            self.assertEqual('in_progress' if state == 'in_progress' else 'queued', result['status'])
            self.assertNotIn('conclusion', result)

    def test_newest_run_wins_including_failed_reruns(self):
        result, _ = self.result([{**RUN, 'conclusion': 'failure'}, RUN])
        self.assertEqual('failure', result['conclusion'])

    def test_upstream_inventory_dispatch_cannot_satisfy_full_suite(self):
        result, _ = self.result([{**RUN, 'event': 'workflow_dispatch'}], 'flink-suite.yml')
        self.assertEqual('action_required', result['conclusion'])
        result, _ = self.result([{**RUN, 'event': 'workflow_dispatch'}])
        self.assertEqual('success', result['conclusion'])

    def test_deleted_forks_and_same_repository_prs_need_migration(self):
        for fork in (None, {'fork': False, 'full_name': 'contributor/StreamFusion'},
                     {'fork': True, 'full_name': 'datafusion-contrib/StreamFusion'}):
            pr = copy.deepcopy(PR)
            pr['head']['repo'] = fork
            result, api = self.result([], pr=pr)
            self.assertEqual('action_required', result['conclusion'])
            api.assert_not_called()

    def test_disabled_workflow_is_action_required_and_api_failure_is_not_hidden(self):
        for code in (404, 403, 500):
            with patch.object(STATUS, 'api', side_effect=HTTPError('url', code, 'error', {}, None)):
                if code == 404:
                    self.assertEqual('action_required', STATUS.payload(PR, 'ci.yml')['conclusion'])
                else:
                    with self.assertRaises(HTTPError):
                        STATUS.payload(PR, 'ci.yml')

    def test_only_update_reporter_owned_checks_when_names_collide(self):
        marker = 'streamfusion:fork-ci:v1:42:ci.yml:All CI tests'
        checks = [
            {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'},
             'external_id': 'ordinary-actions-job-uuid'},
            {'id': 2, 'name': 'Rust tests', 'app': {'slug': 'another-app'}},
            {'id': 3, 'name': 'All CI tests', 'app': {'slug': 'github-actions'},
             'external_id': marker},
        ]
        with patch.object(STATUS, 'api', return_value={'check_runs': checks}) as api, \
                patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        calls = api.call_args_list
        self.assertEqual(5, len(calls))
        self.assertEqual('PATCH', calls[1].args[0])
        self.assertTrue(calls[1].args[1].endswith('/check-runs/3'))
        self.assertEqual(marker, calls[1].args[2]['external_id'])
        created = [call.args[2] for call in calls if call.args[0] == 'POST']
        self.assertEqual({'Rust tests', 'Java tests (debug native build)', 'All upstream integration tests'},
                         {check['name'] for check in created})
        self.assertTrue(all(check['head_sha'] == PR['head']['sha'] for check in created))
        self.assertTrue(all(check['external_id'].startswith('streamfusion:fork-ci:v1:42:') for check in created))

    def test_unmarked_or_other_pr_checks_are_not_adopted(self):
        for external_id in (None, '', 'ordinary-actions-job-uuid',
                            'streamfusion:fork-ci:v1:99:ci.yml:All CI tests'):
            with self.subTest(external_id=external_id):
                check = {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'},
                         'external_id': external_id}
                with patch.object(STATUS, 'api', return_value={'check_runs': [check]}) as api, \
                        patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
                    STATUS.mirror('datafusion-contrib/StreamFusion', PR)
                self.assertFalse(any(call.args[0] == 'PATCH' for call in api.call_args_list))
                self.assertEqual(4, sum(call.args[0] == 'POST' for call in api.call_args_list))

    def test_owned_marker_from_another_app_is_not_adopted(self):
        check = {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'another-app'},
                 'external_id': 'streamfusion:fork-ci:v1:42:ci.yml:All CI tests'}
        with patch.object(STATUS, 'api', return_value={'check_runs': [check]}) as api, \
                patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        self.assertFalse(any(call.args[0] == 'PATCH' for call in api.call_args_list))

    def test_unmigrated_pr_remains_action_required_with_legacy_job(self):
        pr = copy.deepcopy(PR)
        pr['head']['repo'] = {**pr['base']['repo'], 'fork': False}
        check = {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'}}
        with patch.object(STATUS, 'api', return_value={'check_runs': [check]}) as api:
            STATUS.mirror('datafusion-contrib/StreamFusion', pr)
        created = [call.args[2] for call in api.call_args_list if call.args[0] == 'POST']
        self.assertEqual(4, len(created))
        self.assertTrue(all(check['conclusion'] == 'action_required' for check in created))
        self.assertFalse(any(call.args[0] == 'PATCH' for call in api.call_args_list))

    def test_exact_head_failure_is_mirrored_without_touching_workflow_job(self):
        checks = [
            {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'}},
            {'id': 2, 'name': 'All CI tests', 'app': {'slug': 'github-actions'},
             'external_id': 'streamfusion:fork-ci:v1:42:ci.yml:All CI tests'},
        ]
        def response(method, path, data=None):
            if '/check-runs?' in path:
                return {'check_runs': checks}
            if '/actions/workflows/' in path:
                return {'workflow_runs': [{**RUN, 'conclusion': 'failure'}]}
            return {}
        with patch.object(STATUS, 'api', side_effect=response) as api:
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        updates = [call for call in api.call_args_list if call.args[0] in ('POST', 'PATCH')]
        self.assertEqual(4, len(updates))
        self.assertTrue(all(call.args[2]['conclusion'] == 'failure' for call in updates))
        self.assertEqual(['repos/datafusion-contrib/StreamFusion/check-runs/2'],
                         [call.args[1] for call in updates if call.args[0] == 'PATCH'])

    def test_owned_check_is_found_beyond_first_page(self):
        legacy = {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'}}
        owned = {**legacy, 'id': 2,
                 'external_id': 'streamfusion:fork-ci:v1:42:ci.yml:All CI tests'}
        def response(method, path, data=None):
            if method == 'GET':
                return {'check_runs': [legacy] * 100 if path.endswith('&page=1') else [owned]}
            return {}
        with patch.object(STATUS, 'api', side_effect=response) as api, \
                patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        self.assertEqual(2, sum(call.args[0] == 'GET' for call in api.call_args_list))
        updated = [call for call in api.call_args_list if call.args[0] == 'PATCH']
        self.assertEqual(1, len(updated))
        self.assertTrue(updated[0].args[1].endswith('/check-runs/2'))
        self.assertIn('filter=all', api.call_args_list[0].args[1])

    def test_closed_pr_is_not_updated(self):
        with patch.object(STATUS, 'api') as api:
            STATUS.mirror('datafusion-contrib/StreamFusion', {**PR, 'state': 'closed'})
        api.assert_not_called()

    def test_schedule_paginates_all_open_prs(self):
        with patch.object(STATUS, 'api', side_effect=[[{'number': n} for n in range(100)], [{'number': 100}]]) as api:
            self.assertEqual(101, len(list(STATUS.pages('repos/owner/repo/pulls?state=open'))))
        self.assertIn('&per_page=100&page=2', api.call_args_list[1].args[1])

    def test_refresh_continues_after_failed_pr_and_preserves_failure(self):
        for fail_fetch in (True, False):
            with self.subTest(fail_fetch=fail_fetch), tempfile.TemporaryDirectory() as directory:
                event = Path(directory) / 'event.json'
                event.write_text('{}')
                summary = Path(directory) / 'summary.md'
                endpoint = 'https://api.github.com/repos/datafusion-contrib/StreamFusion/check-runs/17'
                error = HTTPError(endpoint, 403, 'Forbidden', {}, None)
                def fetch(method, path):
                    number = int(path.rsplit('/', 1)[-1])
                    if fail_fetch and number == 42:
                        raise error
                    return {**PR, 'number': number}
                def mirror(repository, pr):
                    if not fail_fetch and pr['number'] == 42:
                        raise error
                with patch.dict(STATUS.os.environ, {
                    'GITHUB_REPOSITORY': 'datafusion-contrib/StreamFusion',
                    'GITHUB_EVENT_PATH': str(event), 'GITHUB_STEP_SUMMARY': str(summary),
                }), patch.object(STATUS, 'pages', return_value=[{'number': 42}, {'number': 43}]), \
                        patch.object(STATUS, 'api', side_effect=fetch) as api, \
                        patch.object(STATUS, 'mirror', side_effect=mirror) as mirrored:
                    with self.assertRaises(SystemExit) as failure:
                        STATUS.main()
                self.assertEqual([42, 43], [int(call.args[1].rsplit('/', 1)[-1]) for call in api.call_args_list])
                self.assertEqual(43, mirrored.call_args_list[-1].args[1]['number'])
                self.assertIn('PR #42', str(failure.exception))
                self.assertIn(endpoint, summary.read_text())

    def test_pr_event_refreshes_current_head_without_listing_other_prs(self):
        with tempfile.TemporaryDirectory() as directory:
            event = Path(directory) / 'event.json'
            event.write_text(json.dumps({'pull_request': {'number': 42, 'head': {'sha': 'stale'}}}))
            with patch.dict(STATUS.os.environ, {
                'GITHUB_REPOSITORY': 'datafusion-contrib/StreamFusion', 'GITHUB_EVENT_PATH': str(event),
            }), patch.object(STATUS, 'pages') as pages, \
                    patch.object(STATUS, 'api', return_value=PR) as api, \
                    patch.object(STATUS, 'mirror') as mirror:
                STATUS.main()
            pages.assert_not_called()
            api.assert_called_once_with('GET', 'repos/datafusion-contrib/StreamFusion/pulls/42')
            mirror.assert_called_once_with('datafusion-contrib/StreamFusion', PR)


if __name__ == '__main__':
    unittest.main()
