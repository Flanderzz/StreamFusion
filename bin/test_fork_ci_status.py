"""Exercise fork result selection and required-check reporting with fake GitHub responses."""

import copy
import importlib.util
from pathlib import Path
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

    def test_checks_match_existing_merge_rules_and_only_update_actions_checks(self):
        checks = [
            {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'}},
            {'id': 2, 'name': 'Rust tests', 'app': {'slug': 'another-app'}},
        ]
        with patch.object(STATUS, 'api', return_value={'check_runs': checks}) as api, \
                patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        calls = api.call_args_list
        self.assertEqual(5, len(calls))
        self.assertEqual('PATCH', calls[1].args[0])
        created = [call.args[2] for call in calls if call.args[0] == 'POST']
        self.assertEqual({'Rust tests', 'Java tests (debug native build)', 'All upstream integration tests'},
                         {check['name'] for check in created})
        self.assertTrue(all(check['head_sha'] == PR['head']['sha'] for check in created))

    def test_closed_pr_is_not_updated(self):
        with patch.object(STATUS, 'api') as api:
            STATUS.mirror('datafusion-contrib/StreamFusion', {**PR, 'state': 'closed'})
        api.assert_not_called()

    def test_schedule_paginates_all_open_prs(self):
        with patch.object(STATUS, 'api', side_effect=[[{'number': n} for n in range(100)], [{'number': 100}]]) as api:
            self.assertEqual(101, len(list(STATUS.pages('repos/owner/repo/pulls?state=open'))))
        self.assertIn('&per_page=100&page=2', api.call_args_list[1].args[1])


if __name__ == '__main__':
    unittest.main()
