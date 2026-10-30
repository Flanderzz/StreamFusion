"""Report public fork CI results without executing contributor code."""

import json
import os
import sys
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


WORKFLOWS = {
    'ci.yml': ('All CI tests', 'Rust tests', 'Java tests (debug native build)'),
    'flink-suite.yml': ('All upstream integration tests',),
}
GUIDE = 'https://datafusion-contrib.github.io/StreamFusion/ci/'


def api(method, path, data=None):
    request = Request(
        f'https://api.github.com/{path}',
        data=None if data is None else json.dumps(data).encode(),
        method=method,
        headers={
            'Authorization': f'Bearer {os.environ["GH_TOKEN"]}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
        },
    )
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def pages(path, key=None):
    page = 1
    while True:
        values = api('GET', f'{path}{"&" if "?" in path else "?"}per_page=100&page={page}')
        if key is not None:
            values = values[key]
        yield from values
        if len(values) < 100:
            return
        page += 1


def payload(pr, workflow):
    head = pr['head']
    fork = head.get('repo')
    result = {
        'status': 'completed',
        'conclusion': 'action_required',
        'output': {
            'title': 'Fork CI is required',
            'summary': f'Push this PR head to a personal fork with Actions enabled. See {GUIDE}',
        },
    }
    if not fork or not fork['fork'] or fork['full_name'] == pr['base']['repo']['full_name']:
        return result
    query = urlencode({'head_sha': head['sha'], 'branch': head['ref'], 'per_page': 100})
    try:
        runs = api('GET', f'repos/{fork["full_name"]}/actions/workflows/{workflow}/runs?{query}')
    except HTTPError as error:
        if error.code != 404:
            raise
        return result
    run = next((run for run in runs['workflow_runs']
                if run['head_sha'] == head['sha'] and run['head_branch'] == head['ref']
                and run['event'] in ('push', 'workflow_dispatch')
                # An inventory-only dispatch does not execute the complete upstream suite.
                and (workflow != 'flink-suite.yml' or run['event'] == 'push')), None)
    if run is None:
        return result
    result['details_url'] = run['html_url']
    result['output'] = {
        'title': f'{workflow}: {run["conclusion"] or run["status"]}',
        'summary': (f'[{fork["full_name"]} fork run]({run["html_url"]}) for `{head["sha"]}`. '
                    'Refreshed on PR updates and every 15 minutes; manual refresh is also available. '
                    f'See {GUIDE}'),
    }
    if run['status'] == 'completed':
        # Skipped/neutral runs must never satisfy the required test gates.
        result['conclusion'] = 'success' if run['conclusion'] == 'success' else 'failure'
    else:
        result['status'] = 'in_progress' if run['status'] == 'in_progress' else 'queued'
        del result['conclusion']
    return result


def mirror(repository, pr):
    if pr['state'] != 'open':
        return
    sha = pr['head']['sha']
    checks = list(pages(f'repos/{repository}/commits/{sha}/check-runs?filter=all', 'check_runs'))
    for workflow, names in WORKFLOWS.items():
        result = payload(pr, workflow)
        for name in names:
            external_id = f'streamfusion:fork-ci:v1:{pr["number"]}:{workflow}:{name}'
            owned_result = {'external_id': external_id, **result}
            # Ordinary workflow jobs use the same application and check names.
            check = next((check for check in checks
                          if check['name'] == name and check['app']['slug'] == 'github-actions'
                          and check.get('external_id') == external_id), None)
            if check:
                api('PATCH', f'repos/{repository}/check-runs/{check["id"]}', owned_result)
            else:
                api('POST', f'repos/{repository}/check-runs', {'name': name, 'head_sha': sha, **owned_result})
    print(f'Mirrored PR #{pr["number"]} at {sha}')


def main():
    repository = os.environ['GITHUB_REPOSITORY']
    if repository != 'datafusion-contrib/StreamFusion':
        return
    with open(os.environ['GITHUB_EVENT_PATH']) as source:
        event = json.load(source)
    if 'pull_request' in event:
        numbers = [event['pull_request']['number']]
    else:
        numbers = [pr['number'] for pr in pages(f'repos/{repository}/pulls?state=open')]
    errors = []
    for number in numbers:
        # Refresh the head: a scheduled update must not report on a superseded PR revision.
        try:
            mirror(repository, api('GET', f'repos/{repository}/pulls/{number}'))
        except Exception as error:
            location = f' ({error.filename})' if isinstance(error, HTTPError) else ''
            message = f'PR #{number}: {error}{location}'
            print(message, file=sys.stderr)
            errors.append(message)
    if errors:
        summary = 'Fork CI refresh failed:\n' + '\n'.join(f'- {error}' for error in errors)
        if os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as target:
                target.write(summary + '\n')
        raise SystemExit(summary)


if __name__ == '__main__':
    main()
