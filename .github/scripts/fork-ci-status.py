"""Report public fork CI results without executing contributor code."""

import json
import os
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


def pages(path):
    page = 1
    while True:
        values = api('GET', f'{path}{"&" if "?" in path else "?"}per_page=100&page={page}')
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
    checks = api('GET', f'repos/{repository}/commits/{sha}/check-runs?per_page=100')['check_runs']
    for workflow, names in WORKFLOWS.items():
        result = payload(pr, workflow)
        for name in names:
            # Only update checks owned by this GitHub Actions application.
            check = next((check for check in checks
                          if check['name'] == name and check['app']['slug'] == 'github-actions'), None)
            if check:
                api('PATCH', f'repos/{repository}/check-runs/{check["id"]}', result)
            else:
                api('POST', f'repos/{repository}/check-runs', {'name': name, 'head_sha': sha, **result})
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
    for number in numbers:
        # Refresh the head: a scheduled update must not report on a superseded PR revision.
        mirror(repository, api('GET', f'repos/{repository}/pulls/{number}'))


if __name__ == '__main__':
    main()
