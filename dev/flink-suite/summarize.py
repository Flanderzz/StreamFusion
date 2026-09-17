#!/usr/bin/env python3
"""Summarize Flink Surefire XML reports after an upstream StreamFusion run."""

from __future__ import annotations

import argparse
import base64
from collections import Counter
import pathlib
import re
import sys
import xml.etree.ElementTree as ET


CONTRACT_FILE = (
    pathlib.Path(__file__).parent / "agent/src/main/resources/native-execution.tsv"
)


def execution_contracts(
    path: pathlib.Path = CONTRACT_FILE,
) -> dict[str, dict[str, str]]:
    contracts = {}
    for line in path.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("\t")
        if (
            len(fields) != 3
            or not re.fullmatch(r"[\w.$]+#[\w$]+", fields[0])
            or not re.fullmatch(r"\*|\w+=(?:true|false)", fields[1])
            or not re.fullmatch(r"\w+(?:[+|]\w+)*|!.+", fields[2])
        ):
            raise ValueError(f"Invalid native execution contract: {line}")
        variants = contracts.setdefault(fields[0], {})
        if fields[1] in variants:
            raise ValueError(f"Duplicate native execution contract: {line}")
        variants[fields[1]] = fields[2]
    if not contracts:
        raise ValueError("No native execution contracts")
    return contracts


def check_execution(
    directory: pathlib.Path | None,
    executed: Counter,
    contracts: dict[str, dict[str, str]],
) -> tuple[int, int, list[str]]:
    observed = Counter()
    problems = []
    proved = 0
    fallback = 0
    for path in sorted(directory.glob("*.tsv")) if directory else []:
        try:
            fields = path.read_text().rstrip("\n").split("\t")
            if (
                len(fields) != 4
                or fields[0] not in contracts
                or fields[1] not in contracts[fields[0]]
            ):
                raise ValueError("unknown test or malformed record")
            test, variant, raw_counts, raw_reasons = fields
            reasons = base64.b64decode(raw_reasons, validate=True).decode().splitlines()
            counts = {}
            for item in raw_counts.split(",") if raw_counts else []:
                if not re.fullmatch(r"\w+=\d+", item):
                    raise ValueError(f"invalid operator count: {item}")
                operator, count = item.split("=")
                if operator in counts:
                    raise ValueError(f"duplicate operator count: {operator}")
                counts[operator] = int(count)
            observed[test] += 1
            contract = contracts[test][variant]
            if contract.startswith("!"):
                if not counts and contract[1:] in reasons:
                    fallback += 1
                else:
                    problems.append(
                        f"{test} [{variant}]: expected full fallback with reason {contract[1:]}"
                    )
            elif any(
                all(counts.get(operator, 0) > 0 for operator in route.split("+"))
                for route in contract.split("|")
            ):
                proved += 1
            else:
                problems.append(
                    f"{test}: missing native execution; observed {counts} ({path.name})"
                )
        except (OSError, ValueError) as exc:
            problems.append(f"{path}: invalid native execution evidence: {exc}")
    for test in sorted(executed.keys() | observed.keys()):
        if executed[test] != observed[test]:
            problems.append(
                f"{test}: {executed[test]} executed invocations but {observed[test]} native evidence records"
            )
    return proved, fallback, problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", type=pathlib.Path)
    parser.add_argument("--xfail", action="append", default=[])
    parser.add_argument("--native-reports", type=pathlib.Path)
    parser.add_argument("--require-all-contracts", action="store_true")
    parser.add_argument("--require-contract-prefix", action="append", default=[])
    parser.add_argument("--require-test", action="append", default=[])
    args = parser.parse_args()

    files = sorted(args.reports.rglob("TEST-*.xml"))
    if not files:
        print("No Surefire XML reports found.")
        return 2

    tests = failures = errors = skipped = 0
    problems: list[tuple[str, str, str, str]] = []
    expected: list[tuple[str, str, str, str]] = []
    malformed: list[tuple[pathlib.Path, str]] = []
    contracts = execution_contracts()
    executed = Counter()
    executed_tests = Counter()

    for report in files:
        try:
            suite = ET.parse(report).getroot()
        except (ET.ParseError, OSError) as exc:
            malformed.append((report, str(exc)))
            continue

        tests += int(suite.attrib.get("tests", 0))
        failures += int(suite.attrib.get("failures", 0))
        errors += int(suite.attrib.get("errors", 0))
        skipped += int(suite.attrib.get("skipped", 0))
        for case in suite.findall("testcase"):
            case_key = (
                case.attrib.get("classname", suite.attrib.get("name", "unknown"))
                + "#"
                + case.attrib.get("name", "unknown")
            )
            if case.find("skipped") is None:
                executed_tests[case_key] += 1
                if case_key in contracts:
                    executed[case_key] += 1
            problem = case.find("failure")
            kind = "failure"
            if problem is None:
                problem = case.find("error")
                kind = "error"
            if problem is None:
                continue
            detail = (problem.attrib.get("message") or problem.text or "").strip()
            detail = " ".join(detail.split())[:800]
            item = (
                case.attrib.get("classname", suite.attrib.get("name", "unknown")),
                case.attrib.get("name", "unknown"),
                kind,
                detail,
            )
            key = f"{item[0]}#{item[1]}"
            (expected if key in args.xfail else problems).append(item)

    expected_failures = sum(kind == "failure" for _, _, kind, _ in expected)
    expected_errors = sum(kind == "error" for _, _, kind, _ in expected)
    unexpected_failures = failures - expected_failures
    unexpected_errors = errors - expected_errors
    proved, fallback, execution_problems = check_execution(
        args.native_reports, executed, contracts
    )
    required = set(contracts) if args.require_all_contracts else set()
    for prefix in args.require_contract_prefix:
        matching = {test for test in contracts if test.startswith(prefix)}
        if not matching:
            execution_problems.append(f"No execution contracts match required prefix {prefix}")
        required.update(matching)
    execution_problems.extend(
        f"{test}: contracted test did not execute"
        for test in sorted(required)
        if not executed[test]
    )
    execution_problems.extend(
        f"{test}: required test did not execute"
        for test in args.require_test
        if not executed_tests[test]
    )

    print("# StreamFusion upstream Flink suite")
    print()
    print(f"- Reports: {len(files)}")
    print(f"- Tests: {tests}")
    print(f"- Passed: {tests - failures - errors - skipped}")
    print(f"- Failures: {unexpected_failures}")
    print(f"- Errors: {unexpected_errors}")
    print(f"- Expected upstream failures: {len(expected)}")
    print(f"- Skipped: {skipped}")
    print(
        f"- Execution contracts: {sum(executed.values())} invocations, {proved} native, {fallback} expected fallback"
    )

    if malformed:
        print(f"- Malformed reports: {len(malformed)}")

    if problems:
        print()
        print("## Issues")
        for class_name, test_name, kind, detail in problems:
            print()
            print(f"- `{class_name}#{test_name}` ({kind})")
            if detail:
                print(f"  - {detail}")

    if malformed:
        print()
        print("## Malformed reports")
        for report, detail in malformed:
            print(f"- `{report}`: {detail}")

    if expected:
        print()
        print("## Expected upstream failures")
        for class_name, test_name, kind, detail in expected:
            print(f"- `{class_name}#{test_name}` ({kind})")
            if detail:
                print(f"  - {detail}")

    if execution_problems:
        print()
        print("## Native execution contract failures")
        for problem in execution_problems:
            print(f"- {problem}")

    return (
        1
        if unexpected_failures or unexpected_errors or malformed or execution_problems
        else 0
    )


if __name__ == "__main__":
    sys.exit(main())
