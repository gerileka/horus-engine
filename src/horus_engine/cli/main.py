"""Synchronous console entry point for Horus Engine."""

import argparse
import asyncio
import sys
from collections.abc import Sequence
from typing import TextIO

from .collect import (
    _DEFAULT_DEPENDENCIES,
    CollectionOperationalError,
    _CollectionDependencies,
    add_collect_parser,
    collect,
    parse_collect_options,
)
from .shadow import (
    _DEFAULT_DEPENDENCIES as _DEFAULT_SHADOW_DEPENDENCIES,
)
from .shadow import (
    ShadowOperationalError,
    _ShadowDependencies,
    add_shadow_parser,
    parse_shadow_options,
    shadow,
)
from .simulate_fills import (
    _DEFAULT_DEPENDENCIES as _DEFAULT_SIMULATE_FILLS_DEPENDENCIES,
)
from .simulate_fills import (
    SimulateFillsOperationalError,
    add_simulate_fills_parser,
    parse_simulate_fills_options,
    simulate_fills,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the command parser without coupling parsing to async orchestration."""
    parser = argparse.ArgumentParser(prog="horus")
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_collect_parser(subparsers)
    add_shadow_parser(subparsers)
    add_simulate_fills_parser(subparsers)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    dependencies: _CollectionDependencies = _DEFAULT_DEPENDENCIES,
    shadow_dependencies: _ShadowDependencies = _DEFAULT_SHADOW_DEPENDENCIES,
    simulate_fills_dependencies: _ShadowDependencies = (
        _DEFAULT_SIMULATE_FILLS_DEPENDENCIES
    ),
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    """Run one CLI command and convert ordinary failures into stable exit codes."""
    parser = build_parser()
    arguments = None if argv is None else list(argv)
    namespace = parser.parse_args(arguments)
    try:
        if namespace.command == "collect":
            collect_options = parse_collect_options(
                parser, arguments, dependencies.session_id_factory
            )
            summary_json = asyncio.run(
                collect(collect_options, dependencies, stderr)
            ).as_json()
        elif namespace.command == "shadow":
            shadow_options = parse_shadow_options(parser, arguments)
            summary_json = asyncio.run(
                shadow(shadow_options, shadow_dependencies, stderr)
            ).as_json()
        else:
            simulate_fills_options = parse_simulate_fills_options(parser, arguments)
            summary_json = asyncio.run(
                simulate_fills(
                    simulate_fills_options, simulate_fills_dependencies, stderr
                )
            ).as_json()
    except KeyboardInterrupt:
        interrupted = {
            "collect": "collection",
            "shadow": "shadow",
            "simulate-fills": "fill simulation",
        }[namespace.command]
        print(f"{interrupted} interrupted", file=stderr)
        return 130
    except (
        CollectionOperationalError,
        ShadowOperationalError,
        SimulateFillsOperationalError,
    ) as error:
        print(f"error: {error}", file=stderr)
        return 1
    except Exception:
        failed = {
            "collect": "collection",
            "shadow": "shadow",
            "simulate-fills": "fill simulation",
        }[namespace.command]
        print(f"error: {failed} failed", file=stderr)
        return 1
    print(summary_json, file=stdout)
    return 0
