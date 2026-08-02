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


def build_parser() -> argparse.ArgumentParser:
    """Build the command parser without coupling parsing to async orchestration."""
    parser = argparse.ArgumentParser(prog="horus")
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_collect_parser(subparsers)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    dependencies: _CollectionDependencies = _DEFAULT_DEPENDENCIES,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    """Run the collector and convert ordinary failures into stable exit codes."""
    parser = build_parser()
    options = parse_collect_options(
        parser, None if argv is None else list(argv), dependencies.session_id_factory
    )
    try:
        summary = asyncio.run(collect(options, dependencies, stderr))
    except KeyboardInterrupt:
        print("collection interrupted", file=stderr)
        return 130
    except CollectionOperationalError as error:
        print(f"error: {error}", file=stderr)
        return 1
    except Exception:
        print("error: collection failed", file=stderr)
        return 1
    print(summary.as_json(), file=stdout)
    return 0
