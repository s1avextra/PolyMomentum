#!/usr/bin/env python3
"""Install the local dev-box launchd definition for strategy research."""

from __future__ import annotations

import argparse
import contextlib
import os
from pathlib import Path
import plistlib
import json
import shutil


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "deploy/local/com.polymomentum.strategy-research.plist.in"
DESTINATION = Path.home() / "Library/LaunchAgents/com.polymomentum.strategy-research.plist"
BUNDLE_ROOT = Path.home() / "Library/Application Support/PolyMomentumStrategyResearch"


def render(bundle_root: Path) -> bytes:
    rendered = TEMPLATE.read_text().replace("__REPOSITORY_ROOT__", str(bundle_root))
    plistlib.loads(rendered.encode("utf-8"))
    return rendered.encode("utf-8")


def copy_asset(relative: str, bundle_root: Path) -> None:
    source = ROOT / relative
    if not source.is_file():
        raise FileNotFoundError(source)
    destination = bundle_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name("%s.tmp.%s" % (destination.name, os.getpid()))
    shutil.copy2(str(source), str(temporary))
    os.replace(str(temporary), str(destination))


def link_asset(relative: str, bundle_root: Path) -> None:
    source = ROOT / relative
    if not source.is_file():
        raise FileNotFoundError(source)
    destination = bundle_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name("%s.tmp.%s" % (destination.name, os.getpid()))
    with contextlib.suppress(FileNotFoundError):
        temporary.unlink()
    os.link(source, temporary)
    os.replace(str(temporary), str(destination))


def sync_bundle(bundle_root: Path) -> None:
    config = json.loads((ROOT / "deploy/strategy-research-loop.json").read_text())
    assets = {
        "scripts/strategy_research_loop.py",
        "deploy/strategy-research-loop.json",
        str(config["engine_path"]),
        str(config["registry_path"]),
        str(config["lanes"]["late_window_mechanisms"]["public_snapshot"]),
        str(config["lanes"]["late_window_mechanisms"]["base_variant"]),
        str(config["economic_screen"]["cached_family_screen"]),
        *[str(path) for path in config["lanes"]["baseline_evolution"]["reports"]],
    }
    linked_assets = set()
    opportunity_search = config.get("architecture_migration", {}).get(
        "opportunity_policy_search", {}
    )
    if opportunity_search.get("enabled", False):
        dataset_seal_path = str(opportunity_search["dataset_seal"])
        labels_manifest_path = str(opportunity_search["labels_manifest"])
        assets.update({dataset_seal_path, labels_manifest_path})
        dataset_seal = json.loads((ROOT / dataset_seal_path).read_text())
        for entry in dataset_seal["entries"]:
            assets.add(str(entry["manifest"]["path"]))
            assets.add(str(entry["opportunity_table"]["path"]))
            opportunity_manifest = json.loads(
                (ROOT / str(entry["manifest"]["path"])).read_text()
            )
            linked_assets.add(str(opportunity_manifest["pmxt_parquet"]["path"]))
        labels_manifest = json.loads((ROOT / labels_manifest_path).read_text())
        assets.add(str(labels_manifest["output"]["path"]))
    registry = json.loads((ROOT / config["registry_path"]).read_text())

    def collect(value):
        if isinstance(value, dict):
            for nested in value.values():
                collect(nested)
        elif isinstance(value, list):
            for nested in value:
                collect(nested)
        elif isinstance(value, str) and value.startswith(
            "deploy/promotions/evidence/strategy_registry/"
        ):
            assets.add(value)

    collect(registry)
    for relative in sorted(assets):
        copy_asset(relative, bundle_root)
    for relative in sorted(linked_assets):
        link_asset(relative, bundle_root)
    (bundle_root / "logs/strategy-research").mkdir(parents=True, exist_ok=True)


def install(destination: Path, bundle_root: Path) -> None:
    sync_bundle(bundle_root)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name("%s.tmp.%s" % (destination.name, os.getpid()))
    temporary.write_bytes(render(bundle_root))
    os.replace(str(temporary), str(destination))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, default=DESTINATION)
    parser.add_argument("--bundle-root", type=Path, default=BUNDLE_ROOT)
    parser.add_argument("--print", action="store_true", dest="print_only")
    args = parser.parse_args()
    if args.print_only:
        print(render(args.bundle_root.expanduser().resolve()).decode("utf-8"), end="")
        return
    install(
        args.destination.expanduser().resolve(),
        args.bundle_root.expanduser().resolve(),
    )
    print(args.destination.expanduser().resolve())


if __name__ == "__main__":
    main()
