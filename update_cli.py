"""Command line helper for the RugBase automatic updater."""

from __future__ import annotations

import argparse
import sys

from core import updater


def command_check(args: argparse.Namespace) -> int:
    try:
        status = updater.get_update_status()
    except updater.UpdateError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Local version : {status.local_version}")
    if status.remote_version:
        print(f"Remote version: {status.remote_version}")
    else:
        print("Remote version: unavailable")

    if status.update_available:
        print("A new update is available.")
        if status.release:
            print(f"Latest asset : {status.release.asset_name}")
    else:
        print("Already up to date.")
    return 0


def command_download(args: argparse.Namespace) -> int:
    try:
        status = updater.get_update_status()
    except updater.UpdateError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not status.update_available or not status.release:
        print("Already up to date.")
        return 0

    try:
        asset_path = updater.download_release_asset(status.release)
    except updater.UpdateError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Downloaded update to: {asset_path}")

    if args.prepare:
        try:
            script_path = updater.prepare_updater(asset_path, status.release)
        except updater.UpdateError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        print(f"Prepared updater script: {script_path}")
    return 0


def command_apply(args: argparse.Namespace) -> int:
    try:
        status = updater.get_update_status()
    except updater.UpdateError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not status.update_available or not status.release:
        print("Already up to date.")
        return 0

    try:
        asset_path = updater.download_release_asset(status.release)
        script_path = updater.prepare_updater(asset_path, status.release)
    except updater.UpdateError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.launch:
        try:
            updater.launch_updater(script_path)
        except updater.UpdateError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        print("Update scheduled. Close the application to continue.")
    else:
        print(f"Updater script created at: {script_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RugBase update management tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="Display local and remote versions")
    check_parser.set_defaults(func=command_check)

    download_parser = subparsers.add_parser("download", help="Download the latest release asset")
    download_parser.add_argument("--prepare", action="store_true", help="Prepare the updater script after downloading")
    download_parser.set_defaults(func=command_download)

    apply_parser = subparsers.add_parser(
        "apply",
        help="Download and schedule the latest update",
    )
    apply_parser.add_argument(
        "--launch",
        action="store_true",
        help="Launch the updater immediately after preparing it",
    )
    apply_parser.set_defaults(func=command_apply)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
