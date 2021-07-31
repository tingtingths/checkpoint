#!/usr/bin/env python3
import argparse
import os
import logging as l
import time

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

import archive
from debounce import debounce


def on_event(evt: FileSystemEvent):
    l.debug('Receive event, [%s] - [%s]', evt.event_type, evt.src_path)
    archived = archive.archive_directory('checkpoint', path, dest_path)
    l.info(f'Zipped {path} -> {archived}')


def _setup_parser() -> argparse.ArgumentParser:
    _parser = argparse.ArgumentParser(prog='checkpoint',
                                      description='Monitor and archive folder after contents changed.')
    _parser.add_argument('path', type=str, help='Directory to monitor')
    _parser.add_argument('dest_path', type=str, help='Zip destination')
    _parser.add_argument('--debounce', '-d', type=int, default=5, help='Filesystem event debounce seconds')
    return _parser


if __name__ == '__main__':
    l.basicConfig(
        level=l.DEBUG,
        format='[%(asctime)s] %(levelname)s | %(message)s'
    )

    # argument parser
    parser = _setup_parser()
    args = parser.parse_args()

    # setup with arguments
    path = args.path
    dest_path = args.dest_path
    on_event_handler = debounce(args.debounce)(on_event)
    l.info(f'Watching {path}')
    l.info(f'Destination {dest_path}')
    l.info(f'Debounce interval {args.debounce} seconds')

    # event handling
    event_handler = FileSystemEventHandler()
    event_handler.on_any_event = lambda evt: on_event_handler(evt)

    observer = Observer(timeout=0.1)
    l.debug(f'Observer is {observer.__class__}')
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    l.info(f'Set watchdog on {os.path.abspath(path)}...')
    try:
        while True:
            time.sleep(1)
    except InterruptedError:
        l.info('Interrupted...')
        observer.stop()

    observer.join()
    l.info('Stopped watchdog.')
