#!/usr/bin/python3
import argparse
import logging as l

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

import archive
from debounce import debounce

debounce_sec = 5


@debounce(debounce_sec)
def on_event(evt: FileSystemEvent):
    # debounced, evt will be last event
    l.debug('%s - %s', evt.event_type, evt.src_path)

    """
    if EVENT_TYPE_MOVED == evt.event_type:
        pass
    if EVENT_TYPE_DELETED == evt.event_type:
        pass
    if EVENT_TYPE_CREATED == evt.event_type:
        pass
    if EVENT_TYPE_MODIFIED == evt.event_type:
        pass
    """

    archived = archive.archive_directory('checkpoint', path, dest_path)
    l.info(f'Zipped {path} -> {archived}')


def _setup_parser() -> argparse.ArgumentParser:
    _parser = argparse.ArgumentParser(prog='checkpoint',
                                      description='Monitor and archive folder after contents changed.')
    _parser.add_argument('path', type=str, help='Directory to monitor')
    _parser.add_argument('dest_path', type=str, help='Zip destination')
    return _parser


if __name__ == '__main__':
    l.basicConfig(
        level=l.DEBUG,
        format='[%(asctime)s] %(levelname)s | %(message)s'
    )

    # argument parser
    parser = _setup_parser()
    args = parser.parse_args()

    # args
    path = args.path
    dest_path = args.dest_path

    # event
    event_handler = FileSystemEventHandler()
    event_handler.on_any_event = on_event
    observer = Observer(timeout=0.1)
    l.debug(f'Observer={observer.__class__}')
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    l.info(f'Set watchdog on {path}...')
    try:
        pass
    except InterruptedError:
        observer.stop()
    finally:
        observer.join()
        l.info('Stopped watchdog.')
