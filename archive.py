import datetime as dt
import fnmatch
import glob
import logging
import operator
import os
import re
import shutil
import typing as t
import uuid
import zipfile
from pathlib import Path

ARCHIVE_PATTERN = '{name}_{date}_{millis}_{rand}'
ARCHIVE_ZIP_SUFFIX = '.archive.zip'
ARCHIVE_PARSE_PATTERN = \
    r'(?P<name>.*?)_(?P<y>[0-9]{4})(?P<m>[0-9]{2})(?P<d>[0-9]{2})_(?P<millis>[0-9]+)_(?P<rand_str>.{6})' \
    + ARCHIVE_ZIP_SUFFIX


def _zipdir(path, zf):
    for root, dirs, files in os.walk(path):
        for f in files:
            zf.write(os.path.join(root, f), arcname=os.path.join(os.path.relpath(root, path), f))


def archive_directory(name: str, in_dir: str, out_dir: str, ttl: int = None, max_archive: int = None, file_masks=['*'],
                      purge=False,
                      pre_exec: t.Callable = None, pre_exec_args: t.List = [], post_exec: t.Callable = None,
                      post_exec_args=[]):
    '''
    Archive files in a directory.

    :param name: the name of this archive task
    :param in_dir: the directory to archive
    :param out_dir: the directory to store archived zip file
    :param ttl: time-to-live for the archived zip file, in day
    :param max_archive: maximum archive files to keep
    :param file_masks: list of file mask to filter files to archive
    :param purge: whether to clean the content within in_dir
    :param pre_exec: function to execute before the archive task
    :param post_exec: function to execute after the archive task
    :return: the archive path
    :raises ValueError: Invalid input/output directory
    '''

    if in_dir is None or out_dir is None:
        raise ValueError("Output/Input directory is None.")

    _log = logging.getLogger('archive_dir')

    now = dt.datetime.now()
    date_str = now.strftime('%Y%m%d')
    time_str = str(get_millis(now.timestamp()))
    # archive_workspace = os.path.join(out_dir,
    #                                 ARCHIVE_PATTERN.format(name=name, date=date_str, rand=uuid.uuid4().hex[:6]))
    archive_workspace = os.path.join(out_dir,
                                     ARCHIVE_PATTERN.format(name=name, date=date_str, rand=uuid.uuid4().hex[:6],
                                                            millis=time_str))
    archive_file = archive_workspace + ARCHIVE_ZIP_SUFFIX

    in_path = Path(in_dir).resolve()
    out_path = Path(out_dir).resolve()
    if in_path in out_path.parents:
        raise ValueError("Output directory cannot be under input directory.")
    if in_path.absolute() == out_path.absolute():
        raise ValueError("Input directory cannot be same as output directory.")

    if pre_exec is not None:
        _log.info("%s - Pre-executing task...", name)
        ret = pre_exec(*pre_exec_args)
        _log.info("%s - Pre-executed task, ret=%s", name, str(ret))

    if os.path.exists(in_dir):
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        os.makedirs(archive_workspace)

        pending_files = []

        # calculate files to archive and copy them
        for root, dirs, files in os.walk(in_dir):
            for f in files:
                matched = False
                for pat in file_masks:
                    matched = fnmatch.fnmatch(f, pat)
                    if matched:
                        break

                if matched:
                    path = os.path.join(root, f)
                    pending_files.append(path)

                    # maintain folder structure
                    relative_path = os.path.relpath(path, in_dir)
                    out_sub_folder = os.path.join(archive_workspace, os.path.split(relative_path)[0])
                    if not os.path.exists(out_sub_folder):
                        os.makedirs(out_sub_folder)

                    # copy file
                    shutil.copy(path, os.path.join(out_sub_folder, f))

        _log.info("%s - Found %d files", name, len(pending_files))

        # zip the tmp folder
        zf = zipfile.ZipFile(archive_file, "w")
        _zipdir(archive_workspace, zf)
        zf.close()

        # remove temp workspace
        shutil.rmtree(archive_workspace)

        _clean_archive(out_dir, ttl, max_archive)

        if purge:
            for f in os.listdir(in_dir):
                path = os.path.join(in_dir, f)
                if os.path.isfile(path):
                    os.remove(path)
                if os.path.isdir(path):
                    shutil.rmtree(path)

        if post_exec is not None:
            _log.info("%s - Post-executing task...", name)
            ret = post_exec(*post_exec_args)
            _log.info("%s - Post-executed task, ret=%s", name, str(ret))

        _log.info("%s - Archived and clean.", name)
    else:
        raise ValueError("Input directory not exist")

    return archive_file


def _clean_archive(archive_dir, ttl=None, max_archive=None):
    _log = logging.getLogger('clean_archive')
    files = glob.glob(os.path.join(archive_dir, '*' + ARCHIVE_ZIP_SUFFIX))
    today = dt.date.today()

    kept = []

    for f in files:
        removed = False

        if ttl is not None:
            file_date = _parse_filename_to_date(os.path.basename(f))
            delta = today - file_date
            if delta.days > ttl:
                os.remove(f)
                removed = True
                _log.info("Removed %s", f)

        if not removed:
            kept.append((f, _parse_filename_to_millis(f)))

    if max_archive is not None and len(kept) > 0:
        # sort
        lst = sorted(kept, key=operator.itemgetter(1))
        lst.reverse()
        for f, millis in lst[max_archive:]:
            os.remove(f)
            _log.info("Removed %s", f)


def _parse_filename_to_date(name):
    match = re.match(ARCHIVE_PARSE_PATTERN, name)

    if match is None:
        return None

    return dt.date(int(match.group('y')), int(match.group('m')), int(match.group('d')))


def _parse_filename_to_millis(name):
    match = re.match(ARCHIVE_PARSE_PATTERN, name)

    if match is None:
        return None

    return int(match.group('millis'))


def get_millis(ts):
    return int(round(ts * 1000))
