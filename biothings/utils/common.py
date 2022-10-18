"""
This module contains util functions may be shared by both BioThings data-hub and web components.
In general, do not include utils depending on any third-party modules.
"""
import asyncio
import base64
import glob
import gzip
import hashlib
import importlib
import inspect
import io
import json
import logging
import os
import os.path
import pickle
import random
import string
import sys
import time
import types
from collections import UserDict, UserList
from contextlib import contextmanager
from datetime import date, datetime, timezone
from functools import partial
from itertools import islice
from shlex import shlex

# from json serial, catching special type
# import _sre     # TODO: unused import;remove it once confirmed


# ===============================================================================
# Misc. Utility functions
# ===============================================================================
def ask(prompt, options='YN'):
    '''Prompt Yes or No,return the upper case 'Y' or 'N'.'''
    options = options.upper()
    while 1:
        s = input(prompt+'[%s]' % '|'.join(list(options))).strip().upper()
        if s in options:
            break
    return s


def timesofar(t0, clock=0, t1=None):
    '''return the string(eg.'3m3.42s') for the passed real time/CPU time so far
       from given t0 (return from t0=time.time() for real time/
       t0=time.clock() for CPU time).'''
    t1 = t1 or time.clock() if clock else time.time()
    t = t1 - t0
    h = int(t / 3600)
    m = int((t % 3600) / 60)
    s = round((t % 3600) % 60, 2)
    t_str = ''
    if h != 0:
        t_str += '%sh' % h
    if m != 0:
        t_str += '%sm' % m
    t_str += '%ss' % s
    return t_str


def is_int(s):
    """return True or False if input string is integer or not."""
    try:
        int(s)
        return True
    except (ValueError, TypeError):
        return False


def is_str(s):
    """return True or False if input is a string or not.
        python3 compatible.
    """
    return isinstance(s, str)


def is_seq(li):
    """return True if input is either a list or a tuple.
    """
    return isinstance(li, (list, tuple))


def is_float(f):
    """return True if input is a float.
    """
    return isinstance(f, float)

def is_scalar(f):
    # return type(f) in (int, str, bool, float, bytes) or f is None or is_int(f) or is_float(f) or is_str(f)
    return isinstance(f, (int, str, bool, float, bytes)) or f is None

def iter_n(iterable, n, with_cnt=False):
    '''
    Iterate an iterator by chunks (of n)
    if with_cnt is True, return (chunk, cnt) each time
    ref http://stackoverflow.com/questions/8991506/iterate-an-iterator-by-chunks-of-n-in-python
    '''
    it = iter(iterable)
    if with_cnt:
        cnt = 0
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        if with_cnt:
            cnt += len(chunk)
            yield (chunk, cnt)
        else:
            yield chunk


def addsuffix(filename, suffix, noext=False):
    '''Add suffix in front of ".extension", so keeping the same extension.
       if noext is True, remove extension from the filename.'''
    if noext:
        return os.path.splitext(filename)[0] + suffix
    else:
        return suffix.join(os.path.splitext(filename))


def safewfile(filename, prompt=True, default='C', mode='w'):
    '''return a file handle in 'w' mode,use alternative name if same name exist.
       if prompt == 1, ask for overwriting,appending or changing name,
       else, changing to available name automatically.'''
    suffix = 1
    while 1:
        if not os.path.exists(filename):
            break
        print('Warning:"%s" exists.' % filename, end='')
        if prompt:
            option = ask('Overwrite,Append or Change name?', 'OAC')
        else:
            option = default
        if option == 'O':
            if not prompt or ask('You sure?') == 'Y':
                print("Overwritten.")
                break
        elif option == 'A':
            print("Append to original file.")
            f = open(filename, 'a')
            f.write('\n' + "=" * 20 + 'Appending on ' + time.ctime() + "=" * 20 + '\n')
            return f, filename
        print('Use "%s" instead.' % addsuffix(filename, '_' + str(suffix)))
        filename = addsuffix(filename, '_' + str(suffix))
        suffix += 1
    return open(filename, mode), filename


def anyfile(infile, mode='r'):
    '''
    return a file handler with the support for gzip/zip comppressed files.
    if infile is a two value tuple, then first one is the compressed file;
    the second one is the actual filename in the compressed file.
    e.g., ('a.zip', 'aa.txt')

    '''
    if isinstance(infile, tuple):
        infile, rawfile = infile[:2]
    else:
        rawfile = os.path.splitext(infile)[0]
    filetype = os.path.splitext(infile)[1].lower()
    if filetype == '.gz':
        # import gzip
        in_f = io.TextIOWrapper(gzip.GzipFile(infile, mode))
    elif filetype == '.zip':
        import zipfile
        in_f = io.TextIOWrapper(zipfile.ZipFile(infile, mode).open(rawfile, mode))
    elif filetype == '.xz':
        import lzma
        in_f = io.TextIOWrapper(lzma.LZMAFile(infile, mode))
    else:
        in_f = open(infile, mode)
    return in_f

def is_filehandle(fh):
    '''return True/False if fh is a file-like object'''
    return hasattr(fh, 'read') and hasattr(fh, 'close')


##  This is another (older) implementation of open_anyfile
##   Keep the code here for reference
#
# class open_anyfile(object):
#     '''a context manager can be used in "with" stmt.
#        accepts a filehandle or anything accepted by anyfile function.

#         with open_anyfile('test.txt') as in_f:
#             do_something()
#     '''
#     def __init__(self, infile, mode='r'):
#         self.infile = infile
#         self.mode = mode

#     def __enter__(self):
#         if is_filehandle(self.infile):
#             self.in_f = self.infile
#         else:
#             self.in_f = anyfile(self.infile, mode=self.mode)
#         return self.in_f

#     def __exit__(self, type, value, traceback):
#         self.in_f.close()


@contextmanager
def open_anyfile(infile, mode='r'):
    '''a context manager can be used in "with" stmt.
       accepts a filehandle or anything accepted by anyfile function.

        with open_anyfile('test.txt') as in_f:
            do_something()
    '''
    if is_filehandle(infile):
        in_f = infile
    else:
        in_f = anyfile(infile, mode=mode)
    try:
        yield in_f
    finally:
        in_f.close()


class dotdict(dict):
    def __getattr__(self, attr):
        value = self.get(attr, None)
        if isinstance(value, dict):
            value = dotdict(value)
            setattr(self, attr, value)
            return value
        else:
            return value
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def get_dotfield_value(dotfield, d):
    """
    Explore dictionary d using dotfield notation and return value.
    Example::

        d = {"a":{"b":1}}.
        get_dotfield_value("a.b",d) => 1
    """
    fields = dotfield.split(".")
    if len(fields) == 1:
        return d[fields[0]]
    else:
        first = fields[0]
        return get_dotfield_value(".".join(fields[1:]), d[first])

def split_ids(q):
    '''
    split input query string into list of ids.
    any of ``" \t\n\x0b\x0c\r|,+"`` as the separator,
    but perserving a phrase if quoted
    (either single or double quoted)
    more detailed rules see:
    http://docs.python.org/2/library/shlex.html#parsing-rules

    e.g.::

        >>> split_ids('CDK2 CDK3')
         ['CDK2', 'CDK3']
        >>> split_ids('"CDK2 CDK3"\n CDk4')
         ['CDK2 CDK3', 'CDK4']
    '''
    # Python3 strings are already unicode, .encode
    # now returns a bytearray, which cannot be searched with
    # shlex.  For now, do this terrible thing until we discuss
    if sys.version_info.major == 3:
        lex = shlex(q, posix=True)
    else:
        lex = shlex(q.encode('utf8'), posix=True)
    lex.whitespace = ' \t\n\x0b\x0c\r|,+'
    lex.whitespace_split = True
    lex.commenters = ''
    if sys.version_info.major == 3:
        ids = [x.strip() for x in list(lex)]
    else:
        ids = [x.decode('utf8').strip() for x in list(lex)]
    ids = [x for x in ids if x]
    return ids


def get_compressed_outfile(filename, compress='gzip'):
    '''Get a output file handler with given compress method.
       currently support gzip/bz2/lzma, lzma only available in py3
    '''
    if compress == "gzip":
        # import gzip
        out_f = gzip.GzipFile(filename, 'wb')
    elif compress == 'bz2':
        import bz2
        out_f = bz2.BZ2File(filename, 'wb')
    elif compress == 'lzma' or compress == 'xz':
        import lzma
        out_f = lzma.LZMAFile(filename, 'wb')
    elif compress is None:
        out_f = open(filename, 'wb')
    else:
        raise ValueError("Invalid compress parameter.")
    return out_f


def open_compressed_file(filename):
    '''Get a read-only file-handler for compressed file,
       currently support gzip/bz2/lzma, lzma only available in py3
    '''
    in_f = open(filename, 'rb')
    sig = in_f.read(5)
    in_f.close()
    if sig[:3] == b'\x1f\x8b\x08':
        # this is a gzip file
        # import gzip
        fobj = gzip.GzipFile(filename, 'rb')
    elif sig[:3] == b'BZh':
        # this is a bz2 file
        import bz2
        fobj = bz2.BZ2File(filename, 'r')
    elif sig[:5] == b'\xfd7zXZ':
        # this is a lzma file
        import lzma
        fobj = lzma.LZMAFile(filename, 'r')
    else:
        # assuming uncompressed ?
        fobj = open(filename, 'rb')
    return fobj


def dump(obj, filename, protocol=4, compress='gzip'):
    '''Saves a compressed object to disk
       protocol version 4 is the default for py3.8, supported since py3.4
    '''
    # NOTE: py3.8 added protocol=5 support, 4 is still the default,
    #       We can check later to set 5 as the default when it's time.
    out_f = get_compressed_outfile(filename, compress=compress)
    pickle.dump(obj, out_f, protocol=protocol)
    out_f.close()


def dump2gridfs(obj, filename, db, protocol=2):
    '''Save a compressed (support gzip only) object to MongoDB gridfs.'''
    import gridfs

    # import gzip
    fs = gridfs.GridFS(db)
    if fs.exists(_id=filename):
        fs.delete(filename)
    fobj = fs.new_file(filename=filename, _id=filename)
    try:
        gzfobj = gzip.GzipFile(filename=filename, mode='wb', fileobj=fobj)
        pickle.dump(obj, gzfobj, protocol=protocol)
    finally:
        gzfobj.close()
        fobj.close()


def loadobj(filename, mode='file'):
    '''
    Loads a compressed object from disk file (or file-like handler) or
    MongoDB gridfs file (mode='gridfs')
    ::

        obj = loadobj('data.pyobj')
        obj = loadobj(('data.pyobj', mongo_db), mode='gridfs')
    '''
    # import gzip
    if mode == 'gridfs':
        import gridfs
        filename, db = filename   # input is a tuple of (filename, mongo_db)
        fs = gridfs.GridFS(db)
        fobj = gzip.GzipFile(fileobj=fs.get(filename))
    else:
        if is_str(filename):
            fobj = open_compressed_file(filename)
        else:
            fobj = filename   # input is a file-like handler
    try:
        obj = pickle.load(fobj)
    finally:
        fobj.close()
    return obj


def list2dict(a_list, keyitem, alwayslist=False):
    '''
    Return a dictionary with specified keyitem as key, others as values.
    keyitem can be an index or a sequence of indexes.
    For example::

        li = [['A','a',1],
             ['B','a',2],
             ['A','b',3]]
        list2dict(li, 0)---> {'A':[('a',1),('b',3)],
                             'B':('a',2)}

    If alwayslist is True, values are always a list even there is only one item in it.
    ::

        list2dict(li, 0, True)---> {'A':[('a',1),('b',3)],
                                    'B':[('a',2),]}
    '''
    _dict = {}
    for x in a_list:
        if isinstance(keyitem, int):      # single item as key
            key = x[keyitem]
            value = tuple(x[:keyitem] + x[keyitem + 1:])
        else:
            key = tuple([x[i] for i in keyitem])
            value = tuple([x[i] for i in range(len(a_list)) if i not in keyitem])
        if len(value) == 1:      # single value
            value = value[0]
        if key not in _dict:
            if alwayslist:
                _dict[key] = [value, ]
            else:
                _dict[key] = value
        else:
            current_value = _dict[key]
            if not isinstance(current_value, list):
                current_value = [current_value, ]
            current_value.append(value)
            _dict[key] = current_value
    return _dict

def filter_dict(d, keys):
    """
    Remove keys from dict "d". "keys" is a list
    of string, dotfield notation can be used to
    express nested keys. If key to remove doesn't
    exist, silently ignore it
    """
    if isinstance(keys, str):
        keys = [keys]
    for key in keys:
        if "." in key:
            innerkey = ".".join(key.split(".")[1:])
            rkey = key.split(".")[0]
            if rkey in d:
                d[rkey] = filter_dict(d[rkey], innerkey)
            else:
                continue
        else:
            d.pop(key, None)
    return d

def get_random_string():
    strb = base64.b64encode(os.urandom(6), "".join(random.sample(string.ascii_letters, 2)).encode("ascii"))
    return strb.decode("ascii")

def get_timestamp():
    return time.strftime('%Y%m%d')

class LogPrint:
    def __init__(self, log_f, log=1, timestamp=0):
        '''If this class is set to sys.stdout, it will output both log_f and __stdout__.
           log_f is a file handler.
        '''
        self.log_f = log_f
        self.log = log
        self.timestamp = timestamp
        if self.timestamp:
            self.log_f.write('*'*10 + 'Log starts at ' + time.ctime() + '*'*10 + '\n')

    def write(self, text):
        sys.__stdout__.write(text)
        if self.log:
            self.log_f.write(text)
            self.flush()

    def flush(self):
        self.log_f.flush()

    def start(self):
        sys.stdout = self

    def pause(self):
        sys.stdout = sys.__stdout__

    def resume(self):
        sys.stdout = self

    def close(self):
        if self.timestamp:
            self.log_f.write('*'*10 + 'Log ends at ' + time.ctime() + '*'*10 + '\n')
        sys.stdout = sys.__stdout__
        self.log_f.close()

    def fileno(self):
        return self.log_f.fileno()

def setup_logfile(logfile):
    fh = logging.FileHandler(logfile)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(fh)

def find_doc(k, keys):
    ''' Used by jsonld insertion in www.api.es._insert_jsonld '''
    n = len(keys)
    for i in range(n):
        # if k is a dictionary, then directly get its value
        if isinstance(k, dict):
            k = k[keys[i]]
        # if k is a list, then loop through k
        elif isinstance(k, list):
            tmp = []
            for item in k:
                try:
                    if isinstance(item[keys[i]], dict):
                        tmp.append(item[keys[i]])
                    elif isinstance(item[keys[i]], list):
                        for _item in item[keys[i]]:
                            tmp.append(_item)
                except Exception:
                    continue
            k = tmp
    return k

def SubStr(input_string, start_string='', end_string='', include=0):
    '''Return the substring between start_string and end_string.
        If start_string is '', cut string from the beginning of input_string.
        If end_string is '', cut string to the end of input_string.
        If either start_string or end_string can not be found from input_string, return ''.
        The end_pos is the first position of end_string after start_string.
        If multi-occurence,cut at the first position.
        include=0(default), does not include start/end_string;
        include=1:          include start/end_string.'''

    start_pos = input_string.find(start_string)
    if start_pos == -1:
        return ''
    start_pos += len(start_string)
    if end_string == '':
        end_pos = len(input_string)
    else:
        end_pos = input_string[start_pos:].find(end_string)   # get the end_pos relative with the start_pos
        if end_pos == -1:
            return ''
        else:
            end_pos += start_pos  # get actual end_pos
    if include == 1:
        return input_string[start_pos - len(start_string): end_pos + len(end_string)]
    else:
        return input_string[start_pos:end_pos]

def safe_unicode(s, mask='#'):
    '''replace non-decodable char into "#".'''
    try:
        _s = str(s)
    except UnicodeDecodeError as e:
        pos = e.args[2]
        _s = s.replace(s[pos], mask)
        print('Warning: invalid character "%s" is masked as "%s".' % (s[pos], mask))
        return safe_unicode(_s, mask)

    return _s

def file_newer(source, target):
    '''return True if source file is newer than target file.'''
    return os.stat(source)[-2] > os.stat(target)[-2]

def newer(t0, t1, fmt='%Y%m%d'):
    '''t0 and t1 are string of timestamps matching "format" pattern.
       Return True if t1 is newer than t0.
    '''
    return datetime.strptime(t0, fmt) < datetime.strptime(t1, fmt)


class BiothingsJSONEncoder(json.JSONEncoder):
    '''A class to dump Python Datetime object.
        json.dumps(data, cls=DateTimeJSONEncoder, indent=indent)
    '''

    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        elif isinstance(o, UserDict):
            return dict(o)
        elif isinstance(o, UserList):
            return list(o)
        else:
            return super().default(o)


# https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        if obj.tzinfo is None:
            # assuming UTC if no timezone info
            obj = obj.replace(tzinfo=timezone.utc)
        serial = obj.isoformat()
        return serial
    elif isinstance(obj, type):
        return str(obj)
    elif "SRE_Pattern" in type(obj).__name__:  # can't find the class
        return obj.pattern
    elif isinstance(obj, types.FunctionType):
        return "__function__"
    raise TypeError("Type %s not serializable" % type(obj))


def json_encode(obj):
    """Tornado-aimed json encoder, it does the same job as tornado.escape.json_encode
    but also deals with datetime encoding"""
    return json.dumps(obj, default=json_serial).replace("</", r"<\/")


def rmdashfr(top):
    '''Recursively delete dirs and files from "top" directory, then delete "top" dir'''
    assert top  # prevent rm -fr * ... (let's be explicit there)
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    try:
        os.rmdir(top)
    except FileNotFoundError:
        # top did not exist, silently ignore
        pass

def get_class_from_classpath(class_path):
    str_mod, str_klass = ".".join(class_path.split(".")[:-1]), class_path.split(".")[-1]
    mod = importlib.import_module(str_mod)
    return getattr(mod, str_klass)

def find_classes_subclassing(mods, baseclass):
    """
    Given  a module or a list of modules, inspect and find all classes which are
    a subclass of the given baseclass, inside those modules
    """
    # collect all modules found in given modules
    if not isinstance(mods, list):
        mods = [mods]
    innner_mods = inspect.getmembers(mods, lambda obj: isinstance(obj, types.ModuleType))
    mods.extend(innner_mods)
    classes = []
    for m in mods:
        name_klasses = inspect.getmembers(m, lambda obj: isinstance(obj, type) and issubclass(obj, baseclass))
        if name_klasses:
            for name, klass in name_klasses:
                del name
                classes.append(klass)
    return classes

def sizeof_fmt(num, suffix='B'):
    # http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def unzipall(folder, pattern="*.zip"):
    '''
    unzip all zip files in "folder", in "folder"
    '''
    import zipfile
    for zfile in glob.glob(os.path.join(folder, pattern)):
        zf = zipfile.ZipFile(zfile)
        logging.info("unzipping '%s'", zf.filename)
        zf.extractall(folder)
        logging.info("done unzipping '%s'", zf.filename)

def untargzall(folder, pattern="*.tar.gz"):
    '''
    gunzip and untar all ``*.tar.gz`` files in "folder"
    '''
    import tarfile
    for tgz in glob.glob(os.path.join(folder, pattern)):
        gz = gzip.GzipFile(tgz)
        tf = tarfile.TarFile(fileobj=gz)
        logging.info("untargz '%s'", tf.name)
        tf.extractall(folder)
        logging.info("done untargz '%s'", tf.name)


def gunzipall(folder, pattern="*.gz"):
    '''
    gunzip all ``*.gz`` files in "folder"
    '''
    for f in glob.glob(os.path.join(folder, pattern)):
        # build uncompress filename from gz file and pattern
        # pattern is used to select/filter files, but it may not
        # match the gzip file suffix (usually ".gz"), so assuming it's the last
        # bit after "."
        suffix = ".%s" % pattern.split(".")[1]
        gunzip(f, suffix)

def unxzall(folder, pattern="*.xz"):
    '''
    unxz all xz files in "folder", in "folder"
    '''
    import tarfile
    for xzfile in glob.glob(os.path.join(folder, pattern)):
        logging.info("unxzing '%s'", xzfile)
        with tarfile.open(xzfile, 'r:xz') as t:
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(t, folder)
        logging.info("done unxzing '%s'", xzfile)


def gunzip(f, pattern="*.gz"):
    # build uncompress filename from gz file and pattern
    destf = f.replace(pattern.replace("*", ""), "")
    fout = open(destf, "wb")
    with gzip.GzipFile(f) as gz:
        logging.info("gunzip '%s'", gz.name)
        for line in gz:
            fout.write(line)
        logging.info("Done gunzip '%s'", gz.name)
    fout.close()


async def aiogunzipall(folder, pattern, job_manager, pinfo):
    """
    Gunzip all files in folder matching pattern. job_manager is used
    for parallelisation, and pinfo is a pre-filled dict used by
    job_manager to report jobs in the hub (see bt.utils.manager.JobManager)
    """
    jobs = []
    got_error = None
    logging.info("Unzipping files in '%s'", folder)
    for f in glob.glob(os.path.join(folder, pattern)):
        pinfo["description"] = os.path.basename(f)
        job = await job_manager.defer_to_process(
            pinfo, partial(gunzip, f, pattern=pattern)
        )

        def gunzipped(fut, infile):
            try:
                # res = fut.result()
                fut.result()
            except Exception as e:
                logging.error("Failed to gunzip file %s: %s", infile, e)
                nonlocal got_error
                got_error = e
        job.add_done_callback(partial(gunzipped, infile=f))
        jobs.append(job)
        if got_error:
            raise got_error
    if jobs:
        await asyncio.gather(*jobs)
        if got_error:
            raise got_error

def uncompressall(folder):
    """Try to uncompress any known archive files in folder"""
    unzipall(folder)
    untargzall(folder)
    gunzipall(folder)
    unxzall(folder)

def md5sum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class splitstr(str):
    """Type representing strings with space in it"""

class nan(object):
    """Represents NaN type, but not as a float"""

class inf(object):
    """Represents Inf type, but not as a float"""

def traverse(obj, leaf_node=False):
    """
    Output path-dictionary pairs. For example, input:
    {
        'exac_nontcga': {'af': 0.00001883},
        'gnomad_exome': {'af': {'af': 0.0000119429, 'af_afr': 0.000123077}},
        'snpeff': {'ann': [{'effect': 'intron_variant',
                            'feature_id': 'NM_014672.3'},
                            {'effect': 'intron_variant',
                            'feature_id': 'NM_001256678.1'}]}
    }
    will be translated to a generator:
    (
        ("exac_nontcga", {"af": 0.00001883}),
        ("gnomad_exome.af", {"af": 0.0000119429, "af_afr": 0.000123077}),
        ("gnomad_exome", {"af": {"af": 0.0000119429, "af_afr": 0.000123077}}),
        ("snpeff.ann", {"effect": "intron_variant", "feature_id": "NM_014672.3"}),
        ("snpeff.ann", {"effect": "intron_variant", "feature_id": "NM_001256678.1"}),
        ("snpeff.ann", [{ ... },{ ... }]),
        ("snpeff", {"ann": [{ ... },{ ... }]}),
        ('', {'exac_nontcga': {...}, 'gnomad_exome': {...}, 'snpeff': {...}})
    )
    or when traversing leaf nodes:
    (
        ('exac_nontcga.af', 0.00001883),
        ('gnomad_exome.af.af', 0.0000119429),
        ('gnomad_exome.af.af_afr', 0.000123077),
        ('snpeff.ann.effect', 'intron_variant'),
        ('snpeff.ann.feature_id', 'NM_014672.3'),
        ('snpeff.ann.effect', 'intron_variant'),
        ('snpeff.ann.feature_id', 'NM_001256678.1')
    )
    """
    if isinstance(obj, (dict, UserDict)):  # path level increases
        for key in obj:
            for sub_path, val in traverse(obj[key], leaf_node):
                yield '.'.join((str(key), str(sub_path))).strip('.'), val
        if not leaf_node:
            yield '', obj
    elif isinstance(obj, list):  # does not affect path
        for item in obj:
            for sub_path, val in traverse(item, leaf_node):
                yield sub_path, val
        if not leaf_node or not obj:  # [] count as a leaf node
            yield '', obj
    elif leaf_node:  # including str, int, float, and *None*.
        yield '', obj

def run_once():
    """
    should_run_task_1 = run_once()
    print(should_run_task_1()) -> True
    print(should_run_task_1()) -> False
    print(should_run_task_1()) -> False
    print(should_run_task_1()) -> False

    should_run_task_2 = run_once()
    print(should_run_task_2('2a')) -> True
    print(should_run_task_2('2b')) -> True
    print(should_run_task_2('2a')) -> False
    print(should_run_task_2('2b')) -> False
    ...
    """

    has_run = set()

    def should_run(identifier=None):

        if identifier in has_run:
            return False

        has_run.add(identifier)
        return True

    return should_run

def merge(x, dx):
    """
    Merge dictionary dx (Δx) into dictionary x.
    If __REPLACE__ key is present in any level z in dx,
    z in x is replaced, instead of merged, with z in dx.
    """
    assert isinstance(x, dict)
    assert isinstance(dx, dict)

    if dx.pop("__REPLACE__", None):
        # merge dx with "nothing" just to
        # make sure to remove any "__REPLACE__"
        _y = {}
        merge(_y, dx)
        x.clear()
        x.update(_y)
        return x

    for k, v in dx.items():
        if isinstance(v, dict):
            if v.get("__REMOVE__"):
                x.pop(k, None)
                continue
            if not isinstance(x.get(k), dict):
                x[k] = {}
            merge(x[k], v)
        else:
            x[k] = v
    return x
