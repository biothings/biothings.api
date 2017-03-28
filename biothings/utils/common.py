"""
This module contains util functions may be shared by both BioThings data-hub and web components.
In general, do not include utils depending on any third-party modules.
"""
import base64
import os
import io
import random
import string
import sys
import time
from itertools import islice
from contextlib import contextmanager
import os.path
from shlex import shlex
import pickle
import json
import logging
import importlib
import statistics

if sys.version_info.major == 3:
    str_types = str
    import pickle       # noqa
else:
    str_types = (str, unicode)    # noqa
    import cPickle as pickle


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
    except (ValueError,TypeError):
        return False


def is_str(s):
    """return True or False if input is a string or not.
        python3 compatible.
    """
    return isinstance(s, str_types)


def is_seq(li):
    """return True if input is either a list or a tuple.
    """
    return isinstance(li, (list, tuple))


def is_float(f):
    """return True if input is a float.
    """
    return isinstance(f, float)

def is_scalar(f):
    return type(f) in (int,str,bool,float,bytes) or f is None


def infer(struct,key=None,mapt=None,mode="type",level=0):
    """
    Explore struct and report types contained in it.
    - struct: is the data structure to explore
    - mapt: if not None, will complete that type map with passed struct. This is usefull
      when iterating over a dataset of similar data, trying to find a good type summary 
      contained in that dataset.
    - (level: is for internal purposes, mostly debugging)
    - mode: "type", "stats", "deepstats"
    """
    def report_common(val,drep):
        drep["_count"] += 1
        drep["_sum"] += val
        drep["_min"] = val < drep["_min"] and val or drep["_min"]
        drep["_max"] = val > drep["_max"] and val or drep["_max"]
        drep["_mean"] = drep["_sum"] / drep["_count"]
        if mode== "deepstats":
            # just keep track of vals for now, stats are computed at the end
            drep["__vals"].append(val)

    def report_str(val,drep):
        report_common(val,drep)

    def report_numeric(val,drep):
        report_common(val,drep)

    def report_bool(val,drep):
        report_bool(val,drep)

    indent = 3
    if mapt is None:
        mapt = {}
    #print("%sstruct(%s):%s, mapt:%s" % ((" "*indent*level),type(struct),repr(struct),mapt))
    if type(struct) == dict:
        # was this struct already explored before ? was it a list for that previous doc ?
        # then we have to pretend here it's also a list even if not, because we want to
        # report the list structure
        for k in struct:
            if mapt and list in mapt:# and key == k:
                already_explored_as_list = True
            else:
                already_explored_as_list = False
            if already_explored_as_list:
                mapt[list].setdefault(k,{})
                typ = infer(struct[k],key=k,mapt=mapt[list][k],mode=mode,level=level+1)
                mapt[list].update({k:typ})
            else:
                mapt.setdefault(k,{})
                typ = infer(struct[k],key=k,mapt=mapt[k],mode=mode,level=level+1)
    elif type(struct) == list:
        mapl = {}
        for e in struct:
            typ = infer(e,key=key,mapt=mapl,mode=mode,level=level+1)
            mapl.update(typ)
        ##if mode != "type":
        ##    mapl.update({"_min":0,"_max":0,"_mean":None,"_count":0,"_sum":0,"__vals":[]})
        ##    lenl = len([k for k in mapl if not k.startswith("_")])
        ##    #print("on rpoet %s %s" % (lenl,mapl))
        ##    report_str(lenl,mapl)
        ##    pass
        # if mapt exist, it means it's been explored previously but not a list,
        # instead of mixing dict and list types, we want to normalize so we merge the previous
        # struct into that current list
        if mapt and list in mapt:
            mapt[list].update(mapl)
        else:
            topop = []
            if mapt:
                topop = list(mapt.keys())
                mapl.update(mapt)
            mapt[list] = mapl
            for k in topop:
                mapt.pop(k,None)
    elif is_scalar(struct):
        typ = type(struct)
        if mode == "type":
            mapt[typ] = 1
        else:
            mapt.setdefault(typ,{"_min":0,"_max":0,"_mean":None,"_count":0,"_sum":0,"__vals":[]})
            if is_str(struct):
                report_str(len(struct),mapt[typ])
            elif is_int(struct) or is_float(struct):
                report_numeric(struct,mapt[typ])
            elif type(struct) == bool:
                report_bool(struct,mapt[typ])
    else:
        raise TypeError("Can't analyze type %s" % type(struct))

    return mapt

def infer_docs(docs,mode="type",clean=True):

    def post(mapt, mode,clean):
        if type(mapt) == dict:
            for k in list(mapt.keys()):
                if is_str(k) and k.startswith("__"):
                    if k == "__vals" and mode == "deepstats":
                        if len(mapt["__vals"]) > 1:
                            mapt["_stdev"] = statistics.stdev(mapt["__vals"])
                            mapt["_median"] = statistics.median(mapt["__vals"])
                    if clean:
                        mapt.pop(k)
                else:
                    post(mapt[k],mode,clean)
        elif type(mapt) == list:
            for e in mapt:
                post(e,mode,clean)

    mapt = {}
    for doc in docs:
        #print("")
        #print("mapt is: %s" % mapt)
        #print("doc: %s" % doc)
        infer(doc,mapt=mapt,mode=mode)
    post(mapt,mode,clean)
    return mapt


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
    return a file handler with the support for gzip/zip comppressed files
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
        import gzip
        in_f = io.TextIOWrapper(gzip.GzipFile(infile, 'r'))
    elif filetype == '.zip':
        import zipfile
        in_f = io.TextIOWrapper(zipfile.ZipFile(infile, 'r').open(rawfile, 'r'))
    else:
        in_f = open(infile, mode)
    return in_f

def is_filehandle(fh):
    '''return True/False if fh is a file-like object'''
    return hasattr(fh, 'read') and hasattr(fh, 'close')


class open_anyfile(object):
    '''a context manager can be used in "with" stmt.
       accepts a filehandle or anything accepted by anyfile function.

        with open_anyfile('test.txt') as in_f:
            do_something()
    '''
    def __init__(self, infile, mode='r'):
        self.infile = infile
        self.mode = mode

    def __enter__(self):
        if is_filehandle(self.infile):
            self.in_f = self.infile
        else:
            self.in_f = anyfile(self.infile, mode=self.mode)
        return self.in_f

    def __exit__(self, type, value, traceback):
        self.in_f.close()


@contextmanager
def open_anyfile2(infile, mode='r'):
    '''a context manager can be used in "with" stmt.
       accepts a filehandle or anything accepted by anyfile function.

        with open_anyfile('test.txt') as in_f:
            do_something()

       This is equivelant of above open_anyfile, but simplier code flow.
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
            return dotdict(value)
        else:
            return value
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def split_ids(q):
    '''split input query string into list of ids.
       any of " \t\n\x0b\x0c\r|,+" as the separator,
        but perserving a phrase if quoted
        (either single or double quoted)
        more detailed rules see:
        http://docs.python.org/2/library/shlex.html#parsing-rules

        e.g. split_ids('CDK2 CDK3') --> ['CDK2', 'CDK3']
             split_ids('"CDK2 CDK3"\n CDk4')  --> ['CDK2 CDK3', 'CDK4']

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
        import gzip
        out_f = gzip.GzipFile(filename, 'wb')
    elif compress == 'bz2':
        import bz2
        out_f = bz2.BZ2File(filename, 'wb')
    elif compress == 'lzma':
        import lzma
        out_f = lzma.LZMAFile(filename, 'wb')
    elif compress == None:
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
        import gzip
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
        fobj = open(filename,'rb')
    return fobj


def dump(obj, filename, bin=2, compress='gzip'):
    '''Saves a compressed object to disk
       binary protocol 2 is compatible with py2, 3 and 4 are for py3
    '''
    print('Dumping into "%s"...' % filename, end='')
    out_f = get_compressed_outfile(filename, compress=compress)
    pickle.dump(obj, out_f, protocol=bin)
    out_f.close()
    print('Done. [%s]' % os.stat(filename).st_size)


def dump2gridfs(object, filename, db, bin=2):
    '''Save a compressed (support gzip only) object to MongoDB gridfs.'''
    import gridfs
    import gzip
    print('Dumping into "MongoDB:%s/%s"...' % (db.name, filename), end='')
    fs = gridfs.GridFS(db)
    if fs.exists(_id=filename):
        fs.delete(filename)
    fobj = fs.new_file(filename=filename, _id=filename)
    try:
        gzfobj = gzip.GzipFile(filename=filename, mode='wb', fileobj=fobj)
        pickle.dump(object, gzfobj, protocol=bin)
    finally:
        gzfobj.close()
        fobj.close()
    print('Done. [%s]' % fs.get(filename).length)


def loadobj(filename, mode='file'):
    '''Loads a compressed object from disk file (or file-like handler) or
        MongoDB gridfs file (mode='gridfs')
           obj = loadobj('data.pyobj')

           obj = loadobj(('data.pyobj', mongo_db), mode='gridfs')
    '''
    import gzip
    if mode == 'gridfs':
        import gridfs
        filename, db = filename   # input is a tuple of (filename, mongo_db)
        fs = gridfs.GridFS(db)
        fobj = fs.get(filename)
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
    '''Return a dictionary with specified keyitem as key, others as values.
       keyitem can be an index or a sequence of indexes.
       For example: li=[['A','a',1],
                        ['B','a',2],
                        ['A','b',3]]
                    list2dict(li,0)---> {'A':[('a',1),('b',3)],
                                         'B':('a',2)}
       if alwayslist is True, values are always a list even there is only one item in it.
                    list2dict(li,0,True)---> {'A':[('a',1),('b',3)],
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

def filter_dict(d,keys):
    """
    Remove keys from dict "d". "keys" is a list
    of string, dotfield notation can be used to
    express nested keys. If key to remove doesn't
    exist, silently ignore it
    """
    if type(keys) == str:
        keys = [keys]
    for key in keys:
        if "." in key:
            innerkey = ".".join(key.split(".")[1:])
            rkey = key.split(".")[0]
            if rkey in d:
                d[rkey] = filter_dict(d[rkey],innerkey)
            else:
                continue
        else:
            d.pop(key,None)
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
        if type(k) == dict:
            k = k[keys[i]]
        # if k is a list, then loop through k
        elif type(k) == list:
            tmp = []
            for item in k:
                try:
                    if type(item[keys[i]]) == dict:
                        tmp.append(item[keys[i]])
                    elif type(item[keys[i]]) == list:
                        for _item in item[keys[i]]:
                            tmp.append(_item)
                except:
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

def newer(t0, t1, format='%Y%m%d'):
    '''t0 and t1 are string of timestamps matching "format" pattern.
       Return True if t1 is newer than t0.
    '''
    return datetime.strptime(t0, format) < datetime.strptime(t1, format)


class DateTimeJSONEncoder(json.JSONEncoder):
    '''A class to dump Python Datetime object.
        json.dumps(data, cls=DateTimeJSONEncoder, indent=indent)
    '''
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return super(DateTimeJSONEncoder, self).default(obj)

def rmdashfr(top):
    '''Recursively delete dirs and files from "top" directory, then delete "top" dir'''
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root,name))
        for name in dirs:
            os.rmdir(os.path.join(root,name))
    try:
        os.rmdir(top)
    except FileNotFoundError:
        # top did not exist, silently ignore
        pass

def get_class_from_classpath(class_path):
    str_mod, str_klass = ".".join(class_path.split(".")[:-1]), class_path.split(".")[-1]
    mod = importlib.import_module(str_mod)
    return getattr(mod,str_klass)

def sizeof_fmt(num, suffix='B'):
	# http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

import zipfile, glob
def unzipall(folder,pattern="*.zip"):
    '''
    unzip all zip files in "folder", in "folder"
    '''
    for zfile in glob.glob(os.path.join(folder,pattern)):
        zf = zipfile.ZipFile(zfile)
        logging.info("unzipping '%s'" % zf.filename)
        zf.extractall(folder)
        logging.info("done unzipping '%s'" % zf.filename)

import tarfile, gzip
def untargzall(folder,pattern="*.tar.gz"):
    '''
    gunzip and untar all *.tar.gz files in "folder"
    '''

    for tgz in glob.glob(os.path.join(folder,pattern)):
        gz = gzip.GzipFile(tgz)
        tf = tarfile.TarFile(fileobj=gz)
        logging.info("untargz '%s'" % tf.name)
        tf.extractall(folder)
        logging.info("done untargz '%s'" % tf.name)

def gunzipall(folder,pattern="*.gz"):
    '''
    gunzip all *.gz files in "folder"
    '''

    for f in glob.glob(os.path.join(folder,pattern)):
        # build uncompress filename from gz file and pattern
        destf = f.replace(pattern.replace("*",""),"")
        fout = open(destf,"wb")
        with gzip.GzipFile(f) as gz:
            logging.info("gunzip '%s'" % gz.name)
            for line in gz:
                fout.write(line)
            logging.info("Done gunzip '%s'" % gz.name)
        fout.close()

