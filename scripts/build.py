import os
import sys
import shlex
import shutil
import platform
from subprocess import check_output, call

DEVNULL = open(os.devnull, 'wb')
HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_CONF = os.path.join(HOME, 'conf')
DIR_BUILD = os.path.join(HOME, 'build')
CONF = os.path.join(DIR_CONF, 'build.cfg')

sys.path.append(os.path.join(HOME, 'scripts'))
from configure import *

def _copy_dir(dir_src, dir_dest):
    if not os.path.exists(dir_dest):
        os.makedirs(dir_dest, 0o755)
    for i in os.listdir(dir_src):
        src = os.path.join(dir_src, i)
        dest = os.path.join(dir_dest, i)
        if os.path.isdir(src):
            _copy_dir(src, dest)
        else:
            shutil.copy(src, dest)

def _generate():
    for i in ['src', 'include']:
        dir_dest = os.path.join(DIR_BUILD, i)
        dir_src = os.path.join(HOME, i)
        _copy_dir(dir_src, dir_dest)

def _chkconf():
    name = platform.system()
    if name not in PLAT:
        raise Exception('Error: %s is not supported' % name)

    if not 'tools' in INFO:
        INFO['tools'] = {}

    if not 'version' in INFO:
        INFO['version'] = '0.0'

    if not INFO['name']:
        INFO['name'] = os.path.basename(HOME)

def _chktools():
    tools = ['aclocal', 'autoconf', 'autoheader', 'automake', 'autoreconf']
    for i in tools:
        if i not in DEPS:
            DEPS.append(i)
    for i in DEPS:
        if not check_output(['which', i]):
            raise Exception('cannot find %s' %i)
    version = None
    ret = check_output('autoconf -V | head -n1', shell=True)
    if type(ret) != str:
        ret = ret.decode('utf8')
    if ret:
        version = ret.split(' ')[-1]
    if not version:
        raise Exception('cannot get the version of autoconf')
    INFO['tools']['autoconf'] = {'version': version}

def _do_get_source_files(dirname, files, recursive):
    dir_src = os.path.join(DIR_BUILD, dirname)
    for i in os.listdir(dir_src):
        path = os.path.join(dir_src, i)
        if not os.path.isdir(path):
            if i.endswith('.c'):
                files.append(os.path.join(dirname, i))
        elif not i.startswith('.'):
            if recursive:
                _do_get_source_files(os.path.join(dirname, i), files, recursive)

def _get_source_files(dirname, recursive=False):
    files = []
    _do_get_source_files(dirname, files, recursive)
    return files

def _get_source_list(head, files):
    cnt = 0
    for i in files:
        if cnt % 8 == 7:
            head += '\\\n\t'
        head += '%s ' % i
        cnt += 1
    return head + '\n'

def _conf_proj():
    global DEFS
    lines = []
    lines.append('AC_PREREQ([%s])\n' % INFO['tools']['autoconf']['version'])
    lines.append('AC_INIT([%s], [%s])\n' % (INFO['name'], INFO['version']))
    lines.append('AC_CONFIG_SRCDIR([config.h.in])\n')
    lines.append('AC_CONFIG_HEADERS([config.h])\n')
    lines.append('AC_CONFIG_MACRO_DIR([m4])\n')
    lines.append('LT_INIT\n')
    lines.append('AC_PROG_CC\n')
    lines.append('AC_PROG_RANLIB\n')
    if LIBS:
        lines.append('AC_CHECK_LIB([%s])\n' % ' '.join(LIBS))
    lines.append('AM_INIT_AUTOMAKE([foreign subdir-objects -Werror])\n')
    lines.append('AC_OUTPUT([Makefile])\n')

    path = os.path.join(DIR_BUILD, 'configure.ac')
    with open(path, 'w') as f:
        f.writelines(lines)

    libs = []
    dir_src = os.path.join(DIR_BUILD, 'src')
    for i in os.listdir(dir_src):
        if not i.startswith('.'):
            path = os.path.join(dir_src, i)
            if os.path.isdir(path):
                libs.append(i)

    lines = []
    lines.append('ARFLAGS = cr\n')
    lines.append('ACLOCAL_AMFLAGS = -I m4\n')
    lines.append('LDFLAGS = -L/usr/local/lib\n')
    if LIBS:
        lines.append('LIBS = %s\n' % ' '.join(["-l%s" % i for i in LIBS]))
    cflags = 'AM_CFLAGS = -I/usr/local/include -I./include -I./src -std=gnu11 -Wno-unused-result'
    if INCL:
        cflags += ' %s' % ' '.join(['-I%s' % i for i in INCL])
    if libs:
        cflags += ' %s' % ' '.join(['-I./src/%s' % i for i in libs])
    if DEFS:
        cflags += ' %s' % ' '.join(['-D%s' % i for i in DEFS])
    if platform.system() == 'Linux':
        cflags += ' -DLINUX'
    lines.append(cflags + '\n\n')
    lines.append('bin_PROGRAMS = %s\n' % INFO['name'])

    sources = '%s_SOURCES = ' % INFO['name']
    files = _get_source_files('src')
    lines.append(_get_source_list(sources, files))

    lib_list = ' '.join(['lib%s.a' % i for i in libs])
    lines.append('%s_LDADD = %s\n\n' % (INFO['name'], lib_list))
    lines.append('AUTOMAKE_OPTIONS = foreign\n')
    lines.append('noinst_LIBRARIES = %s\n\n' % lib_list)

    for name in libs:
        sources = 'lib%s_a_SOURCES = ' % name
        path = os.path.join('src', name)
        files = _get_source_files(path, recursive=True)
        lines.append(_get_source_list(sources, files))
        lines.append('\n')

    path = os.path.join(DIR_BUILD, 'Makefile.am')
    with open(path, 'w') as f:
        f.writelines(lines)

    path = os.path.join(DIR_BUILD, 'm4')
    if not os.path.exists(path):
        os.makedirs(path, 0o755)
    shutil.copytree(DIR_CONF, os.path.join(DIR_BUILD, 'conf'))

def _read_args():
    with open(CONF) as f:
        lines = f.readlines()

    for i in lines:
        i = i.strip()
        if i and not i.startswith('#'):
            res = i.split('=')
            if len(res) != 2:
                raise Exception('Error: failed to parse %s' % i)
            key = res[0].upper()
            val = res[1].split('#')[0].strip()
            if key not in ARGS:
                raise Exception('Error: cannot find the definition of %s' % key)
            ARGS[key]['value'] = val

def _chkargs():
    _read_args()
    for i in ARGS:
        res = ARGS[i].get('value')
        if None == res:
            res = ARGS[i].get('default')
            if None == res:
                raise Exception('Error: %s is not set' % i)
        val = ''
        typ = ARGS[i].get('type')
        if typ == 'bool':
            if int(res) != 0:
                val = i.upper()
                if val in DEFS:
                    continue
        elif typ == 'str':
            val = '%s=\'"%s"\'' % (i.upper(), str(res))
        elif typ == 'int':
            if not ARGS[i].get('map'):
                val = '%s=%s' % (i.upper(), str(res))
            else:
                val = '%s' % ARGS[i]['map'][res]
        else:
            raise Exception('Error: invalid type of %s' % i)
        if val:
            DEFS.append(val)

def _configure():
    _chkargs()
    _chkconf()
    _chktools()
    _conf_proj()

def _call(cmd, path=None, quiet=False, ignore=False):
    if path:
        os.chdir(path)
    if not quiet:
        check_output(cmd, shell=True)
    else:
        if call(shlex.split(cmd), stdout=DEVNULL, stderr=DEVNULL):
            if not ignore:
                raise Exception('Error: failed to run %s' % cmd)

def _auto_conf(path):
    _call('aclocal', path, quiet=True)
    _call('autoconf', path, quiet=True)
    _call('autoheader', path, quiet=True)
    _call('touch NEWS README AUTHORS ChangeLog', path)
    _call('automake --add-missing', path, quiet=True, ignore=True)
    _call('autoreconf -if', path, quiet=True)

def _make(path):
    _call('./configure', path, quiet=True)
    _call('make', path)

def _build():
    _auto_conf(DIR_BUILD)
    _make(DIR_BUILD)

if __name__ == '__main__':
    _generate()
    _configure()
    _build()
