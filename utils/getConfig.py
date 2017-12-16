import sys
import os

if sys.hexversion >= 0x03000000:
    import configparser
else:
    import ConfigParser as configparser

def set_conf(section,key,value):
    _config = configparser.ConfigParser()
    ini_file = os.getcwd() + '//config//report.ini'

    try:
        _config.read(ini_file)
        if not _config.has_section(section):
            _config.add_section(str(section))
        _config.set(str(section),str(key),str(value))
        with open(ini_file,'w') as _file:
            _config.write(_file)
            _file.close()
            del _file
    finally:
        del _config

def get_conf(section,key):
    _config = configparser.ConfigParser()
    ini_file = os.getcwd() + '//config//report.ini'

    _config.read(ini_file)
    value = _config.get(str(section), str(key))

    return value

if __name__ == '__main__':
    # path = os.path.split(os.path.realpath(__file__))[0] + '/set.ini'
    set_conf('General','key','123')
    str = get_conf('General','key')
    print str