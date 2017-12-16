# -*- coding: utf-8 -*-
import sys

def main(attrValue):
    try:
        info = open('entityName', 'r')
        # read all content as a string
        strings = info.read()
        dict_list = ['{']
        dict_list.extend(strings.split('{')[1:])
        with open('entityName', 'w') as f:
            for each in dict_list:
                f.write(each)
    except IOError:
        print 'file {0} not find!'.format('entityName')
        sys.exit(1)
    finally:
        info.close()

if __name__ == '__main__':
    main('json_parse.txt','tidy_data.txt')
