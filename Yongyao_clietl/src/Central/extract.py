# -*- coding: utf-8 -*-
"""
Created on Thu Jan 19 22:11:32 2023

@author: Administrator
"""

from etlcli.db import extract
import argparse

parser = argparse.ArgumentParser(description='function for extracting data')
parser.add_argument('--database', '-d', help='name of connect database Required parameters', required=True)
parser.add_argument('--sql', '-s', help='query,Required parameters', required=True)
parser.add_argument('--target', '-t', help='target file save name, Required parameters', required=True)
parser.add_argument('--parmas', '-p', help='parmas for query ,Variable parameters')
args = parser.parse_args()

if __name__ == '__main__':
    try:
        extract(args.database, args.sql, args.target, args.parmas)
    except Exception as e:
        print(e)