# -*- coding: utf-8 -*-
"""
Created on Thu Jan 19 22:22:26 2023

@author: Administrator
"""

from etlcli.db import load
import argparse

parser = argparse.ArgumentParser(description='Test for argparse')
parser.add_argument('--input', '-i', help='address of input csv file', required=True)
parser.add_argument('--database', '-d', help='name of connect database Required parameters', required=True)
parser.add_argument('--target', '-t', help='target table\'s name, Required parameters', required=True)
args = parser.parse_args()

if __name__ == '__main__':
    try:
        load(args.input, args.database, args.target)
    except Exception as e:
        print(e)