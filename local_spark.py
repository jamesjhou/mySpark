#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 29 14:11:42 2019
running a spark job locally on a Mac
@author: james
"""

import os
os.environ['JAVA_HOME'] = "/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"

from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setMaster("local[3]")
conf.setAppName('local_spark')

sc = SparkContext(conf = conf)

def mod(x):
    import numpy as np
    return (x, np.mod(x, 2))

rdd = sc.parallelize( range(1000) ).map(mod).take(10)
print(rdd)