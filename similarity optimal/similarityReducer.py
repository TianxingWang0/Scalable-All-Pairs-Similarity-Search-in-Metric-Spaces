#!/usr/bin/env python

import os
import sys

from itertools import groupby
from operator import itemgetter

SEPARATOR = "\t"

class Streaming(object):

    @staticmethod
    def get_job_conf(name):
        name = name.replace(".", "_").upper()
        return os.environ.get(name)

    def __init__(self, infile=sys.stdin, separator=SEPARATOR):
        self.infile = infile
        self.sep    = separator

    def status(self, message):
        sys.stderr.write("reporter:status:{}\n".format(message))

    def counter(self, counter, amount=1, group="Python Streaming"):
        sys.stderr.write("reporter:counter:{},{},{}\n".format(group, counter, amount))

    def emit(self, key, value):
        sys.stdout.write("{}{}{}\n".format(key, self.sep, value))

    def read(self):               
        for line in self.infile:
            yield line.rstrip()

    def __iter__(self):
        for line in self.read():
            yield line

class Mapper(Streaming):

    def map(self):
        raise NotImplementedError("Mappers must implement a map method")

class Reducer(Streaming):

    def reduce(self):
        raise NotImplementedError("Reducers must implement a reduce method")

    def __iter__(self):
        generator = (line.split(self.sep, 1) for line in self.read())
        for item in groupby(generator, itemgetter(0)):
            yield item

class SimilarityReducer(Reducer):

    def __init__(self, infile=sys.stdin, separator='\t'):
        super(SimilarityReducer, self).__init__(infile, separator)

        self.t = 1.0    # 阈值

    def dist(self, x, y):
        res = 0
        for i in range(20):
            res += (x[i] - y[i]) ** 2
        return res ** 0.5

    def oneIsInner(self, piType, pjType):
        if piType == 'outer' and pjType == 'outer':
            return False
        else:
            return True

    def reduce(self):
        for ci, points in self:
            workset = []
            for point in points:
                workset.append(point[1])
            for i in range(len(workset)):
                pi, piType, ui = workset[i].split(',')
                pi = [int(k) for k in pi]
                for j in range(i+1, len(workset)):
                    pj, pjType, uj = workset[j].split(',')
                    pj = [int(k) for k in pj]
                    if self.oneIsInner(piType, pjType) and self.dist(pi, pj) < self.t:
                        self.emit(ui, uj)

            
if __name__ == '__main__':
    reducer = SimilarityReducer()
    reducer.reduce()
