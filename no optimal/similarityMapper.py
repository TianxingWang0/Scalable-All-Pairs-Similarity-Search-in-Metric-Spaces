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

class similarityMapper(Mapper):
    def __init__(self, infile=sys.stdin, separator='\t'):
        super(similarityMapper, self).__init__(infile, separator)
        self.centroidStats = []
        with open('centroidStats100', 'r') as f:
            for line in f:
                c, dist = line.rstrip().split('\t')
                self.centroidStats.append([c, float(dist)])
        self.t = 1.0         # 阈值

    def dist(self, x, y):
        res = 0
        for i in range(20):
            res += (x[i] - y[i]) ** 2
        return res ** 0.5     

    def map(self):
        for record in self:
            UserID, strpi = record.split('\t')
            pi = [int(i) for i in strpi]
            distList = [self.dist(pi, [int(i) for i in cs[0]]) for cs in self.centroidStats]
            partition = self.centroidStats[distList.index(min(distList))]
            for cs in self.centroidStats:
                if cs[0] == partition[0]:
                    self.emit(partition[0], strpi + "," + "inner" + ","  + UserID)
                elif self.dist(pi, [int(i) for i in cs[0]]) < cs[1] + self.t:
                    self.emit(cs[0], strpi + ',' + 'outer' + ',' + UserID)     
 
                     
if __name__ == "__main__":
    mapper = similarityMapper()
    mapper.map()
