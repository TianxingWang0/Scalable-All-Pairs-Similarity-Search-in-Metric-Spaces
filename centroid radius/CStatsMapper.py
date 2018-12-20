#!/usr/bin/env python

import sys
import csv

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


class CStatsMapper(Mapper):
    def __init__(self, infile=sys.stdin, separator='\t'):
        super(CStatsMapper, self).__init__(infile, separator)

        with open('centroid500.csv', 'r') as infile: 
            self.centroids = []        
            reader = csv.reader(infile)
            for row in reader:
                self.centroids.append([int(i) for i in row])

    def dist(self, x, y):
        res = 0
        for i in range(20):
            res += (x[i] - y[i]) ** 2
        return res ** 0.5

    def map(self):
        for record in self:
            try:
	            UserID, pi = record.split('\t')
                pi = [int(i) for i in pi]            
                key = 0
                minDist = 8 * (5 ** 0.5)
                for i in range(len(self.centroids)):
                    distance = self.dist(pi, self.centroids[i])
                    if distance < minDist:
                        minDist = distance
                        key = i
                self.emit(key, minDist)
            except:
                pass
      

if __name__ == "__main__":
    mapper = CStatsMapper()
    mapper.map()
