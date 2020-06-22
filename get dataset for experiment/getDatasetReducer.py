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

MovieIDList = ['mv_0001905.txt',
               'mv_0002152.txt',
               'mv_0003860.txt',
               'mv_0004432.txt',
               'mv_0005317.txt',
               'mv_0005496.txt',
               'mv_0006037.txt',
               'mv_0006287.txt',
               'mv_0006972.txt',
               'mv_0009340.txt',
               'mv_0011283.txt',
               'mv_0012317.txt',
               'mv_0012470.txt',
               'mv_0014313.txt',
               'mv_0015107.txt',
               'mv_0015124.txt',
               'mv_0015205.txt',
               'mv_0015582.txt',
               'mv_0016242.txt',
               'mv_0016377.txt']

class getDatasetReducer(Reducer):

    def reduce(self):
        for UserID, values in self:
            res = ['0'] * 20
            sum = 0
            counter = 0
            for value in values:
                counter += 1
                MovieID, rate = value[1].split(',')
                res[MovieIDList.index(MovieID)] = rate
                sum += int(rate)
            mean = str(int(sum / 20))
            number_of_zero = 0
            for i in res:
                if i == '0':
                    number_of_zero += 1
            if number_of_zero <= 4:
                for i in range(20):
                    if res[i] == '0':
                        res[i] = mean
                self.emit(UserID, ''.join(res))


if __name__ == '__main__':
    reducer = getDatasetReducer()
    reducer.reduce()
