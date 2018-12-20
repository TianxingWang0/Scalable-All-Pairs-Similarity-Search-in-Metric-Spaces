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


class bicluster:
    def __init__(self, vec, left=None, right=None, id=None, radius=0.0):
        self.left = left
        self.right = right
        self.vec = vec
        self.id = id
        self.radius = radius
                
    def hasChild(self):
        return self.left or self.right


class compress_SimilarityReducer(Reducer):

    def __init__(self, infile=sys.stdin, separator='\t'):
        super(compress_SimilarityReducer, self).__init__(infile, separator)
        self.t = 3.0 

    def dist(self, x, y):
        res = 0
        for i in range(20):
            res += (x[i] - y[i]) ** 2
        return res ** 0.5

    def getLeaves(self, P, leaves):
        if P:
            if not P.hasChild():
                leaves.append([P.vec, P.id])
            self.getLeaves(P.left, leaves)
            self.getLeaves(P.right, leaves)

    def hcluster(self, dataset):
        distance_set = {}
        currentclustid = -1
        clust = [bicluster(dataset[i], id=i) for i in range(len(dataset))]
        while len(clust) > 1:
            lowestpair = (0, 1)
            closest = self.dist(clust[0].vec, clust[1].vec)
            for i in range(len(clust)):
                for j in range(i+1, len(clust)):
                    if (clust[i].id, clust[j].id) not in distance_set:
                        distance_set[(clust[i].id, clust[j].id)] = self.dist(clust[i].vec, clust[j].vec)
                    d = distance_set[(clust[i].id, clust[j].id)]
                    if d < closest:
                        closest = d
                        lowestpair = (i, j)
            mergevec = [(clust[lowestpair[0]].vec[i] + clust[lowestpair[1]].vec[i]) / 2.0 for i in range(len(clust[0].vec))]
            newcluster = bicluster(mergevec, left=clust[lowestpair[0]], right=clust[lowestpair[1]], id=currentclustid, radius=self.dist(clust[lowestpair[0]].vec, clust[lowestpair[1]].vec) / 2.0 + max(clust[lowestpair[0]].radius, clust[lowestpair[1]].radius))
            currentclustid -= 1
            del clust[lowestpair[1]]
            del clust[lowestpair[0]]
            clust.append(newcluster)
        return clust[0]

    def MayHavePairs(self, Pa, Pb):
        return self.dist(Pa.vec, Pb.vec) < Pa.radius + Pb.radius + self.t

    def EmitBiCliqueFormat(self, List1, List2):
        if len(List1) == 1 and len(List2) == 1:
            self.emit(List1[0], List2[0])
        else:
            self.emit('[' + ','.join(List1), ','.join(List2) + ']')

    def EmitBiClique(self, Pa_leaves, Pb_leaves):
        List1 = [UIDset[i[1]] for i in Pa_leaves]
        List2 = [UIDset[i[1]] for i in Pb_leaves if UIDset[i[1]] not in List1]
        if List2:
            if len(List1) == 1 and len(List2) == 1:
                self.emit(List1[0], List2[0])
            else:
                self.emit('[' + ','.join(List1), ','.join(List2) + ']')

    def isAndEmitBiClique(self, Pa, Pb):
        if not Pa.hasChild() and not Pb.hasChild():
            return False
        Pa_leaves = []
        Pb_leaves = []
        self.getLeaves(Pa, Pa_leaves)
        self.getLeaves(Pb, Pb_leaves)
        for i in Pa_leaves:
            for j in Pb_leaves:
                if self.dist(i[0], j[0]) >= self.t:
                    return False
        self.EmitBiClique(Pa_leaves, Pb_leaves)
        return True

    def EmitAdjLists(self, Pa, Pb):
        ida = UIDset[Pa.id]
        idb = UIDset[Pb.id]
        if ida != idb and self.dist(Pa.vec, Pa.vec) < self.t:
            if (idb, ida) not in singlePairs:
                singlePairs.add((ida, idb))
                self.emit(ida, idb)

    def EmitSimSet(self, Pa, Pb):
        if self.isAndEmitBiClique(Pa, Pb):
            pass
        else:
            if self.MayHavePairs(Pa, Pb):
                if Pa.hasChild():
                    self.EmitSimSet(Pa.left, Pb)
                    self.EmitSimSet(Pa.right, Pb)
                elif Pb.hasChild():
                    self.EmitSimSet(Pa, Pb.left)
                    self.EmitSimSet(Pa, Pb.right)
                else:
                    self.EmitAdjLists(Pa, Pb)

    def reduce(self):
        for ci, points in self:
            numInner = 0
            dataset = []
            global UIDset
            global singlePairs
            singlePairs = set()
            UIDset = []
            for point in points:
                pi, piType, ui = point[1].split(',')
                pi = [int(k) for k in pi]
                if piType == 'inner':
                    numInner += 1
                    dataset.insert(0, pi)
                    UIDset.insert(0, ui)
                else:
                    dataset.append(pi)
                    UIDset.append(ui)
            if numInner:
                innerTree = self.hcluster(dataset[:numInner])
                Tree = self.hcluster(dataset)
                self.EmitSimSet(innerTree, Tree)

if __name__ == '__main__':
    reducer = compress_SimilarityReducer()
    reducer.reduce()
