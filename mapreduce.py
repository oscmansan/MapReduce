#!/usr/bin/python	
from os import path
from codecs import encode, decode
from pymongo import MongoClient
from bson.code import Code

conn = MongoClient()
db = conn.groceries
db.transactions.drop()

with open("groceries.csv") as f:
    for line in f:
        items = line.strip().split(',')
        d = {}
        d['content'] = items
        db.transactions.insert_one(d)

# Mapper to count each product
mapperItems = Code("""
function() {
    for (var i = 0; i < this.content.length; i++) {
        emit(this.content[i],1);
    }
}
                   """)

# Mapper to count pairs of products
mapperPairs = Code("""
function() {
    for (var i = 0; i < this.content.length; i++) {
        var a = this.content[i];
        for (var j = i+1; j < this.content.length; j++) {
            var b = this.content[j];
            var cmp = a.localeCompare(b);
            var key = cmp < 0 ? a+","+b : b+","+a;
            emit(key,1);
        }
    }
}
                   """)

# Reducer to count the acumulated values
# Works for both Mappers
reducer = Code("""
function(key,values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
        total += values[i];
    }
    return total;
}
               """) 

db.transactions.map_reduce(mapperItems, reducer, "item_counts")
db.transactions.map_reduce(mapperPairs, reducer, "pair_counts")

supMin = 0.07
confMin = 0.25
n_trans = db.transactions.count()
count = 0
# Association Rule between pair[0]	->	pair [1]
def associationRule (pair , value):	
	n_first= db.item_counts.find({'_id':pair[0]})[0]['value']
	sup = value / n_trans
	conf = value / n_first
	if (sup > supMin and conf > confMin):
		global count
		count += 1
		print str(pair[0]) + '->' + str(pair[1]) + ': sup=' + str(sup) + ' conf=' + str(conf)

# For each pair of products, we calculate bidireccional association rules
ans = db.pair_counts.find()
for pair in ans:
	p = pair['_id'].split(',')
	q = p[::-1]
	v = float(pair['value'])
	associationRule(p, v)
	associationRule(q, v)

print 'count:', count

