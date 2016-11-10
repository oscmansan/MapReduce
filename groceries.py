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


mapperItems = Code("""
function() {
    for (var i = 0; i < this.content.length; i++) {
        emit(this.content[i],1);
    }
}
                   """)

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

s = 0.01
c = 0.01
n = db.transactions.count()
ans = db.pair_counts.find()
count = 0
for pair in ans:
	p = pair['_id'].split(',')
	v = float(pair['value'])
	m = db.item_counts.find({'_id':p[0]})[0]['value']

	sup = v / n
	conf = v / m

	if (sup > s and conf > c):
		count += 1
		print str(p[0]) + '->' + str(p[1]) + ': sup=' + str(sup) + ' conf=' + str(conf)
print 'count:', count