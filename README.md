# DiskTable

## NOTE

This disk format is not stable yet, nor is the API. So unless you can afford to erase the data on a new build, don't use this. 

If you want to use it as simply a diskcache that you create on startup, then it should be fine.  But I make no guarantees.

## Introduction

I needed a very basic NOSQL locally that I could embed in my Go program allowing me to serve off disk with some semblance of speed.

It needed to be:

* Write once
* Read many
* Suppport a main data repo
* Support a bunch of indexes on the data
* Support duplicates in indexes
* Lookup by indexes
* Stream all data fast

These disktables are built off of outcaste.io/dgraph.

Now, this lacks SQL like characteristics. It is simply a bunch of key/value stores that let you do exact matches on indexes in order to find matching data.  So I can say things like: "find cars that are blue with a v8 and made by chevy".

In the future I may allow things like searching by prefix and things like that.  But those aren't in here today.