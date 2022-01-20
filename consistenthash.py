import hashlib, math

HASHMAX = int('f'*32, 16)

def hash(input):
	return int(hashlib.md5(input.encode('utf-8')).hexdigest(), 16)

#NOTES
#shard_id in the rest of the code might not represent the same shard_id here
#   might need to have a data structure for mapping between the two
#shard_id is kind of erratic here, it changes for each thing, so when adding a new shard mappings might change
#might build the mapping into this data structure

# class HashRing(object):
# 	"""docstring for HashRing"""
# 	def __init__(self, num_shards):
# 		self.nshards = num_shards
# 		self.reshard()
# 		#Sets the self.divides variable.
# 		#Shard 1 contains all hashes from [0, divides[0]]
# 		#Shard i contains all hashes from (divides[i-2], divides[i-1]]
# 		#Shard n contains all hashes from (divides[n-2], HASHMAX]
# 		#len(self.divides) == self.nshards - 1
# 		self.mappings = list(range(1, self.nshards + 1))
# 		#mappings[i] means that Shard i in the HashRing maps to what the external source names "Shard mappings[i]"

# 	def reshard(self):
# 		self.divides = []
# 		step = HASHMAX / self.nshards
# 		ctr = 0
# 		for x in range(self.nshards - 1):
# 			ctr += step
# 			self.divides.append(math.floor(ctr))

# 	def boundary(self, shard_id):
# 		#shard_ids start at 1 and go to n
# 		if (shard_id == 1):
# 			return (0, self.divides[0])
# 		elif (shard_id == self.nshards):
# 			return (self.divides[self.nshards - 2], HASHMAX)
# 		else:
# 			return (self.divides[shard_id - 2], self.divides[shard_id - 1])

# 	def addshard(self, shardname):
# 		#find the boundary with the most space
# 		tosplit = max([self.boundary(i)[1] - self.boundary(i)[0] for i in range(1, self.nshards + 1)])
# 		#This horribly disgusting list comprehension says:
# 		#   for i from 1 to self.nshards:
# 		#   	compute boundary(i)[1] - boundary(i)[0]
# 		#   then like, find the max of that
# 		#sorry I really wanted to flex 

# 		#split this shard into two
# 		newmid = math.floor((self.boundary(tosplit)[0] + self.boundary(tosplit)[0]) / 2) #midpt
# 		self.divides.insert(tosplit - 1, newmid)
# 		self.nshards += 1

# 	def removeshard(self, toremove):
# 		#if it's any random fuckin shard then just give its stuff to the dude above it
# 		#if it's the last shard then throw it to the 2nd to last guy
# 		#throw out shard i = just remove divides[i-1]
# 		if (toremove == self.nshards):
# 			self.divides.remove(len(self.divides) - 1) #yeah this'll probably work, remove the thing below it right
# 		else:
# 			self.divides.remove(toremove - 1)
# 		self.nshards -= 1
# 		#this is where we remap if we have the shard mapping thing i was thinking about earlier
# 		#this isn't helpful to anyone but me since i only know what i'm talking about


# 	def chooseshard(self, key):
# 		h = hash(key)
# 		#this should be a binary search but i'm lazy lol
# 		for i in range(1, self.nshards + 1):
# 			b = self.boundary(i)
# 			if h > b[0] and h < b[1]:
# 				return i
# 		return -1

class HashRing(object):
	def __init__(self, num_shards):
		self.num_shards = num_shards
		#OK so the ring consists of the hash values from 0 to HASH_MAX right now
		#there are n-1 boundaries, at i*HASHMAX/(n), from i=1 to n-2.
		#each part of the ring has length HASHMAX/n.

	def check(self, key):
		h = hash(key)
		shard = int(h / math.ceil(HASHMAX / self.num_shards))
		if shard >= self.num_shards: #cap at n-1 to account for floating point error
			shard = self.num_shards - 1
		return shard

	def reshard(self, num_shards):
		self.num_shards = num_shards





if __name__ == '__main__':
	hr = HashRing(5)
	# for x in range(1, 6):
	# 	print(hr.boundary(x))
	print(hr.chooseshard("beanso"))
	print(hr.chooseshard("beansoup"))
	print(hr.chooseshard("beansandwich"))
	print(hr.chooseshard("eggs"))
	print(hr.chooseshard("egg234s"))
	print(hr.chooseshard("eg123ffffgs"))
	print(hr.chooseshard("egddddgs"))
	print(hr.chooseshard("eaaaaaggs"))
