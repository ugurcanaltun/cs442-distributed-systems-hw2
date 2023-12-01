import redis
import random, math
import pickle
import os

REDISHOST = "localhost"
REDISPORT = 6379

NUMDIGITS = 4    # Length in decimal digits for various constants
FORMATSTR = '0' + str(NUMDIGITS) + 'd' # Used to format integers into decent strings for keys
MAXPROCID = 9999 # Used as a dummy placeholder for nonempty lists in redis
MAXCHANID = 9999 # Used as a dummy placeholder for nonempty lists in redis
TIMEOUT   = -1

class Channel():

	"""
	A channel is used as the fundamental means to let messsages be exchanged between several OS-level processes. The
	basic principle is that all processes that want to communicate share the same channel. A channel is implemented
	entirely using the Redis system. This class works as a per-process interface to the Redis implementation of a
	channel. This also means that if we want to pass things between processes, the class instance itself should be fully
	serializable. In particular, this means that a connection to the redis server is to be kept out of the class
	implemenation. 

	We take a simple approach and assume that all processes that want to communicate through the same channel are
	explicitly identified in advance (with a channel-level identifier). Likewise, each channel is also associated with a
	unique channel identifier. The channel identifier works as an index for redis to other channel-related information.
	"""

	#----------------------------------------------------------------------------------------------------------------
	"""
	To ensure that channels can be serialized, we need to set up a connection each time access to the server is
	needed. This private function returns the same connection everywhere, and assumes that all processes are running on
	the same host.
	"""
	def __redisChannel(self):
		return redis.Redis(host=REDISHOST, port=REDISPORT, db=0) # connection to redis

	
	#----------------------------------------------------------------------------------------------------------------
	def __init__(self, channelID, flush=False):
		# Every channel registers its ID with the redis server, as well as the OS-level process IDs and the channel-level
		# process IDs. For the latter two, separate lists are maintained, with a unique key that is derived from the channel
		# ID.
		self.channelID	 = channelID		 # the channel as registered with the redis server
		self.channelKey  = 'C' + format(self.channelID,FORMATSTR) # Prepended to other keys for unique channel association
		self.osmembersID = self.channelKey + 'OID'  # Unique key to OS proc IDs for this channel
		self.membersID	 = self.channelKey + 'MID'  # Unique Key to chan proc IDs for this channel
		self.wakeOnSend  = self.channelKey + 'WOS'  # Unique key to wake up any waiting receiver for this channel
		
		channel = self.__redisChannel()
		if flush: channel.flushall()
		
#		print("* Starting channel", self.channelID, "by", os.getpid())
		
		# Check if this is an existing channel
		channelSet = set(int(i) for i in channel.smembers('channelSet'))
#		print("C channel set", channelSet, "by", os.getpid())

		if not self.channelID in channelSet: # new channel
#			print("C new channel for", os.getpid())
			channel.sadd('channelSet', self.channelID) # Add the new channel to the known ones
			channel.hset(self.osmembersID,-1,-1)			 # Artificial construction because empty lists are not supported
			channel.sadd(self.membersID, MAXPROCID)		 # Add one nonexisting channel-level proc to this channel
#			print(channel.smembers('channelSet'))

		else:
#			print("C existing channel for", os.getpid(), self.channelID, self.osmembersID, self.membersID)
			pass

	#----------------------------------------------------------------------------------------------------------------
	"""
	Only processes that have joined the channel are allowed to call its functions. We systematically check for this
	condition every time.
	"""
	def __checkCaller(self):
		channel	= self.__redisChannel() # set up connection to the redis server

		# First check if the calling OS process is known to this channel. Get its OS ID, construct a decent list of OS
		# members in terms of integer IDs that are mapped to channel-level integer IDs, and check. Note that redis stores
		# everything as bytes.

		ospid			= os.getpid()
		osmembers = dict((int(i),int(j)) for i,j in channel.hgetall(self.osmembersID).items())
		assert ospid in osmembers, 'Calling OS-level process unknown to this channel'

		# Now check if the process had indeed explicitly joined with a given ID. 
		pid				= osmembers[ospid]
		assert channel.sismember(self.membersID, pid), 'Channel-level process unknown to this channel'

		# Everything went fine, so return the IDs of the caller at OS level and channel level, both as integers.
		return (int(ospid), int(pid))

	#----------------------------------------------------------------------------------------------------------------	
	"""
	Redis requires essentially (lists of) single-valued keys. To allow for pairs of (sender,receiver) processes, we
	convert the pairs into simple keys, and vice versa. To that end, we simply concatenate channel-level identifiers
	as strings of digits, prepended by the channel key.
	"""
	def __procs2key(self,i,j):
		key = self.channelKey + format(i,FORMATSTR) + format(j,FORMATSTR)
#		print("C procs2key:", i,j, key)
		return str(key)
		
	def __key2procs(self,key):
		proc0 = key[    NUMDIGITS + 1 : 2 * NUMDIGITS + 1]
		proc1 = key[2 * NUMDIGITS + 1 : 3 * NUMDIGITS + 1]
#		print("C key2procs:", proc0, proc1, key)
		return [int(proc0), int(proc1)]

	"""
	To ensure that a process that is blocked for incoming requests also picks up requests from processes that were
	unknown when blocked, the process waits for a wakeup signal to subsequently recompute the set of potential
	senders. The signal is implemented as another Redis key.
	"""
	def __wakeupKey(self, i):
		return str(self.wakeOnSend) + str(i)
	
	#----------------------------------------------------------------------------------------------------------------		
	"""



	"""
	def join(self, newpid):
		channel = self.__redisChannel()

		# Every joining process will have to pass a hard-coded channel-level identifier. This is by far the easiest way to
		# connect processes that operate on the same channel. A process cannot join more than once to the same channel.
		ospid     = os.getpid()
		osmembers = channel.hgetall(self.osmembersID)

		assert ospid  not in osmembers
		assert newpid not in set(channel.smembers(self.membersID))
		
		# Add yourself to the channel members
		channel.hset(self.osmembersID, ospid, newpid)
#		print("Joined osmembers: ", self.osmembersID, channel.hgetall(self.osmembersID), ospid, newpid)
		channel.sadd(self.membersID, newpid)

		members = set(int(i) for i in channel.smembers(self.membersID))
#		print("Join: members joined", self.osmembersID, self.channelID, members)

		(ospid, pid) = self.__checkCaller()
		return

	def leave(self):
		channel      = self.__redisChannel()
		(ospid, pid) = self.__checkCaller()

		# At this point we know for sure that the process had properly joined the channel. We can now simply remove it from
		# the redis lists.
		channel.hdel(self.osmembersID, ospid)
		channel.sdel(self.membersID, pid)
		return
	
	#----------------------------------------------------------------------------------------------------------------		
	"""
	Sending a message on a channel is implemented by storing the message as a (key,value) entry at the Redis server. To
	this end, the caller identifies the processes for which the message is intended. For each target process, we store
	the message separately with an appropriate key, written always as a (sender,receiver) pair. The message itself is
	written as the value of the entry. 
	"""
	
	def sendTo(self, destination, message):
#		print("* sendTo", destination, message)		
		if isinstance(destination, int):
			destinationSet = [destination]
		else:
			destinationSet = destination

		channel      = self.__redisChannel()
		(ospid, pid) = self.__checkCaller()

		# For each intended receiver, construct a ((sender,receiver), message) pair to store at the redis server.
		for i in destinationSet:
#			print("* sendTo", pid, int(i), destinationSet)

			# Make sure that the destination has also joined this channel.
			members = set(int(i) for i in channel.smembers(self.membersID))
#			print("members", self.channelID, members)
			assert channel.sismember(self.membersID, int(i)), ''

			# The key consists of a (sender, receiver) pair, to which end we transform the pair to a unique
			# key that can be used by the receiver to later select the message. Also wake up the receiver as
			# it may be unaware of this sender.
			channel.rpush(self.__procs2key(pid, int(i)), pickle.dumps(message))
			channel.rpush(self.__wakeupKey(i), 'WAKEUP')
#			print("all redis keys:", channel.keys())

		return
	#----------------------------------------------------------------------------------------------------------------		
			
	def sendToAll(self, message):
		channel      = self.__redisChannel()
		(ospid, pid) = self.__checkCaller()
		
		# Instead of sending the message through repeated calls of sendTo, we just go through all the current processes
		# that have joined the channel.
		for i in channel.smembers(self.membersID):
			if i != MAXPROCID:
				channel.rpush(self.__procs2key(pid, int(i)), pickle.dumps(message) )
				channel.rpush(self.__wakeupKey(i), 'WAKEUP')

	#----------------------------------------------------------------------------------------------------------------		

	def __keysExist(self, procKeySet):
		channel = self.__redisChannel()		
		for key in procKeySet:
			if channel.exists(key):
				return True
		return False

	#----------------------------------------------------------------------------------------------------------------		
	"""
	Receiving messages is a bit tricky and makes use of the blocking, selective operation blpop from redis. The trick is
	to provide the correct set of keys to blpop, after which redis can select any message that matches those keys, or
	blocks the caller until a matching message is stored.
	"""
	def recvFrom(self, sender, block = True, timeout = 0):
		channel      = self.__redisChannel()
		(ospid, pid) = self.__checkCaller()

		if isinstance(sender, int):
			senderSet = [sender]
		else:
			senderSet = sender

		# To receive from specific processes, we need to construct separate (sender,receiver) keys. In this case, the
		# receiver is the caller and comes as the second entry in the key. prockeys consists of a list of keys that we can
		# pass to the blpop operation of redis.
				
		while True:
			members = set(int(i) for i in channel.smembers(self.membersID))
			members.remove(MAXPROCID)
			for i in senderSet: assert i in members, ''

			prockeys = [self.__procs2key(int(i), pid) for i in senderSet]
			prockeys.append(self.__wakeupKey(pid))

			if not block:
				if not self.__keysExist(prockeys):
					return None

			redismsg = channel.blpop(prockeys, timeout)
			
#			print("Redismsg in recvFrom",str(redismsg[0]), str(redismsg[1]))

			if redismsg:
				# Check if the receiver had actually been woken up

				if redismsg[0].decode() != self.__wakeupKey(pid):
					# Redis found a matching (key, value) pair that corresponds to a sent message . Note that we store
					# (key, message) in redis. We want to return # (sender,message) to the caller. 
					#	print("* recvFromAny: (pid,sender)", pid, self.__key2procs(msg[0]))
					return [self.__key2procs(redismsg[0])[0],pickle.loads(redismsg[1])]
			else:
				return TIMEOUT

	#----------------------------------------------------------------------------------------------------------------				
		
	def recvFromAny(self, block = True, timeout = 0):
		channel      = self.__redisChannel()
		(ospid, pid) = self.__checkCaller()

		# To receive from any joined process, we need to construct a separate (sender,receiver) key. In this case, the
		# receiver is the caller and comes as the second entry in the key. prockeys consists of a list of keys that we can
		# pass to the blpop operation of redis. This assumes that there are already senders available. 

		while True:
			members = set(int(i) for i in channel.smembers(self.membersID))
			members.remove(pid)
#			print("recvFromAny", pid, ospid, members)

			prockeys = [self.__procs2key(int(i), pid) for i in members]
			prockeys.append(self.wakeOnSend + str(pid))

			if not block:
				if not self.__keysExist(prockeys):
					return None

			redismsg = channel.blpop(prockeys, timeout) # blocking call to blpop
#			print("Redismsg in recvFromAny", pid, str(redismsg[0]), str(redismsg[1]))
		
			if redismsg:
				# Check if the receiver had actually been woken up

				if redismsg[0].decode() != self.__wakeupKey(pid):
					# Redis found a matching (key, value) pair that corresponds to a sent message. Note that we store
					# (key, message) in redis. We want to return (sender,message) to the caller. 
					#	print("* recvFromAny: (pid,sender)", pid, self.__key2procs(msg[0]))
					return [self.__key2procs(redismsg[0])[0],pickle.loads(redismsg[1])]
			else:
				return TIMEOUT

	#----------------------------------------------------------------------------------------------------------------

