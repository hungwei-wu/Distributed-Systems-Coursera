/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
int debug_count = 0;
/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	this->round = 0;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	//TODO: handle if ring has been changed
	

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	bool change = false;
	if(ring.size()!=curMemList.size()) change = true;
	else {
		//change = equal(ring.begin(), ring.end(), curMemList.begin(), [](Node x, Node y) -> bool{return x.nodeAddress == y.nodeAddress;});
		for(int i=0; i<ring.size(); i++){
			if(ring[i].nodeAddress == curMemList[i].nodeAddress){
				
			}
			else{
				change = true;
				break;
			}
		}
	}
	if(change){
		ring = curMemList;
		stabilizationProtocol();
	}
	
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

void MP2Node::dispatchMessages(Message* message){
	//find replicas
	vector<Node> replicas = findNodes(message->key);
	// send messages to replicas	
	for(auto& rep: replicas){
		send_message(message, &message->fromAddr, rep.getAddress());
	}
}

void MP2Node::send_message(Message* message, Address* from_addr, Address* to_addr){
	string msg_string = message->toString();
	int size = msg_string.length() + 1;	//+1 for '\0'
	char* c_str = new char[size];

	memcpy(c_str, msg_string.c_str(), size);
	emulNet->ENsend(from_addr, to_addr, c_str, size);
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	//construct message
	Message* m = new Message(g_transID, this->memberNode->addr, CREATE, key, value, PRIMARY);
	/*if(round_success.find(round)==round_success.end()){
		round_success.insert({round, vector<int>()});
	}*/
	round_success[round].push_back(m->transID);

	message_map.insert({m->transID, m});
	g_transID++;
	dispatchMessages(m);	
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	Message* m = new Message(g_transID, this->memberNode->addr, READ, key);
	round_success[round].push_back(m->transID);
	message_map.insert({m->transID, m});
	g_transID++;
	dispatchMessages(m);	
	
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	Message* m = new Message(g_transID, this->memberNode->addr, UPDATE, key, value);
	round_success[round].push_back(m->transID);
	message_map.insert({m->transID, m});
	g_transID++;
	dispatchMessages(m);	
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	Message* m = new Message(g_transID, this->memberNode->addr, DELETE, key);
	round_success[round].push_back(m->transID);
	message_map.insert({m->transID, m});
	g_transID++;
	dispatchMessages(m);	
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	bool res = ht->create(key, value);
	
	return res;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
	
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deleteKey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

unordered_map<int, int>& get_element(unordered_map<int, unordered_map<int, int>* >& m, int x){
	return *m[x];
}


/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	char * data;
	int size;
	
	//works in synchronyze version
	round++;
	//store <trans_id, success count>, count replied success message in this round
	unordered_map<int, int>* success_map = new unordered_map<int, int>();
	

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message_string(data, 0, size);
		
		/*
		 * Handle the message types here
		 */
		Message *msg = new Message(message_string);

		bool is_success;
		string read_res;
		
		switch(msg->type){
			case CREATE: 
				is_success = createKeyValue(msg->key, msg->value, msg->replica);
				if(is_success) log->logCreateSuccess(&this->memberNode->addr, false, msg->transID, msg->key, msg->value);
				else log->logCreateFail(&this->memberNode->addr, false, msg->transID, msg->key, msg->value);
				reply(msg->transID, &msg->fromAddr, is_success);
				break;
			case READ:
				read_res = readKey(msg->key);
				if(!read_res.empty()) log->logReadSuccess(&this->memberNode->addr, false, msg->transID, msg->key, read_res);
				else log->logReadFail(&this->memberNode->addr, false, msg->transID, msg->key);
				reply_read(msg->transID, &msg->fromAddr, read_res);
				break;
			case UPDATE:
				is_success = updateKeyValue(msg->key, msg->value, msg->replica);
				if(is_success) log->logUpdateSuccess(&this->memberNode->addr, false, msg->transID, msg->key, msg->value);
				else log->logUpdateFail(&this->memberNode->addr, false, msg->transID, msg->key, msg->value);
				reply(msg->transID, &msg->fromAddr, is_success);
				break;
			case DELETE:
				is_success = deleteKey(msg->key);
				if(is_success) log->logDeleteSuccess(&this->memberNode->addr, false, msg->transID, msg->key);
				else log->logDeleteFail(&this->memberNode->addr, false, msg->transID, msg->key);
				reply(msg->transID, &msg->fromAddr, is_success);
				break;
			case REPLY:
				if(msg->success) (*success_map)[msg->transID] += 1;
				
				break;
			case READREPLY:
				if(!msg->value.empty()) (*success_map)[msg->transID] += 1;//log->logReadSuccess(&this->memberNode->addr, true, msg->transID, msg->key, read_res);
				//else log->logReadFail(&this->memberNode->addr, false, msg->transID, msg->key);
				break;
			default:
				throw runtime_error("invalid message type");
				break;
		}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	 if(round_success.find(round-2) != round_success.end()){
		 // get all trans_ids within previous round
		 // -2 indicate round trip
	 	vector<int>& trans_ids = round_success[round-2];

		cout << this->memberNode->addr.getAddress() << endl;
		cout << trans_ids.size() << " " << success_map->size() << endl;

	 	for(auto trans_id: trans_ids){
			//suc_count default is zero if not exist in success_map
	 		int suc_count = (*success_map)[trans_id];
			log_message_success(trans_id, suc_count);
	 	}
		//round_success.erase(round-2);
	 }

}

/*
	detemine and log whether or not previous transaction succeed
*/
void MP2Node::log_message_success(int trans_id, int suc_count){
	Message* msg = message_map.at(trans_id);	// throw exception if key not exists
	bool qourum = false;
	if(suc_count >= 2) qourum = true;	// if meet majority
	else cout << "failed to meet quorum" << endl;
	
	switch(msg->type){
		case(CREATE): 
			if(qourum) log->logCreateSuccess(&this->memberNode->addr, true, trans_id, msg->key, msg->value);
			else log->logCreateFail(&this->memberNode->addr, true, trans_id, msg->key, msg->value);
			break;
		case(READ): 
			if(qourum) log->logReadSuccess(&this->memberNode->addr, true, trans_id, msg->key, msg->value);
			else log->logReadFail(&this->memberNode->addr, true, trans_id, msg->key);
			break;
		case(UPDATE): 
			if(qourum) log->logUpdateSuccess(&this->memberNode->addr, true, trans_id, msg->key, msg->value);
			else log->logUpdateFail(&this->memberNode->addr, true, trans_id, msg->key, msg->value);
			break;
		case(DELETE): 
			if(qourum) log->logDeleteSuccess(&this->memberNode->addr, true, trans_id, msg->key);
			else log->logDeleteFail(&this->memberNode->addr, true, trans_id, msg->key);
			break;
		default:
			throw runtime_error("invalid message type");
			break;
	}
	message_map.erase(trans_id);
	delete msg;
}

void MP2Node::reply(int trans_id, Address* to_addr, bool success){
	Message* m = new Message(trans_id, this->memberNode->addr, REPLY, success);
	send_message(m, &m->fromAddr, to_addr);
}

void MP2Node::reply_read(int trans_id, Address *to_addr, string read_res){
	Message* m = new Message(trans_id, this->memberNode->addr, read_res);
	send_message(m, &m->fromAddr, to_addr);
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	 for(auto& key_value : ht->hashTable){
		clientCreate(key_value.first, key_value.second);
		vector<Node> replicas = findNodes(key_value.first);
		vector<Address>addrs(3);
		transform(replicas.begin(), replicas.end(), addrs.begin(), [](Node n) -> Address{return n.nodeAddress;});
		if(find(addrs.begin(), addrs.end(), this->memberNode->addr) == addrs.end()){
			// current node is not a node responsible for the key's replication
			bool suc = deleteKey(key_value.first);
			if(!suc) throw runtime_error("key should exist in table when deleting");
		}
	 }
}

void MP2Node::DEBUG(string& s){
	#ifdef MYLOG
	log->LOG(&this->memberNode->addr, s.c_str());
	#endif
}
