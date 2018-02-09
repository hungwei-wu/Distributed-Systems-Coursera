/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#include <unordered_map>

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

	// record round in synchronous system
	int round;
	// record <round, transIDs> which has been issued in certain round
	unordered_map<int, vector<int> >round_success;
	//store message mapping in coordinator and use to log success/fail
	unordered_map<int, Message*> message_map;
	//store mapping of <transID, success_count>
	//unordered_map<int, int> success_map;
public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);
	
	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	//
	void send_message(Message* m, Address* from_addr, Address* to_addr);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message* message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deleteKey(string key);
	void reply(int trans_id, Address *to_addr, bool success);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();
	
	void log_message_success(int trans_id, int suc_count);
	void DEBUG(string& s);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
