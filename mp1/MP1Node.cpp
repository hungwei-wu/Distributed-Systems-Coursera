/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <cassert>
#include <algorithm>
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
	if(*joinaddr == this->memberNode->addr){
		//create first member entry for itself
		MemberListEntry *new_entry = new MemberListEntry(addr_to_id(this->memberNode->addr.addr), addr_to_port(this->memberNode->addr.addr), this->memberNode->heartbeat, this->memberNode->heartbeat);
		this->memberNode->memberList.push_back(*new_entry);
		this->member_pos = this->memberNode->memberList.size() - 1;
		log->logNodeAdd(&this->memberNode->addr, &this->memberNode->addr);
		this->member_states.push_back(State::ALIVE);
		//DEBUG(string("Introducer => my pos is ") + to_string((int)(this->memberNode->myPos - this->memberNode->memberList.begin())));
		//stringstream ss;
		//ss << *this->memberNode->myPos;
		//DEBUG(ss.str());
	}
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
	MessageHdr* msg = (MessageHdr *)data;
	if(msg->msgType == JOINREQ){
		//add new member to membership list
		char *new_addr = (char *)(msg+1);
		int id = addr_to_id(new_addr);
		short port = addr_to_port(new_addr);
		
		//create target address
		Address *new_address = new Address();
		memcpy(new_address->addr, new_addr, sizeof(new_address->addr));
		
		long new_heartbeat = *(long*)((char*)msg + size - sizeof(long));
		MemberListEntry new_entry(id, 
			port, new_heartbeat, this->memberNode->heartbeat);	//use self heartbeat as local time	
		this->memberNode->memberList.push_back(new_entry);
		this->member_states.push_back(State::ALIVE);

		//send back JOINREP with new membership list
		int iter = this->memberNode->memberList.size()-1;

		size_t msgsize = sizeof(MessageHdr) + sizeof(int) + sizeof(MemberListEntry) * this->memberNode->memberList.size();
		MessageHdr* send_msg = (MessageHdr *)malloc(msgsize * sizeof(char));

		send_msg->msgType = JOINREP;
		send_msg->ele_num = this->memberNode->memberList.size();
		
		memcpy((char *)(send_msg+1), &iter, sizeof(int));
		memcpy((char *)(send_msg+1) + sizeof(int), (this->memberNode->memberList.data()), sizeof(MemberListEntry) * send_msg->ele_num);
		
		emulNet->ENsend(&this->memberNode->addr, new_address, (char *)send_msg, msgsize);
				
		log->logNodeAdd(&this->memberNode->addr, new_address);

		free(send_msg);
		delete(new_address);

		//stringstream ss;
		//ss << this->memberNode->memberList;
		//DEBUG(ss.str());
	}
	else if(msg->msgType == JOINREP || msg->msgType == PROPAGATE){
		unsigned int get_data_size = size - ((char *)(msg+1) - (char *)(msg));
		MemberListEntry *entries = (MemberListEntry *)malloc(get_data_size);
		
		int ele_num = msg->ele_num;
		int iter;
		//deserialzie message
		memcpy(&iter, (char *)(msg+1), sizeof(int));
		memcpy(entries, (char *)(msg+1) + sizeof(int), sizeof(MemberListEntry) * ele_num);
		auto new_ml = vector<MemberListEntry>(entries, entries + ele_num);

		int old_size = this->memberNode->memberList.size();

		this->mergeMemberList(new_ml);
		if(msg->msgType == JOINREP){
			//this->memberNode->myPos = this->memberNode->memberList.begin() + iter;
			this->member_pos = iter;
			this->memberNode->inGroup = true;
		}
		//stringstream ss;
		//ss << this->memberNode->memberList;
		//DEBUG(ss.str());
		auto& my_ml = this->memberNode->memberList;
		for(auto i=my_ml.begin()+old_size; i<my_ml.end(); i++){
			Address* tmp_addr = create_address(i->getid(), i->getport());
			log->logNodeAdd(&this->memberNode->addr, tmp_addr);
			delete tmp_addr;
		}
	}

}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
	this->memberNode->heartbeat += 1;

	//update myPos entry
	this->memberNode->memberList[this->member_pos].setheartbeat(this->memberNode->heartbeat);
	this->memberNode->memberList[this->member_pos].settimestamp(this->memberNode->heartbeat);
	
	DEBUG(string("my pos: ") + to_string(this->member_pos));

	//delete failed node entries
	assert(this->memberNode->memberList.size() == this->member_states.size());
	for(unsigned int i=0; i<this->memberNode->memberList.size(); i++){
		auto& e = this->memberNode->memberList[i];
		if(this->memberNode->heartbeat - e.gettimestamp() > this->memberNode->pingCounter && this->member_states[i] == State::ALIVE){
			this->member_states[i] = State::DELETED;
			Address* tmp_addr = create_address(e.getid(), e.getport());
			log->logNodeRemove(&this->memberNode->addr, tmp_addr);
			delete(tmp_addr);
		}
	}
	//propogate to other nodes
	int iter = 0;	//don't care

	size_t msgsize = sizeof(MessageHdr) + sizeof(int) + sizeof(MemberListEntry) * this->memberNode->memberList.size();
	MessageHdr* send_msg = (MessageHdr *)malloc(msgsize * sizeof(char));

	send_msg->msgType = PROPAGATE;
	send_msg->ele_num = this->memberNode->memberList.size();
		
	memcpy((char *)(send_msg+1), &iter, sizeof(int));
	memcpy((char *)(send_msg+1) + sizeof(int), (this->memberNode->memberList.data()), sizeof(MemberListEntry) * send_msg->ele_num);
	auto& my_ml = this->memberNode->memberList;
	for(auto i=my_ml.begin(); i<my_ml.end(); i++){
		if(i == this->memberNode->memberList.begin() + this->member_pos) continue;
		Address* tmp_addr = create_address(i->getid(), i->getport());
			
		emulNet->ENsend(&this->memberNode->addr, tmp_addr, (char *)send_msg, msgsize);
		delete(tmp_addr);
	}	

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::mergeMemberList(vector<MemberListEntry> &new_ml){
	auto& my_ml = this->memberNode->memberList;
	stringstream ss;
	//ss << this->memberNode->memberList;
	//DEBUG(string("original list: ") + ss.str());
	//stringstream ss;
	//ss << new_ml;
	//DEBUG(string("new list: ") + ss.str());
	
	unsigned int min_size = min(my_ml.size(), new_ml.size());	
	for(unsigned int i=0; i < min_size; i++){
		assert(my_ml[i].getid() == new_ml[i].getid());
		assert(my_ml[i].getport() == new_ml[i].getport());
		//update heartbeats and local timestamps
		if(my_ml[i].getheartbeat() < new_ml[i].getheartbeat()){
			my_ml[i].settimestamp(this->memberNode->heartbeat);
			my_ml[i].setheartbeat(new_ml[i].getheartbeat());
		}
	}
	//create new entries locally if new memberlist contains new ones
	for(unsigned int i=my_ml.size(); i<new_ml.size(); i++){
		my_ml.push_back(new_ml[i]);
		this->member_states.push_back(State::ALIVE);
	}
	assert(my_ml.size() >= new_ml.size());
	ss.flush();
	ss << this->memberNode->memberList;
	DEBUG(string("merged list: ") + ss.str());
	
	DEBUG(string("==="));
}

void MP1Node::DEBUG(string s){
	#ifdef MYLOG
	log->LOG(&this->memberNode->addr ,s.c_str());	
	#endif
}

Address *create_address(int id, short port){
	Address *address = new Address();
	//char *addr = new char[6];
	memcpy(&address->addr[0], &id, sizeof(int));
	memcpy(&address->addr[4], &port, sizeof(short));

	return address;
}

std::ostream & operator<<(std::ostream & str, MemberListEntry & e) { 
	str << e.getid() << ":" << e.getport() << " ,heartbeat: " << e.getheartbeat();
	return str;
}

std::ostream & operator<<(std::ostream & str, vector<MemberListEntry> & v) { 
	str << endl;
	for(auto &e : v){
		str << e << endl;
	}
	return str;
}
