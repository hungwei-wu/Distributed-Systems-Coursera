/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <iostream>
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
		this->memberNode->myPos = this->memberNode->memberList.end()-1;
		DEBUG(string("Introducer => my pos is ") + to_string((int)(this->memberNode->myPos - this->memberNode->memberList.begin())));
		stringstream ss;
		ss << *this->memberNode->myPos;
		DEBUG(ss.str());
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
		std::cout << "Receive joinreq" << endl;	
		//add new member to membership list
		char *new_addr = (char *)(msg+1);
		int id = addr_to_id(new_addr);
		cout << "receive id: " << id << endl;
		short port = addr_to_port(new_addr);
		cout << "receive port: " << port << endl;	
		
		//create target address
		Address *new_address = new Address();
		memcpy(new_address->addr, new_addr, sizeof(new_address->addr));
		
		long new_heartbeat = *(long*)((char*)msg + size - sizeof(long));
		std::cout << "receive new heartbeat " << new_heartbeat << endl;
		cout << "local heartbeat: " << this->memberNode->heartbeat << endl;
		MemberListEntry new_entry(id, 
			port, new_heartbeat, this->memberNode->heartbeat);	//use self heartbeat as local time	
		this->memberNode->memberList.push_back(new_entry);
		DEBUG(string("member list size: ") + to_string(this->memberNode->memberList.size()));
		cout << "member list size: " << this->memberNode->memberList.size() << endl;
		//send back JOINREP with new membership list
		auto iter = this->memberNode->memberList.end()-1;	//point to last push backed  item
		JoinRepData send_data;
		send_data.pos = iter;
		send_data.memberList = this->memberNode->memberList;
		//size_t msgsize = sizeof(MessageHdr) + sizeof(this->memberNode->memberList);
		size_t msgsize = sizeof(MessageHdr) + sizeof(send_data);
		MessageHdr* send_msg = (MessageHdr *)malloc(msgsize * sizeof(char));
		send_msg->msgType = JOINREP;
		//memcpy((char *)(send_msg+1), &this->memberNode->memberList, sizeof(this->memberNode->memberList));
		memcpy((char *)(send_msg+1), &send_data, sizeof(send_data));
		emulNet->ENsend(&this->memberNode->addr, new_address, (char *)send_msg, msgsize);
		
		free(send_msg);
		delete(new_address);

		stringstream ss;
		ss << this->memberNode->memberList;
		DEBUG(ss.str());
	}
	else if(msg->msgType == JOINREP){
		unsigned int get_data_size = size + (char *)(msg+1) - (char *)(msg);
		JoinRepData *get_data = (JoinRepData *)malloc(get_data_size);
		memcpy(get_data, (char *)(msg+1), get_data_size);
		//auto myPos_pointer = (vector<MemberListEntry>::iterator *)(msg+1);	
		//auto memberList = *(vector<MemberListEntry> *)(myPos_pointer+1);
		DEBUG(string("recieved JOINREP, list size: ") + to_string(get_data->memberList.size()));
		this->memberNode->myPos = get_data->pos;
		this->memberNode->memberList = get_data->memberList;
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
	//TODO: add itself to memberList?
	// no need? cause will always be introduced by same introducer
	//Address *new_address = memberNode->addr;
	//MemberListEntry* new_entry = new MemberListEntry(new_addr->getid(), new_addr->getport()
	//								, new_addr->heartbeat, this->member->heartbeat);
	
		
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


void MP1Node::DEBUG(string s){
	#ifdef MYLOG
	log->LOG(&this->memberNode->addr ,s.c_str());	
	#endif
}

std::ostream & operator<<(std::ostream & str, MemberListEntry & e) { 
	str << e.getid() << ":" << e.getport();
	return str;
}

std::ostream & operator<<(std::ostream & str, vector<MemberListEntry> & v) { 
	str << endl;
	for(auto &e : v){
		str << e << endl;
	}
	return str;
}
