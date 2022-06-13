/*
 * node.cc
 *
 *  Created on: 24 mag 2022
 *  Author: franc
 */
#include <string.h>
#include <omnetpp.h>
#include <iostream>
#include <cmath>

#include "raftMessage_m.h"
#include "RequestVote_m.h"
#include "AppendEntry_m.h"



using namespace omnetpp;
using namespace std;


class node : public cSimpleModule
{
  protected:
    virtual void initialize() override;
    virtual void handleMessage(cMessage *msg) override;
    virtual RequestVote *generateMessage();
    virtual RequestVote *grantVote(RequestVote* msg);
    virtual RequestVote *refuseVote(RequestVote* msg);

    float timer;
    int nodesNumber;

     //Persistent state
     int currentTerm = 0; //increases monotonically
     int votedFor;
     int log[50];
     int voteReceived = 0;

     //Volatile state
     int commitIndex = 0;
     int lastApplied = 0;

     //Volatile State for Leaders
     int* nextIndex;
     int* matchIndex;

    cFSM fsm;
    enum{

        FOLLOWER = 0,
        CANDIDATE = FSM_Steady(1),
        LEADER = FSM_Steady(2),
    };


};

Define_Module(node);

void node::initialize()
{
    nodesNumber = gateSize("gate")+1;

    fsm.setName("fsm");
    cMessage *msg = new cMessage("start");
    timer = uniform(0.150,0.300);

    nextIndex = new int[nodesNumber];
    matchIndex = new int[nodesNumber];
    scheduleAt(0.0 + timer, msg);


}

void node::handleMessage(cMessage *msg)
{

    FSM_Switch(fsm)
    {

        case FSM_Exit(FOLLOWER):
                if(msg -> isSelfMessage()){
                    currentTerm++;
                    FSM_Goto(fsm, CANDIDATE);
                }
                else
                {
                    FSM_Goto(fsm, FOLLOWER);
                }

                break;
        case FSM_Enter(FOLLOWER):
                bubble("FOLLOWER");
                /*if(msg->isSelfMessage()) {
                    int n = gateSize("gate");
                    for(int k=0; k<n; k++) {
                        raftMessage *msg = generateMessage();
                        send(msg, "gate$o", k);
                    currentTerm++;
                    FSM_Goto(fsm,CANDIDATE)
                    }
                } else {
                    raftMessage *rmsg = check_and_cast<raftMessage *>(msg);
                    int source = rmsg->getArrivalGate()->getIndex();
                    send(rmsg, "gate$o", source);
                }*/
                break;
        case FSM_Exit(CANDIDATE):
                if(voteReceived > ceil(nodesNumber/2))
                    FSM_Goto(fsm, LEADER);
                if(RequestVote *v = dynamic_cast<RequestVote*>(msg)){
                    if(v->getArgs().term >= currentTerm){
                        votedFor = v->getArgs().candidateId;
                        send(grantVote(v), "gate$o", msg->getArrivalGate()->getIndex());
                        FSM_Goto(fsm, FOLLOWER);

                    }
                }
                     bubble("another one");
                FSM_Goto(fsm, CANDIDATE);
                break;
        case FSM_Enter(CANDIDATE):
                bubble("CANDIDATE");
                votedFor = getIndex();
                int n = gateSize("gate");
                for(int k=0; k<n; k++) {
                    RequestVote *msg = generateMessage();
                    send(msg, "gate$o", k);
                }
                break;




    }
}
RequestVote *node::generateMessage()
{
    RequestVote* msg = new RequestVote();
    RequestVote_Args args;

    args.term=currentTerm;
    args.candidateId=getIndex();

    msg->setArgs(args);

    return msg;
}

RequestVote *node::grantVote(RequestVote* msg)
{

    RequestVote_Results res;

    res.term = currentTerm;
    res.voteGranted = true;

    msg->setRes(res);

    return msg;
}

RequestVote *node::refuseVote(RequestVote* msg)
{

    RequestVote_Results res;

    res.term = currentTerm;
    res.voteGranted = false;

    msg->setRes(res);

    return msg;
}




