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
#define FSM_DEBUG    // enables debug output from FSMs

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
    virtual RequestVote *generateRequestVote();
    virtual RequestVote *grantVote(RequestVote* msg);
    virtual RequestVote *refuseVote(RequestVote* msg);
    virtual AppendEntry *refuseAppend(AppendEntry* msg);
    virtual AppendEntry *acceptAppend(AppendEntry* msg);
    virtual AppendEntry *generateAppendEntry();

    float timer;
    cMessage *timerMsg = new cMessage("TimerExpired");
    int nodesNumber;
    int id;

     //Persistent state
     int currentTerm = 0; //increases monotonically
     int votedFor = -1;
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

        INIT = 0,
        FOLLOWER = FSM_Steady(1),
        CANDIDATE = FSM_Steady(2),
        LEADER = FSM_Steady(3),
    };


};

Define_Module(node);

void node::initialize()
{
    nodesNumber = gateSize("gate")+1;
    id = getIndex();

    fsm.setName("fsm");
    timer = uniform(0.150,0.300);

    nextIndex = new int[nodesNumber];
    matchIndex = new int[nodesNumber];
    scheduleAt(0.0, timerMsg);


}

void node::handleMessage(cMessage *msg)
{

    FSM_Switch(fsm)
    {
        case FSM_Exit(INIT):
                 rescheduleAt(simTime()+timer, timerMsg);
                 FSM_Goto(fsm,FOLLOWER);
                 break;
        case FSM_Exit(FOLLOWER):
                if(msg -> isSelfMessage()){
                    currentTerm++;
                    FSM_Goto(fsm, CANDIDATE);
                }
                else{
                    if(RequestVote *v = dynamic_cast<RequestVote*>(msg)){
                        if(v->getArgs().term >= currentTerm && votedFor==-1){
                            currentTerm = v->getArgs().term;
                            votedFor = v->getArgs().candidateId;
                            send(grantVote(v), "gate$o", msg->getArrivalGate()->getIndex());

                        }else{
                            send(refuseVote(v), "gate$o", msg->getArrivalGate()->getIndex());
                        }
                        FSM_Goto(fsm, FOLLOWER);

                    }else if(AppendEntry *v = dynamic_cast<AppendEntry*>(msg)){
                        if(v->getArgs().term >= currentTerm)
                            send(acceptAppend(v), "gate$o", msg->getArrivalGate()->getIndex());
                        else
                            send(refuseAppend(v), "gate$o", msg->getArrivalGate()->getIndex());

                    }
                    FSM_Goto(fsm, FOLLOWER);
                    cancelEvent(timerMsg);
                }
                break;
        case FSM_Enter(FOLLOWER):
                bubble("FOLLOWER");
                rescheduleAt(simTime()+timer, timerMsg); //resetTimer
                break;


        case FSM_Exit(CANDIDATE):
                if(msg -> isSelfMessage()){
                    currentTerm++;
                    votedFor = -1;
                    (EV << "reset " << id);
                }
                if(RequestVote *v = dynamic_cast<RequestVote*>(msg)){
                    (EV << "Vote received " << voteReceived );
                          if(v->getArgs().candidateId == id){
                              if(v->getRes().voteGranted){

                                  voteReceived++;
                              }
                          }
                          else{
                              send(refuseVote(v), "gate$o", msg->getArrivalGate()->getIndex());
                          }
                 }
                 if(voteReceived >= ceil(nodesNumber/2))
                     FSM_Goto(fsm, LEADER);
                 else
                     FSM_Goto(fsm, CANDIDATE);
                 break;
        case FSM_Enter(CANDIDATE):
                bubble("CANDIDATE");
                if(votedFor == -1){
                    for(int k=0; k<nodesNumber-1; k++) {
                        RequestVote *msg = generateRequestVote();
                        send(msg, "gate$o", k);
                    }
                    votedFor =2;
                    voteReceived++;
                    rescheduleAt(simTime()+timer, timerMsg); //resetTimer
                }
                break;
        case FSM_Exit(LEADER):
                FSM_Goto(fsm, LEADER);
                break;
        case FSM_Enter(LEADER):
                bubble("LEADER");
            for(int k=0; k<nodesNumber-1; k++) {
                AppendEntry *msg = generateAppendEntry();
                send(msg, "gate$o", k);
            }
                break;







    }
}

RequestVote *node::generateRequestVote()
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

AppendEntry *node::generateAppendEntry()
{
    AppendEntry* msg = new AppendEntry();
    AppendEntry_Args args;

    args.term = currentTerm;
    args.leaderId = id;


    msg->setArgs(args);

    return msg;
}

AppendEntry *node::acceptAppend(AppendEntry* msg)
{

    AppendEntry_Results res;

    res.success= true;
    res.term = currentTerm;

    msg->setRes(res);

    return msg;
}

AppendEntry *node::refuseAppend(AppendEntry* msg)
{

    AppendEntry_Results res;

    res.success= false;
    res.term = currentTerm;

    msg->setRes(res);

    return msg;
}






