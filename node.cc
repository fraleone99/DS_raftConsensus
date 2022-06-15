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

struct peer{
    int id;
    bool voteGranted;
    int nextIndex;
    int matchIndex;
};

struct log_entry{
    int term;
    int command;

};

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
    float heartBeatTimer;
    float crashTimer;
    cMessage *timerMsg = new cMessage("TimerExpired");
    cMessage *heartBeatMsg = new cMessage("TimerHeartbeat");
    cMessage *crashMsg = new cMessage("crash");


    int nodesNumber;
    int id;
    peer* peers;

     //Persistent state
     int currentTerm = 0; //increases monotonically
     int votedFor = -1;
     log_entry log[50];
     int voteReceived = 0;

     //Volatile state
     int commitIndex = 0;
     int lastApplied = 0;

     //Volatile State for Leaders
     int* nextIndex;
     int* matchIndex;

     string x;

    cFSM fsm;
    enum{

        INIT = 0,
        FOLLOWER = FSM_Steady(1),
        CANDIDATE = FSM_Steady(2),
        LEADER = FSM_Steady(3),
        CRASH = FSM_Steady(4),
        HEARTBEAT = FSM_Transient(1),
    };

    void newTerm(int newTerm){
        (EV<< "updating term from " << currentTerm << " to " << newTerm << " id: " << id);
        this->votedFor = -1;
        this->voteReceived = 0;
        this->currentTerm = newTerm;
        timer = uniform(0.150,0.300);

    }

    void printState(){
        (EV << "node:" << id << " voteReceived: " << voteReceived << " votedFor: " << votedFor << " currentTerm: " << currentTerm);
    }


};

Define_Module(node);

void node::initialize()
{
    nodesNumber = gateSize("gateNode")+1;
    id = getIndex();

    fsm.setName("fsm");
    timer = uniform(0.150,0.300);
    heartBeatTimer = 0.15;

    nextIndex = new int[nodesNumber];
    matchIndex = new int[nodesNumber];
    scheduleAt(0.0, timerMsg);

    if((int)par("crashTime")!= 0){
        crashTimer = (int) par("crashTime");
        crashMsg->setContextPointer(&x);
        scheduleAt(0.0 + crashTimer, crashMsg);
        (EV << "crashTimer" << crashTimer);
    }

    peers = new peer[nodesNumber];
    for(int i = 0; i < nodesNumber; i++){
        if(i != id){
            peers[i].id = i;
            peers[i].voteGranted = false;
            peers[i].matchIndex = 0;
            peers[i].nextIndex = 0;
            (EV << "\ninitializing node i: " << (i) << " in peersArray of node: " << id <<"\n");
        }
    }


}

void node::handleMessage(cMessage *msg)
{

    FSM_Switch(fsm)
    {
        case FSM_Exit(INIT):
                 FSM_Goto(fsm,FOLLOWER);
                 break;
        case FSM_Exit(FOLLOWER):
                if(msg -> isSelfMessage()){ // ElectionTimer expires
                    newTerm(currentTerm + 1);
                    if(nodesNumber != 1){
                        FSM_Goto(fsm, CANDIDATE);
                    }
                    else{
                        FSM_Goto(fsm, LEADER);
                    }

                }
                else{
                    if(RequestVote *v = dynamic_cast<RequestVote*>(msg)){ //Another node has initiated an election
                        if(v->getArgs().term >= currentTerm && votedFor==-1){
                            newTerm(v->getArgs().term);
                            votedFor = v->getArgs().candidateId;
                            send(grantVote(v), "gateNode$o", msg->getArrivalGate()->getIndex());

                        }else{
                            send(refuseVote(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                        }
                        FSM_Goto(fsm, FOLLOWER);

                    }else if(AppendEntry *v = dynamic_cast<AppendEntry*>(msg)){
                        if(v->getArgs().term >= currentTerm){
                            printState();
                            send(acceptAppend(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                            votedFor = -1;
                        }
                        else
                            send(refuseAppend(v), "gateNode$o", msg->getArrivalGate()->getIndex());


                    }
                    FSM_Goto(fsm, FOLLOWER);
                    cancelEvent(timerMsg);
                }
                break;
        case FSM_Enter(FOLLOWER):
                bubble("FOLLOWER");
                scheduleAt(simTime()+timer, timerMsg); //resetTimer
                break;


        case FSM_Exit(CANDIDATE):{
                if(msg -> isSelfMessage()){
                    newTerm(currentTerm +1);
                    FSM_Goto(fsm, CANDIDATE);
                }
                else if(RequestVote *v = dynamic_cast<RequestVote*>(msg)){
                          if(v->getArgs().candidateId == id){
                              if(v->getRes().voteGranted){
                                  voteReceived++;
                              }
                              if(voteReceived >= nodesNumber/2 + 1){
                                   FSM_Goto(fsm, LEADER);
                                   scheduleAt(simTime() + heartBeatTimer, heartBeatMsg);
                                   cancelEvent(timerMsg);
                              }
                              else
                                   FSM_Goto(fsm, CANDIDATE);
                          }
                          else{
                              if(v->getArgs().term <= currentTerm)
                                  send(refuseVote(v), "gateNode$o", msg->getArrivalGate()->getIndex());

                              else{
                                  newTerm(v->getArgs().term);
                                  votedFor = v->getArgs().candidateId;
                                  send(grantVote(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                          }
                 }
                }
                else if(AppendEntry *v = dynamic_cast<AppendEntry*>(msg)){
                    if(v->getArgs().term >= currentTerm){
                        send(acceptAppend(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                        FSM_Goto(fsm, FOLLOWER);
                        voteReceived = 0;
                    }
                    cancelEvent(timerMsg);

                }
        }
                 break;
        case FSM_Enter(CANDIDATE):
                if(votedFor != id){
                    bubble("CANDIDATE");
                    votedFor = id;
                    voteReceived++;
                    for(int k=0; k<nodesNumber-1; k++) {
                        RequestVote *msg = generateRequestVote();
                        send(msg, "gateNode$o", k);
                    }
                    rescheduleAt(simTime()+timer, timerMsg); //resetTimer
                }
                break;
        case FSM_Exit(LEADER):
                FSM_Goto(fsm, LEADER);
                if(msg -> isSelfMessage()){
                    (EV << msg->getName() << crashMsg->getName() );
                    //Send heartBeat
                    if(strcmp(msg->getName(),crashMsg->getName()) != 0){
                        FSM_Goto(fsm, HEARTBEAT);
                        rescheduleAt(simTime() + heartBeatTimer, heartBeatMsg);
                    }
                    //simulating crash
                    else{
                        FSM_Goto(fsm, CRASH);
                    }
                }
                //handling unsuccessful AppendEntry
                else if(AppendEntry *v = dynamic_cast<AppendEntry*>(msg)){
                    if(!(v->getRes().success) && v->getRes().term > currentTerm){
                        FSM_Goto(fsm, FOLLOWER);
                        newTerm(v->getRes().term);
                    }
                    else{
                        FSM_Goto(fsm, LEADER);
                    }
                }
                else{
                    FSM_Goto(fsm, LEADER);
                }
                break;
        case FSM_Enter(LEADER):
                bubble("LEADER");
                printState();
                break;
        case FSM_Exit(HEARTBEAT):
                for(int k=0; k<nodesNumber-1; k++) {
                    AppendEntry *msg = generateAppendEntry();
                    send(msg, "gateNode$o", k);
                }
                FSM_Goto(fsm, LEADER);
                break;

        case FSM_Exit(CRASH):
                FSM_Goto(fsm, CRASH);
                break;
        case FSM_Enter(CRASH):
                bubble("crash");
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
    res.id = id;

    msg->setRes(res);

    return msg;
}

AppendEntry *node::refuseAppend(AppendEntry* msg)
{

    AppendEntry_Results res;

    res.success= false;
    res.term = currentTerm;
    res.id = id;

    msg->setRes(res);

    return msg;
}






