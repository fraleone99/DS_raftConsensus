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
#include "ClientReq_res_m.h"
#include "ClientRequest_m.h"



using namespace omnetpp;
using namespace std;

struct peer{
    int id;
    bool voteGranted;
    int nextIndex;
    int matchIndex;
};

struct logEntry_t{
    int term;
    int command;
    int index;
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
    virtual AppendEntry *generateHeartBeat();
    //virtual AppendEntry *generateAppendEntry();

    virtual ClientReq_res *generateClientReq_res(bool accepted);


    float timer;
    float heartBeatTimer;
    float crashTimer;
    float wakeUpTimer;
    cMessage *timerMsg = new cMessage("TimerExpired");
    cMessage *heartBeatMsg = new cMessage("TimerHeartbeat");
    cMessage *crashMsg = new cMessage("Crash");
    cMessage *wakeUpMsg = new cMessage("WakeUp");
    int nodesNumber;
    int id;
    int leaderId;
        peer* peers;

    int stateMachine = 0;

    //Persistent state
    int currentTerm = 0; //increases monotonically
    int votedFor = -1;
    logEntry_t log[50];
    int logLastFilledPosition = 0;
    int voteReceived = 0;

    //Volatile state
    int commitIndex = 0;
    int lastApplied = 0;

    //Volatile State for Leaders
    int* nextIndex;
    int* matchIndex;

    //needed to simulate crash through a state of the FSM
    int stateBeforeCrash;

    cFSM fsm;
    enum{

        INIT = 0,
        FOLLOWER = FSM_Steady(1),
        CANDIDATE = FSM_Steady(2),
        LEADER = FSM_Steady(3),
        CRASH = FSM_Steady(4),
        HEARTBEAT = FSM_Transient(1),
        APPEND_ENTRY = FSM_Transient(2),
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

    void handleRequests(ClientRequest* req){
        logEntry_t entry;
        entry.command = req->getCommand();
        entry.term = currentTerm;
        entry.index = lastApplied;
        //log[lastFilledPosition+1] = entry;;
        //lastFilledPosition++;
        printLog();
    }

    void printLog(){
        logEntry_t toPrint;
        for(int i = 0 ; i< lastApplied; i++){
            toPrint = log[i];
            (EV <<  "\nposition: "<< i << " command: " << toPrint.command << " term: " << toPrint.term << " index: " <<  toPrint.index << "\n");
        }
    }

    /*void printState(){
        (EV << "state machine: " << stateMachine << " id: " << id);
    }*/

    void crash(){
        stateBeforeCrash = fsm.getState();
    }

    int findChannelById(int toFind){
        return toFind < id ? toFind : toFind - 1;
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
        scheduleAt(0.0 + crashTimer, crashMsg);
        (EV << "crashTimer" << crashTimer);
    }

    if((int)par("wakeUpTime")!= 0){
        wakeUpTimer = (int) par("wakeUpTime");
        scheduleAt(0.0 + wakeUpTimer, wakeUpMsg);
        (EV << "\nwakeUpTimer" << wakeUpTimer);
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

    logEntry_t first;
    first.command = 0;
    first.index = 0;
    first.term = 0;
    log[0] = first;


}

void node::handleMessage(cMessage *msg)
{

    FSM_Switch(fsm)
    {
        case FSM_Exit(INIT):
                 FSM_Goto(fsm,FOLLOWER);
                 break;
        case FSM_Exit(FOLLOWER):
                if(msg -> isSelfMessage()){
                    if(msg == timerMsg){
                        newTerm(currentTerm + 1);
                        if(nodesNumber != 1){
                            FSM_Goto(fsm, CANDIDATE);
                        }
                        else{ //border case in which there is only one state: go directly to leader
                            FSM_Goto(fsm, LEADER);
                        }
                    }
                    else if(msg == crashMsg){
                        FSM_Goto(fsm, CRASH);
                    }

                }
                else{
                    if(RequestVote *v = dynamic_cast<RequestVote*>(msg)){ //Another node has initiated an election
                        if(v->getArgs().term >= currentTerm && votedFor==-1){
                            newTerm(v->getArgs().term);
                            votedFor = v->getArgs().candidateId;
                            send(grantVote(v), "gateNode$o", findChannelById(votedFor));

                        }else{
                            send(refuseVote(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                        }
                        FSM_Goto(fsm, FOLLOWER);

                    }else if(AppendEntry *v = dynamic_cast<AppendEntry*>(msg)){
                        if(v->getArgs().term >= currentTerm){
                            printState();
                            send(acceptAppend(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                            votedFor = -1;
                            leaderId = v->getArgs().leaderId;
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
                rescheduleAt(simTime()+timer, timerMsg); //resetTimer
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
                if(msg -> isSelfMessage()){
                    (EV << msg->getName() << crashMsg->getName() );
                    //Send heartBeat
                    if(msg != crashMsg){
                        FSM_Goto(fsm, HEARTBEAT);
                        //rescheduleAt(simTime() + heartBeatTimer, heartBeatMsg);
                    }
                    //simulating crash
                    else{
                        crash();
                        FSM_Goto(fsm, CRASH);
                    }
                }
                //handling unsuccessful AppendEntry
                else if(AppendEntry *v = dynamic_cast<AppendEntry*>(msg)){
                    if(v->getArgs().term > currentTerm){
                        (EV << "\nThere is a new sheriff in town\n");
                        FSM_Goto(fsm, FOLLOWER);
                        send(acceptAppend(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                        newTerm(v->getRes().term);
                    }
                    else{
                        FSM_Goto(fsm, LEADER);
                    }
                }
                //handling a Client Request
                else if(ClientRequest *req = dynamic_cast<ClientRequest*>(msg)){
                    ClientReq_res *res = generateClientReq_res(true);
                    handleRequests(req);
                    send(res ,"gateToClients$o", req->getArrivalGate()->getIndex());
                    FSM_Goto(fsm, APPEND_ENTRY);
                }
                break;
        case FSM_Enter(LEADER):
                bubble("LEADER");
                rescheduleAt(simTime() + heartBeatTimer, heartBeatMsg);
                printState();
                break;
        case FSM_Exit(HEARTBEAT):
                for(int k=0; k<nodesNumber-1; k++) {
                    AppendEntry *msg = generateHeartBeat();
                    send(msg, "gateNode$o", k);
                }
                FSM_Goto(fsm, LEADER);
                break;
        case FSM_Exit(APPEND_ENTRY):
                for(int k=0; k<nodesNumber-1; k++) {
                    //AppendEntry *msg = generateAppendEntry();
                    send(msg, "gateNode$o", k);
                }
                FSM_Goto(fsm, LEADER);
                break;

        case FSM_Exit(CRASH):
                if(msg != wakeUpMsg)
                    FSM_Goto(fsm, CRASH);
                else
                    FSM_Goto(fsm, stateBeforeCrash);
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

AppendEntry *node::generateHeartBeat()
{
    AppendEntry* msg = new AppendEntry();
    AppendEntry_Args args;

    args.term = currentTerm;
    args.leaderId = id;
    //args.entries = NULL;

    msg->setArgs(args);

    return msg;
}

/*AppendEntry *node::generateAppendEntry(int* entries)
{
    AppendEntry* msg = new AppendEntry();
    AppendEntry_Args args;

    args.term = currentTerm;
    args.leaderId = id;
    args.term = currentTerm;
    args.leaderId = id;
    args.prevLogIndex = logLastFilledPosition - 1;
    args.prevLogTerm = log[logLastFilledPosition - 1].term;
    for
    args.entries = entries;
    args.lederCommit = commitIndex;

    msg->setArgs(args);

    return msg;
}*/



AppendEntry *node::acceptAppend(AppendEntry* msg)
{

    AppendEntry_Results res;

    leaderId = msg->getArgs().leaderId;

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

ClientReq_res *node::generateClientReq_res(bool accepted){
    ClientReq_res *res = new ClientReq_res();

    res->setAccepted(accepted);
    res->setLeaderId(leaderId);

    return res;
}










