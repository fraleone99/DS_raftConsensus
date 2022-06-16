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
#include <vector>
#include <algorithm>



using namespace omnetpp;
using namespace std;

struct peer{
    int id;
    bool voteGranted;
    int nextIndex;
    int matchIndex;
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
    virtual AppendEntry *generateAppendEntry(int peerId);

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
    std::vector<logEntry_t> log;
    //int logLastFilledPosition = 0;
    int voteReceived = 0;

    //Volatile state
    int commitIndex = 0;
    int lastApplied = 0;

    //Volatile State for Leaders
    int* nextIndex;
    int* matchIndex;

    //needed to simulate crash through a state of the FSM
    int stateBeforeCrash;

    //variables needed for Append message
    int entriesSize = 0;
    int prevLogIndex = 0;
    int prevLogTerm  = 0;
    //int peerId = 0;

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
        log.insert(log.end(), entry);;
        printLog();
    }

    void printLog(){
        logEntry_t toPrint;
        for(int i = 0 ; i< log.size(); i++){
            toPrint = log[i];
            (EV <<  "\nposition: "<< i << " command: " << toPrint.command << " term: " << toPrint.term << " index: " <<  toPrint.index << "\n");
        }
    }

    void printStateMachine(){
        (EV << "state machine: " << stateMachine << " id: " << id);
    }

    void crash(){
        stateBeforeCrash = fsm.getState();
    }

    int findChannelById(int toFind){
        return toFind < id ? toFind : toFind - 1;
    }

    //void sendHeartBeat(){

    //}


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
    log.insert(log.begin(), first);


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
                        AppendEntry_Args args = v->getArgs();
                        if(args.term >= currentTerm){
                            printState();

                            //check if our log contain an entry at prevLogIndex whose term matches prevLogTerm
                            if(args.prevLogIndex == 0 || args.prevLogIndex < log.size() &&
                                    args.prevLogTerm == log[args.prevLogIndex].term){
                                send(acceptAppend(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                                //votedFor = -1;
                                leaderId = v->getArgs().leaderId;

                                //find a point where there is a mismatch between the existing log
                                //and the new entries

                                int logInsertIndex = args.prevLogIndex + 1;
                                int newEntriesIndex = 0;

                                while(1){

                                    if(logInsertIndex >= log.size() || newEntriesIndex >= args.entriesSize)
                                        break;
                                    if(log[logInsertIndex].term != args.entries[newEntriesIndex].term)
                                        break;
                                    logInsertIndex++;
                                    newEntriesIndex++;
                                }
                                //Now we have found the point of mismatch if exists
                                std::vector<logEntry_t> entries;
                                entries.insert(entries.begin(), std::begin(args.entries), std::end(args.entries));

                                if(newEntriesIndex < args.entriesSize){
                                    auto itPos = log.begin() + logInsertIndex;
                                    log.insert(itPos, entries.begin() + newEntriesIndex, entries.end());
                                    (EV << "inserting entries from index: " << logInsertIndex);
                                    printLog();
                                }

                                //Set commit index
                                if(args.lederCommit > commitIndex){
                                    if(args.lederCommit > log.size()-1)
                                        commitIndex = log.size() - 1;
                                    else
                                        commitIndex = args.lederCommit;
                                    (EV << "\ncommit index: " << commitIndex);
                                }


                            }

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
                                  for(int i = 0; i < nodesNumber; i++){
                                      if(i != id){
                                          nextIndex[i] = log.size();
                                          matchIndex[i] = -1;
                                      }
                                  }
                                   FSM_Goto(fsm, HEARTBEAT);
                                   //scheduleAt(simTime() + heartBeatTimer, heartBeatMsg);
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
                //handling AppendEntry response
                else if(AppendEntry *v = dynamic_cast<AppendEntry*>(msg)){
                    if(v->getRes().term > currentTerm){
                        (EV << "\nThere is a new leader\n");
                        FSM_Goto(fsm, FOLLOWER);
                        send(acceptAppend(v), "gateNode$o", msg->getArrivalGate()->getIndex());
                        newTerm(v->getRes().term);
                    }
                    else if(v->getRes().term == currentTerm){
                        if(v->getRes().success){
                            nextIndex[v->getRes().id] += v->getArgs().entriesSize;
                            matchIndex[v->getRes().id] = nextIndex[v->getRes().id] - 1;

                            int tempCommitIndex = commitIndex;

                            for(int i = commitIndex + 1; i < log.size(); i++){
                                if(log[i].term == currentTerm){
                                    int matchCount = 1;
                                    for(int peerId = 0; peerId < nodesNumber; peerId++){
                                        if(peerId != id){
                                            if(matchIndex[peerId] >= i)
                                                matchCount++;
                                        }
                                    }
                                    if (matchCount*2 > nodesNumber) {
                                        commitIndex = i;
                                    }
                                }
                            }
                            if(commitIndex != tempCommitIndex){
                                (EV << "new commit Index: " << commitIndex);
                                //TODO send ack to client
                            }
                        }
                        else{
                            nextIndex[v->getRes().id]--;
                            (EV <<  "Append entry not successful updating next id");
                        }
                        FSM_Goto(fsm, LEADER);
                    }
                }
                //handling a Client Request
                else if(ClientRequest *req = dynamic_cast<ClientRequest*>(msg)){
                    ClientReq_res *res = generateClientReq_res(true);
                    handleRequests(req);
                    send(res ,"gateToClients$o", req->getArrivalGate()->getIndex());
                    FSM_Goto(fsm, HEARTBEAT);
                }
                break;
        case FSM_Enter(LEADER):
                bubble("LEADER");

                //rescheduleAt(simTime() + heartBeatTimer, heartBeatMsg);
                printState();
                printLog();
                break;
        case FSM_Exit(HEARTBEAT):
                for(int peerId = 0; peerId < nodesNumber; peerId++){
                    if(peerId != id){
                        AppendEntry *v = generateAppendEntry(peerId);
                        send(v, "gateNode$o", findChannelById(peerId));
                    }
                }
                rescheduleAt(simTime() + heartBeatTimer, heartBeatMsg);
                FSM_Goto(fsm, LEADER);
                break;
        case FSM_Exit(APPEND_ENTRY):
                //for(int k=0; k<nodesNumber-1; k++) {
                    //AppendEntry *msg = generateAppendEntry();
                    //send(msg, "gateNode$o", k);
                //}
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

AppendEntry *node::generateAppendEntry(int peerId)
{

    AppendEntry* msg = new AppendEntry();


    int ni =  nextIndex[peerId];

    prevLogIndex = ni - 1;

    prevLogTerm = -1;

    if (prevLogIndex >= 0){
        prevLogTerm = log[prevLogIndex].term;
    }
    logEntry_t  entries[10];

    entriesSize = 0;

    (EV << "ni " << ni);
    for(int i = ni - 1; i < log.size(); i++){
        entries[entriesSize].command = log[ni].command;
        entries[entriesSize].term = log[ni].term;
        entries[entriesSize].index = log[ni].index;
        entriesSize++;
    }


    AppendEntry_Args args;
    for(int k = 0; k < entriesSize; k++){
        args.entries[k] = entries[k];
    }

    args.entriesSize = entriesSize;
    args.leaderId = id;
    args.lederCommit = commitIndex;
    args.prevLogIndex = prevLogIndex;
    args.prevLogTerm = prevLogTerm;
    args.term = currentTerm;

    //msg->setArgs(args);


    (EV << "sending AppendEntries to" << peerId << " ni: " << ni);

    return msg;
}



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










