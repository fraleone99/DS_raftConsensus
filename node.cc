/*
 * computer.cc
 *
 *  Created on: 24 mag 2022
 *  Author: Utente
 */
#include <string.h>
#include <omnetpp.h>
#include <iostream>
#include "raftMessage_m.h"


using namespace omnetpp;
using namespace std;

//enum status {follower, candidate, leader};

class node : public cSimpleModule
{
  protected:
    virtual raftMessage *generateMessage();
    virtual void initialize() override;
    virtual void handleMessage(cMessage *msg) override;

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
    fsm.setName("fsm");
    cMessage *msg = new cMessage("start");
    float timer = uniform(0.150,0.300);

    scheduleAt(simTime() + timer, msg);
}

void node::handleMessage(cMessage *msg)
{
    FSM_Switch(fsm)
    {
        case FSM_Exit(INIT):
                FSM_Goto(fsm, FOLLOWER);
                break;
        case FSM_Exit(FOLLOWER):
                break;
        case FSM_Enter(FOLLOWER):
                bubble("FOLLOWER");
                if(msg->isSelfMessage()) {
                    int n = gateSize("gate");
                    for(int k=0; k<n; k++) {
                        raftMessage *msg = generateMessage();
                        send(msg, "gate$o", k);
                    }
                } else {
                    raftMessage *rmsg = check_and_cast<raftMessage *>(msg);
                    int source = rmsg->getArrivalGate()->getIndex();
                    send(rmsg, "gate$o", source);
                }
                break;
    }
}

raftMessage *node::generateMessage()
{
    int src = getIndex();

    char msgname[20];
    sprintf(msgname, "tic from %d", src);

    raftMessage *msg = new raftMessage(msgname);

    msg->setSource(src);
    return msg;
}


