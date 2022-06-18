#include <string.h>
#include <omnetpp.h>
#include "ClientRequest_m.h"
#include "ClientReq_res_m.h"


using namespace omnetpp;


class client : public cSimpleModule
{
  protected:
    virtual void initialize() override;
    virtual void handleMessage(cMessage *msg) override;
    virtual ClientRequest *generateClientRequest();


    int leaderId;
    int n_nodes;
    float initTimer;
    float resendTimer;
    int toSend;
    int* commands;
    int n_commands;

    int id;

    cMessage *timerMsg = new cMessage("TimerExpired");



    cFSM fsm;
        enum{
            INIT = 0,
            SEND_REQUESTS = FSM_Transient(1),
            WAITING_ACK = FSM_Steady(1),
            SHUTDOWN = FSM_Steady(2),
        };

};

Define_Module(client);

void client::initialize()
{
    id = getIndex();

    initTimer = uniform(0.5,0.6);
    resendTimer = 0.5;

    toSend = 0;
    commands = new int[100];
    n_commands = 100;
    n_nodes = (int) par("nodes");
    leaderId = -1;

    for(int i = 0; i < n_commands; i++){
        commands[i] = uniform(0,100);
    }

    //timer = uniform(1,2);
    scheduleAt(initTimer, timerMsg);

}

void client::handleMessage(cMessage *msg)
{
    FSM_Switch(fsm){
        case FSM_Exit(INIT):
                FSM_Goto(fsm, SEND_REQUESTS);
                break;
        case FSM_Exit(SEND_REQUESTS):{
                ClientRequest* request = generateClientRequest();
                if(leaderId != -1){
                    send(request, "gateClient$o", leaderId);
                }
                else{
                    send(request, "gateClient$o", 0);
                }
                rescheduleAt(simTime() + initTimer, timerMsg);
                FSM_Goto(fsm, WAITING_ACK);
                break;
        }
        case FSM_Exit(WAITING_ACK):
                rescheduleAt(simTime() + initTimer, timerMsg);
                //resendTimer expired
                if(msg->isSelfMessage()){
                    leaderId = uniform(0 , n_nodes);
                }
                else if(ClientReq_res *r = dynamic_cast<ClientReq_res*>(msg)){
                    //ack received
                    if(r->getAccepted()){
                        toSend++;
                    }
                    //if the message has not been accepted i need to update
                    //the leaderId and send the request again, toSend doesn't have to be updated
                    else{
                        leaderId = r->getLeaderId();

                    }
                }
                FSM_Goto(fsm, SEND_REQUESTS);
                break;
        case FSM_Enter(WAITING_ACK):
                bubble("waiting Ack");
                break;
        case FSM_Exit(SHUTDOWN):
                FSM_Goto(fsm, SHUTDOWN);
                break;
        case FSM_Enter(SHUTDOWN):
                break;

    }

}

ClientRequest *client::generateClientRequest()
{
    ClientRequest* msg = new ClientRequest();
    msg->setCommand(commands[toSend]);
    msg->setId(id);
    return msg;
}




