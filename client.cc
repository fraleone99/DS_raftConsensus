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
    float timer;
    int toSend;
    int* commands;
    int n_commands;

    cMessage *timerMsg = new cMessage("TimerExpired");


    cFSM fsm;
        enum{
            INIT = 0,
            SEND_REQUESTS = FSM_Steady(1),
            WAITING_ACK = FSM_Steady(2),
            SHUTDOWN = FSM_Steady(3),
        };

};

Define_Module(client);

void client::initialize()
{
    timer = uniform(0.5,1);
    toSend = 0;
    commands = new int[10];
    n_commands = 10;
    n_nodes = (int) par("nodes");
    leaderId = -1;

    for(int i = 0; i < n_commands; i++){
        commands[i] = uniform(0,100);
    }

    timer = uniform(1,2);
    scheduleAt(timer, timerMsg);

}

void client::handleMessage(cMessage *msg)
{
    FSM_Switch(fsm){
        case FSM_Exit(INIT):
                FSM_Goto(fsm, SEND_REQUESTS);
                break;
        case FSM_Exit(SEND_REQUESTS):
                if(ClientReq_res *r = dynamic_cast<ClientReq_res*>(msg)){
                    if(r->getAccepted()){
                        toSend++;
                        FSM_Goto(fsm, SEND_REQUESTS);
                    }
                    //if the message has not been accepted i need to update
                    //the leaderId and send the request again, toSend doesn't have to be updated
                    else{
                        leaderId = r->getLeaderId();
                        FSM_Goto(fsm, SEND_REQUESTS);
                    }
                }
                break;
        case FSM_Enter(SEND_REQUESTS):{
                ClientRequest* request = generateClientRequest();
                if(leaderId != -1){
                    send(msg, "gateClient$o", leaderId);
                }
                else{
                    send(request, "gateClient$o", 0);
                }
                break;
        }
        case FSM_Exit(WAITING_ACK):
                break;
        case FSM_Enter(WAITING_ACK):
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
    return msg;
}




