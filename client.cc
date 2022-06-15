#include <string.h>
#include <omnetpp.h>

using namespace omnetpp;


class client : public cSimpleModule
{
  protected:
    // The following redefined virtual function holds the algorithm.
    virtual void initialize() override;
    virtual void handleMessage(cMessage *msg) override;
};

Define_Module(client);

void client::initialize()
{

}

void client::handleMessage(cMessage *msg)
{

}




