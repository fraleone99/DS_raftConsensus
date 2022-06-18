//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
//
//
// TODO documentation
//
simple node
{
    parameters:
        int channels = parent.nodes-1;
        double crashTime = default(0);
        double wakeUpTime = default(0);
        @display("i=device/server");
    gates:
        inout gateNode[];
        inout gateToClients[];
}

simple client
{
    parameters:
        int nodes = parent.nodes;
        int channelsClient = parent.nodes;
        @display("i=device/pc");
    gates:
        inout gateClient[];
}





network Net
{
    parameters:
        int nodes;
        int clients;
    types:
        channel Channel extends ned.DelayChannel
        {
            delay = 8ms;
        }
    submodules:
        node[nodes]: node;
        client[clients]: client;
    connections:
        for i=0..nodes-2{
            node[i].gateNode++ <--> Channel <--> node[j].gateNode++ for j=i+1..nodes-1 ,if i != j;
        }
        for i=0..clients-1, for j=0..nodes-1 {
            client[i].gateClient++ <--> Channel <--> node[j].gateToClients++;
        }
}

