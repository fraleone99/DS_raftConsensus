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
		int channels=parent.nodes-1;
    gates:
        inout gate[channels];
}


//
// TODO documentation
//
network Net
{
    parameters:
        int nodes;
    types:
        channel Channel extends ned.DelayChannel {
            delay = 100ms;
        }
    submodules:
        node[nodes]: node;
    connections:
        for i=0..nodes-2 {
            node[i].gate++ <--> Channel <--> node[j].gate++ for j=i+1..nodes-1, if j!=i;
        }    
}

