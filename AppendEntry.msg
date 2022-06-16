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

struct logEntry_t{
    int term;
    int command;
    int index;
};

struct AppendEntry_Args { 
  		int term;
		int leaderId;
		int prevLogIndex;
		int prevLogTerm;
		logEntry_t entries [10];
		int entriesSize;
		int lederCommit; 
	};
	
struct AppendEntry_Results{
    	int id;
		int term;
		bool success;	
	};

message AppendEntry{
	
	AppendEntry_Args args;
	AppendEntry_Results res;

}