
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
		logEntry_t entries[100];
		int entriesSize;
		int lederCommit; 
	};
	
struct AppendEntry_Results{
    	int id;
		int term;
		bool success;
		int ni;
	};

message AppendEntry{
	
	AppendEntry_Args args;
	AppendEntry_Results res;

}