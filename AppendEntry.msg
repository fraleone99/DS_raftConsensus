
struct logEntry_t{
    int term;
    int command;
    int index;
    
    //needed to answer to client in case of leader crash with uncommitted entries
    //and to save the last executed entries for every client and not executing twice the same commands
    int client_id;
    int command_sn;
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
		
		//next index before appendEntry to correctly update the ni on leader
		int ni;
	};

message AppendEntry{
	
	AppendEntry_Args args;
	AppendEntry_Results res;

}