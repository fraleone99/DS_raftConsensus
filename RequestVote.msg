	struct RequestVote_Args { 
  		int term;
		int candidateId;
		int lastLogIndex;
		int lastLogTerm;
	};
	
	struct RequestVote_Results{
		int term;
		bool voteGranted;	
	};

message RequestVote{
    
    string name = "RequestVote";
	
	RequestVote_Args args;
	RequestVote_Results res;
}