
class CommandBuilder
{
	struct Dependent{
		string name;
		string iv; //iv that name depends on
	};

	struct Location {
		string filename;
		int lineno;
		friend ostream&  operator<<(ostream& os, const Location& loc);
	};

	vector<string> m_ivs; // indepedent variables
	vector<Dependent> m_dvs;
	vector<Argument> m_in;
	vector<Argument> m_out;

	Location m_loc;

	Ent* m_parent;

	void printCommands();
 	int getCommands(vector<Command>* out);

};

ostream& operator<<(ostream& os, const Location& loc)
{
	cerr << loc.filename << ":" << loc.lineno;
}

class Command
{
	vector<string> prereqs; // prerequisite files
	list<string> unmet; // prequisites remaining
};

class Argument
{
	string path;

	// variable name-> position in string
	// where the name should be inserted
	unordered_map<string, size_t> m_vars;

	// global variables to produce multiple arguments from,
	// when creating the command, these variables should
	// all be placed on the command line together
	vector<string> m_mutlivars;
}; 


