#include <regex>
#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <set>
#include <tclap/CmdLine.h>

using std::list;
using std::cerr;
using std::cout;
using std::string;
using std::regex;
using std::regex_match;
using std::sregex_token_iterator;

using namespace std;

int expand(std::string expression, std::list<int>& lout, std::string& sout)
{
	const std::sregex_iterator regEnd;
	const std::regex expRe("(\\{\\*?"
			"(?:\\s*[0-9.]*\\s*:)?)"
			"(?:\\s*([a-zA-Z0-9,\\-]*)\\s*)?"
			"((?:\\s*\\|[^}]*)?\\})");
	const std::regex rangeRe("(?:([0-9]+)-([0-9]+)|([0-9]+))");
	
	std::smatch args;
	lout.clear();
	string prefix;
	string suffix;

	if(regex_search(expression, args, expRe)) {
		prefix = args[1].str();
		suffix = args[3].str();

		string rterm = args[2].str();
		sregex_iterator it(rterm.cbegin(), rterm.cend(), rangeRe);
		for(; it != regEnd; it++) {
			string low = (*it)[1].str();
            string hi = (*it)[2].str();
			string single = (*it)[3].str();

			if(!low.empty() && !hi.empty()) {
				int a = atoi(low.c_str());	
				int b = atoi(hi.c_str());	
				if(a > b) {
					cerr << "Illegal range: " << a << "-" << b << endl;
					return -1;
				}
				for( ; a <= b; a++)
					lout.push_back(a);
			} else if(!single.empty()) {
				lout.push_back(atoi(single.c_str()));
			}
		}
	} else {
		cerr << "Error, syntax not understood: " << expression << endl;
		return -1;
	}

	ostringstream oss; 
	for(auto& v : lout)
		oss << prefix << v << suffix << ", ";
	sout = oss.str();
	return 0;
}

int main(int argc, char** argv)
{
	try {
	/* 
	 * Command Line 
	 */
	TCLAP::CmdLine cmd("This program takes a regular expression and uses it to"
			"find a file.", ' ',__version__);

	// arguments
	TCLAP::ValueArg<std::string> a_regex("e","exp","Expression '{*23.2:1-5,52-5}'"
				,true,"*","regexp");
	cmd.add(a_regex);

	// parse arguments
	cmd.parse(argc, argv);

	/*
	 * Main Processing
	 */
	
	list<int> values;
	string expanded;

	if(expand(a_regex.getValue(), values, expanded) < 0) {
		cerr << "ERROR" << endl;
		return -1;
	}
	
	cerr << "Input: " << a_regex.getValue() << endl;
	cerr << "Expanded to: " << expanded << endl;

	// done, catch all argument errors
	} catch (TCLAP::ArgException &e)  // catch any exceptions
	{ std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl; }
}

