#include <string>
#include <iostream>
#include <fstream>
#include <regex>
#include <vector>
#include <map>

#include "ent.h"

using std::endl;
using std::cerr;
using std::string;
using std::ifstream;
using std::regex;
using std::regex_match;
using std::map;
using std::list;
using std::sregex_token_iterator;
using std::pair;

#define OFF

list<string> split(string in, char c)
{
	list<string> out;
	string tmp = in; 
	size_t len = 0;
	for(int ii = 0 ; ii < in.size(); ii++){
		if(in[ii] == c) {
			//finished with current bit
			out.push_back(tmp.substr(0,len));
			len = 0;
		} else if(!isspace(in[ii])) {
			//copy non-space values into tmp
			tmp[len++] = in[ii];
		}
	}

	if(len > 0) {
		out.push_back(tmp.substr(0,len));
	}

	return out;
}

Ent::Ent(string filename) : m_err(0)
{
	auto varRe = regex("([a-zA-Z0-9 	]+)=(.*)");
	auto commaRe = regex("(\\s*[^,]+\\s*)");
	auto inputRe = regex("\\s*([0-9.]*\\.i)\\s*::\\s*\\[(.*)\\]\\s*=\\s*\\((.*)\\)");
	auto cmdRe = regex("\\s*([0-9.]*)\\s*::\\s*\\[(.*)\\]\\s*=\\s*(.*)\\s*");
	auto emptyRe = regex("\\s*");
	const sregex_token_iterator regEnd;
	std::smatch args;

	map<string,list<string>> variables;


	ifstream ifs(filename);
	string line;
	std::getline(ifs,line);
	int linenum = 0;
	while(!ifs.eof()) {

		// remove comments 
		line = line.substr(0,line.find('#'));
	
		//match var=val[,val[,val[...]]]
		if(regex_match(line, args, emptyRe)) {
			// do nothing
		} else if(regex_match(line, args, varRe)) {
			cerr << "Declare Line: "  << line << endl;

			cerr << args.size() << endl;
			if(args.size() != 3) {
				cerr << "On line " << line << "\nError variables are of type "
					"name=val[,val[...]]\n";
				m_err = -1;
			}
			
			string varname = args[1];
			string value = args[2];

			auto ret = variables.insert(pair<string,list<string>>(varname,
						list<string>()));
				
			// Iterate Through Non-comman values
			cerr << "Adding: " << varname << "=" << endl;
			sregex_token_iterator regIt(value.cbegin(), value.cend(), commaRe);
			for(; regIt != regEnd ; ++regIt) {
				ret.first->second.push_back(*regIt);
			}

		} else if(regex_match(line, args, inputRe)) {
			// check if this is an initializing line (input files)
			cerr << "Input Line: (matches: " << args.size() << "\t" << line << "\t" << endl;
			for(unsigned int ii = 0 ; ii < args.size(); ii++) {
				cerr << args[ii] << endl;
			}
			cerr << endl;

		} else if(regex_match(line, args, cmdRe)) {
			// check if this is an initializing line (input files)
			cerr << "Command Line : (matches: " << args.size() << "\t" << line << "\t" << endl;
			for(unsigned int ii = 0 ; ii < args.size(); ii++) {
				cerr << args[ii] << endl;
			}
			cerr << endl;

		}

		std::getline(ifs,line);
		linenum++;
	}
	for(auto it1 = variables.begin(); it1 != variables.end(); it1++) {
		cerr << it1->first;
		for(auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
			cerr << "\t" << *it2 << endl;

		}
	}

}
