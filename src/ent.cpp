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

Ent::Ent(string filename)
{
	ifstream ifs(filename);

	string line;
	std::getline(ifs,line);
	while(!ifs.eof()) {

		// remove white space, comments 
		line = line.substr(line.find('#'));
		line.erase(remove_if(line.begin(), line.end(), isspace), line.end());
		
		std::cmatch args;
		if(regex_match(line, regex("([0-9.]*\\.x)::*\\[[^]*\\]*=(.*)"))) {
			// check if this is an initializing line (input files)
			cerr << "Input Line: (matches: " << args.size() << "\t" << line << "\t";
			for(unsigned int ii = 0 ; ii < args.size(); ii++) {
				cerr << "<" << args[ii] << ">";
				if(ii < args.size()-1) 
					cerr << ", <";
			}
			cerr << endl;

		} else if(regex_match(line, regex("([0-9.]*)::*\\[[^]*\\]*=(.*)"))) {
			// just a general processing line
			cerr << "Proc Line: (matches: " << args.size() << "\t" << line << "\t";
			for(unsigned int ii = 0 ; ii < args.size(); ii++) {
				cerr << "<" << args[ii] << ">";
				if(ii < args.size()-1) 
					cerr << ", <";
			}
			cerr << endl;
		}
	}

}
