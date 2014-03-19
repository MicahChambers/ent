#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <regex>
#include <vector>
#include <map>
#include <cstdlib>

#include "chain.h"

using std::endl;
using std::cerr;
using std::string;
using std::ifstream;
using std::regex;
using std::regex_match;
using std::sregex_token_iterator;

using std::shared_ptr;

using std::map;
using std::list;
using std::pair;
using std::vector;

#define OFF

/**********************************************************
 * Helper Functions
 ***********************************************************/
vector<int> getId(string in)
{
	const auto periodRe = regex("\\s*([0-9]+)\\s*\\.\\s*");
	const sregex_token_iterator regEnd;
	
	// get the id for this line
	vector<int> id;

	// Iterate Through Non-comman values
	sregex_token_iterator regIt(in.cbegin(), in.cend(), periodRe);
	for(; regIt != regEnd ; ++regIt) {
		id.push_back(atoi(regIt->str().c_str()));
	}

	return id;
}

void printVV(const vector<vector<string>>& in)
{
	for(auto it2 = in.begin(); it2 != in.end(); it2++) {
		cerr << "(";
		for(auto it3 = it2->begin(); it3 != it2->end(); it3++) {
			cerr << *it3 << ",";
		}
		cerr << "),";
	}
	cerr << endl;
}

/* 
 * Zips together two vectors, the members of individual elements are
 * concatinated.
 */
vector<vector<string>> zip(const vector<vector<string>>& a, 
		const vector<vector<string>>& b, int& error)
{
	if(a.empty()) {
		return vector<vector<string>>(b);
	}
	if(b.empty()) {
		return vector<vector<string>>(a);
	}

	if(a.size() != b.size()) {
		cerr << "Error during zipping. Input vectors must be the same size" 
			<< endl;
		error = 1;
		return vector<vector<string>>();
	}

	vector<vector<string>> out(a.size());
	for(unsigned int ii = 0 ; ii < a.size(); ii++) {
		out[ii].resize(a[ii].size()+b[ii].size());

		for(unsigned int kk = 0 ; kk < a[ii].size(); kk++)
			out[ii][kk] = a[ii][kk];

		for(unsigned int kk = 0 ; kk < b[ii].size(); kk++)
			out[ii][a[ii].size() + kk] = b[ii][kk];
	}

	error = 0;
	return out;
}

/* 
 * Nests together two vectors of vectors, expanding every possible combination of the
 * outer vectors. The inner vectors are concatinated in the process
 */
vector<vector<string>> nest(const vector<vector<string>>& a, 
		const vector<vector<string>>& b)
{
	if(a.empty()) {
		return vector<vector<string>>(b);
	}
	if(b.empty()) {
		return vector<vector<string>>(a);
	}

	vector<vector<string>> out(a.size()*b.size());
	for(unsigned int ii = 0 ; ii < a.size(); ii++) {
		for(unsigned int jj = 0 ; jj < b.size(); jj++) {
			out[ii*b.size() + jj].resize(a[ii].size()+b[jj].size());

			for(unsigned int kk = 0 ; kk < a[ii].size(); kk++)
				out[ii*b.size() + jj][kk] = a[ii][kk];
			
			for(unsigned int kk = 0 ; kk < b[jj].size(); kk++)
				out[ii*b.size() + jj][a[ii].size() + kk] = b[jj][kk];
		}
	}

	return out;
}


/**********************************************************
 * Leaf 
 ***********************************************************/

Chain::Leaf::Leaf(string file, unsigned int line, string sid)
{
	m_placeholder = false;
	m_parseline = line;
	m_parsefile = file;
	m_id = getId(sid);
}

Chain::Leaf::Leaf()
{
	m_placeholder = true;
}

size_t 
Chain::Leaf::getNProc()
{
	return m_outputs.size();
}

/**********************************************************
 * Proc
 ***********************************************************/
Chain::Proc::Proc(string file, unsigned int line, string sid, 
		string filepatterns, string cmd, const Chain* parent) 
		: Leaf(file, line, sid), 
		m_unresolved(false), m_parent(parent)
{
	// parse file patterns, they shouldn't contain variables
	
	// parse command line, there may be unresolved inputs
	
}

/**********************************************************
 * Input 
 ***********************************************************/

/**
 * @brief Parses a single input variable variable {base} or {0.1:5-10}
 * or {*base} or {*0.1:4|base}
 *
 * @param varname 	variable string to parse
 * @param patterns	output patterns, expanded by {* } as necessary
 *
 * @return 
 */
int Chain::Input::parseVar(string varname, vector<string>& patterns)
{
// need a lookup table of eventual files, resolved or not
}

int
Chain::Input::parseFiles(string fnames)
{
	auto commaRe = regex("(\\s*[^,]+\\s*)");
	std::regex varRe("\\{\\*\\s([^{]*)\\s\\}");
	std::smatch args;
	int finit;	// initial location in file array
	vector<string> filepatterns;

	// split out output files
	sregex_token_iterator fnIt(fnames.cbegin(), fnames.cend(), commaRe);
	for(; fnIt != regEnd ; ++fnIt) {

		parseVar(
		// initial point to expand from, if necessary from {* }
		finit = filepatterns.size();
		filepatterns.push_back(*fnIt);

		// we have a token filename, now expand  {* } variables
		sregex_token_iterator expIt(fnIt->cbegin(), fnIt->cend(), varRe);
		for(; expIt != regEnd ; ++expIt) {
			auto fret = parent->m_vars.find(*expIt);
			if(fret == parent->m_vars.end()) {
				cerr << "Unknown Reference '" << *expIt << " in " 
						<< m_parsefile << ":" << m_parseline << endl;
				return -1;
			}

			// [{base}/]
			// base=1,2
			int sz = filepatterns.size()-finit;
			filepatterns.resize(filepatterns.size()+sz*fret.second->size());

			//  [{base}/,undefined]
			//0 [{base}/,undefined]
			//0 [1/,undefined]
			for(unsigned int ii = finit; ii < sz; ii++) {
				for(auto it = fret.second->begin(); it != fret.second->end();
							it++) {
					string before = fnInit->str().substr(0,expIt.first);
					string after = fnInit->str().substr(expIt.second);
					filepatterns[ii] = before + (*it) + after;
				}

			}
		}
	}
	return 0;
}

Chain::Input::Input(string file, unsigned int line, string sid, 
		string filepatterns, string mixture, const Chain* parent) 
		: Leaf(file, line, sid), m_filepatterns(filepatterns),
		m_mixture(mixture), m_unresolved(false), m_parent(parent)
{
	// todo determine number of arguments
	// todo determine procs
	int parseResult = parseFiles(filepatterns);

	if(parseResult == 1) {
		// unresolved identifiers, delay
		m_unresolved = true;
		return;
	} else if(parseResult < 0) {
		// error
		cerr << "Error Parsing File" << file << ":" << line << endl;
		m_err = -1;
		return;
	}

	// proceed with converting mixture
	if(mixtureToMetadata(mixture, parent) != 0) {
		cerr << "Error Parsing Mixture" << file << ":" << line << endl;
		m_err(-1);
		return;
	}

#ifndef NDEBUG
	for(auto v : m_outvars) 
		cerr << v << '\t';
	cerr << endl;
	for(auto v : m_metadata) {
		for(auto v2 : v) 
			cerr << v2 << '\t';
		cerr << endl;
	}
#endif //NDEBUG

	// produce file list
	std::smatch match; 
	m_files.resize(m_metadata.size());
	for(unsigned int ii = 0 ; ii < m_files.size(); ii++) {

		m_files[ii].resize(filepatterns.size());

		// for each file pattern, replace the variable name with
		// variables from metadata
		for(unsigned int ff = 0; ff < filepatterns.size(); ff++) {
			m_files[ii][ff] = filepatterns[ff];
			cerr << m_files[ii][ff] << " <-> ";

			// for each variable, replace variable name with metadata
			for(unsigned int vv = 0 ; vv < m_outvars.size(); vv++) {
				std::regex reg("\\{\\s*"+m_outvars[vv]+"\\s*\\}");
				while(std::regex_search(m_files[ii][ff], match, reg)) {
					m_files[ii][ff] = match.prefix().str() + m_metadata[ii][vv] + 
						match.suffix().str();
				}
			}
			cerr << m_files[ii][ff] << endl;
		}
	}
}

int
Chain::Input::mixtureToMetadata(string spec, const Chain* parent)
{
#ifndef NDEBUG
	cerr << "Metavar: " << spec << endl;
#endif//NDEBUG
	string tmp = spec;

	int error;

	list<list<vector<vector<string>>>> stack;
	list<int> methods;
	m_outvars.clear();

	// innermost list: a variant of the inputs [0, 1, file]
	// next list : expanded set of variants [[0,1,file],[0,1,file2]], 
	// next list : unmerged list of expanded variants, e,g {A} and {B} in {A}*{B}
	// top list : stack of different depths
	//
	// stack[0] = list of all expanded varaibles at depth 0
	// stack[1] = list of all expanded variables at depth 1...
	// stack[0][0] = set in need of expansion, level 0
	// stack[0][1] = set in need of expansion, level 0,
	// stack[0][0][0] = one variant of expanded variables at level 0
	// stack[0][1][1] = one variant of expanded variables at level 0
	//
	// When we leave a level (ie close a parenthesis), all the 
	// members of the list at that level will get merged:
	// ({A}*{B}*C), then the result gets added to the level up,
	// for expansion when that level gets merged
	
	// make the top layer, everything else gets merged into it eventually
	stack.push_back(list<vector<vector<string>>>());
	for(unsigned int ii = 0 ; ii < spec.length(); ii++) {
		if(spec[ii] == '(') {
			// we are going down 
			methods.push_back(0);
			stack.push_back(list<vector<vector<string>>>());
		} else if(spec[ii] == ')') {
			// we are going up, split up previous level
			
			// it points to the layer above the current
			list<list<vector<vector<string>>>>::iterator lit = stack.end();
			lit--; lit--;
			
			if(stack.back().size() == 1) {
				// only 1 element, add it to second to above list
				lit->push_back(stack.back().back());
			} else if(methods.back() == 1) {
				// we need to merge together every element in the 
				// current level, we will accumulate them in merged
				vector<vector<string>> merged;

				while(!stack.back().empty()) {
					merged = nest(merged, stack.back().front());
					stack.back().pop_front();
				}
				
				// finally place the merged metadata on the end of the 
				// list in the above level, to be merged there
				lit->push_back(merged);
			} else if(methods.back() == 2) {
				vector<vector<string>> merged;
				
				while(!stack.back().empty()) {
					merged = zip(merged, stack.back().front(), error);
					stack.back().pop_front();
				}

				// zip can fail if the inputs aren't the same size. We make
				// an exception for size 0
				if(error) 
					return -1;
				
				// finally place the merged metadata on the end of the 
				// list in the above level, to be merged there
				lit->push_back(merged);
			}

			// Done on Current level, remove the metadata for this level
			stack.pop_back();
			methods.pop_back();
		} else if(spec[ii] == '*') {
			if(methods.back() != 0 && methods.back() != 1) {
				cerr << "Error, conflicting merge methods '*' vs. ',' given" << endl;
				return -1;
			}
			methods.back() = 1;
		} else if(spec[ii] == ',') {
			if(methods.back() != 0 && methods.back() != 2) {
				cerr << "Error, conflicting merge methods '*' vs. ',' given" << endl;
				return -1;
			}
			methods.back() = 2;
		} else if(isspace(spec[ii])) {
			//ignore

		} else if(spec[ii] == '{') {
			// variable name
			int prev = ii;
			ii = spec.find_first_of('}',ii);
			string varname = spec.substr(prev+1,ii-prev-1);
			m_outvars.push_back(varname);

			auto searchresult = parent->m_vars.find(varname);
			if(searchresult == parent->m_vars.end()) {
				cerr << "Error in input, unknown variable name: " << varname
					<< " referenced " << endl;
				return -1;
			}

			// the inner vectors are just single until they get expanded
			stack.back().push_back(vector<vector<string>>());
			stack.back().back().resize(searchresult->second.size());
			auto it = searchresult->second.begin(); 
			for(int ii = 0; it != searchresult->second.end(); ii++) {
				stack.back().back()[ii].resize(1);
				stack.back().back()[ii][0] = *it;
				it++;
			}
#ifndef NDEBUG
			cerr << "Variable " << varname << " expanded to ";
			printVV(stack.back().back());
#endif //NDEBUG
		} else {
			// literal
			int prev = ii;
			ii = spec.find_first_of("() {}",ii);
			m_outvars.push_back("");

			string str = spec.substr(prev,ii-prev);

			// the inner vectors are just single until they get expanded
			stack.back().push_back(vector<vector<string>>());
			stack.back().back().resize(1);
			stack.back().back()[0].resize(1);
			stack.back().back()[0][0] = str;

#ifndef NDEBUG
			cerr << "Literal" << str << " becomes ";
			printVV(stack.back().back());
#endif //NDEBUG
		}

	}
#ifndef NDEBUG
	cerr << "Metadata expansion:" << endl;
	printVV(stack.back().back());
#endif //NDEBUG

	m_metadata = stack.back().back();
	return 0;
}

/**********************************************************
 * Chain
 ***********************************************************/

/**
 * @brief Parsing works as follows
 *
 * Read all input, creating nodes without resolving any {} variables
 *
 * Then afterward resolve {}
 *
 * First pass: create all nodes, resolve number of jobs and 
 * outputs for every node, create variables
 *
 * Second pass: Resolve file names, linkage
 *
 * @param filename
 */
Chain::Chain(string filename) : m_err(0)
{
	cerr << "Parsing: " << endl;
	auto varRe = regex("\\s*([^:=\\s]*)\\s*=\\s*([^:=\\s]*)\\s*");
	auto commaRe = regex("(\\s*[^,]+\\s*)");
	auto inputRe = regex("\\s*([0-9.]*)\\s*:\\s*(?:input|i)\\s*:\\s*\\[(.*)\\]\\s*=(\\s*[^\\s]+\\s*)");
	auto cmdRe = regex("\\s*([0-9.]*)\\s*:\\s*(?:proc|p)\\s*:\\s*\\[(.*)\\]\\s*=\\s*(.*)\\s*");
	auto emptyRe = regex("\\s*");
	const sregex_token_iterator regEnd;
	std::smatch args;

	vector<int> prevleaf;

	string line;
	int linenum = 0;
	ifstream ifs(filename);
	std::getline(ifs,line);
	for(int linenum = 1; !ifs.eof(); linenum++) {

		// remove comments 
		line = line.substr(0,line.find('#'));
	
		if(regex_match(line, args, emptyRe)) {
			// do nothing
		} else if(regex_match(line, args, varRe)) {
		/**********************************************
		 * Variable declaration: 
		 *		match var=val[,val[,val[...]]]
		 *********************************************/
#ifndef NDEBUG
			cerr << "Declare:'"  << line << "'" << endl;
#endif// NDEBUG

			if(args.size() != 3) {
				cerr << "On line " << line << "\nError variables are of type "
					"name=val[,val[...]]\n";
				m_err = -1;
				return;
			}
			
			string varname = args[1];
			string value = args[2];
			
#ifndef NDEBUG
			cerr << "'" << varname << "=" << value << "'" << endl;
#endif // NDEBUG
			auto ret = m_vars.insert(pair<string,list<string>>(varname,
						list<string>()));
			if(ret.second == false) {
				cerr << "Warning: redeclaration of " << varname << " in " 
					<< filename << ":" << linenum+1 << endl;
			}
				
			// Iterate Through Non-comma values
			sregex_token_iterator regIt(value.cbegin(), value.cend(), commaRe);
			for(; regIt != regEnd ; ++regIt) {
				ret.first->second.push_back(*regIt);
			}
		
		} else if(regex_match(line, args, inputRe)) {
			/*****************************************
			 * Input Declaration
			 * 		match 
			 *		uint[.int[.int]]:s[ource]:[file[,file[,...]]] = 
			 *				({prov}[*,{prov}[*,{prov}]])
			 *****************************************/

			// create new leaf
			auto newleaf = shared_ptr<Input>(new Input(filename, linenum+1, 
						args[1], args[2], args[3], this));

			auto ret = m_inputs.insert(pair<vector<int>,shared_ptr<Input>>(
						getId(sid), newleaf));
			if(ret.second == false) {
				cerr << "Error: redeclaration of " << sid << " in " 
					<< filename << ":" << linenum+1 << endl;
				m_err = -1;
				return;
			}
		} else if(regex_match(line, args, cmdRe)) {
			/*****************************************
			 * Process Declaration
			 * 		match 
			 *		uint[.uint[...]]:p[roc]:[ftemp[,ftemp[...]]] = 
			 *				cmd {<inspec} {>outspec} {!<inspec} {!>outspec}
			 *****************************************/
			// check if this is an initializing line (input files)
			
			auto newleaf = shared_ptr<Proc>(new Input(filename, linenum+1, 
						args[1], args[2], args[3], this));

			auto ret = m_proc.insert(pair<vector<int>,shared_ptr<Proc>>(
						getId(sid), newleaf));
			if(ret.second == false) {
				cerr << "Error: redeclaration of " << sid << " in " 
					<< filename << ":" << linenum+1 << endl;
				m_err = -1;
				return;
			}
		}

		std::getline(ifs,line);
		linenum++;
	}

	// resolve variables
	// todo
	//
	
	for(auto it1 = m_vars.begin(); it1 != m_vars.end(); it1++) {
		cerr << it1->first;
		for(auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
			cerr << "\t" << *it2 << endl;

		}
	}

}
