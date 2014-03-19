#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <regex>
#include <vector>
#include <map>
#include <cstdlib>

#include "ent.h"

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

Ent::Leaf::Leaf(string file, unsigned int line)
{
	m_placeholder = false;
	m_parseline = line;
	m_parsefile = file;
}

Ent::Leaf::Leaf()
{
	m_placeholder = true;
}

size_t 
Ent::Leaf::getNProc()
{
	return m_outputs.size();
}

/**********************************************************
 * Input 
 ***********************************************************/
Ent::Input::Input(string file, unsigned int line, string mixture,
		const vector<string>& filepatterns, const vector<string>& reshapes, 
		const Ent* parent) 
		: Leaf(file, line), m_parent(parent)
{
	if(mixtureToMetadata(mixture, parent) != 0) {
		throw "Error parsing";
	}

#ifndef NDEBUG
	for(auto v : m_outvars) {
		cerr << v << '\t';
	}
	cerr << endl;

	for(auto v : m_metadata) {
		for(auto v2 : v) {
			cerr << v2 << '\t';
		}
		cerr << endl;
	}
#endif //NDEBUG

	// todo use reshape specifiers
	for(unsigned int ii = 0 ; ii < reshapes.size(); ii++) {
		unsigned int which;
		for(int jj = 0 ; jj < m_outvars.size(); jj++) {
			if(m_outvars[jj] == reshapes[ii].substr(1)) {
				which = jj;
				break;
			}
		}

		if(reshapes[ii][0] == "%") {

		} else if(reshapes[ii][0] == "=") {
			
		}
	}

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
Ent::Input::mixtureToMetadata(string spec, const Ent* parent)
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
 * Ent
 ***********************************************************/

Ent::Ent(string filename) : m_err(0)
{
	cerr << "Parsing: " << endl;
	auto varRe = regex("\\s*([^:=\\s]*)\\s*=\\s*([^:=\\s]*)\\s*");
	auto commaRe = regex("(\\s*[^,]+\\s*)");
	auto inputRe = regex("\\s*([0-9.]*)\\s*:\\s*(?:input|i)\\s*:\\s*\\[(.*)\\]\\s*=(\\s*[^\\s]+\\s*)");
	auto cmdRe = regex("\\s*([0-9.]*)\\s*::\\s*\\[(.*)\\]\\s*=\\s*(.*)\\s*");
	auto emptyRe = regex("\\s*");
	const sregex_token_iterator regEnd;
	std::smatch args;

	Leaf* prevleaf = NULL;

	ifstream ifs(filename);
	string line;
	std::getline(ifs,line);
	int linenum = 0;
	while(!ifs.eof()) {

		// remove comments 
		line = line.substr(0,line.find('#'));
	
		/**********************************************
		 * Variable declaration: 
		 *		match var=val[,val[,val[...]]]
		 *********************************************/
		if(regex_match(line, args, emptyRe)) {
			// do nothing
		} else if(regex_match(line, args, varRe)) {
#ifndef NDEBUG
			cerr << "Declare:'"  << line << "'" << endl;
#endif// NDEBUG

			if(args.size() != 3) {
				cerr << "On line " << line << "\nError variables are of type "
					"name=val[,val[...]]\n";
				m_err = -1;
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
			 *		uint[.int[.int]]:s[ource]:[file[,file[,...]]] = ({prov}[*,{prov}[*,{prov}]])
			 *****************************************/
			// check if this is an initializing line (input files)
#ifndef NDEBUG
			cerr << "Input Line: " << line << "\t" << endl;
#endif // NDEBUG
			string sid = args[1]; //string id
			string fnames = args[2];
			string mixture = args[3];
			auto ret = m_leaves.insert(pair<vector<int>,shared_ptr<Leaf>>(
						getId(sid), NULL));
			if(ret.second == false && !ret.first->second->m_placeholder) {
				cerr << "Error: redeclaration of " << sid << " in " 
					<< filename << ":" << linenum+1 << endl;
			}

			// remove reshape commands
			vector<string> reshapes;
			std::regex reg("([%=])\\s*\\{([^{]*)\\}");
			while(std::regex_search(fnames, args, reg)) {
				reshapes.push_back(args[1].str()+args[2].str());
				fnames = args.prefix().str() + args.suffix().str();
			}

#ifndef NDEBUG
			cerr << "Reshape Specifiers: " << endl;
			for(auto vv : reshapes) 
				cerr << "\t" <<  vv << endl;
#endif//DEBUG

			// split out output files
			vector<string> filepatterns;
			sregex_token_iterator regIt(fnames.cbegin(), fnames.cend(), commaRe);
			for(; regIt != regEnd ; ++regIt) 
				filepatterns.push_back(*regIt);

			// create new leaf
			auto newleaf = shared_ptr<Input>(new Input(filename, linenum+1, 
						mixture, filepatterns, reshapes, this));


			prevleaf = newleaf.get();
		} else if(regex_match(line, args, cmdRe)) {
			/*****************************************
			 * Process Declaration
			 * 		match 
			 *		uint[.uint[...]]:p[roc]:[ftemp[,ftemp[...]]] = cmd {<inspec} {>outspec} {!<inspec} {!>outspec}
			 *****************************************/
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
	for(auto it1 = m_vars.begin(); it1 != m_vars.end(); it1++) {
		cerr << it1->first;
		for(auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
			cerr << "\t" << *it2 << endl;

		}
	}

}
