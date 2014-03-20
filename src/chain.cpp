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
	const auto periodRe = regex("\\s*\\.\\s*");
	const sregex_token_iterator regEnd;
	
	// get the id for this line
	vector<int> id;

	// Iterate Through Non-comman values
	sregex_token_iterator regIt(in.cbegin(), in.cend(), periodRe, -1);
	for(; regIt != regEnd ; ++regIt) {
		id.push_back(atoi(regIt->str().c_str()));
	}

	return id;
}

string getSid(vector<int> in)
{
	std::ostringstream oss;
	for(unsigned int ii = 0; ii < in.size(); ii++) {
		oss << in[ii]; 
		if(ii+1 < in.size())
			oss << ".";
	}

	return oss.str();
}

template <typename C>
void printC(const C& in)
{
	for(auto it3 = in.begin(); it3 != in.end(); it3++) {
		cerr << *it3 << ",";
	}
}

template <typename C>
void printCC(const C& in)
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

//	// produce file list
//	std::smatch match; 
//	m_files.resize(m_metadata.size());
//	for(unsigned int ii = 0 ; ii < m_files.size(); ii++) {
//
//		m_files[ii].resize(filepatterns.size());
//
//		// for each file pattern, replace the variable name with
//		// variables from metadata
//		for(unsigned int ff = 0; ff < filepatterns.size(); ff++) {
//			m_files[ii][ff] = filepatterns[ff];
//			cerr << m_files[ii][ff] << " <-> ";
//
//			// for each variable, replace variable name with metadata
//			for(unsigned int vv = 0 ; vv < m_labels.size(); vv++) {
//				std::regex reg("\\{\\s*"+m_labels[vv]+"\\s*\\}");
//				while(std::regex_search(m_files[ii][ff], match, reg)) {
//					m_files[ii][ff] = match.prefix().str() + m_metadata[ii][vv] + 
//						match.suffix().str();
//				}
//			}
//			cerr << m_files[ii][ff] << endl;
//		}
//	}

int
Chain::Link::mixtureToMetadata(string spec, const Chain* parent)
{
#ifndef NDEBUG
	cerr << "Metavar: " << spec << endl;
#endif//NDEBUG
	string tmp = spec;

	int error;

	list<list<vector<vector<string>>>> stack;
	list<int> methods;
	m_labels.clear();

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
			m_labels.push_back(varname);

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
			printCC(stack.back().back());
#endif //NDEBUG
		} else {
			// literal
			int prev = ii;
			ii = spec.find_first_of("() {}",ii);
			m_labels.push_back("");

			string str = spec.substr(prev,ii-prev);

			// the inner vectors are just single until they get expanded
			stack.back().push_back(vector<vector<string>>());
			stack.back().back().resize(1);
			stack.back().back()[0].resize(1);
			stack.back().back()[0][0] = str;

#ifndef NDEBUG
			cerr << "Literal" << str << " becomes ";
			printCC(stack.back().back());
#endif //NDEBUG
		}

	}
#ifndef NDEBUG
	cerr << "Metadata expansion:" << endl;
	printCC(stack.back().back());
#endif //NDEBUG

	m_metadata = stack.back().back();
	return 0;
}

/**********************************************************
 * Link
 ***********************************************************/
Chain::Link::Link(std::string sourcefile, unsigned int line, NodeType type,
		std::string sid, std::string defsource, std::string outspec, 
		std::string inspec, Chain* parent) 
		: m_sourcefile(sourcefile), m_line(line), m_id(getId(sid)), 
		m_sid(getSid(m_id)), m_type(type), m_prevLink(defsource), m_parent(parent)

{
	const sregex_token_iterator regEnd;
	m_resolved = true;
	m_visited = false;

	m_err = 0;

	if(type == INPUT) {
		// parse inspec as metadata
		if(mixtureToMetadata(inspec, parent) != 0) {
			m_err = -1;
			return;
		}

		// parse outspec as files
		m_preinputs.clear();
//		m_preoutputs.clear();
		
		// split out output files, (but ignore commas inside {}
		auto commaRe = regex("\\s*,(?=[^}]*\\{)\\s*");
		sregex_token_iterator fnIt(outspec.cbegin(), outspec.cend(), commaRe, -1);
		for(; fnIt != regEnd ; ++fnIt) {
			m_preinputs.push_back(*fnIt);
//			m_preoutputs.push_back(*fnIt);
		}

		// parse outputs without resolving external references
		if(internalFileParse(m_preinputs) != 0) {
			m_err = -1;
			return;
		}

//		if(internalFileParse(m_preoutputs) != 0) {
//			m_err = -1;
//			return;
//		}
	}
}



/**
 * @brief This function parses and expands any resolvable  {* } variables
 * but ignores external references. It is like a lightweight version of
 * externalFileParse()
 *
 * @param files	filepattern list to expand
 *
 * @return error code
 */
int
Chain::Link::internalFileParse(list<string>& files)
{
	const std::sregex_iterator regEnd;
	std::regex expRe("\\{\\*\\s*([^{|]*)\\s*\\}");
	std::smatch args;
#ifndef NDEBUG
		cerr << "internalFileParse" << endl;
		printC(files);
		cerr << " <-> ";
#endif

	bool changed = false;
	// split out output files
	for(auto fit = files.begin(); fit != files.end(); ) {
		changed = false;

		// we have a token filename, now expand  {* } variables
		std::sregex_iterator expIt(fit->cbegin(), fit->cend(), expRe);
		for(; expIt != regEnd ; ++expIt) {
			// ignore external references
			if(expIt->str().find(':') != string::npos) {
				m_resolved = false;
				continue;
			}

			// resolve varaible references
			auto fret = m_parent->m_vars.find((*expIt)[1]);

			if(fret == m_parent->m_vars.end()) {
				cerr << "Unknown Reference '" << (*expIt)[1] << " in " 
						<< m_sourcefile << ":" << m_line << endl;
				return -1;
			}
			
			changed = true;
			// we are going to insert a lot of new elements, so save
			// the current iterator
			auto previt = fit;

			// get string that goes before and after match
			string before = expIt->prefix();
			string after = expIt->suffix();


			// insert new values after fit
			fit++;
			for(auto vit = fret->second.begin(); vit != fret->second.end(); vit++)
				files.insert(fit, before + (*vit) + after);

			fit = files.erase(previt);
		}

		if(!changed)
			fit++;
	}
#ifndef NDEBUG
			printC(files);
			cerr << endl;
#endif
	return 0;
}

/**
 * @brief This function parses and expands any resolvable  {* } variables
 * by recursing into external references. 
 *
 * @param files	filepattern list to expand
 *
 * @return error code
 */
int Chain::Link::resolveExternal(list<string>& stack)
{
	// push ourselves onto the call stack
	stack.push_back(m_sid);
	
	const std::sregex_iterator regEnd;
	const std::regex expRe("\\{\\*"
			"(?:\\s*([0-9.]*)\\s*:)?"
			"(?:\\s*([0-9,\\-]*)\\s*)?"
			"(?:\\s*\\|([^}]*))?\\}");
	const std::regex rangeRe("(?:([0-9]+)\\s*-\\s*([0-9]+)|([0-9]+))");

	if(m_visited) {
		cerr << "Revisited a link, appears to be a circular dependence" << endl;
		return -1;
	}

	m_visited = true;
	
	auto& files = m_preinputs;

//	std::regex expRe("\\{\\*\\s*([0-9\\.]*)\\s*:?\\s*([0-9,-]*)\\s*\\|?\\}");

#ifndef NDEBUG
		cerr << "externalFileParse" << endl;
		printC(files);
		cerr << endl;
#endif

	bool changed = false;
	// split out output files
	for(auto fit = files.begin(); fit != files.end(); ) {
		changed = false;
		cerr << *fit << endl;

		// we have a token filename, now resolve {* } variables
		std::sregex_iterator expIt(fit->cbegin(), fit->cend(), expRe);
		for(; expIt != regEnd ; ++expIt) {
	
			string sid = (*expIt)[1];
			if(sid.empty()) 
				sid = m_prevLink;

			vector<int> id = getId(sid);
            string args = (*expIt)[2];
            string vargs = (*expIt)[3];

#ifndef NDEBUG
			cerr << "Source      Node: " << sid << endl;
			cerr << "Output Number(s): " << args << endl;
			cerr << "Keeping Constant: " << vargs << endl;
#endif

			// resolve link reference
			auto fret = m_parent->m_links.find(id);

			if(fret == m_parent->m_links.end()) {
				cerr << "Unknown Reference '" << (*expIt)[0] << "' in " 
						<< m_sourcefile << ":" << m_line << endl;
				return -1;
			}
	
			if(!fret->second->m_resolved) {
				if(!fret->second->m_visited) {
					// we haven't visited the node that has our data,
					// so go ahead and visit it.
					if(fret->second->resolveExternal(stack) != 0) {
						return -1;
					}
				} else {
					// found a loop, so fail
					return -2;
				}
			}

			// get arg list
			std::list<int> argnumbers;
			std::sregex_iterator rangeIt(args.cbegin(), args.cend(), rangeRe);
			for(; rangeIt != regEnd; ++rangeIt) {
				cerr << "Number of matches: " << rangeIt->size() << endl;
				for(unsigned int ii = 0 ; ii < rangeIt->size(); ii++) {
					cerr << (*rangeIt)[ii] << endl;
				}

				if(rangeIt->size() == 3) {
					int a = atoi((*rangeIt)[1].str().c_str());
					int b = atoi((*rangeIt)[2].str().c_str());
					if(a > b) {
						cerr << "Invalid range: " << (*rangeIt)[0] << endl;
						return -1;
					}
					for(int ii = a; ii <= b; ii++)
						argnumbers.push_back(ii);

				} else if(rangeIt->size() == 2) {
					argnumbers.push_back(atoi((*rangeIt)[1].str().c_str()));
				}
			}
#ifndef NDEBUG
			cerr << "Range resolved to: ";
			printC(argnumbers);
			cerr << endl;
#endif

			// split up constant terms
			
			// we have now resolved the current input, so we can use the 
			// metadata to figure out which inputs to get


//			changed = true;
//			// we are going to insert a lot of new elements, so save
//			// the current iterator
//			auto previt = fit;
//
//			// get string that goes before and after match
//			string before = expIt->prefix();
//			string after = expIt->suffix();
//
//
//			// insert new values after fit
//			fit++;
//			for(auto vit = fret->second.begin(); vit != fret->second.end(); vit++)
//				files.insert(fit, before + (*vit) + after);
//
//			fit = files.erase(previt);
		}

		if(!changed)
			fit++;
	}
#ifndef NDEBUG
			printC(files);
			cerr << endl;
#endif

	m_resolved = true;
	return 0;

	// remove outself from the call stack
	stack.pop_back();

	return 0;
}

/**********************************************************
 * Chain
 ***********************************************************/

/**
 * @brief Parsing works as follows
 
 * Read all input, creating nodes without resolving any {} variables
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
	vector<int> curleaf; 

	string line;
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
					<< filename << ":" << linenum << endl;
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

			/*
			 * Handle ids
			 */
			if(args[1].str().empty() && prevleaf.empty()) {
				cerr << "Error, first Link MUST have a identifier: "
					"(ie 0, 0.1.0, any set of positive numbers will do) " << endl;
				m_err = -1;
				return;
			}

			// just set previous leaf to current leaf
			if(prevleaf.empty()) {
				prevleaf = getId(args[1]);
			}

			// if current leaf was left blank, just default to the next value
			// from prevleaf
			if(args[1].str().empty()) {
				curleaf = prevleaf;
				curleaf.back()++;
			} else {
				curleaf = getId(args[1].str());
			}

			/*
			 * create new data structure
			 */
			auto newleaf = shared_ptr<Link>(new Link(filename, linenum, 
						Link::INPUT, getSid(curleaf), getSid(prevleaf), 
						args[2], args[3], this));

			auto ret = m_links.insert(pair<vector<int>,shared_ptr<Link>>(
						newleaf->m_id, newleaf));
			if(ret.second == false) {
				cerr << "Error: redeclaration of " << newleaf->m_sid << " in " 
					<< filename << ":" << linenum << endl;
				m_err = -1;
				return;
			}
//		} else if(regex_match(line, args, cmdRe)) {
//			/*****************************************
//			 * Process Declaration
//			 * 		match 
//			 *		uint[.uint[...]]:p[roc]:[ftemp[,ftemp[...]]] = 
//			 *				cmd {<inspec} {>outspec} {!<inspec} {!>outspec}
//			 *****************************************/
//			// check if this is an initializing line (input files)
//			
//			auto newleaf = shared_ptr<Proc>(new Input(filename, linenum, 
//						args[1], args[2], args[3], this));
//
//			auto ret = m_proc.insert(pair<vector<int>,shared_ptr<Proc>>(
//						getId(sid), newleaf));
//			if(ret.second == false) {
//				cerr << "Error: redeclaration of " << sid << " in " 
//					<< filename << ":" << linenum << endl;
//				m_err = -1;
//				return;
//			}
		}

		std::getline(ifs,line);
	}

	list<string> stack;
	for(auto it = m_links.begin(); it != m_links.end(); it++) {

		// prepare for traversal
		for(auto vv : m_links) 
			vv.second->m_visited = false;

		// stack is just for debugging purposes of the user
		stack.clear();

		// resolve external references in inputs of current link 
		// by resolving parents references. Fails if a cycles is
		// detected. (ie traversal tries to revisit a node)
		if(it->second->resolveExternal(stack) != 0) {
			cerr << "Error resolving inputs for " << it->second->m_sid << " from "
				<< it->second->m_sourcefile << ":" << it->second->m_line << endl;
			m_err = -1;

			cerr << "Offending chain:" << endl;
			for(auto vv : stack) {
				cerr << vv << " <- ";
			}
			cerr << endl;
			return ;
		}
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
