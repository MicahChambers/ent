#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <regex>
#include <vector>
#include <map>
#include <algorithm>
#include <cstdlib>

#include "chain.h"

using std::endl;
using std::cerr;
using std::string;
using std::ifstream;
using std::regex;
using std::regex_match;
using std::sregex_token_iterator;
using std::sregex_iterator;

using std::map;
using std::list;
using std::pair;
using std::vector;

#define VERBOSE

// 1 = expansion *
// 2 = everything inside except * 
// 3 = Dependency ID Number ie 1.2
// 4 = Argument Number ie 3-5,3
// 5 = Control ie | hello, this is a test
const std::regex CurlyRe("\\s*\\{(\\*)?\\s*((?:([0-9.]*)\\s*:)?"
		"(?:\\s*([-,_[:alnum:]]*)\\s*)?(?:\\s*"
		"\\|([,_\\s[:alnum:]]*))?)\\s*\\}\\s*");
const std::sregex_iterator ReEnd;


/**********************************************************
 * Helper Functions
 ***********************************************************/
vector<int> getId(string in)
{
	const auto periodRe = regex("\\s*\\.\\s*");
	const sregex_token_iterator tokEnd;
	
	// get the id for this line
	vector<int> id;

	// Iterate Through Non-comman values
	sregex_token_iterator regIt(in.cbegin(), in.cend(), periodRe, -1);
	for(; regIt != tokEnd ; ++regIt) {
		for(auto ival : regIt->str()){
			if(ival > '9' || ival < '0') 
				return vector<int>();
		}
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

string basename(string val)
{
	std::smatch match;
	std::regex pattern("([^/]*)(?!.*/)");
	std::regex_search(val, match, pattern);
	return match[1].str();
}

template <typename C>
void printC(const C& in)
{
	for(auto it3 = in.begin(); it3 != in.end(); it3++) {
		cerr << *it3;
		it3++;
		if(it3 != in.end()) 
			cerr << ",";
		it3--;
	}
}

template <typename C>
void printCC(const C& in)
{
	for(auto it2 = in.begin(); it2 != in.end(); it2++) {
		cerr << "(";
		for(auto it3 = it2->begin(); it3 != it2->end(); it3++) {
			cerr << *it3;
			if(it3+1 != it2->end()) 
				cerr << ",";
		}
		cerr << ")";
		if(it2+1 != in.end()) 
			cerr << ",";
	}
}


int csvToList(string in, list<string>& csv)
{
	const sregex_token_iterator tokEnd;
	regex commaRe("\\s*,\\s*(?![^{]*\\})\\s*");
	csv.clear();
	sregex_token_iterator regIt(in.cbegin(), in.cend(), commaRe, -1);
	for(; regIt != tokEnd ; ++regIt) {
		if(regIt->length() > 0) {
			csv.push_back(*regIt);
		}
	}
	return 0;
}

int csvToList(string in, list<int>& csv)
{
	const sregex_token_iterator tokEnd;
	regex commaRe("\\s*,\\s*(?![^{]*\\})\\s*");
	csv.clear();
	sregex_token_iterator regIt(in.cbegin(), in.cend(), commaRe, -1);
	for(; regIt != tokEnd ; ++regIt) {
		for(auto v : regIt->str()) {
			if(v > '9' || v < '0')
				return -1;
		}
		cerr << "pushing" << regIt->str() << endl;
		csv.push_back(atoi(regIt->str().c_str()));
	}
	return 0;
}

/**
 * @brief Performs an expansion of argument ranges in things 
 * like {*blah:3-5|what} or {1.1:3-9,0|subject}
 *
 * @param expression	Input expresion, output will
 * @param lout			List output
 * @param sout			String output
 *
 * @return Error status, negative is bad
 */
int argExpand(list<string>::iterator& exp, list<string>& lout)
{
	const std::regex expRe("(\\{\\*?"
			"(?:\\s*[0-9.]*\\s*:)?)"
			"(?:\\s*([a-zA-Z0-9,\\-]*)\\s*)?"
			"((?:\\s*\\|[^}]*)?\\})");
	const std::regex rangeRe("(?:([0-9]+)-([0-9]+)|([0-9]+))");
	
	std::smatch args;
	string prefix;
	string suffix;
	std::list<int> intlist;

	if(regex_search(*exp, args, expRe)) {
		prefix = args[1].str();
		suffix = args[3].str();

		string rterm = args[2].str();
		std::sregex_iterator it(rterm.cbegin(), rterm.cend(), rangeRe);
		for(; it != ReEnd; it++) {
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
					intlist.push_back(a);
			} else if(!single.empty()) {
				intlist.push_back(atoi(single.c_str()));
			}
		}
	} else {
		cerr << "Error, syntax not understood: " << *exp << endl;
		return -1;
	}

	// nothing to splice in
	if(intlist.empty()) 
		return 0;

	std::ostringstream oss; 
	auto init = exp;
	for(auto it = intlist.begin(); it != intlist.end(); it++) {
		oss.str("");
		oss << prefix << *it << suffix;
		lout.insert(init, oss.str());
		exp--;
	}
	lout.erase(init);
	return 0;
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


void 
Chain::Link::write(string str)
{
	cerr << m_id << ": " << str;
}


int
Chain::Link::mixtureToMetadata(string spec, const Chain* parent)
{
	// although we don't require () in the input file, it makes it 
	// easier to parse if we have parenthesis, instead of adding 
	// the ) exit to the end of the function
	spec = "(" + spec + ")";
#ifndef NDEBUG
	write("Metavar: " + spec + "\n");
#endif//NDEBUG
	string tmp = spec;

	list<list<MetaData>> stack;
	list<int> methods;
	int litc = 0;

	// next list : unmerged list of expanded variants, e,g {A} and {B} in {A}*{B}
	// top list : stack of different depths
	//
	// stack[0] = list of all expanded varaibles at depth 0
	// stack[1] = list of all expanded variables at depth 1...
	// stack[0][0] = set in need of expansion, level 0
	// stack[0][1] = set in need of expansion, level 0,
	//
	// When we leave a level (ie close a parenthesis), all the 
	// members of the list at that level will get merged:
	// ({A}*{B}*C), then the result gets added to the level up,
	// for expansion when that level gets merged

	// make the top layer, everything else gets merged into it eventually
	stack.push_back(list<MetaData>());
	for(unsigned int ii = 0 ; ii < spec.length(); ii++) {
		if(spec[ii] == '(') {
			// we are going down 
			methods.push_back(0);
			stack.push_back(list<MetaData>());
		} else if(spec[ii] == ')') {
			// we are going up, split up previous level
			// it points to the layer above the current
			list<list<MetaData>>::iterator lit = stack.end();
			lit--; lit--;

			if(stack.back().size() == 1) {
				// only 1 element, add it to second to above list
				lit->push_back(stack.back().back());
			} else if(methods.back() == 1) {
				// we need to merge together every element in the 
				// current level, we will accumulate them in merged
				MetaData merged;

				while(!stack.back().empty()) {
					if(merged.nest(stack.back().front()))
						return -1;
					stack.back().pop_front();
				}

				// finally place the merged metadata on the end of the 
				// list in the above level, to be merged there
				lit->push_back(merged);
			} else if(methods.back() == 2) {
				MetaData merged;

				while(!stack.back().empty()) {
					if(merged.zip(stack.back().front()) != 0) 
						return -1;
					stack.back().pop_front();
				}

				// finally place the merged metadata on the end of the 
				// list in the above level, to be merged there
				lit->push_back(merged);
			} else if(methods.back() == 3) {
				MetaData merged;

				while(!stack.back().empty()) {
					if(merged.ujoin(stack.back().front()) != 0) 
						return -1;
					stack.back().pop_front();
				}

				// finally place the merged metadata on the end of the 
				// list in the above level, to be merged there
				lit->push_back(merged);
			}

			// Done on Current level, remove the metadata for this level
			stack.pop_back();
			methods.pop_back();
		} else if(spec[ii] == '*') {
			if(methods.back() != 0 && methods.back() != 1) {
				cerr << "Error, conflicting merge methods '*' given" << endl;
				return -1;
			}
			methods.back() = 1;
		} else if(spec[ii] == ',') {
			if(methods.back() != 0 && methods.back() != 2) {
				cerr << "Error, conflicting merge methods ',' given" << endl;
				return -1;
			}
			methods.back() = 2;
		} else if(spec[ii] == 'u') {
			if(methods.back() != 0 && methods.back() != 3) {
				cerr << "Error, conflicting merge methods 'u' given" << endl;
				return -1;
			}
			methods.back() = 3;
		} else if(isspace(spec[ii])) {
			//ignore

		} else if(spec[ii] == '{') {
			// variable name
			int prev = ii;
			ii = spec.find_first_of('}',ii);
			string varname = spec.substr(prev+1,ii-prev-1);

			auto searchresult = parent->m_vars.find(varname);
			if(searchresult == parent->m_vars.end()) {
				cerr << "Error in input, unknown variable name: " << varname
					<< " referenced " << endl;
				return -1;
			}

			vector<string> tmp(searchresult->second.size());
			auto it = searchresult->second.begin(); 
			for(int ii = 0; it != searchresult->second.end(); ii++) {
				tmp[ii] = *it;
				it++;
			}

			stack.back().emplace(stack.back().end(),varname, tmp);

#ifndef NDEBUG
			cerr << "Variable " << varname << " expanded to " 
				<< stack.back().back() << endl;
#endif //NDEBUG
		} else {
			// literal, just a single word like ({hello})
			int prev = ii;
			ii = spec.find_first_of("() {}",ii);

			string str = spec.substr(prev,ii-prev);
			std::ostringstream oss;
			oss << "lit" << litc++ << endl;

			// the inner vectors are just single until they get expanded
			vector<string> tmp(1, str);
			stack.back().emplace(stack.back().end(), MetaData(oss.str(), tmp));
#ifndef NDEBUG
			cerr << "Literal" << str << " becomes " << stack.back().back() 
				<< endl;
#endif //NDEBUG
		}

	}

	m_metadata = stack.back().back();
	std::swap(m_metadata, stack.back().back());
	return 0;
}

/**********************************************************
 * Link
 ***********************************************************/
// Process Constructor
Chain::Link::Link(std::string sourcefile, unsigned int line, 
		std::string sid, std::string defsource, 
		std::string inspec, std::string outspec, std::string cmd, 
		Chain* parent)
: m_cmd(cmd), m_sourcefile(sourcefile), m_line(line), 
	m_id(sid), m_type(PROC), m_prevLink(defsource), m_parent(parent)
{
#ifndef NDEBUG
	cerr << "m_prevlink " << m_prevLink << endl;
#endif //NDEBUG
	const auto commaRe = regex("\\s*,(?![^{]*\\})\\s*");
	const sregex_token_iterator tokEnd;

	m_err = 0;
	m_resolved = false;
	m_visited = false;
	m_populated = false;

	// parse outspec as files
	m_preinputs.clear();
	m_preoutputs.clear();

	// split out input files, (but ignore commas inside {})
#ifndef NDEBUG
	cerr << "Initial Input Spec" << endl;
#endif //NDEBUG
	for(sregex_token_iterator fnIt(inspec.cbegin(), inspec.cend(), 
				commaRe, -1); fnIt != tokEnd ; ++fnIt) {
		m_preinputs.push_back(*fnIt);
#ifndef NDEBUG
		cerr << "\t" << *fnIt << endl;
#endif //NDEBUG
	}

//	// TODO move this further down the pipeline, so that people 
//	// can enter non-definite lengths {:10-}
//	// expand multi-argument expressions, and remove the pre-expanded version
//#ifndef NDEBUG
//	cerr << "Expanded Input Spec" << endl;
//#endif //NDEBUG
//	for(auto fit = m_preinputs.begin(); fit != m_preinputs.end(); fit++) {
//		argExpand(fit, m_preinputs);
//#ifndef NDEBUG
//		cerr << "\t" << *fit << endl;
//#endif
//	}

	// split out output files, (but ignore commas inside {})
	for(sregex_token_iterator fnIt(outspec.cbegin(), outspec.cend(), 
				commaRe, -1); fnIt != tokEnd ; ++fnIt) {
		m_preoutputs.push_back(*fnIt);
	}

#ifndef NDEBUG
	cerr << "Source file: " << m_sourcefile << endl;
	cerr << "ID: " << m_id << endl;
	cerr << "Line: " << m_line << endl;
	cerr << "Command: " << m_cmd << endl;
	cerr << "Default Source: " << m_prevLink << endl;
	cerr << "Inspec:  "; printC(m_preinputs); cerr << endl;
	cerr << "Output:  "; printC(m_preoutputs); cerr << endl;
	cerr << "Metadata Expansion:  " << m_metadata << endl;
#endif //NDEBUG

}


// Input Constructor
Chain::Link::Link(std::string sourcefile, unsigned int line, 
		std::string sid, std::string defsource, 
		std::string inspec, std::string mixture,
		Chain* parent)
		: m_cmd(""), m_sourcefile(sourcefile), m_line(line), m_id(sid), 
		m_type(INPUT), m_prevLink(defsource), m_parent(parent)

{
#ifndef NDEBUG
	cerr << "m_prevlink " << m_prevLink << endl;
#endif//ifndef NDEBUG
	const auto commaRe = regex("\\s*,(?![^{]*\\})\\s*");
	const sregex_token_iterator tokEnd;

	m_err = 0;
	m_resolved = false;
	m_visited = false;
	m_populated = false;

	// parse outspec as files
	m_preinputs.clear();
	m_preoutputs.clear();

	// expand metadata
	if(mixtureToMetadata(mixture, parent) != 0) {
		m_err = -1;
		return;
	}
	
	// 
	// split out input files, (but ignore commas inside {})
#ifndef NDEBUG
	cerr << "Initial Input Spec" << endl;
#endif//ifndef NDEBUG
	sregex_token_iterator fnIt(inspec.cbegin(), inspec.cend(), commaRe, -1);
	for(; fnIt != tokEnd ; ++fnIt) {
//		sregex_iterator expIt(fnIt->str().cbegin(), fnIt->str().cend(), CurlyRe);
//		for(; expIt !=  ReEnd; expIt++) {
//			auto varname = (*expIt)[2].str();
//			auto fit = m_parent->m_vars.find(varname);
//			if(fit == m_parent->m_vars.end()) {
//				cerr << "Error, unknown variable " << varname << " referenced "
//					"in input " << m_id << ". Input specs cannot reference "
//					"variables from other processes. " << sourcefile << ":" 
//					<< line << endl;
//				m_err = -1;
//				return;
//			}
//		}
		m_preinputs.push_back(*fnIt);
#ifndef NDEBUG
		cerr << "\t" << *fnIt << endl;
#endif//ifndef NDEBUG
	}

	// make preoutputs equal to preinputs basename
#ifndef NDEBUG
	cerr << "Preoutputs: " << endl;
#endif//NDEBUG
	for(auto fit = m_preinputs.begin(); fit != m_preinputs.end(); fit++) {
		m_preoutputs.push_back(basename(*fit));
#ifndef NDEBUG
		cerr << "\t" << m_preoutputs.back() << endl;
#endif//ifndef NDEBUG
	}

#ifndef NDEBUG
	cerr << "Source file: " << m_sourcefile << endl;
	cerr << "ID: " << m_id << endl;
	cerr << "Line: " << m_line << endl;
	cerr << "Command: " << m_cmd << endl;
	cerr << "Default Source: " << m_prevLink << endl;
	cerr << "Inspec:  "; printC(m_preinputs); cerr << endl;
	cerr << "Metadata Expansion:  " << m_metadata << endl;
#endif //NDEBUG

}


/**
 * @brief This function resolves all the inputs and output file names.
 *
 * The basic gist is 
 *
 * Helper Functions Follow
 *
 * @param callstack
 *
 * @return 
 */
int Chain::Link::resolveTree(list<string>& callstack)
{
	if(m_resolved) { 
		return 0;
	}

#ifndef NDEBUG
	write("resolveTree\n");
#endif //NDEBUG

	if(m_visited) {
		cerr << "Revisited a link, appears to be a circular dependence" << endl;
		return -1;
	}
	m_visited = true;
	
	// push ourselves onto the call stack
	callstack.push_back(m_id);

	// perform expansion so that we resolve metadata without worrying about it
	resolveExpandableGlobals();

	// merge in metadata from all dependencies 
	int ret = mergeExternalMetadata(callstack);
	if(ret < 0) {
		write("Error resolving inputs from \n");
		return -1;
	} else if (ret > 0) {
		write("Warning, cannot currently resolve inputs\n");
	}
// TODO really only the metadata matters because it is what
// we need to determine how many processes run. Everything else
// can be done right before running
//
	// resolve all global varianble references, including 
	// variables of variables of variables.... 
	ret = resolveNonExpandableGlobals();
	if(ret < 0) {
		cerr << "Error resolving global variabes for " << m_id 
			<< " from " << m_sourcefile << ":" << m_line << endl;
		return -1;
	}

	// sets all final inputs
	ret = resolveInputs(callstack);
	if(ret < 0) {
		cerr << "Error resolving inputs for " << m_id 
			<< " from " << m_sourcefile << ":" << m_line << endl;
		return -1;
	}
	
	ret = nameOutputs();

#ifndef NDEBUG
	write("RESOLVED!\n");
#endif //NDEBUG
	m_resolved = true;
	return 0;
}


int Chain::Link::nameOutputs()
{
	regex exp("\\{([^}]*)\\}");
	string otemp; // output template
	string basepath;
	string path;
	if(!m_parent->m_outdir.empty())
		otemp = m_parent->m_outdir;
	std::ostringstream oss;

	m_outputs.resize(m_inputs.size());
	for(size_t jj=0; jj<m_outputs.size(); jj++) {
		m_outputs[jj].resize(m_preoutputs.size());
		oss.str("");

		// for the job, get the basepath, either from OUTDIR or by using
		// all of the metadata fixed together
		if(!otemp.empty()) {
			basepath = otemp;
		} else {
			// no outdir, just set the metadata together
			for(size_t aa=0; aa<m_metadata.m_labels.size(); aa++) {
				oss << m_metadata.gets(jj, aa) << "/";
			}
			basepath = oss.str();
		}

		// add the the final name to the basepath and resolve any
		auto it = m_preoutputs.begin();
		for(size_t aa=0; it != m_preoutputs.end(); it++, aa++) {
#ifndef NDEBUG
			cerr << "Naming Output: " << *it << endl;
#endif //NDEBUG
			oss.str("");
			path = basepath + *it;
#ifndef NDEBUG
			cerr << path << endl;;
#endif //NDEBUG
			// resolve any variables in the path
			std::sregex_iterator expIt(path.cbegin(), path.cend(), exp);
			for(; expIt != ReEnd; expIt++) {
#ifndef NDEBUG
				cerr << (*expIt)[0] << " matched" << endl;
#endif //NDEBUG
				// find variable in metadata
				string val = m_metadata.finds(jj, (*expIt)[1]);
				oss << expIt->prefix() << val;
			}
			oss << expIt->suffix();
#ifndef NDEBUG
			cerr << otemp << *it << " -> " << oss.str() << endl;
#endif //NDEBUG
			m_outputs[jj][aa] = oss.str();
		}
	}

	return 0;
}

/**
 * @brief Before: m_inputs, m_preinputs, m_metadata could be subject to 
 * change by parent links, after m_metadata is in its final form.
 *
 * @param call stack 
 *
 * @return error code
 */
int Chain::Link::resolveExpandableGlobals()
{
#ifndef NDEBUG
	write("resolveExpandableGlobals\n");
#endif //NDEBUG
	
	if(m_resolved)
		return 0;
	
	std::smatch args;
	list<std::string> inExp;

	// If we expand a variable, we need to go back to the very beginning
	bool restart = true;
	while(restart) {
		restart = false;

		// for input term
		for(auto fit = m_preinputs.begin(); !restart && 
						fit != m_preinputs.end(); fit++) {

			// for each variable like "{subject}" in {subject}/{run}
			std::sregex_iterator expIt(fit->cbegin(), fit->cend(), CurlyRe);
			for(; !restart && expIt != ReEnd ; ++expIt) {
				
				// ignore non-expansion variables
				if((*expIt)[1] != "*") 
					continue;

				// ignore non-global variables (ie references)
				auto git = m_parent->m_vars.find((*expIt)[2]);
				if(git == m_parent->m_vars.end()) {
					continue;
				}

				string prefix = fit->substr(0,expIt->position());
				string suffix = expIt->suffix();

#ifndef NDEBUG
				cerr << "Expanding: " << (*expIt)[0].str() << " in " 
					<< *fit << endl;
#endif //NDEBUG
				cerr << git->first << endl;
				for(auto lit = git->second.begin(); lit != git->second.end(); 
								lit++) {
					string expanded = prefix + *lit + suffix;
#ifndef NDEBUG
					cerr << "\t" << expanded << endl; 
#endif //NDEBUG
					inExp.push_back(expanded);
				}

				restart = true;
				fit = m_preinputs.erase(fit);

				m_preinputs.splice(fit, inExp);
			}
		}
	}

	return 0;
}

/**
 * @brief Merges metadata of all input dependencies, does not affect
 * m_inputs or m_preinputs, just m_metadata
 *
 * @param call stack 
 *
 * @return error code
 */
int Chain::Link::mergeExternalMetadata(list<string>& callstack)
{
#ifndef NDEBUG
	write("mergeExternalMetadata");
	write("Initial Metadata:\n");
	cerr << m_metadata << endl; 
#endif //NDEBUG
	
	if(m_resolved)
		return 0;
	
	const std::sregex_iterator ReEnd;

	// resolve inputs
	for(auto fit = m_preinputs.begin(); fit != m_preinputs.end(); fit++) {
#ifndef NDEBUG
		cerr << "Input spec: " << *fit << endl;
#endif//ifndef NDEBUG
		std::sregex_iterator expIt(fit->cbegin(), fit->cend(), CurlyRe);
		for(; expIt != ReEnd; ++expIt) {
			
#ifndef NDEBUG
			cerr << "Prefix " << expIt->prefix().str() << endl;
			cerr << "Match " << (*expIt)[0].str() << endl;
			cerr << "Expand?" << (*expIt)[1].str() << endl;
			cerr << "Variable : " << (*expIt)[2].str() << endl;
			cerr << "Dep Number:\"" << (*expIt)[3].str() << '"'<< endl;
			cerr << "Var/Number: " << (*expIt)[4].str() << endl;
			cerr << "Control: " << (*expIt)[5].str() << endl;
			cerr << "Suffix " << expIt->suffix().str() << endl;
#endif//ifndef NDEBUG

			// ignore global variables for now 
			auto gvalit = m_parent->m_vars.find((*expIt)[2]);
			if(gvalit != m_parent->m_vars.end()) {
				continue;
			}
			
			if(!expIt->prefix().str().empty() || !expIt->suffix().str().empty()) {
				write("Parsing error. References to output of other "
					"jobs cannot include literals. ie. /ifs/{1.2:3} is"
					"not valid. Add /ifs to the parent job instead!\n");
				return -1;
			}

			string depnum;
			if((*expIt)[3].str().empty())
				depnum = m_prevLink;
			else
				depnum = (*expIt)[3].str();

			auto linkit = m_parent->m_links.find(depnum);
			if(linkit != m_parent->m_links.end()) {
#ifndef NDEBUG
				write("External Reference Var! (" + depnum + ")\n");
#endif//ifndef NDEBUG
			} else {
#ifndef NDEBUG
				for(auto vv:m_parent->m_links) {
					write("\"" + vv.first + '"' );
				}
				write("Not External Reference, nothing to merge\n");
#endif //NDEBUG
				continue;
			}
	
			// make sure the inputs' metadta and outputs are up to date
			linkit->second->resolveTree(callstack);

			//merge metadata
			list<string> control;
			if(csvToList((*expIt)[5].str(), control) != 0) 
				return -1;

			// split produces a version of the metadata where the 
			// expanded columns are elminated
			
			// array of list of rows that run together, for each output
			// row, it gives the input rows that matched/were brought together
			auto reduce = linkit->second->m_metadata.split(control);
			if(m_metadata.ujoin(*reduce) < 0) {
				write("Error resolving inputs/metadata\n");
				return -1;
			}
		}
	}
	
#ifndef NDEBUG
	write("Final Metadata:\n");
	cerr << m_metadata << endl;
#endif //NDEBUG
	return 0;
}

/**
 * @brief This function parses expands or replaces any resolvable  {GLOBAL} 
 * variables that do not have valid metadata in the current job
 *
 * @param 
 *
 * @return error code
 */
int
Chain::Link::resolveNonExpandableGlobals()
{
	std::smatch args;

#ifndef NDEBUG
		write("resolveNonExpandableGlobals\n");
		printC(m_preinputs);
		cerr << endl;
#endif //NDEBUG
	
	if(m_resolved)
		return 0;

	assert(m_metadata.m_rows > 0);
	list<std::string> inExp;
	bool restart = true;

	while(restart) {
		restart = false;

		// for input term
		for(auto fit = m_preinputs.begin(); !restart && 
						fit != m_preinputs.end(); fit++) {

			// for each variable like "{subject}" in {subject}/{run}
			std::sregex_iterator expIt(fit->cbegin(), fit->cend(), CurlyRe);
			for(; !restart && expIt != ReEnd ; ++expIt) {

#ifndef NDEBUG
			cerr << "Prefix " << expIt->prefix().str() << endl;
			cerr << "Match " << (*expIt)[0].str() << endl;
			cerr << "Expand?" << (*expIt)[1].str() << endl;
			cerr << "Variables: " << (*expIt)[2].str() << endl;
			cerr << "Dep Number: " << (*expIt)[3].str() << endl;
			cerr << "Var/Number: " << (*expIt)[4].str() << endl;
			cerr << "Control: " << (*expIt)[5].str() << endl;
			cerr << "Suffix " << expIt->suffix().str() << endl;
#endif //NDEBUG

				// ignore global variables that we have metadata for
				auto mit = std::find(m_metadata.m_labels.begin(), 
							m_metadata.m_labels.end(), (*expIt)[2]);
				if(mit != m_metadata.m_labels.end()) {
					continue;
				}

				// ignore non-global variables (ie references)
				auto git = m_parent->m_vars.find((*expIt)[2]);
				if(git == m_parent->m_vars.end()) {
					continue;
				}

				if((*expIt)[1] == "*") {
					cerr << "Expandable globals should have been taken care of"
						" in resolveExpandableGlobals" << endl;
					return -1;
				}

				string prefix = fit->substr(0,expIt->position());
				string suffix = expIt->suffix();

				// perform replacement replace
				if(git->second.size() > 1) {
					// VAR={prefix}/{hello}
					// {prefix} = a,b,c
					// VAR=a/{hello},b/{hello},c/{hello}
					// error, not sure how to handle this
					cerr << "Error, array passed through " << (*expIt)[0]
						<< " but no expansion strategy given to the "
						"process, try adding " << (*expIt)[2] << " to the "
						"METEXPANSION term of the most recently attacked "
						"input line" << endl;
					return -1;
				}
				string newval = prefix + git->second.front() + suffix;
#ifndef NDEBUG
				cerr << "Resolving: " << (*expIt)[0].str() << " in " 
					<< *fit << " to " << newval << endl;
#endif //NDEBUG
				*fit = newval;
				restart = true; // need to resolve any terms inside 
			}
		}
	}
	return 0;
}

/**
 * @brief This function iterates through the inspec list and 
 * finalizes the m_inputs array.
 *
 * @param callstack
 *
 * @return 
 */
int Chain::Link::resolveInputs(list<string>& callstack)
{

	std::smatch match;

#ifndef NDEBUG
	write("resolveInputs");
	printC(m_preinputs);
	cerr << endl;
#endif //NDEBUG
	
	if(m_resolved)
		return 0;
	
	assert(m_metadata.m_rows > 0);

	InputT tmp;
	m_inputs.resize(m_metadata.m_rows);
	std::ostringstream oss;
	string suffix = "";
	
	// for each preinput
	for(auto fit = m_preinputs.begin(); fit != m_preinputs.end(); fit++) {
#ifndef NDEBUG
		cerr << "Input spec: " << *fit << endl;
#endif //NDEBUG

		if(regex_match(*fit, match, CurlyRe)) {
			// single argument

#ifndef NDEBUG
			cerr << "\tPrefix " << match.prefix().str() << endl;
			cerr << "\tMatch " << match[0].str() << endl;
			cerr << "\tExpand?" << match[1].str() << endl;
			cerr << "\tVarname: " << match[2].str() << endl;
			cerr << "\tDep Number: " << match[3].str() << endl;
			cerr << "\tVar/Number: " << match[4].str() << endl;
			cerr << "\tControl: " << match[5].str() << endl;
			cerr << "\tSuffix " << match.suffix().str() << endl;
#endif //NDEBUG
			int argnum = atoi(match[4].str().c_str());
			string varname = match[2].str();
			// if it is in m_metadata, then just fill in the value
			// and create the argument
			auto localit = std::find(m_metadata.m_labels.begin(), 
						m_metadata.m_labels.end(), varname);
			if(localit != m_metadata.m_labels.end()) {
				size_t col = (localit-m_metadata.m_labels.begin());
				for(size_t jj=0; jj<m_metadata.m_rows; jj++) {
					m_inputs[jj].push_back(InputT(m_metadata.gets(col, jj)));
				}
			} else { 
				string depnum;
				if(match[3].str().empty())
					depnum = m_prevLink;
				else
					depnum = match[3].str();
				auto linkit = m_parent->m_links.find(depnum);
				if(linkit != m_parent->m_links.end()) {
#ifndef NDEBUG
					cerr << "External Reference Var!" << endl;
#endif//ifndef NDEBUG
				} else {
					cerr << "Not sure what " << match[0] << " is " << endl;
					return -1;
				}
				linkit->second->resolveTree(callstack);

				// contstruct a mapping between outmetadata and 
				for(size_t jj=0; jj<m_metadata.m_rows; jj++) {
					vector<string> vals(m_metadata.m_cols);
					list<int> matchrows;
					// fill pairs to search for variables in metadata
					for(size_t kk=0; kk<m_metadata.m_cols; kk++) 
						vals[kk]=m_metadata.gets(jj,kk);

					// search for matching metadata in source link
					if(linkit->second->m_metadata.search(m_metadata.m_labels, 
								vals, &matchrows)!=0) {
						return -1;
					}

					if(matchrows.size() <= 0) {
						cerr << "Error, mismatch between input metadata's" << endl;
						return -1;
					}

					for(auto it=matchrows.begin(); it!=matchrows.end(); it++){
						// add all the matching rows to current input
						tmp.outnum = argnum;
						tmp.procnum = *it;
						tmp.source = linkit->second;
						m_inputs[jj].push_back(tmp);
					}
				}
			}
		} else {
			// global arguments with local values
			for(unsigned int jj = 0 ; jj < m_metadata.m_rows; jj++) {
				suffix = "";
				oss.str("");
				std::sregex_iterator expIt(fit->cbegin(), fit->cend(), CurlyRe);
				for(; expIt != ReEnd ; ++expIt) {
					string varname = (*expIt)[2].str(); 
					auto lvalit = std::find(m_metadata.m_labels.begin(),
							m_metadata.m_labels.end(), varname);
					if(lvalit == m_metadata.m_labels.end()) {
						cerr << " Uknown variable in expression " << *fit 
							<< " we don't currenlty allow references to other "
							"processes outputs in the middle of other strings "
							"that could lead to /home/johndoe/home/bill when you "
							"want /home/bill. I hope you understand and forgive." 
							<< endl;
						return -1;
					}

					// keep the suffix in case this is the last
					suffix = expIt->suffix(); 

					// build string
					size_t arg = lvalit-m_metadata.m_labels.begin();
					string mdval = m_metadata.gets(jj,arg);
					oss << expIt->prefix() << mdval;
				}

				oss << suffix;
#ifndef NDEBUG
				cerr << "\t" << oss.str() << endl;
#endif//ifndef NDEBUG
				m_inputs[jj].push_back(InputT(oss.str()));
			}
		}

	}		

	return 0;
}



int
Chain::Link::run()
{

	return 0;
}



/**********************************************************
 * Chain
 ***********************************************************/

int
Chain::parseFile(string filename) 
{
#ifndef NDEBUG
	cerr << "Parsing: " << endl;
#endif//ifndef NDEBUG
	std::string idstR = "\\s*([0-9.]*)\\s*:";			// id string regex
	std::string arstR = "\\s*\\[([^\\]\\s]*)\\]\\s*"; 	// array string regex
	std::string nonwR = "\\s*([^\\s]+)\\s*"; 			// non-white regex
	regex specialRe("\\s*(OUTDIR)\\s*=\\s*([\\{\\},/_[:alnum:]]*)\\s*");
	regex varRe("\\s*([0-9a-zA-Z_]*)\\s*=\\s*([\\{\\},/_[:alnum:]]*)\\s*");
	regex commaRe(",(?![^{]*\\})");
//	regex inputRe("\\s*([0-9.]*)\\s*:\\s*(?:input|i)\\s*:\\s*\\[(.*)\\]\\s*=(\\s*[^\\s]+\\s*)");
	regex inputRe(idstR+"\\s*(?:input|i)\\s*:"+arstR+"~"+nonwR);
	regex cmdRe(idstR+"\\s*(?:proc|p)\\s*:"+arstR+"\\s*->\\s*"+arstR+"(.*)");
	regex contRe("(.*)\\.\\.\\.\\s*");
	regex emptyRe("\\s*");
	regex curlyRe("\\{\\s*([^{]*)\\s*\\}"); // Curly-brace search
	const sregex_token_iterator tokEnd;
	std::smatch args;

	string prevleaf;
	string curleaf; 

	string linebuff; // contains the complete line (in case of line ending with ...)
	string line;
	ifstream ifs(filename);
	for(int linenum = 1; !ifs.eof(); linenum++) {
		std::getline(ifs,linebuff);

		// remove comments 
		linebuff = linebuff.substr(0,linebuff.find('#'));
	
		// line continued on the next line, just append without ...
		if(regex_match(linebuff, args, contRe)) {
			line = line + args[1].str();
			continue;
		} else 
			line = line + linebuff;

		if(regex_match(line, args, emptyRe)) {
			// do nothing
		} else if(regex_match(line, args, specialRe)) {
			// special regular expressions
			if(args[2].str().find_first_of(",") != string::npos) {
				cerr << "Special variables cannot contain commas, they must "
					"be VARIABLE=VALUE" << endl;
				return -1;
			}
			if(args[1] == "OUTDIR") {
				m_outdir = args[2].str();
			} else {
				cerr << "Uknown special variable" << endl;
			}
		} else if(regex_match(line, args, varRe)) {
			/**********************************************
			 * Variable declaration: 
			 *		match var=val[,val[,val[...]]]
			 *********************************************/
#ifndef NDEBUG
			cerr << "---------------------------------------" << endl;
			cerr << "Variables line:" << endl;
			cerr << line << endl;
			cerr << "---------------------------------------" << endl;
#endif// NDEBUG

			if(args.size() != 3) {
				cerr << "On line " << line << "\nError variables are of type "
					"name=val[,val[...]]\n";
				return -1;
			}
			
			string varname = args[1];
			string value = args[2];

			auto ret = m_vars.insert(pair<string,list<string>>(varname,
						list<string>()));
			if(!ret.second) {
				cerr << "Warning: redeclaration of " << varname << " in " 
					<< filename << ":" << linenum << endl;
				cerr << varname << "=" << value << endl;
				cerr << "Old value will be removed!" << endl;
				ret.first->second.clear();
			}

			// Split comma separated values into tokens
			sregex_token_iterator regIt(value.cbegin(), value.cend(), commaRe, -1);
			for(; regIt != tokEnd ; ++regIt) 
				ret.first->second.push_back(*regIt);
		} else if(regex_match(line, args, inputRe)) {
			/*******************************************************************
			 * Input Declaration
			 * 		match 
			 *		uint[.int[.int]]:s[ource]:[file[,file[,...]]] = 
			 *				({prov}[*,{prov}[*,{prov}]])
			 ******************************************************************/
			
#ifndef NDEBUG
			cerr << "---------------------------------------" << endl;
			cerr << "Input line:" << endl;
			cerr << line << endl;
			cerr << "---------------------------------------" << endl;
#endif //NDEBUG
			curleaf = args[1].str();

			/* Handle default ID from prevleaf, or, if this is the first line
			 * then just set previous to current*/
			if(curleaf.empty() && prevleaf.empty()) {
				cerr << "Error, first Link MUST have a identifier: "
					"(ie 0, 0.1.0, any set of positive numbers will do) " << endl;
				return -1 ;
			} else if(prevleaf.empty()) {
				prevleaf = args[1].str();
			} else if(curleaf.empty()) {
				auto tmp = getId(prevleaf);
				tmp.back()++;
				curleaf = getSid(tmp);
			}

#ifndef NDEBUG
			cerr << '"' << curleaf << '"' << endl;
			cerr << '"' << prevleaf << '"' << endl;
#endif//ifndef NDEBUG
			/* create new data structure */
			auto newleaf = new Link(filename, linenum, 
						curleaf, prevleaf, args[2], args[3], this);

			if(newleaf->m_err != 0) {
				cerr << "Failing" << endl;
				return -1;
			}
			auto ret = m_links.insert(make_pair(newleaf->m_id, newleaf));
			if(ret.second == false) {
				cerr << "Error: redeclaration of " << newleaf->m_id << " in " 
					<< filename << ":" << linenum << endl;
				return -1;
			}

			prevleaf = curleaf;
		} else if(regex_match(line, args, cmdRe)) {
#ifndef NDEBUG
			cerr << "---------------------------------------" << endl;
			cerr << "Command line:" << endl;
			cerr << line << endl;
			cerr << "---------------------------------------" << endl;
#endif //NDEBUG
			curleaf = args[1].str();

			/* Handle default ID from prevleaf, or, if this is the first line
			 * then just set previous to current*/
			if(args[1].str().empty() && prevleaf.empty()) {
				cerr << "Error, first Link MUST have a identifier: "
					"(ie 0, 0.1.0, any set of positive numbers will do) " << endl;
				return -1 ;
			} else if(prevleaf.empty()) {
				prevleaf = args[1].str();
			} else if(args[1].str().empty()) {
				auto tmp = getId(prevleaf);
				tmp.back()++;
				curleaf = getSid(tmp);
			}

			/* create new data structure */
#ifndef NDEBUG
			cerr << '"' << curleaf << '"' << endl;
			cerr << '"' << prevleaf << '"' << endl;
#endif//ifndef NDEBUG
			auto newleaf = new Link(filename, linenum, 
						curleaf, prevleaf, args[2], args[3], args[4], this);
			if(newleaf->m_err != 0) {
				cerr << "Failing" << endl;
				return -1;
			}

			auto ret = m_links.insert(make_pair(newleaf->m_id, newleaf));
			if(ret.second == false) {
				cerr << "Error: redeclaration of " << newleaf->m_id << " in " 
					<< filename << ":" << linenum << endl;
				return -1;
			}

			prevleaf = curleaf;
		}

		line = "";
	}

	return 0;
}

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
	m_outdir = "";
	int ret = parseFile(filename);
	if(ret != 0) {
		m_links.clear();
		m_vars.clear();
		m_err = -1;
	}

	resolveTree();
}

void Chain::dumpinputs()
{
	for(auto it = m_links.begin(); it != m_links.end(); it++) {
		cerr << "ID: " << it->first << endl;
		
		auto iij=it->second->m_inputs.begin();
		for(size_t jj=0; iij!=it->second->m_inputs.end(); jj++,iij++) {
			cerr << "\t" << "Job#: " << jj << endl;

			auto iia = iij->begin();
			for(size_t aa=0; iia != iij->end(); iia++, aa++) {
				if(iia->source) {
					cerr << "\t\tFrom Output:" << iia->source->m_id 
						<< ", Arg: " << iia->outnum 
						<< ", Job: " << iia->procnum << endl;
				} else {
					cerr << "\t\tFile:" << iia->filename << endl;
				}
			}
		}
	}
}

void Chain::dumpgraph()
{
	for(auto it = m_links.begin(); it != m_links.end(); it++) {
		cerr << "ID: " << it->first << endl;
		
		auto iij=it->second->m_inputs.begin();
		for(size_t jj=0; iij!=it->second->m_inputs.end(); jj++,iij++) {
			cerr << "\t" << "Job#: " << jj << endl;

			auto iia = iij->begin();
			for(size_t aa=0; iia != iij->end(); iia++, aa++) {
				if(iia->source) {
					cerr << "\t\tFrom Output:" << iia->source->m_id 
						<< ", Arg: " << iia->outnum 
						<< ", Job: " << iia->procnum << endl;
				} else {
					cerr << "\t\tFile:" << iia->filename << endl;
				}
			}
		}
	}
}

// TODO, move expansion here
// TODO, separate input/output numbering
void Chain::buildCommands()
{
	regex exp("\\{(-?[0-9]*)\\}");
	for(auto it = m_links.begin(); it != m_links.end(); it++) {
		cerr << it->second->m_id << ":" << it->second->m_cmd << endl;
		for(size_t jj=0; jj<it->second->m_inputs.size(); jj++) {
			string command = it->second->m_cmd;
			cerr << "jj: " << jj << " " << command << endl;

			std::ostringstream oss;
			
			// fill in all {0} variables with the corresponsing input/output
			std::sregex_iterator rit(command.cbegin(), command.cend(), exp);
			for(; rit != ReEnd; rit++) {
				cerr << (*rit)[0] << "->" << (*rit)[1] << "->" ;
				int in_i = atoi((*rit)[1].str().c_str());
				int out_i = in_i - it->second->m_inputs[jj].size();
				auto& input = it->second->m_inputs[jj];
				auto& output = it->second->m_outputs[jj];

				cerr << in_i << "," << out_i << endl;
				oss << rit->prefix();
				string file;
				if(out_i >= 0) {
					cerr << "Out: " << out_i << endl;
					//its from the output list
					file = output[out_i];
				} else if(in_i >= 0) {
					cerr << "In: " << in_i << endl;
					// its from the input list, resolve to an output
					if(input[in_i].source) {
						size_t job = input[in_i].procnum;
						size_t outnum = input[in_i].outnum;
#ifndef NDEBUG
						cerr << job << "," << outnum << "," << input[in_i].source;
						cerr << "," << input[in_i].source->m_outputs.size();
						cerr << "," << input[in_i].source->m_outputs[job].size() << endl;
#endif //NDEBUG
						// TODO check lengths, to make sure people don't reference outputs
						// that don't exist, maybe do it earlier...
						file = input[in_i].source->m_outputs[job][outnum];
					} else {
						file = input[in_i].filename;
					}
				}

				cerr << file << endl;
				oss << file;
			}
			cerr << it->second->m_cmd << "\nresolved to\n" << oss.str() << endl;

		}
	}
}

int Chain::resolveTree()
{
	// prepare for traversal
	for(auto it = m_links.begin(); it != m_links.end(); it++) {
		for(auto vv : m_links) {
			vv.second->m_visited = false;
			vv.second->m_resolved = false;
		}
	}

	list<string> stack;
	for(auto it = m_links.begin(); it != m_links.end(); it++) {
		// stack is just for debugging purposes of the user
		stack.clear();

		// resolve external references in inputs of current link 
		// by resolving parents references. Fails if a cycles is
		// detected. (ie traversal tries to revisit a node)
		if(it->second->resolveTree(stack) != 0) {
			cerr << "Error resolving inputs for " << it->second->m_id << " from "
				<< it->second->m_sourcefile << ":" << it->second->m_line << endl;
			m_err = -1;

			cerr << "Offending chain:" << endl;
			for(auto vv : stack) {
				cerr << vv << " <- ";
			}
			cerr << endl;
			return -1;
		}
	}

	dumpinputs();
	dumpgraph();
	return 0;
}
