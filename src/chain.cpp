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
	const sregex_token_iterator regEnd;
	regex commaRe(",(?![^{]*\\})");
	csv.clear()
	sregex_token_iterator regIt(in.cbegin(), in.cend(), commaRe, -1);
	for(; regIt != regEnd ; ++regIt) {
		csv.push_back(*regIt);
	}
	return 0;
}

int csvToList(string in, list<int>& csv)
{
	const sregex_token_iterator regEnd;
	regex commaRe(",(?![^{]*\\})");
	csv.clear()
	sregex_token_iterator regIt(in.cbegin(), in.cend(), commaRe, -1);
	for(; regIt != regEnd ; ++regIt) {
		for(auto v : *regIt) {
			if(v > '9' || v < '0')
				return -1;
		}
		csv.push_back(atoi(*regIt));
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
	const std::sregex_iterator regEnd;
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
					intlist.push_back(a);
			} else if(!single.empty()) {
				intlist.push_back(atoi(single.c_str()));
			}
		}
	} else {
		cerr << "Error, syntax not understood: " << *exp << endl;
		return -1;
	}

	ostringstream oss; 
	ostringstream outoss; 

	// nothing to splice in
	if(intlist.empty()) 
		return 0;

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
	m_revlabels.clear();

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

			// todo labels will be screwed up by expansion {*} process

			// variable name
			int prev = ii;
			ii = spec.find_first_of('}',ii);
			string varname = spec.substr(prev+1,ii-prev-1);
			m_labels.push_back(varname);
			m_revlabels[varname] = m_labels.size()-1;

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

	m_metadata.clear();
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
		: m_cmd(cmd), m_sourcefile(sourcefile), m_line(line), m_id(getId(sid)), 
		m_sid(getSid(m_id)), m_type(PROC), m_prevLink(defsource), m_parent(parent)
{
	const auto commaRe = regex("\\s*,(?![^{]*\\})\\s*");
	const sregex_token_iterator regEnd;

	m_err = 0;
	m_resolved = false;
	m_visited = false;
	m_populated = false;
	
	// parse outspec as files
	m_preinputs.clear();
	m_preoutputs.clear();

	// split out input files, (but ignore commas inside {}
	cerr << "Initial Input Spec" << endl;
	for(sregex_token_iterator fnIt(inspec.cbegin(), inspec.cend(), 
				commaRe, -1); fnIt != regEnd ; ++fnIt) {
		list<string> tmp;
		list<string> tmp;
		m_preinputs.push_back(*fnIt);
		cerr << "\t" << *fnIt << endl;
	}

//	// expand multi-argument expressions, and remove the pre-expanded version
//	cerr << "Expanded Input Spec" << endl;
//	for(auto fit = m_preinputs.begin(); fit != m_preinputs.end(); fit++) {
//		argExpand(fit, preinputs);
//		cerr << "\t" << *fit << endl;
//	}

	// split out output files, (but ignore commas inside {}
	for(sregex_token_iterator fnIt(outspec.cbegin(), outspec.cend(), 
				commaRe, -1); fnIt != regEnd ; ++fnIt) {
		m_preoutputs.push_back(*fnIt);
	}

#ifndef NDEBUG
	cerr << "Command: " << cmd<< endl;
	cerr << "Inspec:  "; printC(m_preinputs); cerr << endl;
	cerr << "Output:  "; printC(m_preoutputs); cerr << endl;
#endif //NDEBUG

//	// parse outputs without resolving external references
//	if(resolveInternal() != 0) {
//		m_err = -1;
//		return;
//	}
}


// TODO merge common components of constructors
// Input Constructor
Chain::Link::Link(std::string sourcefile, unsigned int line, 
		std::string sid, std::string defsource, 
		std::string inspec, std::string mixture,
		Chain* parent)
		: m_cmd(""), m_sourcefile(sourcefile), m_line(line), m_id(getId(sid)), 
		m_sid(getSid(m_id)), m_type(INPUT), m_prevLink(defsource), m_parent(parent)

{
	const auto commaRe = regex("\\s*,(?![^{]*\\})\\s*");
	const sregex_token_iterator regEnd;

	m_err = 0;
	m_resolved = false;
	m_visited = false;
	m_populated = false;

	// expand metadata
	if(mixtureToMetadata(mixture, parent) != 0) {
		m_err = -1;
		return;
	}

	// split out output files, (but ignore commas inside {}
	m_preinputs.clear();
	cerr << "Initial Input Spec" << endl;
	sregex_token_iterator fnIt(inspec.cbegin(), inspec.cend(), commaRe, -1);
	for(; fnIt != regEnd ; ++fnIt) {
		m_preinputs.push_back(*fnIt);
		cerr << "\t" << *fnIt << endl;
	}
	
	// expand multi-argument expressions, and remove the pre-expanded version
	cerr << "Expanded Input Spec" << endl;
	for(auto fit = m_preinputs.begin(); fit != m_preinputs.end(); fit++) {
		expand(fit, preinputs);
		cerr << "\t" << *fit << endl;
	}

#ifndef NDEBUG
	cerr << "Inspec:  "; printC(m_preinputs); cerr << endl;
	cerr << "Metadata Expansion:  "; printCC(m_metadata); cerr << endl;
#endif //NDEBUG

//	// parse inputs without resolving external references
//	if(resolveInternal() != 0) {
//		m_err = -1;
//		return;
//	}

}


int Chain::Link::resolveTree(list<string>& callstack)
{
#ifndef NDEBUG
	cerr << "resolveTree " << this << endl;
#endif //NDEBUG
	if(m_resolved) 
		return 0;

	if(m_visited) {
		cerr << "Revisited a link, appears to be a circular dependence" << endl;
		return -1;
	}
	m_visited = true;
	
	// push ourselves onto the call stack
	stack.push_back(m_sid);

	// resolve external inputs (set metadata, and update preinputs)
	int ret = resolveExternalMetadata(callstack);
	if(ret < 0) {
		cerr << "Error resolving inputs for " << it->second->m_sid << " from "
			<< it->second->m_sourcefile << ":" << it->second->m_line << endl;
		return -1;
	} else if (ret > 0) {
		cerr << "Warning, cannot currently resolve inputs for " 
			<< it->second->m_sid << " from "
			<< it->second->m_sourcefile << ":" << it->second->m_line << endl;
	}

	// resolve all global varianble references, including 
	// variables of variables of variables.... 
	ret = resolveGlobals();
	if(ret < 0) {
		cerr << "Error resolving global variabes for " << it->second->m_sid 
			<< " from " << it->second->m_sourcefile << ":" 
			<< it->second->m_line << endl;
		return -1;
	}

	// sets all final inputs
	ret = resolveInputs(callstack);
	if(ret < 0) {
		cerr << "Error resolving inputs for " << it->second->m_sid 
			<< " from " << it->second->m_sourcefile << ":" 
			<< it->second->m_line << endl;
		return -1;
	}

	m_resolved = true;
	return 0;
}


/**
 * @brief This function adds elements to the m_inputs corresponding to a single
 * {1.1:3-4|subject} or {*1.1:3-4|subject}
 * type spec. 
 *
 * @param expand
 * @param dep
 * @param argn
 * @param control
 *
 * @return 
 */
int Chain::Link::resolveInputReference(string expand, string dep, string argn, 
			string controlstr)
{
	if(dep.empty()) {
#ifndef NDEBUG
		cerr << "Dep empty, setting to default" << endl;
		dep = m_prevLink;
#endif //NDEBUG
	}

	vector<int> linkid = getId(dep);
	int argnum = atoi(argn.c_str());
	list<int> control;
	csvToList(controlstr, control);
	
	list<int> args;
	argExpand(argn.str(), args);

	auto linkit = m_parent->m_links.find(linkid);
	if(linkit == m_parent->m_links.end())  {
		cerr << "Could not find a link with the id: " << dep << " (" 
			<< getSid(linkid) << endl;
		return -1;
	}

	InputT tmp;
	tmp.source = &linkit->second;

	list<std::pair<string,string>> job_mdata;

	if(expand != "*" && !control.empty()) {
		cerr << "Control term without expansion {*} term given in " << "{" 
			<< expand << dep << ":" << argn << "|" << controlstr << endl;
		return -1;
	}
	
	if(expand == "*") {
		if(control) {
			// each job group value is a job number -> group number
			// without any control all job numbers have the same
			// with no expansion, all job numbers have unique group numbers,
			// initialize groups as everyone the same, then split up
			// based on metadata
			vector<int> jobgroups(tmp.source->m_metadata.size(),0);;

			int base = 1;
			for(auto cc : control) {
				// get the colum of the control
				// ignore labels that don't vary in the given source
				auto slabelIt = tmp.source->m_revlabels.find(*cc);
				if(slabelIt == tmp.source->m_revlabels.end()) 
					continue;
				int column = slabelIt->second;

				// count of each particular value, next one gets this index
				// ie, the first subject=s1 gets 0, the second subject=s1 gets
				// 1 and so on
				map<string,int> valcounts(varIt->second.size(),0);
				for(size_t jj = 0 ; jj < tmp.source->m_metadata.size(); jj++) {
					string value = tmp.source->m_metadata[jj][column];
					jobgroups[jj] += (valcounts[value]++)*base;
				}

				// TODO test, create a mapping

				int maxcount = INT_MIN;
				for(auto vv : valcounts) {
					maxcount = vv.second > maxcount ? vv.second : maxcount;
				}

				base = base * 
				// get the list of values for the current metadata
				// ie subject = [s1, s2, s3]
				auto varIt = m_parent->m_vars.find(*cc);
				if(varIt == m_parent->m_vars.end()) {
					cerr << "Error, unknown variable used for control in "
							"expansion within " << "{" << expand << dep 
							<< ":" << argn << "|" << controlstr << endl;
					return -1;
				}

				// for each value of the metadata variable, create a list of 
				// jobs that match the particular value, ie all the jobs where
				// subject = 101
				for(auto& lval : varIt->second) {
					job_mdata.clear();
					job_mdata.push_back(make_pair(*cc, *lval));
					auto tmplist = tmp.source->getJobs(job_mdata);
					if(
				}

				// merge tmpjg, 
			}
			for(size_t aa = 0 ; aa < tmp.source->m_outputs.size(); aa++) {
				tmp.argnum = aa;
				for(size_t sjj = 0 ; sjj < tmp.source->m_metadata.size(); sjj++) {
					tmp.procnum = sjj;
					for(size_t jj = 0 ; jj < m_metadata.size(); jj++)
						m_inputs[jj] = tmp;
				}
			}
		} else {
			for(size_t sjj = 0 ; sjj < tmp.source->m_metadata.size(); sjj++) {
				tmp.procnum = sjj;
				for(size_t aa = 0 ; aa < tmp.source->m_outputs[sjj].size(); aa++) {
					tmp.argnum = aa;
					// this may generate a huge number of repitive tasks
					// if this is the only variable on the command line,
					// we will resolve that later by hashing inputs
					for(size_t jj = 0 ; jj < m_metadata.size(); jj++)
						m_inputs[jj] = tmp;
				}
			}
		}
	} else {
		// create InputT for the current term, in all of the jobs
		for(size_t jj = 0; jj < m_metadata.size(); jj++) {
			job_mdata.clear();
			for(size_t aa = 0; aa < m_metadata[jj].size(); aa++) 
				job_mdata.push_back(make_pair(m_labels[aa],m_metadata[jj][aa]));

			// get the matching job in input jobs metadata
			// [(subject,sid),(fmri,fid)...]
			auto jobs = tmp.source->getJobs(job_mdata);
			if(jobs.size() != 1) {
				cerr << "Metadata mismatch between " << m_sid << " and " 
					<< tmp.source->m_sid << ". Relevent metadata: " << endl;
				for(auto& vv : job_mdata) 
					cerr << "\t" << vv.first << "=" << vv.second << endl;
				return -1;
			}

			tmp.procnum = jobs.front();
			if(args.empty()) {
				// since args is empty, default to all the outputs
				for(size_t aa = 0 ; aa < linkit->second->m_outputs[jj].size(); aa++) {
					tmp.outnum = aa;
					m_inputs[jj].push_back(tmp);
				}
			} else {
				// just push the requested args
				for(auto ait = args.begin(); ait = args.end(); ait++) {
					tmp.outnum = *ait;
					m_inputs[jj].push_back(tmp);
				}
			}
		}
	}
}

//int Chain::Link::resolveArgument(list<string>::iterator& argIt) 
//{
//	cerr << "Resolving Argument: " << *inspec << endl;
//
//	// suppose case 0
//		std::sregex_iterator expIt(updated.cbegin(), updated.cend(), expRe);
//		for(; expIt != regEnd ; ++expIt) {
//			cerr << "Prefix " << expIt->prefix().str() << endl;
//			cerr << "Match " << (*expIt)[0].str() << endl;
//			cerr << "Varname: " << (*expIt)[1].str() << endl;
//			cerr << "Expand?" << (*expIt)[2].str() << endl;
//			cerr << "Dep Number: " << (*expIt)[3].str() << endl;
//			cerr << "Var/Number: " << (*expIt)[4].str() << endl;
//			cerr << "Control: " << (*expIt)[5].str() << endl;
//			cerr << "Suffix " << expIt->suffix().str() << endl;
//
//			// if its just a variable name, should match everything but {* }
//			string varname = (*expIt)[1].str(); 
//
//			auto gvalit = m_parent->m_vars.find(varname);
//			auto lvalit = m_revlabels.find(varname);
//
//			if(gvalit != m_parent->m_vars.end() && lvalit == m_revlabels.end()) {
//				if(gvalit->second.size() > 1) {
//					cerr << "Error! Variable lookup resolved to an array, but without"
//						" process has not been provided with an expansion method"
//						" for that variable! (" << m_sid << ")" << endl;
//					return -1;
//				}
//
//				// successfully resolved, recurse on the new inspec,
//				// possible values like like:
//				// {subject}/{run}/
//				cerr << varname << " -> " << gvalit->second.front() << endl;
//				cerr << inspec << " -> " << 
//					return resolveArgument(gvalit->second.front());
//
//				gvalit = m_parent->m_vars.find(varname);
//				lvalit = m_revlabels.find(varname);
//			}
//
//			// if it is an external reference, should match all these
//			bool expand = (*expIt)[2].str() == "*";
//			vector<int> linkid = getId((*expIt)[3].str());
//			int argnum = atoi((*expIt)[4].str());
//			list<int> control;
//			csvToList((*expIt)[5].str(), control);
//
//			auto linkit = m_parent->m_links.find(linkid);
//			
//			oss << expIt->prefix().str() << (*expIt)[0];
//		}
//	
//
//
//	ostringstream prefix;
//	string updated;
//	while(updated != inspec) {
//		updated = inspec;
//		prefix.str("");
//		std::sregex_iterator expIt(updated.cbegin(), updated.cend(), expRe);
//		for(; expIt != regEnd ; ++expIt) {
//			cerr << "Prefix " << expIt->prefix().str() << endl;
//			cerr << "Match " << (*expIt)[0].str() << endl;
//			cerr << "Varname: " << (*expIt)[1].str() << endl;
//			cerr << "Expand?" << (*expIt)[2].str() << endl;
//			cerr << "Dep Number: " << (*expIt)[3].str() << endl;
//			cerr << "Var/Number: " << (*expIt)[4].str() << endl;
//			cerr << "Control: " << (*expIt)[5].str() << endl;
//			cerr << "Suffix " << expIt->suffix().str() << endl;
//
//			// if its just a variable name, should match everything but {* }
//			string varname = (*expIt)[1].str(); 
//
//			auto gvalit = m_parent->m_vars.find(varname);
//			auto lvalit = m_revlabels.find(varname);
//
//			if(gvalit != m_parent->m_vars.end() && lvalit == m_revlabels.end()) {
//				if(gvalit->second.size() > 1) {
//					cerr << "Error! Variable lookup resolved to an array, but without"
//						" process has not been provided with an expansion method"
//						" for that variable! (" << m_sid << ")" << endl;
//					return -1;
//				}
//
//				// successfully resolved, recurse on the new inspec,
//				// possible values like like:
//				// {subject}/{run}/
//				cerr << varname << " -> " << gvalit->second.front() << endl;
//				cerr << inspec << " -> " << 
//					return resolveArgument(gvalit->second.front());
//
//				gvalit = m_parent->m_vars.find(varname);
//				lvalit = m_revlabels.find(varname);
//			}
//
//			// if it is an external reference, should match all these
//			bool expand = (*expIt)[2].str() == "*";
//			vector<int> linkid = getId((*expIt)[3].str());
//			int argnum = atoi((*expIt)[4].str());
//			list<int> control;
//			csvToList((*expIt)[5].str(), control);
//
//			auto linkit = m_parent->m_links.find(linkid);
//			
//			oss << expIt->prefix().str() << (*expIt)[0];
//		}
//	}
//	// if its a global variable that we don't have metadata associated
//	// with, try to resolve it
//
//
//	lvalit = m_revlabels.find(varname);
//	linkit = m_parent->m_links.find(getId(varname));
//	if(gvalit != m_parent->m_vars.end()) {
//		cerr << "Global Var!" << endl;
//		// if we have a value from our metadata for the variable
//		// then fill the value in for the current job
//
//		// otherwise just try to replace
//	} else if(lvalit != m_revlabels.end()) {
//		cerr << "LocalVar!" << endl;
//		// if we have a value from our metadata for the variable
//		// then fill the value in for the current job
//
//		// otherwise just try to replace
//	} else if(linkit != m_parent->m_links.end()) {
//		cerr << "External Reference Var!" << endl;
//
//		// make sure the inputs' metadta and outputs are up to date
//		linkit->resolveTree(callstack);
//
//		InputT tmp;
//		tmp.source = &(*linkit);
//
//		// if {*...}
//		// expand out 
//		for(unsigned int op = 0; op < linkit->m_metadata; op++) {
//			if(linkit->m_metadata[op] == );
//			// if |{subject} == current subject and if {run} = current run
//			// push InputT onto list of arguments
//		}
//
//		m_inputs[jj].push_back(InputT());
//
//	}
//}

/**
 * @brief This function iterates through the inspec list and 
 * updates the m_inputs array.
 *
 * @param callstack
 *
 * @return 
 */
int Chain::Link::resolveInputs(list<string>& callstack)
{

	const std::sregex_iterator regEnd;
	const std::regex expRe("\\s*\\{(\\*)?\\s*((?:([0-9.]*)\\s*:)?"
			"(?:\\s*([-,_[:alnum:]]*)\\s*)?(?:\\s*"
			"\\|([,_[:alnum:]]*))?)\\s*\\}\\s*");

	std::smatch match;

#ifndef NDEBUG
	cerr << "resolveInputs" << this << endl;
	printC(m_preinputs);
#endif //NDEBUG
	
	if(m_resolved)
		return 0;
	
	assert(m_metadata.size() > 0);
	assert(m_labels.size() == m_resolved.size());

	int ret;
	InputT tmp;
	m_inputs.resize(m_metadata.size());
	
	for(auto fit = m_preinputs.begin(); fit != m_preinputs.end(); fit++) {
#ifndef NDEBUG
		cerr << "Input spec: " << *fit << endl;
#endif //NDEBUG

		if(regex_match(*fit, match, expRe)) {

#ifndef NDEBUG
			cerr << "\tPrefix " << match.prefix().str() << endl;
			cerr << "\tMatch " << match[0].str() << endl;
			cerr << "\tVarname: " << match[1].str() << endl;
			cerr << "\tExpand?" << match[2].str() << endl;
			cerr << "\tDep Number: " << match[3].str() << endl;
			cerr << "\tVar/Number: " << match[4].str() << endl;
			cerr << "\tControl: " << match[5].str() << endl;
			cerr << "\tSuffix " << match.suffix().str() << endl;
#endif //NDEBUG

			// single argument
			string varname = match[2].str(); 
			auto lvalit = m_revlabels.find(varname);
			if(lvalit != m_revlabels.end()) {
				assert(match[1].str() != "*");

				// push inputs onto each job
				for(unsigned int jj = 0; jj < m_metadata.size(); jj++)
					m_inputs[jj].push_back(InputT(m_metadata[jj][lvalit->second]));
			} else {
				// persumably this should be a reference 
				ret = resolveInputReference(match[1], match[3], match[4], match[5]);
			}
		}


		// for each variable like "{subject}" in {subject}/{run}
		std::sregex_iterator expIt(fit->cbegin(), fit->cend(), expRe);
		for(; expIt != regEnd ; ++expIt) {

		}

			

			if((*expIt)[2] == "*") {
				// need to expand
				auto gvalit = m_parent->m_vars.find(varname);
				if(gvalit == m_parent->m_vars.end()) {
					cerr << "Error, " << *fit << " specifies expansion of the "
						"variable " << (*expIt)[1] << " but that variable has "
						"not (yet) been defined. Did you place you variable "
						"declaration in the beginning of the file? " << endl;
						return -1;
				}

				for(unsigned int jj = 0; jj < m_metadata.size(); jj++) {
					m_inputs[ii].resize(gvalit->second.size());
					for(unsigned int kk = 0 ; kk < gvalit->second.size(); kk++) {
						tmp.source = NULL;
						tmp.filename = expIt->prefix() + gvalit->second[kk] + 
									expIt->suffix();
						m_inputs[jj][kk] = ;
						// warning TODO, may still have a variable to resolve
					}
				}

			}

			continue;
		}

		// just one variable
		std::sregex_iterator expIt(*fit.cbegin(), *fit.cend(), expRe);
		for(; expIt != regEnd ; ++expIt) {
			cerr << "Prefix " << expIt->prefix().str() << endl;
			cerr << "Match " << (*expIt)[0].str() << endl;
			cerr << "Varname: " << (*expIt)[1].str() << endl;
			cerr << "Expand?" << (*expIt)[2].str() << endl;
			cerr << "Dep Number: " << (*expIt)[3].str() << endl;
			cerr << "Var/Number: " << (*expIt)[4].str() << endl;
			cerr << "Control: " << (*expIt)[5].str() << endl;
			cerr << "Suffix " << expIt->suffix().str() << endl;

			// if its just a variable name, should match everything but {* }

			auto gvalit = m_parent->m_vars.find(varname);
			auto lvalit = m_revlabels.find(varname);
			
			// todo iterate until this is false
			if(gvalit != m_parent->m_vars.end() && lvalit == m_revlabels.end()) {
				if(gvalit->second.size() > 1) {
					cerr << "Error! Variable lookup resolved to an array, but without"
						" process has not been provided with an expansion method"
						" for that variable! (" << m_sid << ")" << endl;
					return -1;
				}

				// successfully resolved, recurse on the new inspec,
				// possible values like like:
				// {subject}/{run}/
				cerr << varname << " -> " << gvalit->second.front() << endl;
				cerr << inspec << " -> " << 
					return resolveArgument(gvalit->second.front());

				gvalit = m_parent->m_vars.find(varname);
				lvalit = m_revlabels.find(varname);
			}

			if(lvalit != m_revlabels.end()) {
				// resolveable input, just replace 
				
			} else {

			}
			// now that all the variables are locally defined or external
			// references...

			for(unsigned int 
			// if it is an external reference, should match all these
			bool expand = (*expIt)[2].str() == "*";
			vector<int> linkid = getId((*expIt)[3].str());
			int argnum = atoi((*expIt)[4].str());
			list<int> control;
			csvToList((*expIt)[5].str(), control);

			auto linkit = m_parent->m_links.find(linkid);
			
			oss << expIt->prefix().str() << (*expIt)[0];
		}
		if(resolveArgument(*fit) < 0) 
			return -1;
	}

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
Chain::Link::resolveGlobals()
{
	const std::sregex_iterator regEnd;
	std::regex expRe("\\{(\\*)?\\s*([-,_[:alnum:]]*)\\s*\\}"); //expansion Rgex
	std::smatch args;

#ifndef NDEBUG
		cerr << "resolveGlobals" << endl;
		printC(m_preinputs);
#endif //NDEBUG
	
	if(m_resolved)
		return 0;

	assert(m_metadata.size() > 0);
	assert(m_labels.size() == m_resolved.size());
	list<std::string> inExp;
	bool restart = false;

	while(!restart) {
		// for input term
		for(auto fit = m_preinputs.begin(); !restart && 
						fit != m_preinputs.end(); fit++) {

			// for each variable like "{subject}" in {subject}/{run}
			std::sregex_iterator expIt(fit->cbegin(), fit->cend(), expRe);
			for(; expIt != regEnd ; ++expIt) {

				// ignore global variables that we have metadata for, unless
				// it calls for expansion ( we will fill just directly use 
				// this later )
				auto mit = m_revlabels.find((*expIt)[2]);
				if(mit != m_revlabels.end() && (*expIt)[1] != "*") {
					continue;
				}

				// ignore non-global variables (ie references)
				auto git = m_parent->m_vars.find((*expIt)[2]);
				if(git == m_parent->m_vars.end()) {
					continue;
				}

				string prefix = fit->substr(0,expIt->position());
				string suffix = expIt->suffix();

				// perform expansion of global variables using metadata
				if((*expIt)[1] == "*") {
#ifndef NDEBUG
					cerr << "Expanding: " << (*expIt)[0].str() << " in " 
						<< *fit << endl;
#endif //NDEBUG
					for(auto lit = git->second.begin(); 
								lit != git->second.begin(); lit++) {
						string expanded = prefix + *lit + suffix;
#ifndef NDEBUG
						cerr << "\t" << expanded << endl; 
#endif //NDEBUG
						inExp.push_back(expanded);
					}

					restart = true;
					fit = m_inputs.erase(fit);
					splice(fit, inExp);
				} else {
				// just replace
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
	}
	return 0;
}

int
Chain::Link::run()
{

	return 0;
}


/**
 * @brief This function takes two sets of metadata and merges them into
 * 	target, which is the output. If expand is specified output metadata
 * 	will be 1xN, if control is specified, the expansion is limited to 
 * 	variables not specififed in control. IE
 *
 * 	expand 
 * 	subject run time
 *
 * 	without control, all subjects, times and runs will be placed onthe 
 * 	command line together.
 *
 * 	with control = subject. The number of rows will be the number of 
 * 	of rows 
 *
 * @param target
 * @param source
 * @param expand
 * @param control
 *
 * @return 
 */
int Chain::Link::mergeMetadata(std::vector<std::vector<std::string>>& target,
		std::vector<std::vector<std::string>> source, 
		bool expand, std::string control)
{
	list<string> contlist;
	list<list<string>> sourcetmp;
	
	return 0;
}

/**
 * @brief Before: m_inputs, m_preinputs, m_metadata could be subject to 
 * change by parent links, after m_metadata and m_inputs are 
 * in their final form.
 *
 * @param call stack 
 *
 * @return error code
 */
int Chain::Link::resolveExternalMetadata(list<string>& callstack)
{
	cerr << "resolveExternalMetadata " << this << endl;
	
	if(m_resolved)
		return 0;
	
	const std::sregex_iterator regEnd;
	const std::regex expRe("\\{(\\*)?"
			"(?:\\s*([0-9.]*)\\s*:)?"
			"(?:\\s*([-,_[:alnum:]]*)\\s*)?"
			"(?:\\s*\\|([^}]*))?\\}");

	// resolve inputs
	for(auto fit = m_preinputs.begin(); fit != m_preinputs.end(); fit++) {
		cerr << "Input spec: " << *fit << endl;
		// we have a token filename, now expand  {* } variables
		std::sregex_iterator expIt(fit->cbegin(), fit->cend(), expRe);
		for(; expIt != regEnd ; ++expIt) {
			
			cerr << "Prefix " << expIt->prefix().str() << endl;
			cerr << "Match " << (*expIt)[0].str() << endl;
			cerr << "Expand?" << (*expIt)[1].str() << endl;
			cerr << "Dep Number: " << (*expIt)[2].str() << endl;
			cerr << "Var/Number: " << (*expIt)[3].str() << endl;
			cerr << "Control: " << (*expIt)[4].str() << endl;
			cerr << "Suffix " << expIt->suffix().str() << endl;

			// ignore global variables for now 
			auto gvalit = m_parent->m_vars.find((*expIt)[3]);
			if(gvalit != m_parent->m_vars.end()) {
				cerr << "Global Var!" << endl;
				continue;
			}
			
			if(!expIt->prefix().empty() || !expIt->suffix().empty()) {
				cerr << "Parsing error. References to output of other "
					"jobs cannot include literals. ie. /ifs/{1.2:3} is"
					"not valid. Add /ifs to the parent job instead!" << endl;
				return -1;
			}


			cerr << (*expIt)[3] << endl;
			printC(getId((*expIt)[3]));
			cerr << endl;
			auto linkit = m_parent->m_links.find(getId((*expIt)[3]));
			if(linkit != m_parent->m_links.end()) {
				cerr << "External Reference Var!" << endl;
			}
	
			// make sure the inputs' metadta and outputs are up to date
			linkit->resolveTree(callstack);

			// TODO merge metadata
			//merge metadata
			string control = (*expIt)[4].str();
			bool expand = (*expIt)[1].str() == "*";
			if(mergeMetadata(m_metadata, linkit->m_metadata, expand, control) < 0) {
				cerr << "Error resolving inputs/metadata" << endl;
				return -1;
			}
		}
	}

	return 0;
}

/**********************************************************
 * Chain
 ***********************************************************/

int
Chain::parseFile(string filename) 
{
	cerr << "Parsing: " << endl;
	std::string idstR = "\\s*([0-9.]*)\\s*:";			// id string regex
	std::string arstR = "\\s*\\[([^\\]\\s]*)\\]\\s*"; 	// array string regex
	std::string nonwR = "\\s*([^\\s]+)\\s*"; 			// non-white regex
	regex varRe("\\s*([0-9a-zA-Z_]*)\\s*=\\s*([\\{\\},/_[:alnum:]]*)\\s*");
	regex commaRe(",(?![^{]*\\})");
//	regex inputRe("\\s*([0-9.]*)\\s*:\\s*(?:input|i)\\s*:\\s*\\[(.*)\\]\\s*=(\\s*[^\\s]+\\s*)");
	regex inputRe(idstR+"\\s*(?:input|i)\\s*:"+arstR+"~"+nonwR);
	regex cmdRe(idstR+"\\s*(?:proc|p)\\s*:"+arstR+"\\s*->\\s*"+arstR+"(.*)");
	regex contRe("(.*)\\.\\.\\.\\s*");
	regex emptyRe("\\s*");
	regex curlyRe("\\{\\s*([^{]*)\\s*\\}"); // Curly-brace search
	const sregex_token_iterator regEnd;
	std::smatch args;

	vector<int> prevleaf;
	vector<int> curleaf; 

	string linebuff; // contains the complete line (in case of line ending with ...)
	string line;
	ifstream ifs(filename);
	for(int linenum = 1; !ifs.eof(); linenum++) {
		std::getline(ifs,linebuff);

		// remove comments 
		linebuff = linebuff.substr(0,linebuff.find('#'));
		cerr << linebuff << endl;
	
		// line continued on the next line, just append without ...
		if(regex_match(linebuff, args, contRe)) {
			line = line + args[1].str();
			continue;
		} else 
			line = line + linebuff;

		cerr << line << endl;
		if(regex_match(line, args, emptyRe)) {
			// do nothing
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
				cerr << "Value will NOT be updated!" << endl;
			}

			// Split comma separated values into tokens
			sregex_token_iterator regIt(value.cbegin(), value.cend(), commaRe, -1);
			for(; regIt != regEnd ; ++regIt) 
				ret.first->second.push_back(*regIt);
		} else if(regex_match(line, args, inputRe)) {
			/*****************************************
			 * Input Declaration
			 * 		match 
			 *		uint[.int[.int]]:s[ource]:[file[,file[,...]]] = 
			 *				({prov}[*,{prov}[*,{prov}]])
			 *****************************************/
			
#ifndef NDEBUG
			cerr << "---------------------------------------" << endl;
			cerr << "Input line:" << endl;
			cerr << line << endl;
			cerr << "---------------------------------------" << endl;
#endif //NDEBUG

			/* Handle default ID from prevleaf, or, if this is the first line
			 * then just set previous to current*/
			if(args[1].str().empty() && prevleaf.empty()) {
				cerr << "Error, first Link MUST have a identifier: "
					"(ie 0, 0.1.0, any set of positive numbers will do) " << endl;
				return -1 ;
			} else if(prevleaf.empty()) {
				prevleaf = getId(args[1]);
			} else if(args[1].str().empty()) {
				curleaf = prevleaf;
				curleaf.back()++;
			} else {
				curleaf = getId(args[1].str());
			}

			/* create new data structure */
			auto newleaf = shared_ptr<Link>(new Link(filename, linenum, 
						getSid(curleaf), getSid(prevleaf), 
						args[2], args[3], this));

			auto ret = m_links.insert(pair<vector<int>,shared_ptr<Link>>(
						newleaf->m_id, newleaf));
			if(ret.second == false) {
				cerr << "Error: redeclaration of " << newleaf->m_sid << " in " 
					<< filename << ":" << linenum << endl;
				return -1;
			}
		} else if(regex_match(line, args, cmdRe)) {
#ifndef NDEBUG
			cerr << "---------------------------------------" << endl;
			cerr << "Command line:" << endl;
			cerr << line << endl;
			cerr << "---------------------------------------" << endl;
#endif //NDEBUG

			/* Handle default ID from prevleaf, or, if this is the first line
			 * then just set previous to current*/
			if(args[1].str().empty() && prevleaf.empty()) {
				cerr << "Error, first Link MUST have a identifier: "
					"(ie 0, 0.1.0, any set of positive numbers will do) " << endl;
				return -1 ;
			} else if(prevleaf.empty()) {
				prevleaf = getId(args[1]);
			} else if(args[1].str().empty()) {
				curleaf = prevleaf;
				curleaf.back()++;
			} else {
				curleaf = getId(args[1].str());
			}

			/* create new data structure */
			auto newleaf = shared_ptr<Link>(new Link(filename, linenum, 
						getSid(curleaf), getSid(prevleaf), 
						args[2], args[3], args[4], this));

			auto ret = m_links.insert(pair<vector<int>,shared_ptr<Link>>(
						newleaf->m_id, newleaf));
			if(ret.second == false) {
				cerr << "Error: redeclaration of " << newleaf->m_sid << " in " 
					<< filename << ":" << linenum << endl;
				return -1;
			}
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
	int ret = parseFile(filename);
	if(ret != 0) {
		m_links.clear();
		m_vars.clear();
		m_err = -1;
	}

	resolveTree();
}

int Chain::resolveTree()
{
	list<string> stack;
	for(auto it = m_links.begin(); it != m_links.end(); it++) {

		// prepare for traversal
		for(auto vv : m_links) {
			vv.second->m_visited = false;
			vv.second->m_resolved = false;
		}

		// stack is just for debugging purposes of the user
		stack.clear();

		// resolve external references in inputs of current link 
		// by resolving parents references. Fails if a cycles is
		// detected. (ie traversal tries to revisit a node)
		if(it->second->resolveTree(stack) != 0) {
			cerr << "Error resolving inputs for " << it->second->m_sid << " from "
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
}
