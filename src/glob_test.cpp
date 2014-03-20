#include <regex>
#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <set>
#include <tclap/CmdLine.h>

#ifdef __unix__
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#endif //__unix__

using std::list;
using std::cerr;
using std::cout;
using std::string;
using std::regex;
using std::regex_match;
using std::sregex_token_iterator;

using namespace std;

#ifdef __unix__
/*function... might want it in some class?*/
int getdir (string dir, list<string>& files)
{
	DIR *dp;
	struct dirent *dirp;
	if((dp = opendir(dir.c_str())) == NULL) {
		cout << "Error(" << errno << ") opening " << dir << endl;
		return errno;
	}

	while ((dirp = readdir(dp)) != NULL) {
		files.push_back(string(dirp->d_name));
	}
	closedir(dp);
	return 0;
}

bool isdir(string path)
{
	struct stat buf;
	int ret = stat(path.c_str(), &buf);
	if(ret == 0 && S_ISDIR(buf.st_mode))
		return true;
	return false;
}

unsigned int getid(string path)
{
	struct stat buf;
	int ret = stat(path.c_str(), &buf);
	if(ret == 0) 
		return buf.st_ino;
	return 0;
}

string join(string a, string b)
{
	return a+'/'+b;
}

#endif //__unix__

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

std::list<string> regpaths(string reg)
{
	list<string> paths;
	list<string> lsret;
	list<string> prevpaths;
	std::set<unsigned int> ignore;
	if(reg[0] == '/') 
		paths.push_back("/");
	else
		paths.push_back("./");

	cerr << "Listing current directory: " << endl;

	// first split based on slashes
	const sregex_token_iterator regEnd;
	const auto slashRe = regex("/");
	std::sregex_token_iterator pathIt(reg.cbegin(), reg.cend(), slashRe, -1);
	for(; pathIt != regEnd; pathIt++) {
		auto tmpreg = pathIt->str();
		if(tmpreg.empty()) 
			continue;
	
		prevpaths.clear();
		std::swap(prevpaths, paths);

		// iterate through resulting paths
		for(auto it = prevpaths.begin(); it != prevpaths.end(); it++ ) {
			lsret.clear();

			// list out this directory
			if(isdir(*it) && getdir(*it, lsret) != 0) {
				continue;
			}

			// ignore this node in the future
			ignore.insert(getid(*it));

			// list directory save matches
			for(auto itt = lsret.begin(); itt != lsret.end(); itt++) {
				if(*itt != "." && *itt != ".." && regex_match(*itt, regex(tmpreg))) {

					// build path 
					string newdir = join(*it,*itt);

					// check whether we have been to this directory before,
					// if we have then ignore it
					unsigned int fid = getid(newdir);
					if(ignore.count(fid) == 0) {
						paths.push_back(newdir);
						ignore.insert(fid);
					}
				}
			}
		}
	}

	return paths;
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
	TCLAP::ValueArg<std::string> a_regex("r","reg","Regular expression",true,"*","regexp");
	cmd.add(a_regex);

	// parse arguments
	cmd.parse(argc, argv);

	/*
	 * Main Processing
	 */
	auto reg = a_regex.getValue();

	auto paths = regpaths(reg);

	cerr << "Final paths:" ;
	for(auto p : paths) {
		cerr << "\t" << p << endl;
	}

	// done, catch all argument errors
	} catch (TCLAP::ArgException &e)  // catch any exceptions
	{ std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl; }
}
