#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <set>
#include <cassert>

#include "metadata.h"

using namespace std;


int main()
{
	vector<string> subjects({"john", "bill", "jack", "jill"});
	vector<string> types({"bp", "euth", "bp", "euth"});
	vector<string> times({"march", "apil", "may"});
	vector<string> runs({"fmri1", "fmri2", "fmri3"});
	MetaData mdsubjects("subjects", subjects);
	MetaData mdtypes("types", types);
	MetaData mdtimes("times", times);
	MetaData mdruns("frun", runs);

	cerr << mdsubjects << endl;
	cerr << mdtimes << endl;
	cerr << mdtypes << endl;
	cerr << mdruns << endl;

	MetaData fmri;
	fmri.nest(mdruns);

	MetaData longit;

	cerr << longit << endl;
	longit.nest(mdsubjects);
	cerr << longit << endl;
	longit.zip(mdtypes);
	cerr << longit << endl;
	longit.nest(mdtimes);
	cerr << longit << endl;
	
	cerr << fmri << endl;
	fmri.nest(mdsubjects);
	cerr << fmri<< endl;

	fmri.merge(longit);
	cerr << fmri << endl;

	MetaData newfmri("subjects", subjects);
	newfmri.zip(mdtypes);
	newfmri.nest(mdruns);
	cerr << "newfmri: "<< newfmri << endl;

	MetaData newtimes("subjects", subjects);
	newtimes.zip(mdtypes);
	newtimes.nest(mdtimes);
	cerr << "newtimes: "<< newtimes << endl;

	newfmri.merge(newtimes);
	cerr << "newfmri: "<< newfmri << endl;
}

