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
	vector<string> types({"bp", "euth", "skiz", "old"});
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

	cerr << "---------------------------------";
	cerr << "Joining fmri:" << endl; 
	cerr << fmri << endl << " and " ;
	cerr << longit << endl;
	fmri.ujoin(longit);
	cerr << fmri << endl;
	cerr << "---------------------------------";

	cerr << "Joining twice:" << 
	fmri.ujoin(longit);
	cerr << fmri << endl;

	MetaData newfmri("subjects", subjects);
	newfmri.zip(mdtypes);
	newfmri.nest(mdruns);
	cerr << "newfmri: "<< newfmri << endl;

	MetaData newtimes("subjects", subjects);
	newtimes.zip(mdtypes);
	newtimes.nest(mdtimes);
	cerr << "newtimes: "<< newtimes << endl;

	newfmri.ujoin(newtimes);
	cerr << "newfmri: "<< newfmri << endl;

	list<string> control;
	{
		auto tmp = newfmri.split(control);
		cerr << "new fmri split to: " << endl;
		cerr << "----------------------------------------"<< endl;
		cerr << *tmp << endl;
		cerr << "----------------------------------------"<< endl;
	}
	{
		control.push_back("types");
		auto tmp = newfmri.split(control);
		cerr << "new types split to: " << endl;
		cerr << "----------------------------------------"<< endl;
		cerr << *tmp << endl;
		cerr << "----------------------------------------"<< endl;
	}
	
	{
		control.push_back("subjects");
		auto tmp = newfmri.split(control);
		cerr << "new subjects split to: " << endl;
		cerr << "----------------------------------------"<< endl;
		cerr << *tmp << endl;
		cerr << "----------------------------------------"<< endl;
	}
	{
		control.push_back("frun");
		auto tmp = newfmri.split(control);
		cerr << "new fmri split to: " << endl;
		cerr << "----------------------------------------"<< endl;
		cerr << *tmp << endl;
		cerr << "----------------------------------------"<< endl;
	}
	
	control.clear();
	control.push_back("subjects");
	control.push_back("types");
	control.push_back("times");
	auto freduce = newfmri.split(control);;
	cerr << "freduce: "<< *freduce << endl;
	cerr << "newfmri: "<< newfmri << endl;

	vector<string> s1(freduce->m_cols);
	vector<string> v1(freduce->m_cols);
	// find rows that match in original, should map to both fmri
	for(size_t cc=0; cc<freduce->m_cols; cc++) {
		s1[cc]=freduce->m_labels[cc];
	}

	list<int> match;
	for(size_t rr=0; rr<freduce->m_rows; rr++){
		for(size_t cc=0; cc<freduce->m_cols; cc++) {
			v1[cc]=freduce->gets(rr,cc);
		}
		cerr << "Matching Values: " << endl;
		for(size_t ii=0; ii<v1.size(); ii++) {
			cerr << v1[ii] << ":" << s1[ii]<< ",";
		}
		cerr << endl;
		newfmri.search(s1, v1, &match);
		for(auto it=match.begin(); it!=match.end(); it++){
			cerr << "Row: ";
			for(size_t cc=0; cc<newfmri.m_cols; cc++) {
				cerr << newfmri.gets(*it, cc) << ",";
			}
			cerr << endl;
		}
	}
}

