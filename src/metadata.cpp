#include "metadata.h"

#include <iomanip>
#include <sstream>
#include <set>
#include <memory>
#include <unordered_set>

using namespace std;

//#define VERYDEBUG


struct vintComp {
	bool operator() (const std::vector<int>& lhs, const std::vector<int>& rhs) const
	{
		for(unsigned int ii = 0; ii < std::min(lhs.size(), rhs.size()); ii++) {
			if(lhs[ii] < rhs[ii])
				return true;
			if(lhs[ii] > rhs[ii])
				return false;
		}
		
		// if all members are identical, then whichever is shorter is lesser
		return lhs.size() < rhs.size();
	}
};

int MetaData::search(vector<string> vars, vector<string> vals, list<int>* out)
{
	if(!out)
		return 0;
	out->clear();
	if(vars.size() != vals.size()) {
		return -1;
	}
	
	bool found = true;
	bool valid = true;
	// resolve column names to column numbers
	vector<int> cols;
	vector<int> ivals;
	for(size_t jj=0; jj<vars.size(); jj++) {
		found = false;
		valid = true;
		for(size_t ii=0; ii<m_labels.size(); ii++) {
			if(vars[jj] == m_labels[ii]) {
				// found a match between labels and requested variables
				cols.push_back(ii);

				found = true;
				valid = false;
				// try to find the value from vals that matches from m_lookup
				for(size_t kk=0; kk<m_lookup[ii].size(); kk++){
					if(m_lookup[ii][kk] == vals[jj]) {
						valid = true;
						ivals.push_back(kk);
					}
				}
				break;
			}
		}
		if(!found) {
			cerr << "Warning, controlling for a variable that does not exist" 
				<< " in current metadata." << endl;
		}
		if(!valid) {
			cerr << "Error, unknown value (" << vals[jj] << 
				") for column : " << cols.back() << endl;
			return -1;
		}
	}

#ifdef VERYDEBUG
	cerr << endl;
	for(auto vv:vars)
		cerr << vv <<",";
	cerr << endl;
	for(auto vv:vals)
		cerr << vv <<",";
	cerr << endl;
	for(auto vv:cols)
		cerr <<vv<<",";
	cerr << endl;
	for(auto vv:ivals)
		cerr <<vv<<",";
	cerr << endl;
#endif// VERYDEBUG

	if(cols.empty()) {
		out->clear();
		return 0;
	}

	// trying to find the columns that match all the overlapping metadata
	// we start with the first list of matching metadata,
	// then reduce the list size with each successive metadata
	// pass
	
	// initialize to the first;
	(*out) = m_search[cols[0]][ivals[0]];
	out->sort();
	for(size_t ii=1; ii<cols.size(); ii++){
#ifdef VERYDEBUG
		cerr << "Current: " << endl;
		for(auto vv:*out)
			cerr <<vv<<",";
		cerr << endl;
#endif VERYDEBUG

		auto l2 = m_search[cols[ii]][ivals[ii]];
		l2.sort();
		auto it2 = l2.begin();
		auto it1 = out->begin();
		while(it1 != out->end()) {
			if(it2 == l2.end() || *it1 < *it2) {
				it1 = out->erase(it1);
			} else if(*it1 > *it2) {
				it2++;
			} else {
				it1++;
				it2++;
			}
		}
	}
#ifdef VERYDEBUG
	cerr << "Current: " << endl;
	for(auto vv:*out)
		cerr <<vv<<",";
	cerr << endl;
#endif
	return 0;
}

shared_ptr<MetaData> MetaData::split(const list<string>& control)
{
	// resolve column names to column numbers
	list<int> cols;
	for(auto it=control.begin(); it!=control.end(); it++) {
		bool found = false;
		for(size_t ii=0; ii<m_labels.size(); ii++) {
			if(*it == m_labels[ii]) {
				cols.push_back(ii);
				found = true;
				break;
			}
		}
		if(!found) {
			cerr << "Warning, controlling for a variable that does not exist" 
				<< " in current metadata." << endl;
		}
	}

	if(cols.empty()) {
		vector<string> tmp(1,"dummy");
		return shared_ptr<MetaData>(new MetaData("dummy", tmp));
	}

	// find unique rows after only keeping control cols
	std::set<vector<int>,vintComp> uniquerows;
	vector<int> values(cols.size());
	for(size_t ii=0; ii<m_rows; ii++){
		auto it=cols.begin();
		for(size_t cc=0; it!=cols.end(); cc++,it++){
			values[cc] = geti(ii, *it);
		}
		uniquerows.insert(values);
	}
#ifndef NDEBUG
	for(auto it=uniquerows.begin(); it!=uniquerows.end(); it++){
		cerr << "Unique Row:";
		for(auto it2=it->begin(); it2!=it->end(); it2++)
			cerr << *it2 << ',';
		cerr << endl;
	}
#endif//NDEBUG

	// create output data
	shared_ptr<MetaData> out(new MetaData(cols.size(), uniquerows.size()));

	auto it=uniquerows.begin();
	for(size_t rr=0; it!=uniquerows.end(); rr++,it++){
		for(size_t cc=0; cc<out->m_cols; cc++) {
			out->seti(rr, cc, (*it)[cc]);
		}
	}

	auto it2=cols.begin();
	for(size_t cc=0; cc<out->m_cols; it2++,cc++){
		out->m_labels[cc] = m_labels[*it2];
		out->m_lookup[cc] = m_lookup[*it2];
	}

	// set up search
	for(unsigned int rr=0 ; rr<out->m_rows; rr++) {
		for(unsigned int cc=0 ; cc<out->m_cols; cc++) {
			out->m_search[cc][out->geti(rr,cc)].push_back(rr);
		}
	}

	return out;
}

/* This needs more rigorous testing */
int MetaData::ujoin(MetaData& rhs)
{
	if(&rhs == this) {
		cerr << "Error, can't nest metadat with itself" << endl;
		return -1;
	}

	// we are empty, just copy the other guy
	if(m_cols == 0 || m_rows == 0) {
		m_cols = rhs.m_cols;
		m_rows = rhs.m_rows;

		m_data = rhs.m_data;
		m_lookup = rhs.m_lookup;
		m_labels = rhs.m_labels;
		m_search = rhs.m_search;
		return 0;
	}
	if(rhs.m_rows == 0 || rhs.m_cols == 0) 
		return 0;

	size_t oldcols = m_cols;
	size_t oldrows = m_rows;

	// for each original column map to -1 or the column in rhs
	vector<int> colmatch(m_labels.size(), -1);
	
	list<int> uniqueCols; // columns only in rhs

	// create new array of labels
	for(size_t ii=0; ii<rhs.m_labels.size(); ii++) {
		bool match = false;
		for(size_t jj=0; jj<m_labels.size(); jj++) {
			if(rhs.m_labels[ii] == m_labels[jj]) {
				if(colmatch[jj] != -1) {
					cerr << "two columns have the identical labels in input!";
					return -1;
				}
				colmatch[jj] = ii;
				match = true;
			}
		}

		if(!match) 
			uniqueCols.push_back(ii);
	}

	if(uniqueCols.size() == rhs.m_cols) {
		cerr << "Error, no metadata match between requested merge";
		return -1;
	}

	// for each row in original, find our value and the matching column in rhs
	vector<list<int>> rhsMatches(oldrows);
	size_t newrows = 0;
	for(size_t rr=0; rr<oldrows; rr++) {
		
		//TODO just use the search function
		// .first = column in rhs of matching tag .second = value in original 
		list<pair<int,int>> limits; 
		for(size_t cc=0; cc<colmatch.size(); cc++) {
			if(colmatch[cc]>=0) {
				limits.push_back(make_pair(colmatch[cc],geti(rr,cc)));
			}
		}

		// trying to find the columns that match all the overlapping metadata
		// we start with the first list of matching metadata,
		// then reduce the list size with each successive metadata
		// pass
		rhsMatches[rr] = rhs.m_search[limits.back().first][limits.back().second];
		limits.pop_back();
		while(!limits.empty()) {
			auto& l2 = rhs.m_search[limits.back().first][limits.back().second];
			auto it1 = rhsMatches[rr].begin();
			
			auto it2 = l2.begin();
			while(it1 != rhsMatches[rr].end() && it2 != l2.end()) {
				if(*it1 < *it2) {
					it1 = rhsMatches[rr].erase(it1);
				} else if(*it1 > *it2) {
					it2++;
				} else {
					it1++;
					it2++;
				}
			}
			limits.pop_back();
		}

		for(auto it = rhsMatches[rr].begin(); it != rhsMatches[rr].end(); it++) 
			newrows++;
	}

	// create the new datastructure
	m_rows = newrows;
	m_cols += uniqueCols.size();
	vector<int> olddata(m_cols*m_rows);
	std::swap(olddata, m_data);

	// for each original row, and row in rhs that matches the 
	// original row...
	for(size_t rr=0,r1=0; r1<rhsMatches.size(); r1++) {
		for(auto it=rhsMatches[r1].begin(); it!=rhsMatches[r1].end(); rr++,it++) {

			// add the colums from the rhs row to the original columns,
			size_t cc;
			for(cc=0; cc < oldcols; cc++) {
				m_data[rr*m_cols + cc] = olddata[r1*oldcols+cc];
			}
			for(auto it2=uniqueCols.begin(); cc < m_cols; it2++,cc++) {
				m_data[rr*m_cols + cc] = rhs.geti(*it,*it2);
			}
		}
	}

	// update labels and lookup
	auto it3 = uniqueCols.begin();
	m_labels.resize(m_cols);
	m_lookup.resize(m_cols);
	for(size_t cc = oldcols; cc < m_cols; it3++,cc++) {
		m_labels[cc] = rhs.m_labels[*it3];
		m_lookup[cc] = rhs.m_lookup[*it3];

	}
	
	// there is probably a faster way to handle search updating...
	m_search.clear();
	m_search.resize(m_cols);
	for(unsigned int cc=0 ; cc<m_cols; cc++) {
		for(unsigned int rr=0 ; rr<m_rows; rr++) {
			m_search[cc][geti(rr,cc)].push_back(rr);
		}
	}

	return 0;
}

int MetaData::nest(const MetaData& rhs)
{
	if(&rhs == this) {
		cerr << "Error, can't nest metadat with itself" << endl;
		return -1;
	}

	// we are empty, just copy the other guy
	if(m_cols == 0 || m_rows == 0) {
		m_cols = rhs.m_cols;
		m_rows = rhs.m_rows;

		m_data = rhs.m_data;
		m_lookup = rhs.m_lookup;
		m_labels = rhs.m_labels;
		m_search = rhs.m_search;
		return 0;
	}
	if(rhs.m_rows == 0 || rhs.m_cols == 0) 
		return 0;

	m_lookup.resize(m_cols+rhs.m_cols);
	m_labels.resize(m_cols+rhs.m_cols);
	for(size_t ii=m_cols; ii<m_cols+rhs.m_cols; ii++){
		m_lookup[ii] = rhs.m_lookup[ii-m_cols];
		m_labels[ii] = rhs.m_labels[ii-m_cols];
	}

	size_t oldcols = m_cols;
	size_t oldrows = m_rows;
	m_cols += rhs.m_cols;
	m_rows *= rhs.m_rows;

	vector<int> tmpdata(m_rows*m_cols);
	std::swap(tmpdata, m_data);

	for(unsigned int ii = 0 ; ii < oldrows; ii++) {
		for(unsigned int jj = 0 ; jj < rhs.m_rows; jj++) {
			for(unsigned int kk = 0 ; kk < oldcols; kk++)
				this->geti(ii*rhs.m_rows+jj, kk) = tmpdata[ii*oldcols+kk];

			for(unsigned int kk = 0 ; kk < rhs.m_cols; kk++)
				this->geti(ii*rhs.m_rows+jj, oldcols+kk) = rhs.geti(jj,kk);
		}
	}

	// there is probably a faster way to handle search updating
	m_search.clear();
	m_search.resize(m_cols);
	for(unsigned int rr=0 ; rr<m_rows; rr++) {
		for(unsigned int cc=0 ; cc<m_cols; cc++) {
			m_search[cc][geti(rr,cc)].push_back(rr);
		}
	}

	return 0;
}

int MetaData::zip(const MetaData& rhs)
{
	// we are empty, just copy the other guy
	if(m_cols == 0 || m_rows == 0) {
		m_cols = rhs.m_cols;
		m_rows = rhs.m_rows;

		m_data = rhs.m_data;
		m_lookup = rhs.m_lookup;
		m_labels = rhs.m_labels;
		m_search = rhs.m_search;
		return 0;
	}
	if(rhs.m_rows == 0 || rhs.m_cols == 0)
		return 0;

	if(m_rows != rhs.m_rows) {
		cerr << "Error during zipping. Input vectors must be the same size" 
			<< endl;
		return -1;
	}

	m_lookup.resize(m_cols+rhs.m_cols);
	m_labels.resize(m_cols+rhs.m_cols);
	for(size_t ii=m_cols; ii<m_cols+rhs.m_cols; ii++){
		m_lookup[ii] = rhs.m_lookup[ii-m_cols];
		m_labels[ii] = rhs.m_labels[ii-m_cols];
	}

	size_t oldcols = m_cols;
	m_cols += rhs.m_cols;

	vector<int> tmpdata(m_rows*m_cols);
	std::swap(tmpdata, m_data);
	for(unsigned int ii = 0 ; ii < m_rows; ii++) {
		for(unsigned int kk = 0 ; kk < oldcols; kk++)
			geti(ii,kk) = tmpdata[ii*oldcols+kk];

		for(unsigned int kk = 0; kk < rhs.m_cols; kk++)
			geti(ii, oldcols+kk) = rhs.geti(ii,kk);
	}
	
	// there is probably a faster way to handle search updating
	m_search.clear();
	m_search.resize(m_cols);
	for(unsigned int rr=0 ; rr<m_rows; rr++) {
		for(unsigned int cc=0 ; cc<m_cols; cc++) {
			m_search[cc][geti(rr,cc)].push_back(rr);
		}
	}

	return 0;
}

ostream& operator<<(ostream& os, const MetaData& md)
{
	os << "MetaData:"; 
	os << md.m_rows << "x" << md.m_cols << endl << endl;

	for(size_t cc=0 ; cc<md.m_cols; cc++) {
		os << std::setw(10) << md.m_labels[cc];
	}
	os << endl;

	for(size_t rr=0 ; rr<md.m_rows; rr++) {
		for(size_t cc=0 ; cc<md.m_cols; cc++) {
			os << std::setw(10) << md.geti(rr, cc);
		}
		os << endl;
	}
	
	os << "Converions: " << endl;
	for(size_t cc=0 ; cc<md.m_cols; cc++) {
		os << std::setw(10) << md.m_labels[cc];
		for(size_t rr=0 ; rr<md.m_lookup[cc].size(); rr++) {
			os << std::setw(5) << rr << "=" << setw(5) << md.m_lookup[cc][rr];
			if(rr+1 != md.m_lookup.size()) os << ",";;
		}
		os << endl;
	}

	os << "Search: " << endl;
	for(size_t cc=0; cc<md.m_search.size(); cc++) {
		os << std::setw(10) << md.m_labels[cc] << endl;
		for(auto it=md.m_search[cc].begin(); it!=md.m_search[cc].end(); it++) {
			os << "\t" << std::setw(5) << it->first << "->";
			for(auto it2 = it->second.begin(); it2!=it->second.end(); it2++) {
				os << *it2 << ",";
			}
			os << endl;
		}
		os << endl;
		
	}
	return os;
}


