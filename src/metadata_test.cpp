#include <regex>
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <set>
#include <cassert>

using namespace std;


class MetaData
{
public:
	MetaData() {
		m_cols = 0;
		m_rows = 0;
		m_lookup.clear();
		m_data.clear();
		m_labels.clear();
		m_search.clear();
	};

	~MetaData(){
		cerr << *this << endl;
		m_lookup.clear();
		m_data.clear();
		m_labels.clear();
		m_search.clear();
	}

	MetaData(string label, const vector<string>& input)
	{
		m_rows=input.size();
		m_cols=1;
		m_lookup.resize(1);
		m_lookup[0].resize(m_rows);
		m_data.resize(m_rows*m_cols);
		m_labels.resize(1);
		m_labels[0] = label;
		m_search.resize(1);

		list<int> tmp;
		tmp.push_back(0);
		for(size_t rr=0; rr<m_rows; rr++) {
			m_data[rr]=rr;
			m_lookup[0][rr] = input[rr];

			tmp.back() = rr;
			m_search[0][rr] = tmp;
		}
	};

	int nest(const MetaData& rhs);
	int zip(const MetaData& rhs);
	int merge(MetaData& rhs);
	
	
	/* Data Structures */
	// [row*m_cols+column]
	vector<int> m_data; 

	// [column][code] = "value of code"
	vector<vector<string>> m_lookup; 

	// [column]
	vector<string> m_labels;

	// [column][value] = list of rows with the given value
	vector<map<int,list<int>>> m_search; 

	size_t m_rows;
	size_t m_cols;

	const int& geti(size_t rr, size_t cc) const {
		assert(rr < m_rows && cc < m_cols);
		return m_data[rr*m_cols+cc];
	}

	int& geti(size_t rr, size_t cc) {
		assert(rr < m_rows && cc < m_cols);
		return m_data[rr*m_cols+cc];
	}

	string gets(size_t rr, size_t cc) {
		assert(rr < m_rows && cc < m_cols);
		return m_lookup[cc][m_data[rr*m_cols+cc]];
	}
	
	friend ostream& operator<<(ostream& os, const MetaData& md);
};

int MetaData::merge(MetaData& rhs)
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

	cerr << "This: " << *this << endl;
	cerr << "Other: " << rhs << endl;
	vector<list<int>> rhsMatches(oldrows);

	cerr << "Colmatch (" << colmatch.size() << ")" << endl;
	for(auto vv: colmatch) {
		cerr << "\t" << vv << endl;
	}

	// for each row in original, find our value and the matching column in rhs
	size_t newrows = 0;
	for(size_t rr=0; rr<oldrows; rr++) {
		
		// .first = column in rhs of matching tag .second = value in original 
		list<pair<int,int>> limits; 
		for(size_t cc=0; cc<colmatch.size(); cc++) {
			if(colmatch[cc]>=0) {
				limits.push_back(make_pair(colmatch[cc],geti(rr,cc)));
			}
		}

		cerr << "Matching" << endl;
		for(auto vv : limits) {
			cerr << " col of rhs: " << vv.first << "input #:" << vv.second << endl;
		}

		// trying to find the columns that match all the overlapping metadata
		rhsMatches[rr] = rhs.m_search[limits.back().first][limits.back().second];
		limits.pop_back();
		while(!limits.empty()) {
			auto& l2 = rhs.m_search[limits.back().first][limits.back().second];
			cerr << "List: ";
			for(auto vv : rhsMatches[rr]) 
				cerr << vv << ", ";
			cerr << endl;
			auto it1 = rhsMatches[rr].begin();
			
			cerr << "List: ";
			for(auto vv : l2) 
				cerr << vv << ", ";
			cerr << endl;
			auto it2 = l2.begin();
			for(; it1 != rhsMatches[rr].end() && it2 != l2.end(); it1++, it2++) {
				cerr << *it1 << ", " << *it2 << endl;
				if(*it1 < *it2) {
					it1 = rhsMatches[rr].erase(it1);
				} else if(*it1 > *it2) {
					it2++;
				}
			}
			cerr << "Reduced List: ";
			for(auto vv : rhsMatches[rr]) 
				cerr << vv << ", ";
			cerr << endl;
			limits.pop_back();
		}

		// l1 now has the rows of rhs that match metadata in rr
		cerr << "Matching rows to " << rr << endl;

		for(auto it = rhsMatches[rr].begin(); it != rhsMatches[rr].end(); it++) 
			newrows++;
	}

	m_rows = newrows;
	m_cols += uniqueCols.size();
	vector<int> olddata(m_cols*m_rows);
	std::swap(olddata, m_data);

	for(size_t rr=0,r1=0; r1<rhsMatches.size(); r1++) {
		for(auto it=rhsMatches[r1].begin(); it!=rhsMatches[r1].end(); rr++,it++) {
			size_t cc;
			for(cc=0; cc < oldcols; cc++) {
				m_data[rr*m_cols + cc] = olddata[r1*oldcols+cc];
			}
			for(auto it2=uniqueCols.begin(); cc < m_cols; it2++,cc++) {
				m_data[rr*m_cols + cc] = rhs.geti(*it,*it2);
			}
		}
	}

	auto it3 = uniqueCols.begin();
	m_labels.resize(m_cols);
	m_lookup.resize(m_cols);
	for(size_t cc = oldcols; cc < m_cols; it3++,cc++) {
		m_labels[cc] = rhs.m_labels[*it3];
		m_lookup[cc] = rhs.m_lookup[*it3];

	}

//	m_lookup.resize(m_cols+rhs.m_cols);
//	m_labels.resize(m_cols+rhs.m_cols);
//	for(size_t ii=m_cols; ii<m_cols+rhs.m_cols; ii++){
//		m_lookup[ii] = rhs.m_lookup[ii-m_cols];
//		m_labels[ii] = rhs.m_labels[ii-m_cols];
//	}
//
//	size_t oldcols = m_cols;
//	size_t oldrows = m_rows;
//	m_cols += rhs.m_cols;
//	m_rows *= rhs.m_rows;
//
//	vector<int> tmpdata(m_rows*m_cols);
//	std::swap(tmpdata, m_data);
//
//	for(unsigned int ii = 0 ; ii < oldrows; ii++) {
//		for(unsigned int jj = 0 ; jj < rhs.m_rows; jj++) {
//			for(unsigned int kk = 0 ; kk < oldcols; kk++)
//				this->geti(ii*rhs.m_rows+jj, kk) = tmpdata[ii*oldcols+kk];
//
//			for(unsigned int kk = 0 ; kk < rhs.m_cols; kk++)
//				this->geti(ii*rhs.m_rows+jj, oldcols+kk) = rhs.geti(jj,kk);
//		}
//	}
//
	// there is probably a faster way to handle search updating
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
	os << "Metadata:"; 
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

int main(int argc, char** argv)
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
}

