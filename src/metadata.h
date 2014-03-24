#include <vector>
#include <string>
#include <list>
#include <iostream>
#include <map>
#include <cassert>

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

	MetaData(std::string label, const std::vector<std::string>& input)
	{
		m_rows=input.size();
		m_cols=1;
		m_lookup.resize(1);
		m_lookup[0].resize(m_rows);
		m_data.resize(m_rows*m_cols);
		m_labels.resize(1);
		m_labels[0] = label;
		m_search.resize(1);

		std::list<int> tmp;
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
	std::vector<int> m_data; 

	// [column][code] = "value of code"
	std::vector<std::vector<std::string>> m_lookup; 

	// [column]
	std::vector<std::string> m_labels;

	// [column][value] = list of rows with the given value
	std::vector<std::map<int,std::list<int>>> m_search; 

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

	std::string gets(size_t rr, size_t cc) {
		assert(rr < m_rows && cc < m_cols);
		return m_lookup[cc][m_data[rr*m_cols+cc]];
	}
	
	friend std::ostream& operator<<(std::ostream& os, const MetaData& md);
};

