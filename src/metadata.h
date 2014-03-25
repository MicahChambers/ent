#include <vector>
#include <string>
#include <list>
#include <iostream>
#include <map>
#include <cassert>
#include <memory>

class MetaData
{
public:
	
	/**
	 * @brief This form only does datastructure initialization since there
	 * are no known values to put in the data structures.
	 *
	 * @param cols
	 * @param rows
	 */
	MetaData(size_t cols = 0, size_t rows = 0) 
	{
		m_cols = cols;
		m_rows = rows;
		m_lookup.clear();
		m_lookup.resize(cols);
		
		m_data.clear();
		m_data.resize(cols*rows);

		m_labels.clear();
		m_labels.resize(cols);

		m_search.clear();
		m_search.resize(cols);
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

	shared_ptr<MetaData> splitApart(const std::list<std::string>& control,
			std::vector<std::list<int>>* simul);
	int nest(const MetaData& rhs);
	int zip(const MetaData& rhs);
	int ujoin(MetaData& rhs);

	// TODO reimplement split, should basically removed non-split cols
	int split(const std::list<std::string>& control) const;
	
	/* Data Structures */
	// [row*m_cols+column]
	std::vector<int> m_data; 

	// [column][code] = "value of code"
	std::vector<std::vector<std::string>> m_lookup; 

	// [column]
	std::vector<std::string> m_labels;

	// [column][value] = list of rows with the given value
	std::vector<std::map<int,std::list<int>>> m_search; 
	
	// TODO implement search
	int search(std::vector<std::string> vars, std::vector<std::string> vals,
			std::list<int>* out); 
	int search(std::vector<std::string> vars, std::vector<int> vals,
			std::list<int>* out); 

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

private:
	int splitHelp(const std::vector<std::shared_ptr<MetaData>>& in);

};

