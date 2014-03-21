#include <iostream>
#include <memory>
#include <future>
#include <list>
#include <cmath>
#include <unordered_map>
#include <map>

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

template<typename C>
class Hash
{
public:
	std::size_t operator()(C const& v) const 
	{
		std::size_t h = 0;
		for(auto it = v.begin(); it != v.end(); it++) {
			h = (std::hash<typename C::value_type>()(*it))^(h << 1);
		}
		return h;
	}
};

/* 
 * Chain Class
 *
 * This class is used to store the entire tree for a processing 
 * pipeline
 */
class Chain
{
public:
	Chain(std::string filename);
	int parseFile(std::string filename);

	std::ostream& operator<<(std::ostream &o) 
	{
		return o <<  std::endl; 
	};

	class Link;
	
	int resolveTree();

private:
	std::map<std::vector<int>,std::shared_ptr<Link>, vintComp> m_links;
	std::map<std::string,std::list<std::string>> m_vars;

//	std::vector<int> getId(std::string);
	int m_err;
};

/*
 * Link Class
 * 
 * This class is the base class for Proc and Input
 */
class Chain::Link
{
	public:
		class Argument;
		enum NodeType {INPUT, PROC};

		//process initialize
		Link(std::string sourcefile, unsigned int line, 
				std::string sid, std::string defsource, 
				std::string inspec, std::string outspec, std::string cmd,
				Chain* parent); 

		//input initialize
		Link(std::string sourcefile, unsigned int line, 
				std::string sid, std::string defsource, 
				std::string inspec, std::string outspec, 
				Chain* parent);

		// for traversal
		bool m_visited;
		bool m_resolved;
		bool m_populated;
		bool m_success;
		
		// command
		const std::string m_cmd;

		// basic info
		const std::string m_sourcefile;
		const unsigned int m_line;
		const std::vector<int> m_id;
		const std::string m_sid;
		const NodeType m_type;
		const std::string m_prevLink;
		
		int m_err;

		int resolveExternal(std::list<std::string>& stack);
		int resolveTree(std::list<std::string>& stack);
		int populate();

		int run();


	protected:

		// J x A = jobs x args
		std::vector<Argument> m_arguments;
		std::vector<std::vector<std::string>> m_inputs;
		std::vector<std::vector<std::string>> m_outputs;

		// J x M = jobx x relavent metadata
		// [run1,subject1]
		// [run1,subject2]
		// [run2,subject1]
		// [run2,subject2] ...
		std::vector<std::vector<std::string>> m_metadata;

		// labels for columns of m_metadata, 
		std::vector<std::string> m_labels;					// column -> label
		std::unordered_map<std::string, int> m_revlabels; 	// label -> colum

		// lookup table for jobs, 
		// M x N x J' = rows indexed by varnmae corresponding to 
		// m_labels, N indexed by particular value of metadata from
		// first index, interior vector is a list of jobs indexes
		// that have the metadata value pair specified
		// 
		// so to find a subject named subject1,
		// int ii = m_labels.find('subject')
		// int jj = m_index[ii].find('subject1')
		// m_index[ii][jj] = jobs where subject=subject1
		std::vector<std::vector<std::vector<int>>> m_index;

		// inputs and outputs before they have been mapped to particular 
		// jobs/metadata, ie still has {var} values
		std::list<std::string> m_preinputs;
		std::list<std::string> m_preoutputs;
		std::list<std::list<std::string>> m_postinputs;
		std::list<std::list<std::string>> m_postoutputs;

		const Chain* m_parent;
		friend Chain;

		int mixtureToMetadata(std::string spec, const Chain* parent);
		int resolveInternal();

};
