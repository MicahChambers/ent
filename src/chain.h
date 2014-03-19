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

	std::ostream& operator<<(std::ostream &o) 
	{
		return o <<  std::endl; 
	};

	class Input;
	class Proc;

private:
	std::map<std::vector<int>,std::shared_ptr<Proc>, vintComp> m_procs;
	std::map<std::vector<int>,std::shared_ptr<Input>, vintComp> m_inputs;
	std::map<std::string,std::list<std::string>> m_vars;

//	std::vector<int> getId(std::string);
	int m_err;
};

/*
 * Leaf Class
 * 
 * This class is the base class for Proc and Input
 */
class Chain::Leaf
{
	public:
		Leaf(std::string file, unsigned int line, string sid);
		Leaf();

		// nothing yet
		size_t getNProc();
		const std::vector<std::string>& getArgs(size_t proc);

	protected:
		std::vector<int> m_id;
		bool m_placeholder;
		std::string m_parsefile;
		unsigned int m_parseline;

		// for each process, vector of output files
		std::vector<std::vector<std::string>> m_outputs;
	
	friend Chain;
};

/* 
 * Proc Class
 *
 * Parses out input and creates output files for other Proc class
 *
 */
class Chain:: Proc : private Chain::Leaf
{
	public:
	Proc(std::string file, unsigned int line, std::string mixture,
		const std::vector<std::string>& filepatterns, 
		const std::vector<std::string>& reshapes, const Chain* parent);

	private: 

	/* Metadata */
	// [0..N-1,0..M-1] 
	// where N is the number of permutations of the metadata, and
	// M is the number of output files
	//
	// rows are permutations of metadata
	// columns are index metadata
	std::vector<std::vector<std::string>> m_metadata;
	
	// outer vector matches the m_metadata vector, inner is the
	// corresponding files, in the order listed in the ent file
	// rows are permutations of metadata
	// columns are index files
	std::vector<std::vector<std::string>> m_files; 
	
	// names of each of the metadata variables corresponding
	// to the outer vector of m_outvars;
	std::vector<std::string> m_outvars; 

	std::list<std::shared_ptr<Leaf>> m_children;	//run after
	
	std::list<std::shared_ptr<std::thread>> m_proc; // running process pointer

	// helper function that mixes metadata
	int mixtureToMetadata(std::string file, const Chain* parent);

	const Chain* m_parent;
	friend Chain;
};


/* 
 * Input Class
 *
 * Parses out input and creates output files for other Proc class
 *
 */
class Chain::Input : private Chain::Leaf
{
	public:
	Input(std::string file, unsigned int line, std::string mixture,
		const std::vector<std::string>& filepatterns, 
		const std::vector<std::string>& reshapes, const Chain* parent);

	private: 

	/* Metadata */
	// [0..N-1,0..M-1] 
	// where N is the number of permutations of the metadata, and
	// M is the number of output files
	//
	// rows are permutations of metadata
	// columns are index metadata
	std::vector<std::vector<std::string>> m_metadata;
	
	// outer vector matches the m_metadata vector, inner is the
	// corresponding files, in the order listed in the ent file
	// rows are permutations of metadata
	// columns are index files
	std::vector<std::vector<std::string>> m_files; 
	
	// names of each of the metadata variables corresponding
	// to the outer vector of m_outvars;
	std::vector<std::string> m_outvars; 

	std::list<std::shared_ptr<Leaf>> m_children;	//run after
	
	std::list<std::shared_ptr<std::thread>> m_proc; // running process pointer

	// helper function that mixes metadata
	int mixtureToMetadata(std::string file, const Chain* parent);

	const Chain* m_parent;
	friend Chain;
};

