#include <iostream>
#include <memory>
#include <future>
#include <list>

class Ent
{
public:
	Ent(std::string filename);

	std::ostream& operator<<(std::ostream &o) 
	{
		return o <<  std::endl; 
	};

	class Leaf
	{
		bool visited = false;
		std::string cmd;				// command to be run
		std::shared_ptr<std::thread> proc; 	// running process pointer
		std::list<std::shared_ptr<Leaf>> parents;	//run before
		std::list<std::shared_ptr<Leaf>> children;	//run after
	};

private:
	std::list<std::shared_ptr<Leaf>> dag;
};

