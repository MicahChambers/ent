#ifndef ENT_H
#define ENT_H


#include <unordered_map>

class Ent
{
	public:
	Ent();


	int m_err;

	private:

	unordered_map<string, list<string>> m_globals;
	list<LeafT> m_leafs;
	
	int parseEntV1(string filename);
};

#endif //ENT_H
