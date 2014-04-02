#ifndef ENT_H
#define ENT_H


#include <unordered_map>

class Ent
{
	public:
	Ent();


	int m_err;

	private:

	unordered_map<string, File> m_files;
	unordered_map<string, list<string>> m_globals;
	list<LeafT> m_leafs;
	
	int parseEntV1(string filename);
};

class File
{
	public:
	File();
	string abs; //absolute path
	Ring* generator; //Ring that could generated this 
	Time

};

#endif //ENT_H
