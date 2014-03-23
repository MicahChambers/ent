#include <regex>
#include <iostream>
#include <sstream>

using namespace std;

const std::regex CurlyRe("\\{(\\*)?\\s*((?:([0-9.]*)\\s*:)?"
		"(?:\\s*([-,_[:alnum:]]*)\\s*)?(?:\\s*"
		"\\|([,_\\s[:alnum:]]*))?)\\s*\\}");
const std::sregex_iterator ReEnd;

int main()
{
	const int COUNT = 28;
	string tests[COUNT] = {
		"{}",
		"{ }",
		"{* }",
		"{*}",
		"{*| hello }",
		"{*| }",
		"{:}",
		"{ : }",
		"{*: }",
		"{*: test }",
		"{1.3: }",
		"{*1.3.: }",
		"{*1.3: }",
		"{*1.3.: | control, ton }",
		"{*1.3: | wrong | way}",
		"{*1.3: | right,  way}",
		"{0}",
		"{0-1,23,9}",
		"{:0-1,23,9}",
		"{1.3:0-1,23,9}",
		"{*0-1,23,9}",
		"{*:0-1,23,9}",
		"{* 19.3:0-1,23,9}",
		"{* 19.3:0-1,23,9}",
		"{*0-1,23,9 | , blank }",
		"{*:0-1,23,9 | h b spaces }",
		"{* 19.3:0-1,23,9, | more }",
		"{* 19.3:0-1,23,9 | who, this , is ,, a lot}"};
	smatch m;
	
	string giant;
	{
		std::ostringstream oss;
		for(unsigned int ii = 0 ; ii < COUNT; ii++) 
			oss << "  " << tests[ii];
		giant = oss.str();
	}

	cerr << giant << endl;

	sregex_iterator it(giant.cbegin(), giant.cend(), CurlyRe);
	for(int ii = 0; it != ReEnd; ii++, it++) {
		while((*it)[0] != tests[ii]) {
			cerr << "NO MATCH FOR " << tests[ii] << endl;
			ii++;
		}
		cerr << "------------------------------" << endl;
		cerr << tests[ii] << endl;
//		cerr << "Prefix " << it->prefix().str() << "'" << endl;
		cerr << "Match '" << (*it)[0].str() << "'" << endl;
		cerr << "Expand? '" << (*it)[1].str() << "'" << endl;
		cerr << "Variable: '" << (*it)[2].str() << "'" << endl;
		cerr << "Dep Number: '" << (*it)[3].str() << "'" << endl;
		cerr << "Arg Number: '" << (*it)[4].str() << "'" << endl;
		cerr << "Control: '" << (*it)[5].str() << "'" << endl;
//		cerr << "Suffix " << it->suffix().str() << "'" << endl;
		cerr << "------------------------------" << endl;
	}
}
