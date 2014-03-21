#include <tclap/CmdLine.h>
#include <string>
#include <iostream>

#include "chain.h"

using std::endl;
using std::cerr;
using std::string;

int main(int argc, char** argv)
{
	try {
	/* 
	 * Command Line 
	 */
	TCLAP::CmdLine cmd("This program runs a series of jobs in qsub or on the "
			"local machine. The jobs are described by a .chain file.", ' ', __version__ );

	// arguments
	TCLAP::ValueArg<std::string> a_chain("i","in","Input pipe",true,"pipe.chain","file");
	cmd.add(a_chain);

	// parse arguments
	cmd.parse(argc, argv);

	/*
	 * Main Processing
	 */
	auto filename = a_chain.getValue();
	
	Chain tree(filename);

	tree.simulate();
	//cerr << tree << endl;

	// done, catch all argument errors
	} catch (TCLAP::ArgException &e)  // catch any exceptions
	{ std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl; }

}

