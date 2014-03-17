#include <tclap/CmdLine.h>
#include <string>
#include <iostream>

#include "ent.h"

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
			"local machine. The jobs are described by a .ent file.", ' ', __version__ );

	// arguments
	TCLAP::ValueArg<std::string> a_ent("i","in","Input pipe",true,"pipe.ent","file");
	cmd.add(a_ent);

	// parse arguments
	cmd.parse(argc, argv);

	/*
	 * Main Processing
	 */
	auto filename = a_ent.getValue();
	
	Ent tree(filename);

	//cerr << tree << endl;

	// done, catch all argument errors
	} catch (TCLAP::ArgException &e)  // catch any exceptions
	{ std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl; }

}

