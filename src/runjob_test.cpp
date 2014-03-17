// launch::async vs launch::deferred
#include <iostream>     // std::cout
#include <cstdlib>       
#include <vector>       

#include <future>
#include <thread>
#include <chrono>

using std::future;
using std::async;
using std::vector;
using std::endl;
using std::string;
using std::cerr;

int main (int argc, char** argv)
{
	printf ("Checking if processor is available...");
	if(system(NULL)) {
		cerr << "It is!" << endl;
	} else {
		cerr << "Error your operating system is not supported" << endl;
		exit(EXIT_FAILURE);
	}

	vector<string> jobs;
	vector<int> jobstat;

	vector<future<int>> running;
	for(int ii = 1 ; ii < argc; ii++) {
		cerr << "Launching: \"" << argv[ii] << "\"" << endl;
		running.push_back(async(std::launch::async, system, argv[ii]));
		jobs.push_back(argv[ii]);
		jobstat.push_back(std::numeric_limits<int>::min());
	}

	auto tick = std::chrono::milliseconds(100);
	bool alldone = false;

	/* 
	 * Continue Polling Until All the Jobs are Done. Since we don't know the order
	 * they will finish in.
	 */
	while(!alldone) {
		alldone = true;

		/* 
		 * Check on each job
		 */
		for(unsigned int ii = 0; ii != running.size(); ii++) {

			// job done already so ignore
			if(jobstat[ii] != std::numeric_limits<int>::min())
				continue;

			// check to see if jobs is done
			auto status = running[ii].wait_for(tick);

			// if its done, add return to jobstat
			if(status == std::future_status::ready) {
				jobstat[ii] = running[ii].get();
				cerr << "Job: \"" << jobs[ii] << "\" Finished with status: " 
					<< jobstat[ii] << endl;
			} else {
				alldone = false;
			}
		}
	}

	cerr << "All done. Summary:" << endl;
	for(unsigned int ii = 0; ii != running.size(); ii++) {
		cerr << "\tJob : \"" << jobs[ii] << "\" Finished with status: " 
			<< jobstat[ii] << endl;
	}

	return 0;
}
