
int CommandBuilder::getCommands(vector<Command>* out)
{
	if(!out) 
		return -1;

	vector<int> iter(m_ivs.size(), 0); //iterates through variations of IV's
	vecotr<vector<string>*> realiv(m_ivs.size());

	// resolve IV names to actual vectors
	for(size_t ii=0; ii<m_ivs.size(); ii++){
		auto mit = m_parent->m_globals.find(m_ivs[ii]);
		if(mit == m_parent->m_globals.end()) {
			cerr << "Uknown variable referenced. " << endl << m_loc << endl;
			return -1;
		}
		realiv[ii] = &mit->second;
	}

	bool done = false;
	while(!done) {

		// build line
		vector<string> 
		for(size_t aa=0; aa<
		for(size_t ii=0; ii<iter.size(); ii++){
			cerr << realiv[iter[ii]]  <<"," ;
		}
		cerr << endl;

		// iterate
		done = true;
		for(size_t ii=0; ii<iter.size(); ii++) {

			// if we can add one, do so
			if(iter[ii]+1 < realiv[ii]->size()) {
				iter[ii]++;
				done = false;
				break;
			} else {
				// otherwise set to 0 and continue (roll over)
				iter[ii] = 0;
			}
		}
	}

	return 0;
}


