# ent
A branching processing 'pipeline'.

## Overview
Processing pipelines are called trees, and they have an extension *.ent. Ent files will be a variant of Makefiles, with extensions to indicate how multiple inputs and outputs get forked into multiple jobs (or are produced from a single job).

## Initial Plan
Users will initiat trees by running ent on the qsub server then daemonizing ent. Eventually it might be worth having a master daemon that manages all of a user's trees but for now each tree will be run by a single ent session.
