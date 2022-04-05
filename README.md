# condor_mc_lxplus
This repository contains all codes to run the Monte Carlo analyis with condor on lxplus.

# To run

First one should create the text file with the paths to the *NanoAODPlus.root* files.

In order to do so, one need to run the script named *make_path.sh*.

There, one needs to provide the path (normally it is just a *pwd* command) where the *.txt* file should be produced. This should go on the variable *here*. Besides, one needs to provide the path where the *NanoAODPlus.root* files are. This should go on the variable *file_name*.

Note: It is important to consider how many 000x folders the MC production. For this version it is hardcoded: We consider from 0000/ to 0009/. Don't worry, you can simple change it or construct a better logic.

Finally, one can do:

  . make_path.sh
