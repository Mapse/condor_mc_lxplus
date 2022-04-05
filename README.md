# condor_mc_lxplus
This repository contains all codes to run the Monte Carlo analyis with condor on lxplus.

# To run

There are several codes that should be run separately. Below one can find the steps.

## Make path for the files

First one should create the text file with the paths to the *NanoAODPlus.root* files.

In order to do so, one need to run the script named *make_path.sh*.

There, one needs to provide the path (normally it is just a *pwd* command) where the *.txt* file should be produced. This should go on the variable *here*. Besides, one needs to provide the path where the *NanoAODPlus.root* files are. This should go on the variable *file_name*.

Note: It is important to consider how many 000x folders the MC production. For this version it is hardcoded: We consider from 0000/ to 0009/. Don't worry, you can simple change it or construct a better logic.

Finally, one can do:

    . make_path.sh
## Run condor

The second steps one need to run condor in order to produce *.coffea* files. First, activate *voms* certificate:

    voms-proxy-init -voms cms
    
Then, copy it to the current directory_

    cp /tmp/x509up_u128055 .

Now, the script *condor.py* can be run:

    python3 condor.py -n=*name* -s

Where *name* is the content of *file_name* without *_path.txt*

Example:

> If *file_name* is *Monte_Carlo_path.txt* then *name* should be Monte_Carlo:

    python3 condor.py -n=*Monte_Carlo* -s

After that, condor should run normally

> Comment: It is important follow the condor process by typing *condor_q user_name* and also look on the *output.err* files.

## Merge data and make histograms

In this step the files produced in the last step are going to be merged and moved to *OniaOpenCharmRun2ULAna* folder to produce the histogram files

The code that do that is *merged.sh*. There, one should provide the name of the folder where all files (coffea data and coffea histogram) are going to be moved. Remember that this folder is located at OniaOpenCharmRun2ULAna/output. The name of the folder is stored on the variable *name*.

> Comment: Be aware, always use the same processors version (histogram and event selection) in both cases, condor and  OniaOpenCharmRun2ULAna.

Finnaly, run the code:

    . merge.sh
    
If everthing went well, you should have your .coffea and hists.coffea on the folder OniaOpenCharmRun2ULAna/output/*name*



    
