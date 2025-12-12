# Processing nanoAODPlus files - SPS and DPS Monte Carlo
This repository contains all codes to run the Monte Carlo analysis with condor on lxplus.
The input files are in the nanoAODPlus format and are produced using this [repository](https://github.com/Mapse/NanoAOD). The output files
are in the _.coffea_ format.

## Files used in the processing

* quick_setup.sh
* get_files_xrootd.py
* condor.py
* jobs_template.jdl
* submit.sh
* config_files.py

The first thing to do here is to activate the conda enviroment with the files being used and use your grid credentials. These two things are
performed using:

``` quick_setup.sh ```

Now, you need to identify the path of your files. Using T2_Caltec_US as an example, the  basic command used for this is,

``` xrdfs k8s-redir.ultralight.org:1094 ls -u /store/group/uerj/mabarros ```

This is implemented in the get_files_xrood.py script. To run it, just do:

``` python3 get_files_xrootd.py ```

Then, you just need to pick up the sample you want to obtain the txt file for condor (the options you see below will be available when you run the get_files_xrootd.py ):

```
1: 2016-pre-VFP-DPS-ccbar
2: 2016-pre-VFP-DPS-bbbar    
3: 2016-pos-VFP-DPS-ccbar
4: 2016-pos-VFP-DPS-bbbar
5: 2017-DPS-ccbar
6: 2017-SPS-3FS-ccbar
7: 2017-SPS-3FS-bbbar
8: 2017-DPS-bbbar
9: 2018-DPS-ccbar
10: 2018-DPS-bbbar
```
For instance, if you type '14' you will create three .txt files:

```
D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO_path.txt
D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO_path.txt
D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO_path.txt
```
Before lunching the processing, it is important to guarantee that your _x509userproxy_ is at the right place. To guarantee this, edit the path on
jobs_template.jdl file:

``` x509userproxy = /afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/condor/condor_mc_lxplus/x509up_u128055 ```

## Run condor

Before running the condor script, it is important to choose the dataset (2016APV, 2016, 2017, 2018). Go to OniaOpenCharmRun2ULAna/config/config_files.py and edit the variable _year_ to choose your dataset.
Finally, one need to run condor in order to produce *.coffea* files. In this example, the files refers to D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO_path.txt: 

```    python3 condor.py -n=D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO -s ```

After that, condor should run normally

> Comment: It is important follow the condor process by typing *condor_q user_name* and also look on the *output.err* files.

Finally, when your files are ready, you just need to seed then to another repository where the other steps of the anaylsis can be performed.
