''' 

This script is used before merge_coffea in order to merge only data files (not histograms!)
Then, the merged content is moved to the folder on the conda work area and the data file is read to produce the histograms files

'''

name=Monte_Carlo_2017_new_cuts

rm -rf outputs/

mkdir outputs/

mv *.coffea output/

echo 'merging data files.. (For MC 2017 this takes around 8 minutes)'
python3 merge_coffea.py -d

echo 'moving files to process area'

mv data_Monte_Carlo_2017.coffea $name.coffea

mv $name.coffea /afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/OniaOpenCharmRun2ULAna/output

cd /afs/cern.ch//work/m/mabarros/public/CMSSW_10_6_12/src/OniaOpenCharmRun2ULAna/output

rm -r $name

mkdir $name

mv $name.coffea $name

cd ..

python3 nanoAODplus_analyzer.py -n="$name" -p -c
