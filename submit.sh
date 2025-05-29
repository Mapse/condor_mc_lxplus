#!/bin/bash

set -e

hostname
date

echo "---------------working dir---------------"
ls
#tar -zxf WORKDIR.tar.gz
#rm -rf WORKDIR.tar.gz
#cat *_jobs/*
mv *_jobs/ OniaOpenCharmRun2ULAna/.
echo $1

echo "Done"
echo "---------------Creating Python Environment---------------"

export HOME=$PWD
export PATH
sh Miniconda3-latest-Linux-x86_64.sh -b -p $PWD/miniconda3
export PATH=$PWD/miniconda3/bin:$PATH
rm -rf Miniconda3-latest-Linux-x86_64.sh
#python -m pip install --upgrade pip
#python -m pip install --upgrade --ignore-installed --force-reinstall coffea

eval "$($PWD/miniconda3/bin/conda shell.bash hook)"
conda create -n coffea-env python=3.9.7 -y
conda activate coffea-env

ls

echo "installing xrootd"
conda install -c conda-forge xrootd
#conda install -c conda-forge root=6.24.02

echo "creating env for specific coffea version"
#conda create -n coffea-en
#conda activate coffea-en

echo "installing coffea 0.7.7"
pip3 install uproot==4.1.8
pip3 install fsspec-xrootd
pip3 install coffea==0.7.7
pip3 install matplotlib==3.5.0
pip3 install mplhep==0.3.15
pip3 install uncertainties==3.1.6
pip3 install awkward==1.7.0  #2.4.6     #1.7.0
pip3 install numpy==1.22.4

export XRD_TIMEOUT=60

echo "Done"
echo "---------------Starting the processing---------------"

cd OniaOpenCharmRun2ULAna
ls -a

python nanoAODplus_condor.py $1

