import subprocess, os, re

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split(r'(\d+)', text) ]

def generate_path(mc, dataset,  crab_folder, n_folders, active_config, file_location):

    if file_location == 'Caltech':
        cmds = [ 
        f'xrdfs k8s-redir.ultralight.org:1094 ls -u /store/group/uerj/mabarros/{mc}/{dataset}/{crab_folder}/{i:04}/' for i in range(n_folders)
        ]
        print("Command used to get the paths: \n")
        print(cmds)
    elif file_location == 'CERNBOX':
        cmds = [ 
        f'xrdfs eosuser.cern.ch ls -u /eos/user/s/sfonseca/{mc}/{dataset}/{crab_folder}/{i:04}/' for i in range(n_folders)
        ]
        print("Command used to get the paths: \n")
        print(cmds)
        

    cat = ""
    out_file = dataset + "_path.txt"
    for i in cmds:
        os.system(f"{i} > {i.split('/')[-2]}")
        cat += f" {i.split('/')[-2]}"

    os.system(f"cat {cat} > {out_file}")
    os.system(f"rm -rf {cat}")
    file_list = open(out_file, 'r').readlines()

    for idx, f in enumerate(file_list):
        file_list[idx] = re.sub('transfer-\d*', 'transfer-12', f)

    list(set(file_list))
    file_list.sort(key=natural_keys)

    final_list = []
    for i in file_list:
        if i not in final_list:
            final_list.append(i)

    if '20UL' not in out_file:
            insert_text = f"20UL{active_config[2:4]}_"
            out_file = out_file.replace("_path.txt", f"_{insert_text}path.txt")
            
    with open(out_file, 'w') as f:

        for i in final_list:
            f.write(i)
            #print(i)
        print(f'File: {out_file}')

if __name__ == '__main__':
    
    fil_loc = {1: 'Caltech', 2: 'CERNBOX'}

    configurations = {
    1: "2016-pre-VFP-DPS-ccbar",
    2: "2016-pre-VFP-DPS-bbbar",    
    3: "2016-pos-VFP-DPS-ccbar",
    4: "2016-pos-VFP-DPS-bbbar",
    5: "2017-DPS-ccbar",
    6: "2017-SPS-3FS-ccbar",
    7: "2017-SPS-3FS-bbbar",
    8: "2017-DPS-bbbar",
    9: "2018-DPS-ccbar",
    10: "2018-DPS-bbbar",}  

    """ 3: "2016-pre-VFP-SPS-3FS-4FS-ccbar",
    4: "2016-pre-VFP-SPS-VFNS-ccbar",
    5: "2016-pre-VFP-SPS-3FS-4FS-bbbar",
    6: "2016-pre-VFP-SPS-VFNS-bbbar",
    9: "2016-pos-VFP-SPS-3FS-4FS-ccbar",
    10: "2016-pos-VFP-SPS-VFNS-ccbar",
    11: "2016-pos-VFP-SPS-3FS-4FS-bbbar",
    12: "2016-pos-VFP-SPS-VFNS-bbbar",
    15: "2017-SPS-3FS-4FS-ccbar",
    16: "2017-SPS-VFNS-ccbar",
    17: "2017-SPS-3FS-4FS-bbbar",
    18: "2017-SPS-VFNS-bbbar",
    21: "2018-SPS-3FS-4FS-ccbar",
    22: "2018-SPS-VFNS-ccbar",
    23: "2018-SPS-3FS-4FS-bbbar",
    24: "2018-SPS-VFNS-bbbar" """

    config_data = {
        
        ################ 2016-pre-VFP
        "2016-pre-VFP-DPS-ccbar": {
        "mc": [
            'DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
        ],
        "dataset": [
            'DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',
        ],
        "crab_folder": ['221021_225931', '220823_051146', '220823_051151'],
        "n_folders": [1, 1, 1],},

        "2016-pre-VFP-DPS-bbbar": {
        "mc": [
            'D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
            'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
            'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',
        ],
        "crab_folder": ['241216_183033', '250122_185626', '241216_183045'],
        "n_folders": [1, 1, 1],},

        "2016-pre-VFP-SPS-3FS-4FS-ccbar": {
        "mc": [
            'SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
        ],
        "crab_folder": ['230211_202103', '230211_202109', '230211_202116'],
        "n_folders": [1, 1, 1],},

        "2016-pre-VFP-SPS-VFNS-ccbar": {
        "mc": [
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISumme',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISumm',
        ],
        "crab_folder": ['250124_143436', '250124_143441', '250127_141725'],
        "n_folders": [1, 1, 1],},

        "2016-pre-VFP-SPS-3FS-4FS-bbbar": {
        "mc": [
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
        ],
        "crab_folder": ['250124_004537', '250124_004545', '250128_133620'],
        "n_folders": [1, 1, 1],},

        "2016-pre-VFP-SPS-VFNS-bbbar": {
        "mc": [
            'D0ToKpi_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKpi_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'D0ToKpi_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
            'D0ToKpi_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
        ],
        "crab_folder": ['250124_121647', '250124_121654', '250124_122025'],
        "n_folders": [1, 1, 1],},

        ################ 2016-pos-VFP
        "2016-pos-VFP-DPS-ccbar": {
        "mc": [
            'DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
        ],
        "dataset": [
            'DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',
        ],
        "crab_folder": ['230516_124038', '230516_124045', '230516_124052'],
        "n_folders": [1, 1, 1],},

        "2016-pos-VFP-DPS-bbbar": {
        "mc": [
            'D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
            'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
            'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',
            'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',
            'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',
        ],
        "crab_folder": ['250122_170519', '241216_184206', '241216_184214'],
        "n_folders": [1, 1, 1],},

        "2016-pos-VFP-SPS-3FS-4FS-ccbar": {
        "mc": [
            'SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
            'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
            'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
        ],
        "crab_folder": ['230211_203748', '230211_203753', '230211_203759'],
        "n_folders": [1, 1, 1],},

        "2016-pos-VFP-SPS-VFNS-ccbar": {
        "mc": [
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISumme',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISumm',
        ],
        "crab_folder": ['250122_203041', '250206_142259', '250206_142304'],########## 2 last are wrong -> redoing!!!
        "n_folders": [1, 1, 1],},

        "2016-pos-VFP-SPS-3FS-4FS-bbbar": {
        "mc": [
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
        ],
        "crab_folder": ['250124_004958', '250124_005006', '250124_005014'],
        "n_folders": [1, 1, 1],},

        "2016-pos-VFP-SPS-VFNS-bbbar": {
        "mc": [
            'D0ToKpi_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKpi_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
            'D0ToKpi_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
            'D0ToKpi_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
        ],
        "crab_folder": ['250124_122217', '250124_122224', '250124_122231'],
        "n_folders": [1, 1, 1],},
       
        ################ 2017
        "2017-DPS-ccbar": {
        "mc": [
            'DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
        ],
        "dataset": [
            'DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
        ],
        "crab_folder": ['230124_161004', '230124_161011', '220823_050416'],
        "n_folders": [2, 1, 1],},

        "2017-DPS-bbbar": {
        "mc": [
            'D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
            'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
            'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
            'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
            'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
        ],  
        "crab_folder": ['241128_165410', '241128_165417', '241128_165423'],
        "n_folders": [1, 1, 1],},

        "2017-SPS-3FS-ccbar": {
        "mc": [
            'LHE_SPS',
        ],
        "dataset": [
            'jpsi_ccbar_25to100_3FS_SPS_2017_nanoaodplus',
        ],
        "crab_folder": [''],
        "n_folders": [7],},

        "2017-SPS-3FS-4FS-ccbar": {
        "mc": [
            'SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
            'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
            'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
        ],
        "crab_folder": ['230211_204256', '230211_204302', '230211_204309'],
        "n_folders": [1, 1, 1],},

        "2017-SPS-VFNS-ccbar": {
        "mc": [
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
        ],
        "crab_folder": ['241202_174604', '241202_174817', '241202_174028'],
        "n_folders": [1, 1, 1],},

        "2017-SPS-3FS-4FS-bbbar": {
        "mc": [
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
        ],
        "crab_folder": ['241129_133609', '241129_133616', '241129_133623'],
        "n_folders": [1, 1, 1],},

        "2017-SPS-VFNS-bbbar": {
        "mc": [
            'D0ToKpi_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKpi_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
            'D0ToKpi_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
            'D0ToKpi_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
        ],
        "crab_folder": ['241202_170634', '241202_170640', '241202_170646'],
        "n_folders": [1, 1, 1],},

        ################ 2018

        ## DPS-cccbar
        "2018-DPS-ccbar": {
        "mc": [
            'DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
        ],
        "dataset": [
            'DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',
        ],
        "crab_folder": ['230516_020037', '230124_190421', '220823_052048'],
        "n_folders": [2, 1, 1],},

        ## DPS-bbbar
        "2018-DPS-bbbar": {
        "mc": [
            'D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
            'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
            'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',
            'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',
            'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',
        ],
        "crab_folder": ['241216_185612', '250122_185754', '250122_185738'],
        "n_folders": [1, 1, 1],},

        ## SPS-ccbar-3FS_4FS
        "2018-SPS-3FS-4FS-ccbar": {
        "mc": [
            'SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
            'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
            'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
        ],
        "crab_folder": ['230211_204657', '230214_124749', '230211_204713'],
        "n_folders": [1, 1, 1],},

        ## SPS-ccbar-VFNS
        "2018-SPS-VFNS-ccbar": {
        "mc": [
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISumme',
            'SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISumm',
        ],
        "crab_folder": ['250122_204908', '250123_135450', '250123_135455'],
        "n_folders": [1, 1, 1],},

        ## SPS-bbbar-3FS_4FS
        "2018-SPS-3FS-4FS-bbbar": {
        "mc": [
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
            'D0ToKpi_SPS_3FSPlus4FS_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
        ],
        "crab_folder": ['250124_005308', '250124_005314', '250124_005320'],
        "n_folders": [1, 1, 1],},

        ## SPS-bbbar-VFNS
        "2018-SPS-VFNS-bbbar": {
        "mc": [
            'D0ToKpi_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
            'D0ToKpi_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgen',
        ],
        "dataset": [
            'D0ToKpi_JpsiPt9to30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
            'D0ToKpi_JpsiPt30to50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
            'D0ToKpi_JpsiPt50to100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
        ],
        "crab_folder": ['250124_121415', '250124_121422', '250124_121430'],
        "n_folders": [1, 1, 1],},


    }
    
    # Display available configurations with numbers
    print("Available configurations:")
    for number, config_name in configurations.items():
        print(f"  {number}: {config_name}")

    # Prompt the user to select a configuration by number
    try:
        selected_number = int(input("\nEnter the number of the desired configuration from the list above: ").strip())
        if selected_number not in configurations:
            raise ValueError("Invalid number.")
    except ValueError as e:
        print(f"Error: {e}. Please enter a valid number from the list.")
    else:
      # Get the configuration name from the selected number
      active_config = configurations[selected_number]
      print(f"You selected: {active_config}")    
    
    # Display available file locations with numbers
    print("Available file locations:")
    for number, fil in fil_loc.items():
        print(f"{number}: {fil}")

    # Prompt the user to select file location by number
    try:
        selected_file_location = int(input("\nEnter the number of the desired configuration from the list above: ").strip())
        if selected_file_location not in fil_loc:
            raise ValueError("Invalid location")
    except ValueError as e:
        print(f"Error: {e}. Please enter a valid number from the list.")
    else:
        # Get the configuration name from the selected number
        active_file_list = fil_loc[selected_file_location]
        print(f"You selected: {active_file_list}")  

    
    #active_config = "2017-SPS-ccbar-VFNS"
    # Extract the active configuration
    config = config_data[active_config]
    # Use the active configuration in the function
    for m, d, c, n in zip(config["mc"], config["dataset"], config["crab_folder"], config["n_folders"]):
      generate_path(m, d, c, n, active_config, active_file_list)     


############################# Important

''' DPS

mc = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
      'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
      'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',]
    
dataset = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',]
    
crab_folder = ['230124_161004', '230124_161011', '220823_050416',] '''






