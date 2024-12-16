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

def generate_path(mc, dataset,  crab_folder, n_folders):

    cmds = [ 
    f'xrdfs k8s-redir.ultralight.org:1094 ls -u /store/group/uerj/mabarros/{mc}/{dataset}/{crab_folder}/{i:04}/' for i in range(n_folders)
    ]
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

    with open(out_file, 'w') as f:
        for i in final_list:
            f.write(i)
            #print(i)

if __name__ == '__main__':
    
    ## Private SPS-VFNS-ccbar 
    # 2017
    '''mc = ['CRAB_PrivateMC_RunII_UL_SPS_2017',
          'CRAB_PrivateMC_RunII_UL_SPS_2017',
          'CRAB_PrivateMC_RunII_UL_SPS_2017',]
    
    dataset = ['jpsi_ccbar_9to30_VFNS_SPS_2017_13TeV',
               'jpsi_ccbar_30to50_VFNS_SPS_2017_13TeV_path',
               'jpsi_ccbar_50to100_VFNS_SPS_2017_13TeV_path',]
    
    crab_folder = ['230822_214052', '231012_042325', '231012_042456',]
    
    n_folders = [1, 1, 1,]'''

    # 2016-pre-SPS-ccbar
    """ mc = ['SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
          'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
          'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',]
    
    dataset = ['SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
               'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',
               'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECOAPV',]
    
    crab_folder = ['230211_202103', '230211_202109', '230211_202116',]
    
    n_folders = [1, 1, 1,]  """
    
    # 2016-pre-DPS-ccbar
    """mc = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
          'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
          'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',]
    
    dataset = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',
               'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',
               'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECOAPV',]
    
    crab_folder = ['221021_225931', '220823_051146', '220823_051151',]
    
    n_folders = [1, 1, 1,]"""

    # 2016-pos-DPS-ccbar
    """ mc = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
          'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
          'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',]
    
    dataset = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',
               'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',
               'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL16RECO',]
    
    crab_folder = ['230516_124038', '230516_124045', '230516_124052',]
    
    n_folders = [1, 1, 1,] """

    # 2016-pos-SPS-ccbar
    """ mc = ['SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen',
          'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
          'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',]
    
    dataset = ['SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
               'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',
               'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL16RECO',]
    
    crab_folder = ['230211_203748', '230211_203753', '230211_203759',]
    
    n_folders = [1, 1, 1,] """

    # 2017-SPS-ccbar
    """ mc = ['SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen/',
          'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
          'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',]
    
    dataset = ['SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
               'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',
               'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL17RECO',]
    
    crab_folder = ['230211_204256', '230211_204302', '230211_204309',]
    
    n_folders = [1, 1, 1,] """

    # 2018-SPS-ccbar
    """ mc = ['SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgen/',
          'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgen',
          'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgen',]
    
    dataset = ['SPS_D0ToKPi_JPsiPt-9To30_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
               'SPS_D0ToKPi_JPsiPt-30To50_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',
               'SPS_D0ToKPi_JPsiPt-50To100_TuneCP5_13TeV-helaconia-pythia8-evtgenRunIISummer20UL18RECO',]
    
    crab_folder = ['230211_204657', '230214_124749', '230211_204713',]
    
    n_folders = [1, 1, 1,] """

    # 2017-DPS-ccbar
    mc = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
          'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
          'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',]
    
    dataset = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
               'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
               'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',]
    
    crab_folder = ['230124_161004', '230124_161011', '220823_050416',]
    
    n_folders = [1, 1, 1,]

    # 2018-DPS-ccbar
    """ mc = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
          'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
          'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',]
    
    dataset = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',
               'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',
               'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL18RECO',]
    
    crab_folder = ['230516_020037', '230124_190421', '220823_052048',]
    
    n_folders = [1, 1, 1,] """

    # 2017-DPS-bbbar
    mc = ['D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
          'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgen',
          'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgen',]
    
    dataset = ['D0ToKPi_Jpsi9to30_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
               'D0ToKPi_Jpsi30to50_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
               'D0ToKPi_Jpsi50to100_HardQCD_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',]
    
    crab_folder = ['241128_165410', '241128_165417', '241128_165423',]
    
    n_folders = [1, 1, 1,]

    for m, d, c, n in zip(mc, dataset, crab_folder, n_folders):
        generate_path(m, d, c, n)      


############################# Important

''' DPS

mc = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
      'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',
      'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgen',]
    
dataset = ['DPS_D0ToKPi_JPsiPt-9To30_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
            'DPS_D0ToKPi_JPsiPt-30To50_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',
            'DPS_D0ToKPi_JPsiPt-50To100_JPsiFilter_TuneCP5_13TeV-pythia8-evtgenRunIISummer20UL17RECO',]
    
crab_folder = ['230124_161004', '230124_161011', '220823_050416',] '''






