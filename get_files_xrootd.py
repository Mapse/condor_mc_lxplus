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

    with open(out_file, 'w') as f:
        for i in final_list:
            f.write(i)
            #print(i)

if __name__ == '__main__':
    
    configurations = {
    1: "2017-DPS-ccbar",
    2: "2017-DPS-bbbar",
    3: "2017-SPS-3FS-4FS-ccbar",
    4: "2017-SPS-VFNS-ccbar",
    5: "2017-SPS-3FS-4FS-bbbar",
    6: "2017-SPS-VFNS-bbbar",}  

    config_data = {
        
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
    
    #active_config = "2017-SPS-ccbar-VFNS"
    # Extract the active configuration
    config = config_data[active_config]
    # Use the active configuration in the function
    for m, d, c, n in zip(config["mc"], config["dataset"], config["crab_folder"], config["n_folders"]):
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






