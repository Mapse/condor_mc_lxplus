import os
import shutil


datasets = {
    1: "2016-pre-VFP",
    2: "2016-pos-VFP",
    3: "2017",
    4: "2018"}

# Display available datasets with numbers
print("Available datasets:")
for number, config_name in datasets.items():
    print(f"  {number}: {config_name}")

# Prompt the user to select a dataset by number
try:
    selected_number = int(input("\nEnter the number of the desired dataset from the list above: ").strip())
    if selected_number not in datasets:
        raise ValueError("Invalid number.")
except ValueError as e:
    print(f"Error: {e}. Please enter a valid number from the list.")
else:
    # Get the dataset name from the selected number
    active_config = datasets[selected_number]
    print(f"You selected: {active_config}")  

#print("Available datasets:")
# Main and subpath definitions

main_path = "/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_data/analysis_fit/data/" + active_config
subpaths = {
    # DPS-ccbar
    "dps_ccbar_9to30": "dps_ccbar_9to30",
    "dps_ccbar_30to50": "dps_ccbar_30to50",
    "dps_ccbar_50to100": "dps_ccbar_50to100",
    # DPS-bbbar
    "dps_bbbar_9to30": "dps_bbbar_9to30",
    "dps_bbbar_30to50": "dps_bbbar_30to50",
    "dps_bbbar_50to100": "dps_bbbar_50to100",
    # SPS-ccbar-3FS_4FS
    "sps_3fs_4fs_ccbar_9to30": "sps_3fs_4fs_ccbar_9to30",
    "sps_3fs_4fs_ccbar_30to50": "sps_3fs_4fs_ccbar_30to50",
    "sps_3fs_4fs_ccbar_50to100": "sps_3fs_4fs_ccbar_50to100",
    # SPS-ccbar-VFNS
    "sps_vfns_ccbar_9to30": "sps_vfns_ccbar_9to30",
    "sps_vfns_ccbar_30to50": "sps_vfns_ccbar_30to50",
    "sps_vfns_ccbar_50to100": "sps_vfns_ccbar_50to100",
    # SPS-bbbar-3FS_4FS
    "sps_3fs_4fs_bbbar_9to30": "sps_3fs_4fs_bbbar_9to30",
    "sps_3fs_4fs_bbbar_30to50": "sps_3fs_4fs_bbbar_30to50",
    "sps_3fs_4fs_bbbar_50to100": "sps_3fs_4fs_bbbar_50to100",
    # SPS-bbbar-VFNS
    "sps_vfns_bbbar_9to30": "sps_vfns_bbbar_9to30",
    "sps_vfns_bbbar_30to50": "sps_vfns_bbbar_30to50",
    "sps_vfns_bbbar_50to100": "sps_vfns_bbbar_50to100",
    
}

# Create subdirectories if they do not exist
for subpath in subpaths.values():
    os.makedirs(os.path.join(main_path, subpath), exist_ok=True)

# Function to generate file names based on pattern
""" def generate_files(prefix, range_end):
    return [f"{prefix}_{i}.coffea" for i in range(range_end)] """

# Path containing the files to organize
source_path = "/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/condor/condor_mc_lxplus"

# List all .coffea files in the source path
files = [f for f in os.listdir(source_path) if f.endswith(".coffea")]

# Function to move files based on their range and type
def move_files(files, main_path, subpaths):
    for file in files:
        
        if "9to30" in file or "9To30" in file:  
            print(file)
            print("File 9-30 GeV")
            if "D0ToKPi_Jpsi" in file:
                subpath = subpaths["dps_bbbar_9to30"]
            elif "DPS_D0ToKPi_JPsiPt" in file:
                subpath = subpaths["dps_ccbar_9to30"]
            elif "SPS_D0ToKPi_JPsiPt" in file:
                subpath = subpaths["sps_3fs_4fs_ccbar_9to30"]
            elif "SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt" in file:
                subpath = subpaths["sps_vfns_ccbar_9to30"]
            elif "D0ToKpi_SPS_3FSPlus4FS_JpsiPt" in file:
                subpath = subpaths["sps_3fs_4fs_bbbar_9to30"]
            elif "D0ToKpi_JpsiPt" in file:
                subpath = subpaths["sps_vfns_bbbar_9to30"]
            else:
                print(f"Skipping file: {file} (no matching type)")
                continue
        elif "30to50" in file or "30To50" in file:
            print("File 30-50 GeV")
            if "D0ToKPi_Jpsi" in file:
                subpath = subpaths["dps_bbbar_30to50"]
            elif "DPS_D0ToKPi_JPsiPt" in file:
                subpath = subpaths["dps_ccbar_30to50"]
            elif "SPS_D0ToKPi_JPsiPt" in file:
                subpath = subpaths["sps_3fs_4fs_ccbar_30to50"]
            elif "SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt" in file:
                subpath = subpaths["sps_vfns_ccbar_30to50"]
            elif "D0ToKpi_SPS_3FSPlus4FS_JpsiPt" in file:
                subpath = subpaths["sps_3fs_4fs_bbbar_30to50"]
            elif "D0ToKpi_JpsiPt" in file:
                subpath = subpaths["sps_vfns_bbbar_30to50"]
            else:
                print(f"Skipping file: {file} (no matching type)")
                continue
        elif "50to100" in file or "50To100" in file:
            print("File 50-100 GeV")
            if "D0ToKPi_Jpsi" in file:
                subpath = subpaths["dps_bbbar_50to100"]
            elif "DPS_D0ToKPi_JPsiPt" in file:
                subpath = subpaths["dps_ccbar_50to100"]
            elif "SPS_D0ToKPi_JPsiPt" in file:
                subpath = subpaths["sps_3fs_4fs_ccbar_50to100"]
            elif "SPS_JpsiDStar_JMM_DStarToD0Pi_D0ToKpi_JPsiPt" in file:
                subpath = subpaths["sps_vfns_ccbar_50to100"]
            elif "D0ToKpi_SPS_3FSPlus4FS_JpsiPt" in file:
                subpath = subpaths["sps_3fs_4fs_bbbar_50to100"]
            elif "D0ToKpi_JpsiPt" in file:
                subpath = subpaths["sps_vfns_bbbar_50to100"]
            else:
                print(f"Skipping file: {file} (no matching type)")
                continue
        else:
            print(f"Skipping file: {file} (no matching range)")
            continue
        
        # Source and destination paths
        source = os.path.join(os.getcwd(), file)
        destination = os.path.join(main_path, subpath, file)
        
        # Move the file
        try:
            shutil.move(source, destination)
            print(f"Moved {file} to {destination}")
        except FileNotFoundError:
            print(f"File not found: {file}")
        except Exception as e:
            print(f"Error moving {file}: {e}")

# Execute the file organization
move_files(files, main_path, subpaths)
