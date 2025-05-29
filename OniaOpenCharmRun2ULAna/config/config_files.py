
# Dataset (possibilities: 2016APV, 2016, 2017, 2018)
year = '2016APV'

# Pileup reweight file
#pileup_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies_new/OniaOpenCharmRun2ULAna/data/corrections/pile_up_reweight_' + year + '.root'

pileup_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies_per_evt/data/corrections/pile_up_reweight_' + year + '.root'

# Muon ID correction files
#reco_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_generalTracks_Run' + year + '_UL_trackerMuon.json'
reco_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies_per_evt/data/corrections/Efficiency_muon_generalTracks_Run' + year + '_UL_trackerMuon.json'

#id_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_trackerMuon_Run' + year + '_UL_ID.json'
id_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies_per_evt/data/corrections/Efficiency_muon_trackerMuon_Run' + year + '_UL_ID.json'

# Trigger selection
if year == '2016APV' or year == '2016':
    hlt_filter = 'HLT_Dimuon16_Jpsi'
elif year == '2017' or year == '2018':
    hlt_filter = 'HLT_Dimuon25_Jpsi'
