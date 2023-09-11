import awkward as ak
import numpy as np
import coffea.processor as processor
from coffea.util import save
from coffea import hist

from coffea.nanoevents.methods import candidate
ak.behavior.update(candidate.behavior)

import random

from tools.collections import *

from coffea.lookup_tools import extractor

D0_PDG_MASS = 1.864

# 2016-pre
""" pileup_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies_new/OniaOpenCharmRun2ULAna/data/corrections/pile_up_reweight_2016APV.root'
reco_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_generalTracks_Run2016preVFP_UL_trackerMuon.json'
id_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_trackerMuon_Run2016preVFP_UL_ID.json'
 """
# 2016-pos
""" pileup_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies_new/OniaOpenCharmRun2ULAna/data/corrections/pile_up_reweight_2016.root'
reco_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_generalTracks_Run2016postVFP_UL_trackerMuon.json'
id_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_trackerMuon_Run2016postVFP_UL_ID.json'
 """
# 2017
 
pileup_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies_new/OniaOpenCharmRun2ULAna/data/corrections/pile_up_reweight_2017.root'
reco_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_generalTracks_Run2017_UL_trackerMuon.json'
id_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_trackerMuon_Run2017_UL_ID.json'


# 2018
""" pileup_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies_new/OniaOpenCharmRun2ULAna/data/corrections/pile_up_reweight_2018.root'
reco_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_generalTracks_Run2018_UL_trackerMuon.json'
id_file = '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/analysis_monte_carlo/efficiencies/scale_factor_muon_pog/Efficiency_muon_trackerMuon_Run2018_UL_ID.json'
  """
def get_weight(evaluator, Muon, Dimu, PVtx):
    pileup_weight = evaluator['weight_histogram'](ak.num(PVtx))[ak.num(Dimu)>0]
    muon = Muon[ak.num(Dimu)>0]
    #dimu = Dimu[ak.num(Dimu)>0]
    mu0_reco_weight = evaluator['Reco_NUM_TrackerMuons_DEN_genTracks/abseta_pt_value'](np.absolute(muon.slot0.eta[:,0]), muon.slot0.pt[:,0])
    mu1_reco_weight = evaluator['Reco_NUM_TrackerMuons_DEN_genTracks/abseta_pt_value'](np.absolute(muon.slot1.eta[:,0]), muon.slot1.pt[:,0])
    mu0_id_weight = evaluator['Id_NUM_SoftID_DEN_TrackerMuons/abseta_pt_value'](np.absolute(muon.slot0.eta[:,0]), muon.slot0.pt[:,0])
    mu1_id_weight = evaluator['Id_NUM_SoftID_DEN_TrackerMuons/abseta_pt_value'](np.absolute(muon.slot1.eta[:,0]), muon.slot1.pt[:,0])
    weight = pileup_weight*mu0_reco_weight*mu1_reco_weight*mu0_id_weight*mu1_id_weight
    """ print(pileup_weight*mu0_reco_weight*mu1_reco_weight*mu0_id_weight*mu1_id_weight)
    for idx, i in enumerate(weight):
        print(muon.slot0.eta[idx,0], muon.slot0.pt[idx,0], dimu.pt[idx, 0], dimu.eta[idx,0])
        print(weight[idx], pileup_weight[idx], mu0_reco_weight[idx], mu1_reco_weight[idx], mu0_id_weight[idx], mu0_id_weight[idx]) """
    
    return weight

def association(cand1, cand2):
    ''' Function for association of the particles. The cuts that operates on all of them and 
    computation of quantities can go here. individual cuts can go on the main processing'''
    cand2 = cand2[cand2.associationIdx > -1]
    asso = ak.zip({'0': cand1[cand2.associationIdx], '1': cand2})
    
    cand1 = ak.zip({
            'pt': asso.slot0.pt,
            'eta': asso.slot0.eta,
            'phi': asso.slot0.phi,
            'mass': asso.slot0.mass,
            'charge': asso.slot0.charge}, with_name="PtEtaPhiMCandidate")

    cand2 = ak.zip({
            'pt': asso.slot1.pt,
            'eta': asso.slot1.eta,
            'phi': asso.slot1.phi,
            'mass': asso.slot1.mass,
            'charge': asso.slot1.charge}, with_name="PtEtaPhiMCandidate")

    asso['deltarap'] = asso.slot0.rap - asso.slot1.rap
    asso['deltapt'] = asso.slot0.pt - asso.slot1.pt
    asso['deltaeta'] = asso.slot0.eta - asso.slot1.eta
    asso['deltaphi'] = np.remainder(np.abs(asso.slot0.phi - asso.slot1.phi), np.pi)
    asso['cand'] = cand1 + cand2
    
    return asso

def build_p4(acc):

    '''
    This function is used to build the lorentzvector for a given particle accumulator
    
    acc (dict_accumulator): Accumulator with the particle information.

    It returns the four vector for the particle (x, y, z, t)

    '''
    # Uses awkward array zip method to build the four vector    
    p4 = ak.zip({'x': acc['x'].value, 
                 'y': acc['y'].value,
                 'z': acc['z'].value,
                 't': acc['t'].value}, with_name="LorentzVector")

    return p4

class EventSelectorProcessor(processor.ProcessorABC):
    def __init__(self, analyzer_name):
        self.analyzer_name = analyzer_name

        self._accumulator = processor.dict_accumulator({
            'cutflow': processor.defaultdict_accumulator(int),
            'JpsiDstar': processor.dict_accumulator({
                'Jpsi_mass': hist.Hist("Events", hist.Bin("mass", "$M_{\mu^+\mu^-}$ [GeV/$c^2$]", 100, 2.95, 3.25)), 
                'Jpsi_p': hist.Hist("Events", 
                                    hist.Bin("pt", "$p_{T,\mu^+\mu^-}$ [GeV/c]", 100, 0, 150),
                                    hist.Bin("eta", "$\eta_{\mu^+\mu^-}$", 60, -2.5, 2.5),
                                    hist.Bin("phi", "$\phi_{\mu^+\mu^-}$", 70, -3.5, 3.5)),
                'Jpsi_rap': hist.Hist("Events", hist.Bin("rap", "y", 60, -2.5, 2.5)),
                'Jpsi_dl': hist.Hist("Events", hist.Bin("dl", "decay length", 100, -0.5, 1.5)),
                'Jpsi_dlSig': hist.Hist("Events", hist.Bin("dlSig", "dl Significance", 100, -50, 200)),
                'JpsiDstar_deltarap': hist.Hist("Events", hist.Bin("deltarap", "$\Delta y$", 50, -5, 5)),
                'JpsiDstar_deltaphi': hist.Hist("Events", hist.Bin("deltaphi", r"$|\phi_{J/\psi} - \phi_{D^*}|$ [rad]", 80, 0, 5)),
                'JpsiDstar_deltapt': hist.Hist("Events", hist.Bin("deltapt", r"$|\p_{T, J/\psi} - \p_{T, D^*}|$ GeV/c", 100, 0, 120)),
                'JpsiDstar_deltaeta': hist.Hist("Events", hist.Bin("deltaeta", r"$|\eta_{J/\psi} - \eta_{D^*}|$ [rad]", 80, -5, 5)),
                'JpsiDstar_mass': hist.Hist("Events", hist.Bin("mass", "$m_{J/\psi D*}$ [$GeV/c^2$]", 50, 0, 120)),
                'JpsiDstar_pt': hist.Hist("Events", hist.Bin("pt", "$p_{T, J/\psi D*}$ [$GeV/c$]", 50, 0, 150)),
                'Dstar_p': hist.Hist("Events",
                                 hist.Cat("chg", "charge"), 
                                 hist.Bin("pt", "$p_{T,D*}$ [GeV/c]", 100, 0, 100),
                                 hist.Bin("eta", "$\eta_{D*}$", 80, -2.5, 2.5),
                                 hist.Bin("phi", "$\phi_{D*}$", 70, -3.5, 3.5)),
                'Dstar_rap': hist.Hist("Events", 
                                    hist.Cat("chg", "charge"), 
                                    hist.Bin("rap", "y", 60, -2.5, 2.5)),
                'Dstar_deltam': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("deltam", "$\Delta m$ [$GeV/c^2$]", 50, 0.138, 0.162)),
                'Dstar_deltamr': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("deltamr", "$\Delta m_{refit}$ [$GeV/c^2$]", 50, 0.138, 0.162)),
                'Dstar_D0dl': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("d0dl", "D$^0$ from D$^*$ - Decay length [mm]", 80, 0, 2)),
                'Dstar_D0dlSig': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("d0dlsig", "D$^0$ from D$^*$ - Decay length significance", 120, 0, 50)),
            }),
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator.identity()

        ############### Cuts
        # Dimu cuts: charge = 0, mass cuts and chi2...
        # test if there is any events in the file
        if len(events) == 0:
            return output

        ############### Get the primary vertices  ############### 
        PVtx = ak.zip({**get_vars_dict(events, primary_vertex_aod_cols)})

        ############### Get the gen particles ############### 

        # All gen particles
        Gen_particles = ak.zip({**get_vars_dict(events, gen_part_cols)}, with_name="PtEtaPhiMCandidate")
        # Gen Muons
        #Gen_Muon = Gen_particles[np.absolute(Gen_particles.pdgId) == 13]
        # Gen Jpsi
        #Gen_Jpsi = Gen_particles[Gen_particles.pdgId == 443]
        # Gen Dstar
        #Gen_Dstar = Gen_particles[(Gen_particles.pdgId == 413) | (Gen_particles.pdgId == -413)]
                              
        ############### Get All the interesting candidates from NTuples
        Dimu = ak.zip({**get_vars_dict(events, dimu_cols)}, with_name="PtEtaPhiMCandidate")
        Muon = ak.zip({**get_vars_dict(events, muon_cols)}, with_name="PtEtaPhiMCandidate")
        D0 = ak.zip({'mass': events.D0_mass12, **get_vars_dict(events, d0_cols)}, with_name="PtEtaPhiMCandidate")
        Dstar = ak.zip({'mass': (events.Dstar_D0mass + events.Dstar_deltamr),
                        'charge': events.Dstar_pischg,
                        **get_vars_dict(events, dstar_cols)}, 
                        with_name="PtEtaPhiMCandidate")
        # Triggers for charmonium
        
        #hlt_char_2016 = ak.zip({**get_vars_dict(events, hlt_cols_charm_2016)})
        #hlt_char_2017 = ak.zip({**get_vars_dict(events, hlt_cols_charm_2017)})
        hlt_char_2018 = ak.zip({**get_vars_dict(events, hlt_cols_charm_2018)})

        ##### Vertices cut
        #good_pvtx = Primary_vertex['isGood']
        #Primary_vertex = Primary_vertex[good_pvtx]

        ##### Trigger cut

        # Activate trigger
        hlt = True
        # HLT to be used
        hlt_filter = 'HLT_Dimuon25_Jpsi'

        # Trigger choice
        if hlt:
            print(f"You are running with the trigger: {hlt_filter}")
            trigger_cut = hlt_char_2018[hlt_filter]
            hlt_char_2018 = hlt_char_2018[hlt_filter]
        if not hlt:
            print("You are not running with trigger")
            # Assign 1 to all events.
            trigger_cut = np.ones(len(Dimu), dtype=bool)

        ext = extractor()
        ext.add_weight_sets(["weight_histogram weight_histogram " + pileup_file])
        ext.add_weight_sets(['Reco_ * ' + reco_file])
        ext.add_weight_sets(['Id_ * ' + id_file])
        ext.finalize()
        evaluator = ext.make_evaluator()  

        # Trigger filter for gen particles
        #Gen_Muon = Gen_Muon[trigger_cut]
        #Gen_Jpsi = Gen_Jpsi[trigger_cut]
        #Gen_Dstar = Gen_Dstar[trigger_cut]
        PVtx = PVtx[trigger_cut]

        Dimu = Dimu[trigger_cut]
        Muon = Muon[trigger_cut]
        Dstar = Dstar[trigger_cut]

        ############### Dimu cuts charge = 0, mass cuts and chi2...
        Dimu = Dimu[Dimu.charge == 0]

        Dimu = ak.mask(Dimu, ((Dimu.mass > 2.95) & (Dimu.mass < 3.25)))
        
        ############### Get the Muons from Dimu, for cuts in their params
        Muon = ak.zip({'0': Muon[Dimu.t1muIdx], '1': Muon[Dimu.t2muIdx]})

        # SoftId and Global Muon cuts
        soft_id = (Muon.slot0.softId > 0) & (Muon.slot1.softId > 0)
        Dimu = ak.mask(Dimu, soft_id)
        Muon = ak.mask(Muon, soft_id)

        #!!!!!!!!!!!!!!!!! We decided to remove the global cuts !!!!!!!! #

        #global_muon = (Muon.slot0.isGlobal > 0) & (Muon.slot1.isGlobal > 0)
        #Dimu = Dimu[global_muon]
        #Muon = Muon[global_muon]
        #output['cutflow']['Dimu muon global'] += ak.sum(ak.num(Dimu))

        ## pT and eta/rapidity cuts
    
        # Muon pT
        muon_pt_cut = (Muon.slot0.pt > 3) & (Muon.slot1.pt > 3)
        Dimu = ak.mask(Dimu, muon_pt_cut)
        Muon = ak.mask(Muon, muon_pt_cut)
        
        # Muon eta 
        muon_eta_cut = (np.absolute(Muon.slot0.eta) <= 2.4) & (np.absolute(Muon.slot1.eta) <= 2.4)
        Dimu = ak.mask(Dimu, muon_eta_cut)
        Muon = ak.mask(Muon, muon_eta_cut)

        # Dimuon pT
        dimu_pt_cut = (Dimu.pt > 25) & (Dimu.pt < 100)
        Dimu = ak.mask(Dimu, dimu_pt_cut)
        Muon = ak.mask(Muon, dimu_pt_cut)

        # Dimuon rapidity
        dimu_rap_cut = (np.absolute(Dimu.rap) <= 1.2)
        Dimu = ak.mask(Dimu, dimu_rap_cut)
        Muon = ak.mask(Muon, dimu_rap_cut)

        Dimu['is_ups'] = (Dimu.mass > 8.5) & (Dimu.mass < 11.5)
        Dimu['is_jpsi'] = (Dimu.mass > 2.95) & (Dimu.mass < 3.25)
        Dimu['is_psi'] = (Dimu.mass > 3.35) & (Dimu.mass < 4.05)

        ############### Cuts for Dstar

        # trks cuts
        Dstar = Dstar[~Dstar.hasMuon]

        Dstar = Dstar[Dstar.Kchg != Dstar.pichg]

        Dstar = Dstar[(Dstar.pt > 4) & (Dstar.pt < 60)]

        Dstar = Dstar[np.absolute(Dstar.rap) <= 2.1]

        Dstar = Dstar[(Dstar.Kpt > 1.6) & (Dstar.pipt > 1.6)]

        Dstar = Dstar[(Dstar.Kchindof < 2.5) & (Dstar.pichindof < 2.5)]

        Dstar = Dstar[(Dstar.KnValid > 4) & (Dstar.pinValid > 4) & (Dstar.KnPix > 1) & (Dstar.pinPix > 1)]

        Dstar = Dstar[(Dstar.Kdxy < 0.5) & (Dstar.pidxy < 0.5)]

        K_theta = 2 * np.arctan(np.exp(-Dstar.Keta))
        pi_theta = 2 * np.arctan(np.exp(-Dstar.pieta))
        Dstar = Dstar[(Dstar.Kdz < 0.5/np.sin(K_theta)) & (Dstar.pidz < 0.5/np.sin(pi_theta))]

        # pis cuts
        Dstar = Dstar[Dstar.pispt > 0.3]

        Dstar = Dstar[Dstar.pischindof < 3]

        Dstar = Dstar[Dstar.pisnValid > 2]

        # D0 of Dstar cuts
        Dstar = Dstar[Dstar.D0cosphi > 0.99]

        Dstar = Dstar[(Dstar.D0mass < D0_PDG_MASS + 0.028) & (Dstar.D0mass > D0_PDG_MASS - 0.028)]

        Dstar = Dstar[Dstar.D0pt > 4]

        Dstar = Dstar[Dstar.D0dlSig > 2.5]

        Dstar['wrg_chg'] = (Dstar.Kchg == Dstar.pichg)

        ############### Dimu + OpenCharm associations

        DimuDstar = association(Dimu, Dstar)
        DimuDstar = DimuDstar[DimuDstar.slot1.associationProb > 0.05]
        DimuDstar = DimuDstar[ak.fill_none(DimuDstar.slot0.pt, -1) > -1]

        Dstar_cut = Dstar[Dstar.associationIdx > -1]
        MuonDstar = ak.zip({'0': Muon[Dstar_cut.associationIdx], '1': Dstar_cut})
        MuonDstar = MuonDstar[MuonDstar.slot1.associationProb > 0.05]
        MuonDstar = MuonDstar[ak.fill_none(MuonDstar.slot0.slot0.pt, -1) > -1]
       
        # Takes the candidate with highest pT
        arg_sort = ak.argsort(DimuDstar['cand'].pt, axis=1, ascending=False)
        DimuDstar = DimuDstar[arg_sort]
        MuonDstar = MuonDstar[arg_sort]

        weight = get_weight(evaluator, MuonDstar.slot0, DimuDstar.slot1, PVtx)
        DimuDstar = DimuDstar[ak.num(DimuDstar)>0]
         
        ############### Leading and Trailing muon separation Gen_particles
        leading_mu = (Muon.slot0.pt > Muon.slot1.pt)
        Muon_lead = ak.where(leading_mu, Muon.slot0, Muon.slot1)
        Muon_trail = ak.where(~leading_mu, Muon.slot0, Muon.slot1)

        ############### Create the accumulators to save output

        ## Weights accumulator
        weight_acc = processor.dict_accumulator({})
        weight_acc = processor.column_accumulator(ak.to_numpy(weight))
        output['weight'] = weight_acc

        ## Primary vertex accumulator
        PVtx_acc = processor.dict_accumulator({})
        for var in PVtx.fields:
            PVtx_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(PVtx[var])))
        PVtx_acc['nPVtx'] = processor.column_accumulator(ak.to_numpy(ak.num(PVtx)))
        output['PVtx'] = PVtx_acc

        ## Muon accumulator
        muon_lead_acc = processor.dict_accumulator({})
        for var in Muon_lead.fields:
            muon_lead_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Muon_lead[var])))
        muon_lead_acc["nMuon"] = processor.column_accumulator(ak.to_numpy(ak.num(Muon_lead)))
        output["Muon_lead"] = muon_lead_acc

        muon_trail_acc = processor.dict_accumulator({})
        for var in Muon_trail.fields:
            muon_trail_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Muon_trail[var])))
        muon_trail_acc["nMuon"] = processor.column_accumulator(ak.to_numpy(ak.num(Muon_trail)))
        output["Muon_trail"] = muon_trail_acc

        ## Trigger accumulator

        # 2016 triggers
        """ trigger_2016_acc = processor.dict_accumulator({})
        for var in hlt_char_2016.fields:
            trigger_2016_acc[var] = processor.column_accumulator(ak.to_numpy(hlt_char_2016[var]))
        output["HLT_2016"] = trigger_2016_acc """  

        # 2017 triggers
        """ trigger_2017_acc = processor.dict_accumulator({})
        for var in hlt_char_2017.fields:
            trigger_2017_acc[var] = processor.column_accumulator(ak.to_numpy(hlt_char_2017[var]))
        output["HLT_2017"] = trigger_2017_acc  """  

        # 2018 triggers
        trigger_2018_acc = processor.dict_accumulator({})
        for var in hlt_char_2018.fields:
            trigger_2018_acc[var] = processor.column_accumulator(ak.to_numpy(hlt_char_2018[var]))
        output["HLT_2018"] = trigger_2018_acc        

        # Accumulator for the associated candidates
        DimuDstar_acc = processor.dict_accumulator({})
        DimuDstar_acc['Dimu'] = processor.dict_accumulator({})
        DimuDstar_acc['Dstar'] = processor.dict_accumulator({})
        for var in DimuDstar.fields:
            if (var == '0') or (var =='1'):
                continue
            elif var == 'cand':
                for i0 in DimuDstar[var].fields:
                    DimuDstar_acc[i0] = processor.column_accumulator(ak.to_numpy(ak.flatten(DimuDstar[var][i0])))
            else:
                DimuDstar_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(DimuDstar[var])))

        for var in DimuDstar.slot0.fields:
            DimuDstar_acc['Dimu'][var] = processor.column_accumulator(ak.to_numpy(ak.flatten(DimuDstar.slot0[var])))

        for var in DimuDstar.slot1.fields:
            DimuDstar_acc['Dstar'][var] = processor.column_accumulator(ak.to_numpy(ak.flatten(DimuDstar.slot1[var])))
        DimuDstar_acc['nDimuDstar'] = processor.column_accumulator(ak.to_numpy(ak.num(DimuDstar)))
        output['DimuDstar'] = DimuDstar_acc

        ### Histograms 

        ## Associated Jpsi
        #for i in DimuDstar.slot0.pt:
        #    print(f'Dimu pT after: {i}')
        output['JpsiDstar']['Jpsi_mass'].fill(mass=DimuDstar.slot0.mass[:,0], weight=weight)
        output['JpsiDstar']['Jpsi_p'].fill(pt=DimuDstar.slot0.pt[:,0],
                                        eta=DimuDstar.slot0.eta[:,0],
                                        phi=DimuDstar.slot0.phi[:,0], weight=weight)
        output['JpsiDstar']['Jpsi_rap'].fill(rap=DimuDstar.slot0.rap[:,0], weight=weight)
        output['JpsiDstar']['Jpsi_dlSig'].fill(dlSig=DimuDstar.slot0.dlSig[:,0], weight=weight)
        output['JpsiDstar']['Jpsi_dl'].fill(dl=DimuDstar.slot0.dl[:,0], weight=weight)

        ## Associated Dstar
        output['JpsiDstar']['Dstar_deltamr'].fill(chg='right charge', deltamr=DimuDstar.slot1.deltamr[:,0], weight=weight)
        output['JpsiDstar']['Dstar_p'].fill(chg='right charge',
                                            pt=DimuDstar.slot1.pt[:,0],
                                            eta=DimuDstar.slot1.eta[:,0],
                                            phi=DimuDstar.slot1.phi[:,0], weight=weight)       
        output['JpsiDstar']['Dstar_rap'].fill(chg='right charge', rap=DimuDstar.slot1.rap[:,0], weight=weight)
        output['JpsiDstar']['Dstar_D0dl'].fill(chg='right charge', d0dl=DimuDstar.slot1.D0dl[:,0], weight=weight)
        output['JpsiDstar']['Dstar_D0dlSig'].fill(chg='right charge', d0dlsig=DimuDstar.slot1.D0dlSig[:,0], weight=weight)
        
        ## JpsiDstar

        # Builds p4 vector to calculate variables
        DimuDstar_p4 = build_p4(DimuDstar_acc)
        # Reconstructs DimuDstar with desired variables
        DimuDstar_vars = ak.zip({  
                        'jpsi_pt' : DimuDstar_acc['Dimu']['pt'].value,
                        'mass' : DimuDstar_p4.mass,
                        'pt' : DimuDstar_p4.pt,       
                        'deltarap' : DimuDstar_acc['deltarap'].value,
                        'deltapt'  : DimuDstar_acc['deltapt'].value,  
                        'deltaeta' : DimuDstar_acc['deltaeta'].value, 
                        'deltaphi' : DimuDstar_acc['deltaphi'].value,}, with_name='PtEtaPhiMCandidate')
        # Unflatten it to take the first element
        DimuDstar_vars = ak.unflatten(DimuDstar_vars, DimuDstar_acc['nDimuDstar'].value)

        output['JpsiDstar']['JpsiDstar_mass'].fill(mass=DimuDstar_vars.mass[:,0], weight=weight)
        output['JpsiDstar']['JpsiDstar_pt'].fill(pt=DimuDstar_vars.pt[:,0], weight=weight)
        output['JpsiDstar']['JpsiDstar_deltarap'].fill(deltarap=DimuDstar.deltarap[:,0], weight=weight)
        output['JpsiDstar']['JpsiDstar_deltaphi'].fill(deltaphi=DimuDstar.deltaphi[:,0], weight=weight)
        output['JpsiDstar']['JpsiDstar_deltapt'].fill(deltapt=DimuDstar.deltapt[:,0], weight=weight)
        output['JpsiDstar']['JpsiDstar_deltaeta'].fill(deltaeta=DimuDstar.deltaeta[:,0], weight=weight)

        
        file_hash = str(random.getrandbits(128)) + str(len(events))
        save(output, "output/" + self.analyzer_name + "/" + self.analyzer_name + "_" + file_hash + ".coffea")

        # return dummy accumulator
        return processor.dict_accumulator({
                'cutflow': output['cutflow']
        })

    def postprocess(self, accumulator):
        return accumulator
