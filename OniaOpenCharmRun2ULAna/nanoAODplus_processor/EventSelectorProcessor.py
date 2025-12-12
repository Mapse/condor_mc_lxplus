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

import sys

sys.path.insert(1, '/afs/cern.ch/work/m/mabarros/public/CMSSW_10_6_12/src/condor/condor_mc_lxplus/OniaOpenCharmRun2ULAna/config')

import config_files as config 

D0_PDG_MASS = 1.864
 
pileup_file = config.pileup_file
reco_file = config.reco_file
id_file = config.id_file

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
            'nevents': processor.value_accumulator(int),
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
                'JpsiDstar_mass': hist.Hist("Events", hist.Bin("mass", "$m_{J/\psi D*}$ [$GeV/c^2$]", 50, 0, 120)),
                'JpsiDstar_deltapt': hist.Hist("Events", hist.Bin("deltapt", r"$|\p_{T, J/\psi} - \phi_{T, D^*}|$ [GeV/c]", 50, 0, 150)),
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
                'Dstar_D0pt': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("d0pt", "D$^0$ from D$^*$ - p$_{T}$ [GeV/c]", 80, 0, 80)),
                'Dstar_D0cosphi': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("d0cosphi", "D$^0$ from D$^*$ - pointing angle", 120, 0.9, 1)),
                'Dstar_D0dl': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("d0dl", "D$^0$ from D$^*$ - Decay length [mm]", 80, 0, 2)),
                'Dstar_D0dlSig': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("d0dlsig", "D$^0$ from D$^*$ - Decay length significance", 120, 0, 50)),
                'Dstar_D0dca': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("d0dca", "D$^0$ from D$^*$ - DCA  (cm)", 80, 0, 0.1)),
                'Dstar_Kpt': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("kpt", "K from D$^*$ - p$_{T}$ [GeV/c]", 80, 0, 100)),
                'Dstar_Kchindof': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("kchindof", "K from D$^*$  - reduced $\chi^2$", 60, 0, 10)),
                'Dstar_KnValid': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("knvalid", "K from D$^*$ - tracker hits", 80, 0, 30)),
                'Dstar_Kdxy': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("kdxy", "K from D$^*$ - dxy ", 60, 0, 5)),
                'Dstar_Kdz': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("kdz", "K from D$^*$ - dz", 80, 0, 10)),
                'Dstar_pispt': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("pispt", "$\pi_s$ from D$^*$ - p_{T}$ [GeV/c]", 60, 0, 30)),
                'Dstar_pischindof': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("pischindof", "$\pi_s$ from D$^*$ - reduced $\chi^2$", 60, 0, 10)),
                'Dstar_pisnValid': hist.Hist("Events", 
                                        hist.Cat("chg", "charge"), 
                                        hist.Bin("pisnValid", "$\pi_s$ from D$^*$ - tracker hits", 80, 0, 30)),
                    
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
        
        # Saves the number of events
        output['nevents'].add(len(events))

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
        if config.year == '2016preVFP' or config.year == '2016postVFP':
            hlt_char = ak.zip({**get_vars_dict(events, hlt_cols_charm_2016)})
        elif config.year == '2017':
            hlt_char = ak.zip({**get_vars_dict(events, hlt_cols_charm_2017)})
        elif config.year == '2018':
            hlt_char = ak.zip({**get_vars_dict(events, hlt_cols_charm_2018)})       

        ext_normalization = extractor()
        ext_normalization.add_weight_sets(["weight_histogram weight_histogram " + pileup_file])
        ext_normalization.add_weight_sets(['Reco_ * ' + reco_file])
        ext_normalization.add_weight_sets(['Id_ * ' + id_file])
        ext_normalization.finalize()
        evaluator_normalization = ext_normalization.make_evaluator()

        ############### Get the Muons from Dimu, for cuts in their params
        Muon_normalization = ak.zip({'0': Muon[Dimu.t1muIdx], '1': Muon[Dimu.t2muIdx]})

        weight_normalization = get_weight(evaluator_normalization, Muon_normalization, Dimu, PVtx)


        ##### Vertices cut
        #good_pvtx = Primary_vertex['isGood']
        #Primary_vertex = Primary_vertex[good_pvtx]

        ##### Trigger cut

        # Activate trigger
        hlt = True
        # HLT to be used
        hlt_filter = config.hlt_filter

        # Trigger choice
        if hlt:
            print(f"You are running with the trigger: {hlt_filter}")
            trigger_cut = hlt_char[hlt_filter]
            hlt_char = hlt_char[hlt_filter]
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

        ## Cut per MC sample
        print(f"Your analyzer name is: {self.analyzer_name}")
        if '9To30' in self.analyzer_name or '9to30' in self.analyzer_name:

            print('MC sample: 9To30')
            dimu_pt_cut = (Dimu.pt > 25) & (Dimu.pt < 30)
            Dimu = ak.mask(Dimu, dimu_pt_cut)
            Muon = ak.mask(Muon, dimu_pt_cut)

        elif '30To50' in self.analyzer_name or '30to50' in self.analyzer_name:

            print('MC sample: 30To50')
            dimu_pt_cut = (Dimu.pt > 30) & (Dimu.pt < 50)
            Dimu = ak.mask(Dimu, dimu_pt_cut)
            Muon = ak.mask(Muon, dimu_pt_cut)

        elif '50To100' in self.analyzer_name or '50to100' in self.analyzer_name:

            print('MC sample: 50To100')
            dimu_pt_cut = (Dimu.pt > 50) & (Dimu.pt < 100)
            Dimu = ak.mask(Dimu, dimu_pt_cut)
            Muon = ak.mask(Muon, dimu_pt_cut)

        else:
            print('You are not using a good dataset!!')

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

        Dstar['D0dca'] = Dstar.D0dl * (1-Dstar.D0cosphi**2)**0.5

        ############### Dimu + OpenCharm associations

        DimuDstar = association(Dimu, Dstar)
        # Invariant mass to avoid peak (first time applied on 7 October 2024)
        DimuDstar_p4 = ak.zip({'x': DimuDstar.cand.x, 
                               'y': DimuDstar.cand.y,
                               'z': DimuDstar.cand.z,
                               't': DimuDstar.cand.t}, with_name="LorentzVector")
        DimuDstar['dimu_dstar_mass'] = DimuDstar_p4.mass
        #DimuDstar = DimuDstar[DimuDstar.dimu_dstar_mass > 18]
        DimuDstar = DimuDstar[DimuDstar.slot1.associationProb > 0.05]
        DimuDstar = DimuDstar[ak.fill_none(DimuDstar.slot0.pt, -1) > -1]        
        

        Dstar_cut = Dstar[Dstar.associationIdx > -1]
        MuonDstar = ak.zip({'0': Muon[Dstar_cut.associationIdx], '1': Dstar_cut})
        #MuonDstar = MuonDstar[DimuDstar.dimu_dstar_mass > 18]
        MuonDstar = MuonDstar[MuonDstar.slot1.associationProb > 0.05]
        MuonDstar = MuonDstar[ak.fill_none(MuonDstar.slot0.slot0.pt, -1) > -1]
       
        # Takes the candidate with highest pT
        arg_sort = ak.argsort(DimuDstar['cand'].pt, axis=1, ascending=False)
        DimuDstar = DimuDstar[arg_sort] # hint: if I get 'ValueError: only arrays of integers or booleans may be used as a slice', it is because the array is empty
        # Fast solution for this problem: run more jobs so the problematic file will be present in a short amount of events
        MuonDstar = MuonDstar[arg_sort]

        cut_dstar = ak.num(DimuDstar)>0
        DimuDstar = DimuDstar[cut_dstar]
        MuonDstar = MuonDstar[cut_dstar]
        weight = get_weight(evaluator, MuonDstar.slot0, DimuDstar.slot1, PVtx)
         
        ############### Leading and Trailing muon separation Gen_particles
        leading_mu = (Muon.slot0.pt > Muon.slot1.pt)
        Muon_lead = ak.where(leading_mu, Muon.slot0, Muon.slot1)
        Muon_trail = ak.where(~leading_mu, Muon.slot0, Muon.slot1)

        ############### Create the accumulators to save output

        ## Weights - test accumulator
        weight_normalization_acc = processor.dict_accumulator({})
        weight_normalization_acc = processor.column_accumulator(ak.to_numpy(weight_normalization))
        output['weight_normalization'] = weight_normalization_acc

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
        '''muon_lead_acc = processor.dict_accumulator({})
        for var in Muon_lead.fields:
            muon_lead_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Muon_lead[var])))
        muon_lead_acc["nMuon"] = processor.column_accumulator(ak.to_numpy(ak.num(Muon_lead)))
        output["Muon_lead"] = muon_lead_acc

        muon_trail_acc = processor.dict_accumulator({})
        for var in Muon_trail.fields:
            muon_trail_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Muon_trail[var])))
        muon_trail_acc["nMuon"] = processor.column_accumulator(ak.to_numpy(ak.num(Muon_trail)))
        output["Muon_trail"] = muon_trail_acc'''

        ## Trigger accumulator

        # Triggers
        '''trigger_acc = processor.dict_accumulator({})
        for var in hlt_char.fields:
            trigger_acc[var] = processor.column_accumulator(ak.to_numpy(hlt_char[var]))
        output["HLT"] = trigger_acc'''        

        # Accumulator for the associated candidates
        # This accumulator is created in order to have access to build_p4 method, so we can create the invariant mass.
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
        

        # Builds p4 vector to calculate variables
        DimuDstar_p4 = build_p4(DimuDstar_acc)
        # Reconstructs DimuDstar with desired variables
        DimuDstar_vars = ak.zip({
                'jpsi_mass' : DimuDstar_acc['Dimu']['mass'].value,
                'jpsi_pt' : DimuDstar_acc['Dimu']['pt'].value,
                'jpsi_eta' : DimuDstar_acc['Dimu']['eta'].value,
                'jpsi_phi' : DimuDstar_acc['Dimu']['phi'].value,
                'jpsi_rap' : DimuDstar_acc['Dimu']['rap'].value,
                'jpsi_dl' : DimuDstar_acc['Dimu']['dl'].value,
                'jpsi_dlErr' : DimuDstar_acc['Dimu']['dlErr'].value,
                'jpsi_dlsig' : DimuDstar_acc['Dimu']['dlSig'].value,
                'dstar_deltam' : DimuDstar_acc['Dstar']['deltam'].value,
                'dstar_deltamr' : DimuDstar_acc['Dstar']['deltamr'].value,
                'dstar_pt' : DimuDstar_acc['Dstar']['pt'].value,
                'dstar_eta' : DimuDstar_acc['Dstar']['eta'].value,
                'dstar_phi' : DimuDstar_acc['Dstar']['phi'].value,
                'dstar_rap' : DimuDstar_acc['Dstar']['rap'].value,
                'dstar_d0pt' : DimuDstar_acc['Dstar']['D0pt'].value,
                'dstar_d0cosphi' : DimuDstar_acc['Dstar']['D0cosphi'].value,
                'dstar_d0dl' : DimuDstar_acc['Dstar']['D0dl'].value,
                'dstar_d0dlsig' : DimuDstar_acc['Dstar']['D0dlSig'].value,
                'dstar_d0dca' : DimuDstar_acc['Dstar']['D0dl'].value * (1-(DimuDstar_acc['Dstar']['D0cosphi'].value)**2)**0.5,
                'dstar_Kpt' : DimuDstar_acc['Dstar']['Kpt'].value,
                'dstar_Kchindof' : DimuDstar_acc['Dstar']['Kchindof'].value,
                'dstar_KnValid' : DimuDstar_acc['Dstar']['KnValid'].value,
                'dstar_Kdxy' : DimuDstar_acc['Dstar']['Kdxy'].value,
                'dstar_pidxy' : DimuDstar_acc['Dstar']['pidxy'].value,
                'dstar_Kdz' : DimuDstar_acc['Dstar']['Kdz'].value,
                'dstar_pispt' : DimuDstar_acc['Dstar']['pispt'].value,
                'dstar_pischindof' : DimuDstar_acc['Dstar']['pischindof'].value,
                'dstar_pisnValid' : DimuDstar_acc['Dstar']['pisnValid'].value,
                'associationProb' : DimuDstar_acc['Dstar']['associationProb'].value,            
                'dimu_dstar_deltarap' : abs(DimuDstar_acc['deltarap'].value),
                'dimu_dstar_deltapt' : DimuDstar_acc['deltapt'].value,
                'dimu_dstar_deltaeta' : DimuDstar_acc['deltaeta'].value,
                'dimu_dstar_deltaphi' : DimuDstar_acc['deltaphi'].value,                
                'dimu_dstar_mass' : DimuDstar_p4.mass, #is_jpsi & ~wrg_chg & dlSig & dlSig_D0Dstar
                'dimu_dstar_pt' : DimuDstar_p4.pt, #is_jpsi & ~wrg_chg & dlSig & dlSig_D0Dstar
                'is_jpsi' : DimuDstar_acc['Dimu']['is_jpsi'].value,
                'wrg_chg': DimuDstar_acc['Dstar']['wrg_chg'].value,}, with_name='PtEtaPhiMCandidate')
        # Unflatten it to take the first element
        DimuDstar = ak.unflatten(DimuDstar_vars, DimuDstar_acc['nDimuDstar'].value)

        # Saving new accumulator with JpsiDstar invariant mass
        DimuDstar_acc = processor.dict_accumulator({})
        DimuDstar_acc['Dimu'] = processor.dict_accumulator({})
        DimuDstar_acc['Dstar'] = processor.dict_accumulator({})
        for var in DimuDstar.fields:
            if (var == '0') or (var =='1'):
                continue
            elif var == 'cand':
                for i0 in DimuDstar[var].fields:
                    DimuDstar_acc[i0] = processor.column_accumulator(ak.to_numpy(DimuDstar[var][i0][:,0]))
            else:
                DimuDstar_acc[var] = processor.column_accumulator(ak.to_numpy(DimuDstar[var][:,0]))

        for var in DimuDstar.slot0.fields:
            DimuDstar_acc['Dimu'][var] = processor.column_accumulator(ak.to_numpy(DimuDstar.slot0[var][:,0]))

        for var in DimuDstar.slot1.fields:
            DimuDstar_acc['Dstar'][var] = processor.column_accumulator(ak.to_numpy(DimuDstar.slot1[var][:,0]))
        DimuDstar_acc['nDimuDstar'] = processor.column_accumulator(ak.to_numpy(DimuDstar[:,0]))
        output['DimuDstar'] = DimuDstar_acc 

        ### Histograms 

        ## Associated Jpsi
        output['JpsiDstar']['Jpsi_mass'].fill(mass=DimuDstar.jpsi_mass[:,0], weight=weight)
        output['JpsiDstar']['Jpsi_p'].fill(pt=DimuDstar.jpsi_pt[:,0],
                                        eta=DimuDstar.jpsi_eta[:,0],
                                        phi=DimuDstar.jpsi_phi[:,0], weight=weight)
        output['JpsiDstar']['Jpsi_rap'].fill(rap=DimuDstar.jpsi_rap[:,0], weight=weight)
        output['JpsiDstar']['Jpsi_dlSig'].fill(dlSig=DimuDstar.jpsi_dlsig[:,0], weight=weight)
        output['JpsiDstar']['Jpsi_dl'].fill(dl=DimuDstar.jpsi_dl[:,0], weight=weight)

        ## Associated Dstar
        output['JpsiDstar']['Dstar_deltamr'].fill(chg='right charge', deltamr=DimuDstar.dstar_deltamr[:,0], weight=weight)
        output['JpsiDstar']['Dstar_p'].fill(chg='right charge',
                                            pt=DimuDstar.dstar_pt[:,0],
                                            eta=DimuDstar.dstar_eta[:,0],
                                            phi=DimuDstar.dstar_phi[:,0], weight=weight)       
        output['JpsiDstar']['Dstar_rap'].fill(chg='right charge', rap=DimuDstar.dstar_rap[:,0], weight=weight)
        output['JpsiDstar']['Dstar_D0dl'].fill(chg='right charge', d0dl=DimuDstar.dstar_d0dl[:,0], weight=weight)
        output['JpsiDstar']['Dstar_D0dlSig'].fill(chg='right charge', d0dlsig=DimuDstar.dstar_d0dlsig[:,0], weight=weight)
        output['JpsiDstar']['Dstar_D0dca'].fill(chg='right charge', d0dca=DimuDstar.dstar_d0dca[:,0], weight=weight)

        ## JpsiDstar
        output['JpsiDstar']['JpsiDstar_mass'].fill(mass=DimuDstar.dimu_dstar_mass[:,0], weight=weight)
        output['JpsiDstar']['JpsiDstar_deltarap'].fill(deltarap=DimuDstar.dimu_dstar_deltarap[:,0], weight=weight)
        output['JpsiDstar']['JpsiDstar_deltaphi'].fill(deltaphi=DimuDstar.dimu_dstar_deltaphi[:,0], weight=weight)
        output['JpsiDstar']['JpsiDstar_deltapt'].fill(deltapt=DimuDstar.dimu_dstar_deltapt[:,0], weight=weight)
        
        file_hash = str(random.getrandbits(128)) + str(len(events))
        save(output, "output/" + self.analyzer_name + "/" + self.analyzer_name + "_" + file_hash + ".coffea")

        # return dummy accumulator
        return processor.dict_accumulator({
                'cutflow': output['cutflow']
        })

    def postprocess(self, accumulator):
        return accumulator