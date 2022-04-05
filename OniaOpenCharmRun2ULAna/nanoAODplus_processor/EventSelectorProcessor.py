import awkward as ak
import numpy as np
import numba
import coffea.processor as processor
from coffea.util import save

from coffea.nanoevents.methods import candidate
ak.behavior.update(candidate.behavior)

import random

from tools.collections import *

D0_PDG_MASS = 1.864

loose = 0

@numba.njit
def get_gendimu_id(muon, genpart, builder):
    for i0 in range(len(muon['0'])):
        builder.begin_list()
        for i1 in range(len(muon['0'][i0])):
            index0 = index1 = -1
            for i2 in range(len(genpart[i0])):
                if genpart[i0][i2].Id == muon['0'][i0][i1].simIdx: index0 = i2
                if genpart[i0][i2].Id == muon['1'][i0][i1].simIdx: index1 = i2
            if ((index0 > -1) and (index1 > -1) and (genpart[i0][index0].genPartIdxMother == genpart[i0][index1].genPartIdxMother)):
                index_gen = -1
                for i3 in range(len(genpart[i0])):
                    if genpart[i0][index0].genPartIdxMother != genpart[i0][i3].Id: continue
                    index_gen = i3
                builder.begin_record()
                builder.field('muon0').integer(index0)
                builder.field('muon1').integer(index1)
                builder.field('dimu').integer(index_gen)
                builder.end_record()
            else:
                builder.begin_record()
                builder.field('muon0').integer(-1)
                builder.field('muon1').integer(-1)
                builder.field('dimu').integer(-1)
                builder.end_record()
        builder.end_list()
            
    return builder

def dimu_match(muon, dimu, genpart):
    
    # Filter genparticles for muon with jpsi as mother or jpsi
    genpart = genpart[((np.absolute(genpart.pdgId) == 13) & (genpart.parpdgId == 443)) | (genpart.pdgId == 443)]
    
    ## Gen muon cuts
    
    # pT cut
    gen_muon_pt_cut = (np.absolute(genpart.pdgId) == 13) & (genpart.parpdgId == 443) & (genpart.pt < 3)        
    genpart = genpart[~gen_muon_pt_cut]
    # eta cut
    gen_muon_eta_cut = (np.absolute(genpart.pdgId) == 13) & (genpart.parpdgId == 443) & (np.absolute(genpart.eta >= 2.4))        
    genpart = genpart[~gen_muon_eta_cut]

    # Get the gendimuon id
    gen_idx = get_gendimu_id(muon, genpart, ak.ArrayBuilder()).snapshot()
    
    valid_idx = (gen_idx.muon0 > -1) & (gen_idx.muon1 > -1) & (gen_idx.dimu > -1)
    gen_idx = gen_idx[valid_idx]
    dimu = dimu[valid_idx]
    muon = muon[valid_idx]
    
    dimu_gen = ak.zip({'0': dimu, '1': genpart[gen_idx.dimu]})
    muon0_gen = ak.zip({'0': muon.slot0, '1': genpart[gen_idx.muon0]})
    muon1_gen = ak.zip({'0': muon.slot1, '1': genpart[gen_idx.muon1]})
    
    ##### To control variables
    '''for i0 in range(len(dimu_gen['0'])):
        if len(dimu_gen['0']) == 0: continue
        print(f"Event: {i0}")
        for i1 in range(len(dimu_gen['0'][i0])):
            print(f"dimu, mu1, mu2: ({dimu_gen['1'][i0][i1].pdgId}, {muon0_gen['1'][i0][i1].pdgId}, {muon1_gen['1'][i0][i1].pdgId}")'''
    
    return dimu_gen, muon0_gen, muon1_gen


class EventSelectorProcessor(processor.ProcessorABC):
    def __init__(self, analyzer_name):
        self.analyzer_name = analyzer_name

        self._accumulator = processor.dict_accumulator({
            'cutflow': processor.defaultdict_accumulator(int),
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
        Primary_vertex = ak.zip({**get_vars_dict(events, primary_vertex_aod_cols)})

        ############### Get the gen particles ############### 

        # All gen particles
        Gen_particles = ak.zip({**get_vars_dict(events, gen_part_cols)}, with_name="PtEtaPhiMCandidate")
        # Gen Muons
        Gen_Muon = Gen_particles[np.absolute(Gen_particles.pdgId) == 13]
        # Gen Jpsi
        Gen_Jpsi = Gen_particles[Gen_particles.pdgId == 443]
        # Gen Psi(2S)
        Gen_Psi = Gen_particles[Gen_particles.pdgId == 100443]
        # Gen Upsilon(1S)
        Gen_Ups1S = Gen_particles[Gen_particles.pdgId == 553]
        # Gen D0
        Gen_D0 = Gen_particles[(Gen_particles.pdgId == 421) | (Gen_particles.pdgId == -421)]
        # Gen Dstar
        Gen_Dstar = Gen_particles[(Gen_particles.pdgId == 413) | (Gen_particles.pdgId == -413)]
                              
        ############### Get All the interesting candidates from NTuples
        Dimu = ak.zip({**get_vars_dict(events, dimu_cols)}, with_name="PtEtaPhiMCandidate")
        Muon = ak.zip({**get_vars_dict(events, muon_cols)}, with_name="PtEtaPhiMCandidate")
        D0 = ak.zip({'mass': events.D0_mass12, **get_vars_dict(events, d0_cols)}, with_name="PtEtaPhiMCandidate")
        Dstar = ak.zip({'mass': (events.Dstar_D0mass + events.Dstar_deltamr),
                        'charge': events.Dstar_pischg,
                        **get_vars_dict(events, dstar_cols)}, 
                        with_name="PtEtaPhiMCandidate")
        # Triggers for 2017 charmonium
        try:
            hlt_char_2017 = ak.zip({**get_vars_dict(events, hlt_cols_charm_2017)})
        except:
            hlt_char_2017 = ak.zip({"HLT_2017" : "HLT_2017"})

        # Triggers for 2018 charmonium
        try:
            hlt_char_2018 = ak.zip({**get_vars_dict(events, hlt_cols_charm_2018)})
        except:
            hlt_char_2018 = ak.zip({"HLT_2018" : "HLT_2018"})
                     
        output['cutflow']['Number of events'] += len(events)
        output['cutflow']['Number of Dimu'] += ak.sum(ak.num(Dimu))
        output['cutflow']['all D0']      += ak.sum(ak.num(D0))
        output['cutflow']['all Dstar']   += ak.sum(ak.num(Dstar))

        ##### Vertices cut
        #good_pvtx = Primary_vertex['isGood']
        #Primary_vertex = Primary_vertex[good_pvtx]

        ##### Trigger cut

        # Activate trigger
        hlt = False
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

        # Trigger filter for gen particles
        Gen_Muon = Gen_Muon[trigger_cut]
        Gen_Jpsi = Gen_Jpsi[trigger_cut]
        Gen_Dstar = Gen_Dstar[trigger_cut]

        ############### Dimu cuts charge = 0, mass cuts and chi2...
        Dimu = Dimu[trigger_cut]
        Dimu = Dimu[Dimu.charge == 0]
        output['cutflow']['Dimu 0 charge'] += ak.sum(ak.num(Dimu))

        #Dimu = Dimu[((Dimu.mass > 8.5) & (Dimu.mass < 11.5)) | ((Dimu.mass > 2.9) & (Dimu.mass < 3.3)) | ((Dimu.mass > 3.35) & (Dimu.mass < 4.05))]
        Dimu = Dimu[(Dimu.mass > 2.95) & (Dimu.mass < 3.25)]
        output['cutflow']['Quarkonia mass'] += ak.sum(ak.num(Dimu))
        
        # Prompt/nomprompt cut for jpsi
        dimuon_prompt_cut = (Dimu.dlSig > 0) & (Dimu.dlSig < 2.5)
        dimuon_nonprompt_cut = (Dimu.dlSig > 4) 
        #Dimu = Dimu[dimuon_nonprompt_cut]
        #output['cutflow']['Dimu prompt'] += ak.sum(ak.num(Dimu))

        # Pointing angle cut for jpsi
        dimuon_pointing_cut = (Dimu.cosphi > 0.99)
        #Dimu = Dimu[dimuon_pointing_cut]

        ############### Get the Muons from Dimu, for cuts in their params
        Muon = Muon[trigger_cut]
        Muon = ak.zip({'0': Muon[Dimu.t1muIdx], '1': Muon[Dimu.t2muIdx]})

        # SoftId and Global Muon cuts
        soft_id = (Muon.slot0.softId > 0) & (Muon.slot1.softId > 0)
        Dimu = Dimu[soft_id] 
        Muon = Muon[soft_id] 
        output['cutflow']['Dimu muon softId'] += ak.sum(ak.num(Dimu))

        #!!!!!!!!!!!!!!!!! We decided to remove the global cuts !!!!!!!! #

        #global_muon = (Muon.slot0.isGlobal > 0) & (Muon.slot1.isGlobal > 0)
        #Dimu = Dimu[global_muon]
        #Muon = Muon[global_muon]
        #output['cutflow']['Dimu muon global'] += ak.sum(ak.num(Dimu))

        # pt and eta cuts
        if loose:
            muon_pt_cut = (Muon.slot0.pt > 1) & (Muon.slot1.pt > 1)

        else:
            
            muon_pt_cut = (Muon.slot0.pt > 3) & (Muon.slot1.pt > 3) 
        
        Dimu = Dimu[muon_pt_cut]
        Muon = Muon[muon_pt_cut]
        output['cutflow']['Dimu muon pt cut'] += ak.sum(ak.num(Dimu))

        muon_eta_cut = (np.absolute(Muon.slot0.eta) <= 2.4) & (np.absolute(Muon.slot1.eta) <= 2.4)
        Dimu = Dimu[muon_eta_cut]
        Muon = Muon[muon_eta_cut]
        
        output['cutflow']['Dimu muon eta cut'] += ak.sum(ak.num(Dimu))

        # Dimu and muon matching
        dimu_gen, muon0_gen, muon1_gen = dimu_match(Muon, Dimu, Gen_particles) #dimu_gen: rec dimuon + gen dimuon

        # Cut for gen jpsi
        last_element = ak.cartesian([ak.num(Gen_Jpsi)-1, ak.Array([[1]])]).slot0
        Gen_Jpsi = Gen_Jpsi[last_element]

        ## Gen muon cuts

        # gen muon pT
        gen_muon_pt_cut = Gen_Muon.pt > 3
        Gen_Muon = Gen_Muon[gen_muon_pt_cut]

        # gen muon eta
        gen_muon_eta_cut = (np.absolute(Gen_Muon.eta) <= 2.4)
        Gen_Muon = Gen_Muon[gen_muon_eta_cut]

        # Take events with always two muons
        two_muons_cut = ak.num(Gen_Muon) == 2

        # Filter the events
        Gen_Jpsi = Gen_Jpsi[two_muons_cut]
        Gen_Muon = Gen_Muon[two_muons_cut]

        # Takes gen muon with the same mother
        Gen_Muon = ak.combinations(Gen_Muon, 2)
        same_mother = ((Gen_Muon.slot0.genPartIdxMother == Gen_Muon.slot1.genPartIdxMother) & (Gen_Muon.slot0.parpdgId == 443))
                        
        Gen_Muon = Gen_Muon[same_mother]

        ## Ressonances cuts

        # Retrieves matched muon
        #Dimu = dimu_gen.slot0

        '''##### Test for efficiency !!!!
        print("Efficiency!!!!!!!!!")
        print(ak.sum(ak.num(dimu_gen.slot0))/ak.sum(ak.num(Gen_Jpsi)) * 100) '''

        # Non matched
        Dimu['is_ups'] = (Dimu.mass > 8.5) & (Dimu.mass < 11.5)
        Dimu['is_jpsi'] = (Dimu.mass > 2.95) & (Dimu.mass < 3.25)
        Dimu['is_psi'] = (Dimu.mass > 3.35) & (Dimu.mass < 4.05)

        # Matched
        #Dimu_match['is_ups'] = (Dimu_match.mass > 8.5) & (Dimu_match.mass < 11.5)
        #Dimu_match['is_jpsi'] = (Dimu_match.mass > 2.9) & (Dimu_match.mass < 3.3)
        #Dimu_match['is_psi'] = (Dimu_match.mass > 3.35) & (Dimu_match.mass < 4.05)

        ############### Cuts for D0
        D0 = D0[trigger_cut]
        D0 = D0[~D0.hasMuon]
        D0noncut = D0
        output['cutflow']['D0 trk muon cut'] += ak.sum(ak.num(D0))

        if loose:
            D0 = D0[(D0.t1pt > 0.4) & (D0.t2pt > 0.4)]
            output['cutflow']['D0 trk pt cut'] += ak.sum(ak.num(D0))

            D0 = D0[(D0.t1chindof < 4) & (D0.t2chindof < 4)]
            output['cutflow']['D0 trk chi2 cut'] += ak.sum(ak.num(D0))

            D0 = D0[(D0.t1nValid > 2) & (D0.t2nValid > 2) & (D0.t1nPix > 1) & (D0.t2nPix > 1)]
            output['cutflow']['D0 trk hits cut'] += ak.sum(ak.num(D0))

            D0 = D0[(D0.t1dxy < 0.1) & (D0.t2dxy < 0.1)]
            output['cutflow']['D0 trk dxy cut'] += ak.sum(ak.num(D0))

            D0 = D0[(D0.t1dz < 1.) & (D0.t2dz < 1.)]
            output['cutflow']['D0 trk dz cut'] += ak.sum(ak.num(D0))
        
        else:
            
            D0 = D0[(D0.t1pt > 0.8) & (D0.t2pt > 0.8)]
            output['cutflow']['D0 trk pt cut'] += ak.sum(ak.num(D0))

            D0 = D0[(D0.t1chindof < 2.5) & (D0.t2chindof < 2.5)]
            output['cutflow']['D0 trk chi2 cut'] += ak.sum(ak.num(D0))

            D0 = D0[(D0.t1nValid > 4) & (D0.t2nValid > 4) & (D0.t1nPix > 1) & (D0.t1nPix > 1)]
            output['cutflow']['D0 trk hits cut'] += ak.sum(ak.num(D0))

            D0 = D0[(D0.t1dxy < 0.1) & (D0.t2dxy < 0.1)]
            output['cutflow']['D0 trk dxy cut'] += ak.sum(ak.num(D0))

            D0 = D0[(D0.t1dz < 1.) & (D0.t2dz < 1.)]
            output['cutflow']['D0 trk dz cut'] += ak.sum(ak.num(D0))

        # D0 cosphi
        if loose:
            D0 = D0[D0.cosphi > 0.1]

        else:

            D0 = D0[D0.cosphi > 0.99]
            
        output['cutflow']['D0 cosphi cut'] += ak.sum(ak.num(D0))

        # D0 dl Significance
        if loose:
            D0 = D0[D0.dlSig > 5.]
        else:

            D0 = D0[D0.dlSig > 5.]
            
        output['cutflow']['D0 dlSig cut'] += ak.sum(ak.num(D0))

        # D0 pt
        D0 = D0[D0.pt > 3.]
        output['cutflow']['D0 pt cut'] += ak.sum(ak.num(D0))

        #D0 = D0noncut

        ############### Cuts for Dstar

        # trks cuts
        Dstar = Dstar[trigger_cut]
        Dstar = Dstar[~Dstar.hasMuon]
        output['cutflow']['Dstar trk muon cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[(Dstar.Kpt > 0.5) & (Dstar.pipt > 0.5)]
        output['cutflow']['Dstar trk pt cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[(Dstar.Kchindof < 2.5) & (Dstar.pichindof < 2.5)]
        output['cutflow']['Dstar trk pt cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[(Dstar.KnValid > 4) & (Dstar.pinValid > 4) & (Dstar.KnPix > 1) & (Dstar.pinPix > 1)]
        output['cutflow']['Dstar trk hits cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[(Dstar.Kdxy < 0.1) & (Dstar.pidxy < 0.1)]
        output['cutflow']['Dstar trk pt cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[(Dstar.Kdz < 1) & (Dstar.pidz < 1)]
        output['cutflow']['Dstar trk pt cut'] += ak.sum(ak.num(Dstar))

        # pis cuts
        Dstar = Dstar[Dstar.pispt > 0.3]
        output['cutflow']['Dstar pis pt cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[Dstar.pischindof < 3]
        output['cutflow']['Dstar pis chi2 cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[Dstar.pisnValid > 2]
        output['cutflow']['Dstar pis hits cut'] += ak.sum(ak.num(Dstar))

        # D0 of Dstar cuts
        Dstar = Dstar[Dstar.D0cosphi > 0.99]
        output['cutflow']['Dstar D0 cosphi cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[(Dstar.D0mass < D0_PDG_MASS + 0.028) & (Dstar.D0mass > D0_PDG_MASS - 0.028)]
        output['cutflow']['Dstar D0 mass cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[Dstar.D0pt > 3]
        output['cutflow']['Dstar D0 pt cut'] += ak.sum(ak.num(Dstar))

        Dstar = Dstar[Dstar.D0dlSig > 3]
        output['cutflow']['Dstar D0 dlSig cut'] += ak.sum(ak.num(Dstar))

        Dstar['wrg_chg'] = (Dstar.Kchg == Dstar.pichg)
        #Dstar = Dstar[~Dstar.wrg_chg]

        #################### Dstar Matching (TO BE ADDED IN THE FUTURE!!!!!!)
        
        '''# If statment to avoid null array.
        if (ak.sum(ak.num(Dstar)) != 0):
            # Associate each gen and each reco Dstar
            Dstar_match = ak.cartesian([Dstar, Gen_Dstar_all])
            # Association using Ids.
            Dstar_match = Dstar_match[Dstar_match.slot0.simIdx == Dstar_match.slot1.Id]
            # Remove null events.
            Dstar_match= Dstar_match[ak.num(Dstar_match) > 0]
        else:
            Dstar_match = ak.cartesian([Dstar, Gen_Dstar_all])    
            Dstar_match = Dstar_match[ak.num(Dstar_match) > 0]'''
            
        ## Build the associated candidates
            
        # Dimu + Dstar
        asso = ak.cartesian([Dimu, Dstar]) # Don't need to apply vtxid because they are already supposed to be in the same vtx???
        
        Dimu_asso = ak.zip({
                   'pt': asso.slot0.pt,
                   'eta': asso.slot0.eta,
                   'phi': asso.slot0.phi,
                   'mass': asso.slot0.mass,
                   'charge': asso.slot0.charge}, with_name="PtEtaPhiMCandidate")
       
        Dstar_asso = ak.zip({
                    'pt': asso.slot1.pt,
                    'eta': asso.slot1.eta,
                    'phi': asso.slot1.phi,
                    'mass': asso.slot1.mass,
                    'charge': asso.slot1.charge}, with_name="PtEtaPhiMCandidate")
        
        asso['deltarap'] = asso.slot0.rap - asso.slot1.rap
        asso['cand'] = Dimu_asso + Dstar_asso

        DimuDstar = asso

        ############### Final computation of number of objects
        output['cutflow']['Dimu final']    += ak.sum(ak.num(Dimu))
        #output['cutflow']['Dimu final']    += ak.sum(ak.num(Dimu_match))
        output['cutflow']['D0 final']      += ak.sum(ak.num(D0))
        output['cutflow']['Dstar final']   += ak.sum(ak.num(Dstar))
        #output['cutflow']['Dstar matched final']    += ak.sum(ak.num(Dstar_match.slot0))
        
        ############### Leading and Trailing muon separation Gen_particles
        leading_mu = (Muon.slot0.pt > Muon.slot1.pt)
        Muon_lead = ak.where(leading_mu, Muon.slot0, Muon.slot1)
        Muon_trail = ak.where(~leading_mu, Muon.slot0, Muon.slot1)

        ############### Create the accumulators to save output

        ## Trigger accumulator

        # 2017 triggers
        trigger_2017_acc = processor.dict_accumulator({})
        for var in hlt_char_2017.fields:
            trigger_2017_acc[var] = processor.column_accumulator(ak.to_numpy(hlt_char_2017[var]))
        output["HLT_2017"] = trigger_2017_acc

        # 2018 triggers
        trigger_2018_acc = processor.dict_accumulator({})
        for var in hlt_char_2018.fields:
            trigger_2018_acc[var] = processor.column_accumulator(ak.to_numpy(hlt_char_2018[var]))
        output["HLT_2018"] = trigger_2018_acc

        # Primary vertex accumulator
        primary_vertex_acc = processor.dict_accumulator({})
        for var in Primary_vertex.fields:
            #primary_vertex_acc[var] = processor.column_accumulator(ak.to_numpy(Primary_vertex[var]))
            primary_vertex_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Primary_vertex[var])))
        primary_vertex_acc["nPVtx"] = processor.column_accumulator(ak.to_numpy(ak.num(Primary_vertex[var]))) 
        output["Primary_vertex"] = primary_vertex_acc

        # Gen Particles accumulator
        gen_part_acc = processor.dict_accumulator({})
        for var in Gen_particles.fields:
            gen_part_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Gen_particles[var])))
        gen_part_acc['nGenPart'] = processor.column_accumulator(ak.to_numpy(ak.num(Gen_particles))) 
        output["Gen_particles"] = gen_part_acc

        # Gen Muon accumulator
        gen_muon_acc = processor.dict_accumulator({})
        for var in Gen_Muon.fields:
            gen_muon_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Gen_Muon[var])))
        gen_muon_acc['nGenMuon'] = processor.column_accumulator(ak.to_numpy(ak.num(Gen_Muon))) 
        output["Gen_Muon"] = gen_muon_acc

        # Gen Jpsi accumulator
        gen_jpsi_acc = processor.dict_accumulator({})
        for var in Gen_Jpsi.fields:
            gen_jpsi_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Gen_Jpsi[var])))
        gen_jpsi_acc['nGenJpsi'] = processor.column_accumulator(ak.to_numpy(ak.num(Gen_Jpsi))) 
        output["Gen_Jpsi"] = gen_jpsi_acc

        # Gen Psi accumulator
        gen_psi_acc = processor.dict_accumulator({})
        for var in Gen_Psi.fields:
            gen_psi_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Gen_Psi[var])))
        gen_psi_acc['nGenPsi'] = processor.column_accumulator(ak.to_numpy(ak.num(Gen_Psi))) 
        output["Gen_Psi"] = gen_psi_acc

        # Gen ps1S accumulator
        gen_ups1S_acc = processor.dict_accumulator({})
        for var in Gen_Ups1S.fields:
            gen_ups1S_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Gen_Ups1S[var])))
        gen_ups1S_acc['nGenUps1S'] = processor.column_accumulator(ak.to_numpy(ak.num(Gen_Ups1S))) 
        output["Gen_Ups1S"] = gen_ups1S_acc
            

        # Gen D0 accumulator
        gen_d0_acc = processor.dict_accumulator({})
        for var in Gen_D0.fields:
            gen_d0_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Gen_D0[var])))
        gen_d0_acc['nGenD0'] = processor.column_accumulator(ak.to_numpy(ak.num(Gen_D0)))
        output["Gen_D0"] = gen_d0_acc

        # Gen Dstar accumulator
        gen_dstar_acc = processor.dict_accumulator({})
        for var in Gen_Dstar.fields:
            gen_dstar_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Gen_Dstar[var])))
        gen_dstar_acc['nGenDstar'] = processor.column_accumulator(ak.to_numpy(ak.num(Gen_Dstar)))
        output["Gen_Dstar"] = gen_dstar_acc
            
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

        dimu_acc = processor.dict_accumulator({})
        for var in Dimu.fields:
            if (var.startswith('t')): continue
            dimu_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Dimu[var])))
        dimu_acc["nDimu"] = processor.column_accumulator(ak.to_numpy(ak.num(Dimu)))
        output["Dimu"] = dimu_acc

        """ dimu_match_acc = processor.dict_accumulator({})
        for var in Dimu_match.fields:
            if (var.startswith('t')): continue
            dimu_match_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Dimu_match[var])))
        dimu_match_acc["nDimu_match"] = processor.column_accumulator(ak.to_numpy(ak.num(Dimu_match)))
        output["Dimu_match"] = dimu_match_acc """

        D0_acc = processor.dict_accumulator({})
        D0_trk_acc = processor.dict_accumulator({})
        for var in D0.fields:
            if (var.startswith('t')):
                D0_trk_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(D0[var])))
            else:
                D0_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(D0[var])))
        D0_acc["nD0"] = processor.column_accumulator(ak.to_numpy(ak.num(D0)))
        output["D0"] = D0_acc
        output["D0_trk"] = D0_trk_acc

        Dstar_acc = processor.dict_accumulator({})
        Dstar_D0_acc = processor.dict_accumulator({})
        Dstar_trk_acc = processor.dict_accumulator({})
        for var in Dstar.fields:
            if var.startswith('D0'):
                Dstar_D0_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Dstar[var])))
            elif (var.startswith('K') or var.startswith('pi')):
                Dstar_trk_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Dstar[var])))
            else:
                Dstar_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Dstar[var])))
        Dstar_acc["nDstar"] = processor.column_accumulator(ak.to_numpy(ak.num(Dstar)))
        output["Dstar"] = Dstar_acc
        output["Dstar_D0"] = Dstar_D0_acc
        output["Dstar_trk"] = Dstar_trk_acc

        ## Matched Dstar accumulator      (TO BE ADDED IN THE FUTURE)   
        '''Dstar_match_acc = processor.dict_accumulator({})
        Dstar_match_D0_acc = processor.dict_accumulator({})
        Dstar_match_trk_acc = processor.dict_accumulator({})
        for var in Dstar_match.slot0.fields:
            if var.startswith('D0'):
                Dstar_match_D0_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Dstar_match.slot0[var])))
            elif (var.startswith('K') or var.startswith('pi')):
                Dstar_match_trk_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Dstar_match.slot0[var])))
            else:
                Dstar_match_acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(Dstar_match.slot0[var])))
        Dstar_match_acc["nDstar"] = processor.column_accumulator(ak.to_numpy(ak.num(Dstar_match.slot0)))
        output["Dstar_match"] = Dstar_match_acc
        output["Dstar_match_D0"] = Dstar_match_D0_acc
        output["Dstar_match_trk"] = Dstar_trk_acc'''
        

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

        file_hash = str(random.getrandbits(128)) + str(len(events))
        save(output, "output/" + self.analyzer_name + "/" + self.analyzer_name + "_" + file_hash + ".coffea")

        # return dummy accumulator
        return processor.dict_accumulator({
                'cutflow': output['cutflow']
        })

    def postprocess(self, accumulator):
        return accumulator
