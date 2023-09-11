import os, re
import awkward as ak

D0_PDG_MASS = 1.864

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split(r'(\d+)', text) ]

def build_p4(acc):
    p4 = ak.zip({'x': acc['x'].value,
                 'y': acc['y'].value,
                 'z': acc['z'].value,
                 't': acc['t'].value}, with_name="LorentzVector")

    return p4

def build_acc(obj, name=None):
    from coffea import processor
    acc = processor.dict_accumulator({})
    for var in obj.fields:
        acc[var] = processor.column_accumulator(ak.to_numpy(ak.flatten(obj[var])))
    if name is not None:
        acc[f"n{name}"] = processor.column_accumulator(ak.to_numpy(ak.num(obj)))
    return acc


def association(cand1, cand2):
    ''' Function for association of the particles. The cuts that operates on all of them and 
    computation of quantities can go here. individual cuts can go on the main processing'''
    asso = ak.cartesian([cand1, cand2])
    
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
    asso['deltaphi'] = asso.slot0.phi - asso.slot1.phi
    asso['cand'] = cand1 + cand2
    
    return asso

def get_files(paths, pattern='.root', exclude=None):
    files = []
    for path in paths:
        with os.scandir(path) as it:
            for file in it:
                if file.name.find(pattern) > -1 and (file.stat().st_size != 0):
                    if not exclude == None: 
                        if file.name.find(exclude) > -1: continue
                    files.append(file.path)
    files.sort(key=natural_keys)
    return files

def save_kin_hists(hists, cand, gen=False, get_deltam=False):
    hists['pt'].fill(pt=ak.flatten(cand.pt))
    hists['eta'].fill(eta=ak.flatten(cand.eta))
    hists['phi'].fill(phi=ak.flatten(cand.phi))
    if not gen: 
        if get_deltam: hists['deltam'].fill(deltam=ak.flatten(cand.deltamr))
        else: hists['mass'].fill(mass=ak.flatten(cand.mass))

def get_n(array):
    array2 = array[ak.fill_none(array, -1) > -1]
    return ak.num(array2)

def remove_none(array):
    return ak.fill_none(array, -99) > -1

def save_eff(coffea_file, csv_file):
    import pandas as pd
    from coffea.util import load
    from hist.intervals import ratio_uncertainty

    # Loads coffea file and get the parameters.
    output = load(coffea_file)
    for i, out in enumerate(output):
        if i == 0:
            N_gen_dimu           = out['N_gen_dimu']
            N_reco_dimu          = out['N_reco_dimu']
            N_reco_dimu_pu       = out['N_reco_dimu_pu']
            N_reco_dimu_pu_sf    = out['N_reco_dimu_pu_sf']
            N_cuts_dimu          = out['N_cuts_dimu']
            N_cuts_dimu_pu       = out['N_cuts_dimu_pu']
            N_cuts_dimu_pu_sf    = out['N_cuts_dimu_pu_sf']
            N_trigger_dimu       = out['N_trigger_dimu']
            N_trigger_dimu_pu    = out['N_trigger_dimu_pu']
            N_trigger_dimu_pu_sf = out['N_trigger_dimu_pu_sf']
            N_gen_dstar          = out['N_gen_dstar']
            N_reco_dstar         = out['N_reco_dstar']
            N_reco_dstar_pu      = out['N_reco_dstar_pu']
            N_cuts_dstar         = out['N_cuts_dstar']
            N_cuts_dstar_pu      = out['N_cuts_dstar_pu']
            N_num_asso           = out['N_num_asso']
            N_num_asso_pu        = out['N_num_asso_pu']
            N_num_asso_pu_sf     = out['N_num_asso_pu_sf']
            N_num_asso_rap_pu_sf = out['N_num_asso_rap_pu_sf']
            N_den_asso           = out['N_den_asso']
            N_den_asso_pu        = out['N_den_asso_pu']
            N_den_asso_pu_sf     = out['N_den_asso_pu_sf']
            N_den_asso_rap_pu_sf = out['N_den_asso_rap_pu_sf']
        else:
            N_gen_dimu           += out['N_gen_dimu']
            N_reco_dimu          += out['N_reco_dimu']
            N_reco_dimu_pu       += out['N_reco_dimu_pu']
            N_reco_dimu_pu_sf    += out['N_reco_dimu_pu_sf']
            N_cuts_dimu          += out['N_cuts_dimu']
            N_cuts_dimu_pu       += out['N_cuts_dimu_pu']
            N_cuts_dimu_pu_sf    += out['N_cuts_dimu_pu_sf']
            N_trigger_dimu       += out['N_trigger_dimu']
            N_trigger_dimu_pu    += out['N_trigger_dimu_pu']
            N_trigger_dimu_pu_sf += out['N_trigger_dimu_pu_sf']
            N_gen_dstar          += out['N_gen_dstar']
            N_reco_dstar         += out['N_reco_dstar']
            N_reco_dstar_pu      += out['N_reco_dstar_pu']
            N_cuts_dstar         += out['N_cuts_dstar']
            N_cuts_dstar_pu      += out['N_cuts_dstar_pu']
            N_num_asso           += out['N_num_asso']
            N_num_asso_pu        += out['N_num_asso_pu']
            N_num_asso_pu_sf     += out['N_num_asso_pu_sf']
            N_num_asso_rap_pu_sf += out['N_num_asso_rap_pu_sf']
            N_den_asso           += out['N_den_asso']
            N_den_asso_pu        += out['N_den_asso_pu']
            N_den_asso_pu_sf     += out['N_den_asso_pu_sf']
            N_den_asso_rap_pu_sf += out['N_den_asso_rap_pu_sf']

    df = pd.read_csv(csv_file, )

    # Jpsi acceptance
    acc_jpsi_prec_val = N_reco_dimu_pu_sf['global']/N_gen_dimu['global']
    acc_jpsi_prec_sta_down, acc_jpsi_prec_sta_up = ratio_uncertainty(N_reco_dimu_pu_sf['global'], N_gen_dimu['global'], uncertainty_type='efficiency')
    df['acc_jpsi_prec_val'] = acc_jpsi_prec_val
    df['acc_jpsi_prec_sta'] = acc_jpsi_prec_sta_up

    # D* acceptance
    acc_dstar_prec_val = N_reco_dstar_pu['global']/N_gen_dstar['global']
    acc_dstar_prec_sta_down,acc_dstar_prec_sta_up = ratio_uncertainty(N_reco_dstar_pu['global'], N_gen_dstar['global'], uncertainty_type='efficiency')
    df['acc_dstar_prec_val'] = acc_dstar_prec_val
    df['acc_dstar_prec_sta'] = acc_dstar_prec_sta_down

    # D* cuts efficiency
    eff_dstar_cuts_val = N_cuts_dstar_pu['global']/N_reco_dstar_pu['global']
    eff_dstar_cuts_sta_down, eff_dstar_cuts_sta_up = ratio_uncertainty(N_cuts_dstar_pu['global'], N_reco_dstar_pu['global'], uncertainty_type='efficiency')
    df['eff_dstar_cuts_val'] = eff_dstar_cuts_val
    df['eff_dstar_cuts_sta'] = eff_dstar_cuts_sta_down

    # Jpsi cuts efficiency
    eff_jpsi_cuts_val = N_cuts_dimu_pu_sf['global']/N_reco_dimu_pu_sf['global']
    print(f'{eff_jpsi_cuts_val:.3f}')
    eff_jpsi_cuts_sta_down, eff_jpsi_cuts_sta_up = ratio_uncertainty(N_cuts_dimu_pu_sf['global'], N_reco_dimu_pu_sf['global'], uncertainty_type='efficiency')
    df['eff_jpsi_cuts_val'] = eff_jpsi_cuts_val
    df['eff_jpsi_cuts_sta'] = eff_jpsi_cuts_sta_down

    # HLT efficiency
    eff_hlt_val = N_trigger_dimu_pu_sf['global']/N_cuts_dimu_pu_sf['global']
    eff_hlt_sta_down, eff_hlt_sta_up = ratio_uncertainty(N_trigger_dimu_pu_sf['global'], N_cuts_dimu_pu_sf['global'], uncertainty_type='efficiency')
    df['eff_hlt_val'] = eff_hlt_val
    df['eff_hlt_sta'] = eff_hlt_sta_down

    # Association efficiency
    eff_asso_val = N_num_asso_pu_sf['global']/N_den_asso_pu_sf['global']
    eff_asso_sta_down, eff_asso_sta_up = ratio_uncertainty(N_num_asso_pu_sf['global'], N_den_asso_pu_sf['global'], uncertainty_type='efficiency')
    df['eff_asso_val'] = eff_asso_val

    # Save new values in the csv file
    df.to_csv(csv_file,)

    print(f'Global acceptance and J$\psi$ pre-cuts efficiency with pile-up and muon scale factor corrections: {acc_jpsi_prec_val:.3f}+{acc_jpsi_prec_sta_up:.3f}-{acc_jpsi_prec_sta_down:.3f}')
    print(f'Global acceptance and D* pre-cuts efficiency with pile-up correction: {acc_dstar_prec_val:.3f}+{acc_dstar_prec_sta_up:.3f}-{acc_dstar_prec_sta_down:.3f}')
    print(f'Global D* cuts efficiency with pile-up correction: {eff_dstar_cuts_val:.3f}+{eff_dstar_cuts_sta_up:.3f}-{eff_dstar_cuts_sta_down:.3f}')
    print(f'Global J/psi muon ID cut efficiency with pile-up and muon scale factor corrections: {eff_jpsi_cuts_val:.3f}+{eff_jpsi_cuts_sta_up:.3f}-{eff_jpsi_cuts_sta_down:.3f}')    
    print(f'Global HLT with pile-up and muon scale factor corrections: {eff_hlt_val:.3f}+{eff_hlt_sta_up:.3f}-{eff_hlt_sta_down:.3f}')
    print(f'Global association cut efficiency with pile-up and muon scale factor corrections: {eff_asso_val:.3f}+{eff_asso_sta_up:.3f}-{eff_asso_sta_down:.3f}')

    return acc_jpsi_prec_val, acc_dstar_prec_val, eff_dstar_cuts_val, eff_jpsi_cuts_val, eff_hlt_val, eff_asso_val
