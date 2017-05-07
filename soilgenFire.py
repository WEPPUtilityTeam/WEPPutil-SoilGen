import csv, pyodbc, time, argparse
import math, sys, operator
from collections import defaultdict
'''
soilgenFire
v3.0F
Created by: Dylan Quinn
Email: quinnd@uidaho.edu or ebrooks@uidaho.edu


Creates WEPP soil files for fire scenarios (low, moderate, severe) using the USDA STATSGO2 soils database or the USDA SSURGO soils database (2006).

Soil Survey Staff, Natural Resources Conservation Service, United States Department of Agriculture. Soil Survey Geographic (SSURGO)Database. 
Available online at http://sdmdataaccess.nrcs.usda.gov/. Accessed 11/01/16.

Soil Survey Staff, Natural Resources Conservation Service, United States Department of Agriculture. U.S. General Soil Map (STATSGO2). 
Available online at http://sdmdataaccess.nrcs.usda.gov/. Accessed 11/01/16.

---------------------------------------------------------------------------

input:
    interface: 
        component key (cokey), database directory ([database].mdb), chorizon table, component table
    comand line:
        >soilgen.py -c [cokey] -l [cokey_list] -m [mukey] -k [mukey_list] -d [database] -o [component table] -t [chorizon table]
        >soilgen.py --cokey [cokey] --colist [cokey_list] --mukey [mukey] --mulist [mukey_list] --database [database] --cotable [component table] --chtable [chorizon table]
        (use only one cokey or colist options)
        colist [cokey_list] is a comma seperated list (.csv) of individual cokeys
        mulist [mukey_list] is a comma seperated list (.csv) of individual mukeys (dominant soil type will be chosen based upon percent composition)
        
output: a WEPP soil file ([soil_name].sol)
'''

'''Global Vars'''
#default wepp file type, using 7778 or 7777
weppfile = '7778'
#default soil albedo
assumed_albedo = 0.23
#default initial soil saturation
assumed_initalsat = 0.753
#path to output .sol (none if in current directory)
write_path = 'sol/'
#directory of the soils database if not in current directory (.mdb)
database_dir = 'STATSGO2_AZ.mdb'

chorizon_table = 'chorizon'

component_table = 'component'

#construct the cokey_dic using defaultdict()
cokey_dic = defaultdict(list)

def find_dominant_soil(mukey):
    '''Returns the cokey(s) of the dominant soil(s) in the mukey as a list'''
    MDB = database_dir; DRV = '{Microsoft Access Driver (*.mdb)}'; PWD = 'pw'
    con = pyodbc.connect('DRIVER={};DBQ={};PWD={}'.format(DRV,MDB,PWD))
    cur = con.cursor()
    SQL = "SELECT mukey, cokey, comppct_r FROM %s WHERE mukey = '%s' ORDER BY cokey" %(component_table,str(mukey))
    soils = cur.execute(SQL).fetchall()
    soils_list = []
    pct_list = []
    for s in soils:
        pct_list.append(s.comppct_r)
        soils_list.append(dict([('mukey', str(s.mukey)),('cokey', str(s.cokey)),('pct', str(s.comppct_r)),]))
    cokey_list = []
    for s in soils_list:
        if int(s['pct']) == int(max(pct_list)):
            cokey_list.append(s['cokey'])
    return cokey_list
        
def fetch_data(cokey):
    '''Returns a dictionary of dictionaries, with the lower dictionaries representing one horizon layer'''    
    #open ODBC connections
    MDB = database_dir; DRV = '{Microsoft Access Driver (*.mdb)}'; PWD = 'pw'
    con = pyodbc.connect('DRIVER={};DBQ={};PWD={}'.format(DRV,MDB,PWD))
    cur = con.cursor()
    
    
    
    #query to retrieve data from the database
    #             cokey  chkey  depth     smr_db        ksat    sand         clay         om    ec      d      rocks10     rocks3-10    designatio   s            fc           wp             bl_cl_ki  name
    #             0      1      2         3             4       5            6            7     8       9      10          11           12           13           14           15             16        17        
    #
    #SQL = "SELECT cokey, chkey, hzdepb_r, dbthirdbar_r, ksat_r, sandtotal_r, claytotal_r, om_r, ecec_r, awc_l, fraggt10_r, frag3to10_r, desgnmaster, sieveno10_r, wthirdbar_r, wfifteenbar_r, sandvf_r, compname FROM " + cokey_table + " WHERE cokey = '" + cokey + "'"
    SQL = "SELECT co.mukey, ch.cokey, ch.chkey, ch.hzname, co.compname, co.comppct_r, ch.hzdepb_r, ch.dbthirdbar_r, ch.ksat_r, ch.sandtotal_r, ch.claytotal_r, ch.om_r, ch.ecec_r, ch.awc_l, ch.fraggt10_r, ch.frag3to10_r, ch.desgnmaster, ch.sieveno10_r, ch.wthirdbar_r, ch.wfifteenbar_r, ch.sandvf_r FROM " + chorizon_table + " AS ch RIGHT JOIN " + component_table + " AS co ON ch.cokey = co.cokey WHERE ch.cokey = '" + cokey + "'"

    horizons = cur.execute(SQL).fetchall()
    horizon_list = []
    for horizon in horizons:
        #populate the  dictionary of dictionaries by setting chkey as the main dictictionary key
        horizon_list.append(dict([('cokey', str(horizon.cokey)),
                                           ('chkey', str(horizon.chkey)),
                                           ('hzdepb_r', (horizon.hzdepb_r)),
                                           ('dbthirdbar_r', (horizon.dbthirdbar_r)),
                                           ('ksat_r', (horizon.ksat_r)),
                                           ('sandtotal_r', (horizon.sandtotal_r)),
                                           ('claytotal_r', (horizon.claytotal_r)),
                                           ('om_r', (horizon.om_r)),
                                           ('ecec_r', (horizon.ecec_r)),
                                           ('awc_l', (horizon.awc_l)),
                                           ('fraggt10_r', (horizon.fraggt10_r)),    
                                           ('frag3to10_r', (horizon.frag3to10_r)),
                                           ('desgnmaster', str(horizon.desgnmaster)),
                                           ('sieveno10_r', (horizon.sieveno10_r)),
                                           ('wthirdbar_r', (horizon.wthirdbar_r)),
                                           ('wfifteenbar_r', (horizon.wfifteenbar_r)),
                                           ('sandvf_r', (horizon.sandvf_r)),
                                           ('compname', str(horizon.compname))]))
    
    #close ODBC connections
    cur.close()
    con.close()
    
    
    return horizon_list
       
def sort_values(values_list): 
    '''Retrieves horizon data (h1, h2, h3, ...) from the cokey_dic under the defined cokey as well as associated
    composition name and albedo values
    returns an array of dictionaries and a dictionary'''
    horizon_arr = []
    layers_array = []
    compname = ''
    for horizon in values_list:
        
        #corrected variables
        rocks_soil = ( 0.0 if horizon['fraggt10_r'] is None else horizon['fraggt10_r']) + (0.0 if horizon['frag3to10_r'] is None else horizon['frag3to10_r'])
        smr_pct_rocks = 0.0 if horizon['desgnmaster'] is 'O' else (100-rocks_soil)/100*(100-(0.0 if horizon['sieveno10_r'] is None else horizon['sieveno10_r']))+rocks_soil
        #corrected field capacity
        fc_no_rocks = 0.0 if horizon['wthirdbar_r'] is None or rocks_soil is None else horizon['wthirdbar_r']/(100-rocks_soil)*100
        #corrected wilting point
        wp_no_rocks = 0.0 if horizon['wfifteenbar_r'] is None or rocks_soil is None else horizon['wfifteenbar_r']/(100-rocks_soil)*100

        depth = round(horizon['hzdepb_r']*10, 1)
        try: smr_bd = round(horizon['dbthirdbar_r'],2) 
        except: 
            smr_bd = 0.0 #defaut
            print '    Warning! Using default smr_bd value'
        try: sand = round(horizon['sandtotal_r'], 1)
        except: 
            sand = 55.0 #defaut
            print '    Warning! Using default sand value'
        try: clay = round(horizon['claytotal_r'],1)
        except: 
            clay = 10.0 #defaut
            print '    Warning! Using default clay value'
        try: om = round(horizon['om_r'],1)
        except: 
            om = 5.0 #defaut
            print '    Warning! Using default om value'
        try: cec = round(horizon['ecec_r'],1)
        except: 
            cec = 15.0 #defaut
            print '    Warning! Using default cec value'
        try: rocks = round(smr_pct_rocks, 1)
        except: 
            rocks = 25.0
            print '    Warning! Using default smr_pct_rocks value'
        
        #
        
        ksat = round(horizon['ksat_r']*3.6, 2) if horizon['ksat_r'] is not None else 0
        anisotropy = 1 if horizon['hzdepb_r']>50 else 10
        fc = round(((horizon['wfifteenbar_r'] if horizon['wfifteenbar_r'] is not None else 0) + horizon['awc_l']*100)/100, 3) if fc_no_rocks is None else round(fc_no_rocks/100, 3)
        wp =  0 if wp_no_rocks is None else round(wp_no_rocks/100, 3)
        
        #baseline cropland variables        
        bl_cl_keff = ('' if horizon['sandtotal_r'] is None else (-0.265+0.0086 * pow(horizon['sandtotal_r'],1.8)+11.46*pow(4 if horizon['ecec_r'] is None else horizon['ecec_r'],(-0.75)) if horizon['claytotal_r'] <= 40 else 0.0066*math.exp(244/horizon['claytotal_r'])))
#        IF(AI33="","na",IF(BJ33<=40,-0.265+0.0086*AI33^1.8+11.46*IF(EA33="",4,EA33)^(-0.75),0.0066*EXP(244/BJ33)))
        bl_cl_ki = ('' if horizon['sandtotal_r'] is None else round((2728000 + 192100 * min(40,horizon['sandvf_r'])), 0) if horizon['sandtotal_r'] > 30 else round(6054000-55130*max(10,horizon['claytotal_r']), 0))
        bl_cl_kr = ('' if horizon['sandtotal_r'] is None else (0.00197 + 0.0003 * min(40,horizon['sandvf_r']) + 0.3863 * math.exp(-1.84 * max(0.35,horizon['om_r'])) if horizon['sandtotal_r'] > 30 else 0.0069 + 0.134 * math.exp(-0.2 * max(10,horizon['claytotal_r']))))
        bl_cl_tauc = ('' if horizon['sandtotal_r'] is None else ((2.67 + 0.065 * min(40,horizon['claytotal_r']) - 0.058 * min(40,horizon['sandvf_r'])) if horizon['sandtotal_r'] > 30 else 3.5))
        
        compname = str(horizon['compname']).replace (" ", "_")
        

        horizon_arr.append(dict([('depth', depth),
                                 ('smr_bd', smr_bd),
                                 ('sand', sand),
                                 ('clay', clay),
                                 ('om', om),
                                 ('cec', cec),
                                 ('rocks', rocks),
                                 ('ksat', ksat),
                                 ('anisotropy', anisotropy),
                                 ('fc', fc),
                                 ('wp', wp)
                                 ]))
         
        baseline_cropland = (dict([('keff',bl_cl_keff),('ki',bl_cl_ki),('kr',bl_cl_kr),('tauc',bl_cl_tauc)]))
        
    print 'Soil Name: ' + compname      
    return  sorted(horizon_arr,key=operator.itemgetter('depth')), compname, baseline_cropland

def create_file(cokey, mukey=99999,file_version='7778'):
    '''Populates 'horizon_data' with values from the database'''
    try:
        cokey = cokey[0]
    except:
        pass
    
    horizon, compname, bl_cl_dic = sort_values(fetch_data(str(cokey)))
    

    
    soil_name = compname.lower()
    albedo = assumed_albedo
    bl_cl_head = "%s    %s    %s %s" % (bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff'])
    init_sat = assumed_initalsat
    
    rec_id = '<...>'
    
    ksat_list = []
    for layer in horizon:
        ksat_list.append(layer['ksat'])
        
    ksat_min = min(ksat_list)/100 if min(ksat_list) < 10 else min(ksat_list)
    horizon = [layer for layer in horizon if layer['ksat'] >= 11]
    #try:
    sand, clay = int(horizon[0]['sand']),int(horizon[0]['clay'])    
    silt = 100 - (sand + clay)
    tex, col = get_texture(sand, silt, clay)
    headder = '%s\n#  This WEPP soil input file was made using USDA STATSGO2 (2006) data\n#  base. Assumptions: soil albedo = %s, initial sat. = %s. If you have\n#  any question, please contact Erin Brooks, Ph: 208-885-6562\n#  Soil Name: %s    Component Key: %s    Rec. ID: %s    Tex.: %s\nsoil file\n%s 1\n' % (weppfile, albedo, init_sat, compname, str(cokey), rec_id, tex, '1')
    layer_depth = horizon[-1]['depth']/10
    if mukey != 99999:
            
            o2.write("{},{}\n".format(soil_name, mukey))
    for sev in ('unb','low','mod','high','norm'):
        
        if sev == 'unb':
            filename = '%s%s_%s.sol' % (write_path, soil_name, sev)
            o = open(filename, 'w')
            o.write(headder)
            o.write("'{}'    '{}'    {}    {}    {}    {0:.2f}    {0:.2f}    {0:.2f}    {0:.2f}\n".format(soil_name,tex,len(horizon),albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff']))
            for r in range(len(horizon)):
                o.write("{}    {}    {}    {}    {}    {}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],horizon[r]['smr_bd'],horizon[r]['ksat'],horizon[r]['anisotropy'],horizon[r]['fc'],horizon[r]['wp'],horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks']))


        elif sev == 'low':
            filename = '%s%s_%s.sol' % (write_path, soil_name, sev)
            o = open(filename, 'w')
            o.write(headder)
            o.write("'{}'    '{}'    {}    {}    {}    {0:.2f}    {0:.2f}    {0:.2f}    {0:.2f}\n".format(soil_name,tex,len(horizon),albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff']))
            for r in range(len(horizon)):
                o.write("{}    {}    {}    {}    {}    {}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],horizon[r]['smr_bd'],horizon[r]['ksat'],horizon[r]['anisotropy'],horizon[r]['fc'],horizon[r]['wp'],horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks']))
  
        elif sev == 'mod':
            filename = '%s%s_%s.sol' % (write_path, soil_name, sev)
            o = open(filename, 'w')
            o.write(headder)
            o.write("'{}'    '{}'    {}    {}    {}    {0:.2f}    {0:.2f}    {0:.2f}    {0:.2f}\n".format(soil_name,tex,len(horizon),albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff']))
            for r in range(len(horizon)):
                o.write("{}    {}    {}    {}    {}    {}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],horizon[r]['smr_bd'],horizon[r]['ksat'],horizon[r]['anisotropy'],horizon[r]['fc'],horizon[r]['wp'],horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks']))
 
        elif sev == 'high':
            filename = '%s%s_%s.sol' % (write_path, soil_name, sev)
            o = open(filename, 'w')
            o.write(headder)
            o.write("'{}'    '{}'    {}    {}    {}    {0:.2f}    {0:.2f}    {0:.2f}    {0:.2f}\n".format(soil_name,tex,len(horizon),albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff']))
            for r in range(len(horizon)):
                o.write("{}    {}    {}    {}    {}    {}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],horizon[r]['smr_bd'],horizon[r]['ksat'],horizon[r]['anisotropy'],horizon[r]['fc'],horizon[r]['wp'],horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks']))

        elif sev == 'norm':
            filename = '%s%s.sol' % (write_path, soil_name)
            o = open(filename, 'w')
            o.write(headder)
            
            
            o.write("'{}'    '{}'    {}    {}    {}    {0:.2f}    {0:.2f}    {0:.2f}    {0:.2f}\n".format(soil_name,tex,len(horizon),albedo,init_sat,bl_cl_dic['ki']*2.5,bl_cl_dic['kr']*2.6,bl_cl_dic['tauc'],bl_cl_dic['keff']*.4))
            #o.write("'%s'    '%s'    %s    %s    %s    %s    %s    %s    %s\n" % (soil_name,tex,str(len(horizon)),str(albedo),str(init_sat),str(int(bl_cl_dic['ki'])*2.5),str(int(bl_cl_dic['kr'])*2.6),str(bl_cl_dic['tauc']),bl_cl_dic['keff']*.4))
            for r in range(len(horizon)):
                o.write("{}    {}    {}    {}    {}    {}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],horizon[r]['smr_bd'],horizon[r]['ksat'],horizon[r]['anisotropy'],horizon[r]['fc'],horizon[r]['wp'],horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks']))
  
        o.write("1 13 1000 %s" % ksat_min)
        o.close()
    return

def create_957(cokey, mukey=99999, file_version='95.7'):
    '''Populates 'horizon_data' with values from the database and creates a 
    WEPP soil file version 95.7'''
    try:
        cokey = cokey[0]
    except:
        pass
    
    horizon, compname, bl_cl_dic = sort_values(fetch_data(str(cokey)))
    

    
    soil_name = compname.lower()
    albedo = assumed_albedo
    bl_cl_head = "{}    {}    {} {}".format(bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff'])
    init_sat = assumed_initalsat
    
    rec_id = '<...>'
    
    ksat_list = []
    for layer in horizon:
        ksat_list.append(layer['ksat'])
        
    ksat_min = min(ksat_list)/100 if min(ksat_list) < 10 else min(ksat_list)
    horizon = [layer for layer in horizon if layer['ksat'] >= 11]
    #try:
    sand, clay = int(horizon[0]['sand']),int(horizon[0]['clay'])    
    silt = 100 - (sand + clay)
    tex, col = get_texture(sand, silt, clay)
    col = col.split(',')
    headder = '%s\n#  This WEPP soil input file was made using USDA STATSGO2 (2006) data\n#  base. Assumptions: soil albedo = %s, initial sat. = %s. If you have\n#  any question, please contact Erin Brooks, Ph: 208-885-6562\n#  Soil Name: %s    Component Key: %s    Rec. ID: %s    Tex.: %s\nsoil file\n%s 1\n' % (file_version, albedo, init_sat, compname, str(cokey), rec_id, tex, '1')
    layer_depth = horizon[-1]['depth']/10
    if mukey != 99999: 
        o2.write("{},{}\n".format(soil_name, mukey))
    for sev in ('unb','low','mod','high','norm'):
        
        if sev == 'unb':
            filename = '{}{}_{}.sol'.format(write_path, soil_name, sev)
            o = open(filename, 'w')
            o.write(headder)
            
            tup = (albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff'])
            
            #6
            mult_tup = (1.1,1,1,1,1,1)
            tup =  tuple([float(a)*b for a,b in zip(tup,mult_tup)])
            
            
            o.write("'{}'    '{}'    {}    {}    {}    {:.1f}    {:.2f}    {:.2f}    {:.2f}\n".format(soil_name,tex,len(horizon),*tup))
            for r in range(len(horizon)):
                
                tup = (horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks'])
                print tup
                #6
                mult_tup = (1,1,1,1,1)
                tup =  [float(a)*b for a,b in zip(tup,mult_tup)]
                o.write("{}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],*tup))


        elif sev == 'low':
            filename = '%s%s_%s.sol' % (write_path, soil_name, sev)
            o = open(filename, 'w')
            o.write(headder)
            tup = (albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff'])
            #6
            
            mult_tup = (1,0.9,1.2,1.2,1,0.9)
            
            tup =  tuple([a*b for a,b in zip(tup,mult_tup)])
            o.write("'{}'    '{}'    {}    {}    {}    {:.1f}    {:.2f}    {:.2f}    {:.2f}\n".format(soil_name,tex,len(horizon),*tup))
            for r in range(len(horizon)):
                tup = (horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks'])
                #6
                mult_tup = (1,1,.9,1,1)
                tup =  tuple([a*b for a,b in zip(tup,mult_tup)])
                o.write("{}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],*tup))
                
        elif sev == 'mod':
            filename = '%s%s_%s.sol' % (write_path, soil_name, sev)
            o = open(filename, 'w')
            o.write(headder)
            tup = (albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff'])
            #6
            mult_tup = (1, .8, 1.5, 1.25, 1, 0.8)
            tup =  tuple([a*b for a,b in zip(tup,mult_tup)])
            o.write("'{}'    '{}'    {}    {}    {}    {:.1f}    {:.2f}    {:.2f}    {:.2f}\n".format(soil_name, tex,len(horizon), *tup))
            for r in range(len(horizon)):
                
                tup = (horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks'])
                #6
                mult_tup = (1,1,.75,1,1)
                tup =  tuple([a*b for a,b in zip(tup,mult_tup)])
                o.write("{}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],*tup))
                
        elif sev == 'high':
            filename = '%s%s_%s.sol' % (write_path, soil_name, sev)
            o = open(filename, 'w')
            o.write(headder)
            tup = (albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff'])
            #6
            mult_tup = (1, 0.5, 2.6, 2.5, 1, 0.4)
            tup =  tuple([a*b for a,b in zip(tup,mult_tup)])
            
            o.write("'{}'    '{}'    {}    {}    {}    {:.1f}    {:.2f}    {:.2f}    {:.2f}\n".format(soil_name,tex,len(horizon), *tup))
            for r in range(len(horizon)):
                
                tup = (horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks'])
                #6
                mult_tup = (1,1,.5,.9,1)
                tup =  tuple([a*b for a,b in zip(tup,mult_tup)])
                o.write("{}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],*tup))
                
        elif sev == 'norm':
            filename = '%s%s.sol' % (write_path, soil_name)
            o = open(filename, 'w')
            o.write(headder)
            o.write("'{}'    '{}'    {}    {}    {}    {:.1f}    {:.2f}    {:.2f}    {:.2f}\n".format(soil_name,tex,len(horizon),albedo,init_sat,bl_cl_dic['ki'],bl_cl_dic['kr'],bl_cl_dic['tauc'],bl_cl_dic['keff']))
            #o.write("'%s'    '%s'    %s    %s    %s    %s    %s    %s    %s\n" % (soil_name,tex,str(len(horizon)),str(albedo),str(init_sat),str(int(bl_cl_dic['ki'])*2.5),str(int(bl_cl_dic['kr'])*2.6),str(bl_cl_dic['tauc']),bl_cl_dic['keff']*.4))
            for r in range(len(horizon)):
                
                tup = (horizon[r]['sand'],horizon[r]['clay'],horizon[r]['om'],horizon[r]['cec'],horizon[r]['rocks'])
                o.write("{}    {}    {}    {}    {}    {}\n".format(horizon[r]['depth'],*tup))
        o.write("{} {} {}".format(col[0],col[1],col[2]))
        o.close()
    return




def get_texture(sand, silt, clay):
    ''' Calcuations taken from http://www.nrcs.usda.gov/wps/portal/nrcs/detail/soils/survey/?cid=nrcs142p2_054167 
        Accessed 11/01/16
    '''
    wepp_color = ''
    texture = ''
    if (silt + 1.5*clay) < 15:
        texture = 'Sand'
        wepp_color = '246,232,195'
    elif (silt + 1.5*clay >= 15) and (silt + 2*clay < 30):
        texture = 'Loamy Sand'
        wepp_color = '223,194,125'
    elif (clay >= 7 and clay < 20) and (sand > 52) and ((silt + 2*clay) >= 30) or (clay < 7 and silt < 50 and (silt+2*clay)>=30):
        texture = 'Sandy Loam'
        wepp_color = '191,129,45'
    elif (clay >= 7 and clay < 27) and (silt >= 28 and silt < 50) and (sand <= 52):
        texture = 'Loam'
        wepp_color = '84,48,5'
    elif (silt >= 50 and (clay >= 12 and clay < 27)) or ((silt >= 50 and silt < 80) and clay < 12):
        texture = 'Silt Loam'
        wepp_color = '140,81,10'
    elif (silt >= 80 and clay < 12):
        texture = 'Silt'
        wepp_color = '245,245,245'
    elif (clay >= 20 and clay < 35) and (silt < 28) and (sand > 45):
        texture = 'Sandy Clay Loam'    
        wepp_color = '199,234,229'
    elif (clay >= 27 and clay < 40) and (sand > 20 and sand <= 45):
        texture = 'Clay Loam'
        wepp_color = '128,205,193'
    elif (clay >= 27 and clay < 40) and (sand  <= 20):
        texture = 'Silty Clay Loam'
        wepp_color = '53,151,143'
    elif (clay >= 35 and sand > 45):
        texture = 'Sandy Clay'
        wepp_color = '223,194,125'
    elif (clay >= 40 and silt >= 40):
        texture = 'Silty Clay'
        wepp_color = '1,102,94'
    elif (clay >= 40 and sand <= 45 and silt < 40):
        texture = 'Clay'
        wepp_color = '0,60,48'
    else:
        #default
        wepp_color = '84,48,5'
        texture = 'Sandy Loam'


    return texture, wepp_color

if __name__ == "__main__":
    
    cokey_set = ''
    cokey_arr_dir = ''
    error_list = []
    
    #Create argument parser
    parser = argparse.ArgumentParser()
    coparse = parser.add_mutually_exclusive_group()
    coparse.add_argument('-c', '--cokey', help='A single cokey value (mukey:cokey')
    coparse.add_argument('-l', '--colist', help='A comma delimited list of cokey values (.csv)')
    coparse.add_argument('-m', '--mukey', help='A single mukey value')
    coparse.add_argument('-k', '--mulist', help='A comma delimited list of mukey values')
    parser.add_argument('-d', '--database', help='The name (and location) of the database (.mdb)')
    parser.add_argument('-o', '--cotable', help='The name of the component table')
    parser.add_argument('-t', '--chtable', help='The name of the chorizon table')
    
    args = parser.parse_args()
    
    
    o2 = open('soildic.txt','w+')
    #handel arguments
           
    
    
    #component, chorizon, and database args
    if args.database:
        database_dir = args.database
    else:
        print 'Using default database: ' + database_dir
    if args.cotable:
        component_table = args.cotable
    else:
        print 'Using default component table: ' + component_table
    if args.chtable:    
        chorizon_table = args.chtable
    else:
        print 'Using default chorizon table: ' + chorizon_table
        
        
    #args for cokey, colist, mukey, mulist as cmd line arguments
    if args.cokey:
        print 'Using cokey'
        create_file(args.cokey)
    elif args.colist:
        print 'Using cokey list'
        with open(args.colist, 'rb') as cokey_file:
            cokey_arr = csv.reader(cokey_file)
            for row in cokey_arr:
                create_file(row[0])
    elif args.mukey:
        print 'Using mukey'
        for c in find_dominant_soil(args.mukey):
            create_file(str(c))
    elif args.mulist:
        print 'Using mukey list'
        with open(args.mulist, 'rb') as mukey_file:
            mukey_arr = csv.reader(mukey_file)
            for mu in mukey_arr:
                for c in find_dominant_soil(mu[0]):
                    create_file(c,mu[0])
                
        
        
    #raw input from post-run cmd line interface for cokey, colist, mukey, mulist    
    else:
        coinput = raw_input('cokey or cokey list (.csv): ')
        if coinput:
            if coinput.find('.csv') > -1:
                with open(coinput, 'rb') as cokey_file:
                    cokey_arr = csv.reader(cokey_file)
                    for row in cokey_arr:
                        create_file(row[0])
            else:
                create_file(coinput)
        else:
            muinput = raw_input('mukey or mukey list (.csv): ')
            if muinput:
                
                if muinput.find('.csv') > -1:
                    
                    with open(muinput, 'rb') as mukey_file:
                        mukey_arr = csv.reader(mukey_file)
                        for mu in mukey_arr:
                            try:
                                create_957(find_dominant_soil(mu[0]),mu[0])
                            except:
                                error_list.append(mu[0])
                                o2.write("{}, {}\n".format('none',mu[0]))
                                continue#fix this line
                            
                else:
                    
                    create_file(find_dominant_soil(muinput))
    print error_list
    o2.close()
    
    
    
    
    
    
