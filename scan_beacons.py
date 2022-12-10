import os
import datetime
import pandas as pd
import time


#sudo btmgmt find | grep "\|20:22:07:01:01:C6" | sort -n | uniq -w 33


mac_df = pd.DataFrame([["alexkey","20:22:07:01:01:C3"],["katiekey","20:22:07:01:01:C6"]]) 
mac_df.columns = ["name","mac"]

raw_save = "rssi_out.txt"
rssi_csv = "rssi.csv"

rssi_fetch_time = 10

"""
Search for mac address and save the raw grep file
input:
    mac_addr : list of mac addr to look for
    out_file : location to save the results
"""
def search_rssi(mac_addr,out_file, debug=False):

    #combine macs with or
    mac_grp = "\|".join(mac_addr)
    

    cmd_str = "sudo btmgmt find | grep '" + mac_grp + "' > " + out_file  
    
    if (debug):
        print(cmd_str)
    os.system(cmd_str)
    
    
    
def search_rssi_clean(md=mac_df,r_rssi=raw_save,rs_csv = rssi_csv, debug=False,add_nas=False):
    
    # get time to save data
    ct = str(datetime.datetime.now)
    search_rssi(list(mac_df.mac), raw_save,debug=debug)
    
    #bluetooth program takes time tor un - wait a bit
    time.sleep(rssi_fetch_time)
    
    #new_rssi_df = pd.DataFrame(
    ## read in rssi data
    lines = open(raw_save,"r").readlines()
    
    out = open(rs_csv,"a")
    seen = []
    
    for l in lines:
        rssi = l.split("rssi")[1].strip().split(" ")[0].strip()
        mc =  l.split(" ")[1]
        
        cn = mac_df.loc[mac_df.mac==mc,"name"]
        
        outl = ",".join([ct,cn,rssi])
        if debug:
            print(outl)
        out.write(outl)
        seen.append(cn)
        
    # put an na next to all missing if asked
    
    if add_nas:
        missing = set(mac.name)- set(seen)
        for m in missing:
            outl = ",".join([ct,m,"NA"])
            out.write(outl)
            
    out.close()
    
    
    
        
        
        
    
    
    

