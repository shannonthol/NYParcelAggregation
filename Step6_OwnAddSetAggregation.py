#IMPORT LIBRARIES
from __future__ import print_function
print('importing libraries...')
import csv
import sys
import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import itertools

csv.field_size_limit(100000000)

#create frame shift pairs list for queries
frameList = list()

x = 1
while x <= 1070000:
    y = x + 10000
    frameList.append([x,y])
    x+=10000

pairs = [x for x in itertools.combinations(frameList, 2)]

for frame in frameList:
    pairs.append((frame,frame))
    
pairs = sorted(pairs)

#SET UP BIGQUERY PARAMETERS

project_id = 'ny-state-parcel-ownership'
credentials = service_account.Credentials.from_service_account_file(r"C:\Users\shannon.thol\Documents\ArcGIS\Projects\ParcelAggregation\ny-state-parcel-ownership-263142cccd1a.json")
client = bigquery.Client(credentials = credentials, project = project_id)

#create empty dictionaries for saving results
setidCollid = dict() ##setidCollid = {setid: collection id, ...}
collidSetid = dict() ##collidSetid = {collection id: [list of setids in the collection], ...}
collNum = 1 ##collNum = collection number variable

#create empty dictionary for storing all raw results
allRes = dict() ##allRes = {record number: [t1setid, t2setid, t1stdown, t2stdown, t1stdadd, t2stdadd, 
                ##ownratio, ownsratio, addratio, addsratio] ...}

#create number variable for keeping track of progress
num = 1

#create a log text file for writing all of the raw results
log = open(r"C:\Users\shannon.thol\Documents\ArcGIS\Projects\ParcelAggregation\NonadjacentAggregation\nonadjacentagg_log_111720.txt", 'w')

#iterate through pairings list
for pair in pairs:
    #do a time check
    start = time.time()
    
    #get the t1 and t2 start and end numbers
    t1start = str(pair[0][0])
    t1end = str(pair[0][1])
    t2start = str(pair[1][0])
    t2end = str(pair[1][1])
    
    #print progress statement
    print("working on " + t1start + "-" + t1end + " cross join " + t2start + "-" + t2end + "; " + 
          str(num) + " of " + str(len(pairs)) + " pairs")
    
    #define fuzzy string query 
    fuzzyQuery = "with results as \
    (select t1row, t2row, t1setid, t2setid, t1stdown, t2stdown, t1stdadd, t2stdadd, \
    tnc_ny_parcels.dq_fm_ldist_ratio(t1stdown,t2stdown) as ownratio, \
    tnc_ny_parcels.dq_fm_ldist_token_sort_ratio(t1stdown,t2stdown) as ownsratio, \
    tnc_ny_parcels.dq_fm_ldist_ratio(t1stdadd,t2stdadd) as addratio, \
    tnc_ny_parcels.dq_fm_ldist_token_sort_ratio(t1stdadd,t2stdadd) as addsratio \
    from \
    (select row_number() over() as t1row, setid as t1setid, upper(std_owner) as t1stdown, upper(std_addr) as t1stdadd \
    from tnc_ny_parcels.rps_stdown_stdaddr_set as t1 where std_owner != '' and std_addr != '' and sum_acres > 1 \
    order by setid desc) \
    cross join \
    (select row_number() over() as t2row, setid as t2setid, upper(std_owner) as t2stdown, upper(std_addr) as t2stdadd \
    from tnc_ny_parcels.rps_stdown_stdaddr_set as t2 where std_owner != '' and std_addr != '' and sum_acres > 1 \
    order by setid desc) \
    where (t1setid != t2setid) and \
    (t1row >= " + t1start + " and t1row < " + t1end + ") and \
    (t2row >= " + t2start + " and t2row < " + t2end + ")) \
    select t1setid, t2setid, t1stdown, t2stdown, t1stdadd, t2stdadd, ownratio, ownsratio, addratio, addsratio \
    from results \
    where addratio = 1.0 or addsratio = 1 or (ownratio > 0.9 and addratio > 0.9) or (ownsratio > 0.9 and addsratio > 0.9)"
    
    #run the query and retrieve the results
    fuzzyJob = client.query(fuzzyQuery)
    fuzzyRes = fuzzyJob.result()

    #iterate through rows in the results
    for row in fuzzyRes:
        #0 = t1setid, 1 = t2setid, 2 = t2stdown, 3 = t2stdown, 4 = t1stdadd, 5 = t2stdadd, 6 = ownratio, 7 = ownsratio
        #8 = addratio, 9 = addsratio
        
        #write results to the allRes dictionary 
        allRes[num] = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]] 
        ##allRes = {record number: [t1setid, t2setid, t1stdown, t2stdown, t1stdadd, t2stdadd, 
                                    ##ownratio, ownsratio, addratio, addsratio] ...}
            
        #write raw results to the log text file
        print(str(row[0]) + ", " + str(row[1]) + ", " + str(row[2]) + ", " + str(row[3]) + ", " + str(row[4]) + ", " + str(row[5]) + ", " + str(row[6]) + ", " + str(row[7]) + ", " + str(row[8]) + ", " + str(row[9]), file = log)
        
        #retrieve current setids
        t1setid = row[0]
        t2setid = row[1]
        
        ##setidCollid = {setid: collection id, ...}
        ##collidSetid = {collection id: [list of setids in the collection], ...}
        
        #if neither setid is already in the setidCollid dictionary, assign them to the same new collection id
        if t1setid not in setidCollid and t2setid not in setidCollid:
            setidCollid[t1setid] = collNum #setidCollid = {setid: collectionid, ...}
            setidCollid[t2setid] = collNum
            collidSetid[collNum] = [t1setid, t2setid] #collidSetid = {collectionid: [setid, setid, ...], ...}
            #advance collNum because a new one was just used
            collNum+=1

        #if t1setid is already in the setidCollid dictionary but t2setid isn't, assign them both to t1setid's coll id
        elif t1setid in setidCollid and t2setid not in setidCollid:
            #get the t1setid collection number
            t1setidColl = setidCollid[t1setid]
            #assign t2setid to the same collection number
            setidCollid[t2setid] = t1setidColl
            #retrive the list of setids with the collection number
            currColl = collidSetid[t1setidColl]
            #append the t2setid to the list of setids
            currColl.append(t2setid)
            #assign the appended list of setids to collection number
            collidSetid[t1setidColl] = currColl                    

        #if t2setid is already in the setidCollid dictionary but t2setid isn't, assign them both to t2setid's coll id
        elif t2setid in setidCollid and t1setid not in setidCollid:
            #get the t2setid collection number
            t2setidColl = setidCollid[t2setid]
            #assign t1setid to the same collection number
            setidCollid[t1setid] = t2setidColl
            #retrieve the list of setids with the collection number
            currColl = collidSetid[t2setidColl]
            #append the t1setid to the list of setids
            currColl.append(t1setid)
            #assign the appended list of setids to collection number
            collidSetid[t2setidColl] = currColl
            
        #if both t1setid and t2setid already in the setidCollid dictionary, check if they already have the same collid
        elif t1setid in setidCollid and t2setid in setidCollid:
            #get the t1setid collection number
            t1setidColl = setidCollid[t1setid]
            #get the t2setid collection number
            t2setidColl = setidCollid[t2setid]
            
            #if they are assigned to the same collid, pass
            if t1setidColl == t2setidColl:
                pass
            
            #if t1setid and t2setid are not assigned to the same collid, resassign everything in their collections
            #to the same new collection id
            elif t1setidColl != t2setidColl:
                #retrieve all setids in the same collection as t1setid
                currt1Sets = collidSetid[t1setidColl]
                #retrieve all setids in the same collection as t2setid
                currt2Sets = collidSetid[t2setidColl]
                #combine the lists of setids
                combColl = currt1Sets + currt2Sets
                #assign the combined lists to a new collection id
                collidSetid[collNum] = combColl
                #reassign all setids in the combined list to the same new collection id
                for comb in combColl:
                    setidCollid[comb] = collNum 
                #remove the old collid assignments from the dictionary
                collidSetid.pop(t1setidColl)
                collidSetid.pop(t2setidColl)
                #advance collNum because a new one was just used
                collNum+=1
    
    #do a time check
    end = time.time()
    #print a progress report
    print("   " + str(round((end-start),2)) + " seconds")
       
    #advance the num variable
    num+=1
    
print('all done!')

#write results of the ls ratio comparisons to a csv file
setidCollidCsv = r"C:\Users\shannon.thol\Documents\ArcGIS\Projects\ParcelAggregation\NonadjacentAggregation\dev\setidcollid_results_111720.csv"
with open(setidCollidCsv, "w") as setCsv:
    pass
print('writing setidCollid results to csv file...')
with open(setidCollidCsv, 'w', newline='') as csvFile:
    csvWriter = csv.writer(csvFile, delimiter = ',')
    csvWriter.writerow(['setid','collid'])
    for currSetid in setidCollid:
        currCollid = setidCollid[currSetid]
        csvWriter.writerow([currSetid,currCollid])
print('done writing setidCollid results to csv file')

collidSetidCsv = r"C:\Users\shannon.thol\Documents\ArcGIS\Projects\ParcelAggregation\NonadjacentAggregation\dev\collidsetid_results_111720.csv"
with open(collidSetidCsv, "w") as collCsv:
    pass
print('writing collidSetid results to csv file ...')

with open(collidSetidCsv, 'w', newline = '') as csvFile:
    csvWriter = csv.writer(csvFile, delimiter = ',')
    csvWriter.writerow(['collid','setids'])
    for currCollid in collidSetid:
        currSetid = collidSetid[currCollid]
        csvWriter.writerow([currCollid,currSetid])
        
print('done writing collidSetid results to csv file')
