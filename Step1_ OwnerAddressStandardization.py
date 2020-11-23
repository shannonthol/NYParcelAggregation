#load libraries
import psycopg2
import re
from scourgify import normalize_address_record
import time
import usaddress

#connect to postgres database - this returns a postgres connection object
connection = psycopg2.connect(user = 'postgres',
                             password = 'postgres',
                             host = '127.0.0.1',
                             port = '5432',
                             database = 'postgres')
#create a cursor object that lets us execute postgres commands through python console
cursor = connection.cursor()

#define query for reading in all owner name and address data from the statewide RPS centroids
readData = "select id, primary_owner, mail_addr, po_box, mail_city, mail_state, mail_zip, add_owner, add_mail_addr,\
            add_mail_po_box, add_mail_city, add_mail_state, add_mail_zip, owner_type, geom\
            from parcels.rpscentroids\
            "

cursor.execute(readData)

#retrieve results of the query
readTable = cursor.fetchall()

#loop through records in readTable creating a dictionary of results
readDict = dict() #readDict = {id: [primary_owner, mail_addr, po_box, mail_city, mail_state, mail_zip, add_owner,
                    #add_mail_addr, add_mail_po_box, add_mail_city, add_mail_state, add_mail_zip, owner_type, geom]}

for row in readTable:
    currList = []
    for i in range(14):
        #replace no data with a blank
        if row[i+1] is None:
            currList.insert(i,'')
        #replace white space with a blank
        elif row[i+1].replace(" ","") == "":
            currList.insert(i,'')
        else:
            currList.insert(i,row[i+1])
    readDict[row[0]] = currList
    
# create dictionary of standardized names and addresses
stdDict = dict() #stdDict = {id: [prim_owner, prim_addr, sec_owner, sec_addr, id]}

#iterate through the results in the readDict created above
for currId in readDict: #readDict = {id: [primary_owner, mail_addr, po_box, mail_city, mail_state, mail_zip, add_owner,
                    #add_mail_addr, add_mail_po_box, add_mail_city, add_mail_state, add_mail_zip, owner_type]}
    currOwnType = readDict[currId][12]
    #if the currOwnType is unknown (value -999) proceed with writing blanks for std_owner and std_addr in the stdDict
    if currOwnType == '-999':
        stdDict[currId] = ['','','','',currId] #stdDict = {id: [prim_owner, prim_addr, sec_owner, sec_addr, id]}
        
    #else if the currOwnType is not unknown, proceed with creating standardized owner names and mailing addresses
    else:
        #get current primary owner name and use it to create standardized primary owner name
        currOwn = readDict[currId][0].upper() 
        if currOwn == '':
            stdOwn = ''
        else:
            #REPLACE most non-alphanumeric characters with blanks or appropriate strings
            #RETAIN # and $ as is
            stdOwn = re.sub(' +', ' ',currOwn.replace('@', ' at ').replace('&', ' and ').replace(',',' ').replace('.',' ').
                            replace(';',' ').replace(':',' ').replace('<',' ').replace('>',' ').replace('!',' ').replace('^',' ').
                            replace('*', ' ').replace('(',' ').replace(')',' ').replace('-',' ').replace('_',' ').replace('+',' ').
                            replace('=',' ').replace('{',' ').replace('}',' ').replace('[',' ').replace(']',' ').replace('"\"',' ').
                            replace('|',' ').replace('?',' ').replace('/',' ').replace('"',' ').replace("'",' ').replace('~',' ').
                            replace('`',' ').replace('%',' ')).strip()
                            
        #get current secondary owner name and use it to create standardized secondary owner name
        currSecOwn = readDict[currId][6].upper()
        if currSecOwn == '':
            stdSecOwn = ''
        else:
            #REPLACE most non-alphanumeric characters with blanks or appropriate strings
            #RETAIN # and $ as is
            stdSecOwn = re.sub(' +', ' ',currSecOwn.replace('@', ' at ').replace('&', ' and ').replace(',',' ').replace('.',' ').
                            replace(';',' ').replace(':',' ').replace('<',' ').replace('>',' ').replace('!',' ').replace('^',' ').
                            replace('*', ' ').replace('(',' ').replace(')',' ').replace('-',' ').replace('_',' ').replace('+',' ').
                            replace('=',' ').replace('{',' ').replace('}',' ').replace('[',' ').replace(']',' ').replace('"\"',' ').
                            replace('|',' ').replace('?',' ').replace('/',' ').replace('"',' ').replace("'",' ').replace('~',' ').
                            replace('`',' ').replace('%',' ')).strip()
                
        #get current data from all primary mailing address related fields and covert to upper case
        #readDict = {id: [primary_owner, mail_addr, po_box, mail_city, mail_state, mail_zip, add_owner,
                    #add_mail_addr, add_mail_po_box, add_mail_city, add_mail_state, add_mail_zip, owner_type]}
        currAddr = readDict[currId][1].upper()
        currPobox = readDict[currId][2].upper()
        currCity = readDict[currId][3].upper()
        currState = readDict[currId][4].upper()
        currZip = readDict[currId][5].upper()[0:5] #limit to the first five characters (remove four digit zip extensions)
    
        #if both PO Box and Mailing Address data are recorded, PO Box data will be used and Mailing Address will be dropped
        #if there is no PO Box data given, proceed with checking the mailing address field
        if currPobox == '':
            
            #create a concatenated string for the current address
            concatAddr = re.sub(' +', ' ',currAddr + ' ' + currCity + ' ' + currState + ' ' + currZip).strip() 
            #if the concatenated address is blank, record the standardized address as a blank
            if concatAddr == '':
                stdAdd = ''
            
            else:
                #try to parse the address using normalize address record
                try:
                    normDict = normalize_address_record(concatAddr)
                    normAddr = (' ').join([normDict[x] for x in normDict if normDict[x] is not None])
                    stdAdd = normAddr
                except:
                    #try to standardize the address using the usaddress package tag method
                    try:
                        poboxParsed = usaddress.tag(concatAddr)
                        if poboxParsed[1] == 'PO Box':
                            currDict = poboxParsed[0]
                            currBoxId = currDict['USPSBoxID']
                            currPlace = currDict['PlaceName']
                            currState = currDict['StateName']
                            currZip = currDict['ZipCode'][0:5]
                            currConcat = 'PO BOX'+' '+currBoxId+' '+currPlace+' '+currState+' '+currZip
                            stdAdd = currConcat
                        else:
                            stdAdd = ''
                    except:
                        stdAdd = ''
                
        #if there is PO Box data given, proceed with trying to parse it 
        else:
            #create a concatenated string for the current address
            concatAddr = re.sub(' +',' ','PO BOX ' + currPobox + ' ' + currCity + ' ' + currState + ' ' + currZip).strip()
            #try to standardize the po box address using the usaddress package tag method
            try:
                poboxParsed = usaddress.tag(concatAddr)
                if poboxParsed[1] == 'PO Box':
                    currDict = poboxParsed[0]
                    currBoxId = currDict['USPSBoxID']
                    currPlace = currDict['PlaceName']
                    currState = currDict['StateName']
                    currZip = currDict['ZipCode'][0:5]
                    currConcat = 'PO BOX'+' '+currBoxId+' '+currPlace+' '+currState+' '+currZip
                    stdAdd = currConcat
                else:
                    stdAdd = ''
            except:
                stdAdd = ''
                
                
        #get current data from all secondary mailing address related fields and covert to upper case
        #readDict = {id: [primary_owner, mail_addr, po_box, mail_city, mail_state, mail_zip, add_owner,
                    #add_mail_addr, add_mail_po_box, add_mail_city, add_mail_state, add_mail_zip, owner_type]}
        currSecAddr = readDict[currId][7].upper()
        currSecPobox = readDict[currId][8].upper()
        currSecCity = readDict[currId][9].upper()
        currSecState = readDict[currId][10].upper()
        currSecZip = readDict[currId][11].upper()[0:5] #limit to the first five characters
    
        #if both PO Box and Mailing Address data are recorded, PO Box data will be used and Mailing Address will be dropped
        #if there is no PO Box data given, proceed with checking the mailing address field
        if currSecPobox == '':
            
            #create a concatenated string for the current address
            concatAddr = re.sub(' +', ' ',currSecAddr + ' ' + currSecCity + ' ' + currSecState + ' ' + currSecZip).strip() 
            if concatAddr == '':
                stdSecAdd = ''
            
            else:
                #try to parse the address using normalize address record
                try:
                    normDict = normalize_address_record(concatAddr)
                    normAddr = (' ').join([normDict[x] for x in normDict if normDict[x] is not None])
                    stdSecAdd = normAddr
                except:
                    #try to standardize the po box address using the usaddress package tag method
                    try:
                        poboxParsed = usaddress.tag(concatAddr)
                        if poboxParsed[1] == 'PO Box':
                            currDict = poboxParsed[0]
                            currBoxId = currDict['USPSBoxID']
                            currPlace = currDict['PlaceName']
                            currState = currDict['StateName']
                            currZip = currDict['ZipCode'][0:5]
                            currConcat = 'PO BOX'+' '+currBoxId+' '+currPlace+' '+currState+' '+currZip
                            stdSecAdd = currConcat
                        else:
                            stdSecAdd = ''
                    except:
                        stdSecAdd = ''           
                
        #if there is PO Box data given, proceed with trying to parse it 
        else:
            #create a concatenated string for the current address
            concatAddr = re.sub(' +',' ','PO BOX ' + currSecPobox + ' ' + currSecCity + ' ' + currSecState + ' ' + currSecZip).strip()
            #try to standardize the po box address using the usaddress package tag method
            try:
                poboxParsed = usaddress.tag(concatAddr)
                if poboxParsed[1] == 'PO Box':
                    currDict = poboxParsed[0]
                    currBoxId = currDict['USPSBoxID']
                    currPlace = currDict['PlaceName']
                    currState = currDict['StateName']
                    currZip = currDict['ZipCode'][0:5]
                    currConcat = 'PO BOX'+' '+currBoxId+' '+currPlace+' '+currState+' '+currZip
                    stdSecAdd = currConcat
                else:
                    stdSecAdd = ''
            except:
                stdSecAdd = ''
                
        #add current ID and standardized owner and address data to the stdDict          
        stdDict[currId] = [stdOwn, stdAdd, stdSecOwn, stdSecAdd, currId] 
        #stdDict = {id: [prim_owner, prim_addr, sec_owner, sec_addr, currId]}          
        
#define an update query for the standardized owner and address data
updateSql = """UPDATE parcels.rpscentroids
                SET std_owner = %s,
                    std_addr = %s,
                    std_add_owner = %s,
                    std_add_addr = %s
                WHERE id = %s
                """

#iterate through ids in the stdDict, running the update query on each in turn
for currId in stdDict: #stdDict = {id: [prim_owner, prim_addr, sec_owner, sec_addr]}  
        #retrieve update list from the stdDict #upList = [prim_owner, prim_addr, sec_owner, sec_addr, id]
        upList = stdDict[currId]
        #execute update query for current id
        cursor.execute(updateSql, upList)
        
#commit changes to db
connection.commit()

#close cursor and db connection
cursor.close()
connection.close()
print('done with db updates')

