import os, requests
from flask import Flask, request, json, jsonify
app = Flask(__name__)
import json, time, random, hashlib, copy

s_address = os.environ.get('SOCKET_ADDRESS')
print(s_address, flush = True)

#Creates a view
full_view = os.environ.get('VIEW').split(',')
view = os.environ.get('VIEW').split(',')

#Create a dictionary for our key-value store
store = {}

#the addresses of the nodes in our shard
shard = {}

#Vector clock
'''
Vector clock dictionary, each IP address mapped 
to a specific value, starting at 0 for each
'''
VC = {}

sharding_done = False


#This will send the socket address to the other replicas to add to their view
for socket in full_view:
    if socket != s_address:
        try:
            requests.put('http://' + socket + '/key-value-store-view', json = {"s_address":s_address}, timeout=2)
        except:
            view.remove(socket)
            continue
#should have regardless
shard_cnt = 0
curr_shard = 0
#Initialize if it is one of the first nodes created
if os.environ.get('SHARD_COUNT') is not None:
    #determining which shard each node is in, odds in shard1 even shard2
    shard_cnt = int(os.environ.get('SHARD_COUNT'))
    count = 1
    for i in full_view:
        if count == shard_cnt + 1:
            count = 1
        if count in shard:
            shard[count] = shard[count] + [i]
        else:
            shard[count] = [i]
        count += 1

    #just gets the shard of the current replica
    curr_shard = 0
    for i in range(1, len(shard) + 1):
        if s_address in shard[i]:
            curr_shard = i

    for socket in shard[curr_shard]:
        VC[socket] = 0


# ****************************************  This is the shard request endpoint ****************************************

@app.route('/key-value-store-shard/shard-ids', methods=["GET"])
def shard_ids_requests_endpoint():
    global store, VC, shard, curr_shard, s_address, view
    rnt_lst = []
    for i in range(1, len(shard) + 1):
        rnt_lst = rnt_lst + [i]
    return jsonify({'message': 'Shard IDs retrieved successfully', 'shard-ids': rnt_lst}), 200


@app.route('/key-value-store-shard/node-shard-id', methods=["GET"])
def node_shard_requests_endpoint():
    global store, VC, shard, curr_shard, s_address, view
    return jsonify({'message': 'Shard ID of the node retrieved successfully', 'shard-id': curr_shard}), 200


@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods=["GET"])
def shard_id_members_requests_endpoint(shard_id = None):
    global store, VC, shard, curr_shard, s_address, view
    if int(shard_id) in shard:
        return jsonify({'message': 'Members of shard ID retrieved successfully', 'shard-id-members': shard[int(shard_id)]}), 200
    return 'error'

@app.route('/key-value-store-shard/shard-id-key-count/<shard_id>', methods=["GET"])
def shard_id_key_count(shard_id = None):
    global store, VC, shard, curr_shard, s_address, view
    if int(shard_id) == curr_shard:
        return jsonify({'message':'"Key count of shard ID retrieved successfully', 'shard-id-key-count': len(store)}), 200
    if int(shard_id) in shard:
        forward_shard = shard[int(shard_id)]
    else:
        return 'error'
    for socket in forward_shard:
        try:
            #Ask node located on other shard to fulfil the request
            rsp = requests.get('http://' + socket + '/store-return')
            rsp_json = rsp.json()
            key_length_store = len(rsp_json['store'])
            return jsonify({'message':'"Key count of shard ID retrieved successfully', 'shard-id-key-count': key_length_store}), 200
        except Exception as e:
            print(e,flush=True)
            continue

    return jsonify({'message': 'Key count of shard ID retrieved successfully', 'shard-id-key-count': len(shard)}), 200

# ****************************************  This is the add node to shard endpoint ************************************
#  Add node to any shard
# INFO:
# - First a docker run command to create the node 
# - Then a put request send to the newly created node

@app.route('/key-value-store-shard/add-member/<shard_id>', methods=["PUT"])
def add_node_to_shard_endpoint(shard_id = None):
    global store, VC, shard, curr_shard, s_address, view, shard_cnt
    if int(shard_id) not in shard:
        return 'error'
    newSocketAddr = request.get_json(force=True)['socket-address']
    #updating shard
    if newSocketAddr not in shard[int(shard_id)]:
        shard[int(shard_id)] = shard[int(shard_id)] + [newSocketAddr]
    if int(shard_id) != curr_shard:
        for socket in shard[int(shard_id)]:
            try:
                #Ask node located on other shard to fulfil the request
                jsonDict = {'socket-address': newSocketAddr}
                rsp = requests.put('http://' + socket + '/key-value-store-shard/add-member/' + shard_id, json = jsonDict, timeout=2)
                return rsp.text, rsp.status_code
            except:
                continue
    else:
        #updates personal shard list and view and sends new info to all
        if newSocketAddr not in view:
            view = view + [newSocketAddr]
        if newSocketAddr not in shard[int(shard_id)]:
            shard[int(shard_id)] = shard[int(shard_id)] + [newSocketAddr]
        jsonDict = json.dumps({'socket-address': newSocketAddr, 'store': store, 'shard': shard, 'view': view})
        try:
            for socket in view:
                rsp = requests.put('http://' + socket + '/key-value-store-shard/add-member-helper', json = jsonDict, timeout=1)
        except Exception as e:
                print(e, flush=True)
    return jsonify({}), 200


@app.route('/key-value-store-shard/add-member-helper', methods=["PUT"])
def add_node_to_shard_endpoint_helper():
    global store, VC, shard, curr_shard, s_address, view, shard_cnt
    loadDict = json.loads(request.get_json())
    newSocketAddr = loadDict['socket-address']
    #global changes, taken in from json data attatched
    #below updates shard we have accounting for json changing ints to string
    temp_shard = loadDict['shard']
    for i in temp_shard:
        shard[int(i)] = temp_shard[i]
    #shard = loadDict['shard']
    view = loadDict['view']
    shard_cnt = len(shard)
    #if the address matches then work with added node
    if newSocketAddr == s_address:
        store = loadDict['store']
        #updating curr_shard for it
        for i in shard:
            if newSocketAddr in shard[i]:
                #sets curr_shard for new node
                curr_shard = i

                #requests store VC from node in the same shard
                for socket in shard[i]:
                    if socket in view:
                        try:
                            #Ask node located on other shard to fulfil the request
                            rsp = requests.get('http://' + socket + '/store-return', timeout=2)
                            data = rsp.json()
                            #updates gloabal VC 
                            VC = data['VC']
                            break
                            #return rsp.text, rsp.status_code
                        except:
                            continue
    
    #others, updates VC with new node for correct shard value: 0
    for i in shard:
        if newSocketAddr in shard[i] and i == curr_shard:
            VC[newSocketAddr] = 0
            break
    #Anyone's code here
    return 'ok'

# ****************************************  This is the resharding endpoint *******************************************

@app.route('/key-value-store-shard/reshard', methods=["PUT"])
def reshard_endpoint():
    global store, VC, shard, curr_shard, s_address, view, shard_cnt

    shard_cnt = request.get_json(force=True)['shard-count']#update global shard_cnt on local replica

    if len(view) / shard_cnt < 2: #if not possible to reshard with 2 in each
        return jsonify({'message': 'Not enough nodes to provide fault-tolerance with the given shard count!'}), 400
    
    new_shard = {} #create new shard and fill it with values
    giant_dic = {} #store every store object added into here to then redistribute
    sharding_done = True

    #reshard based off of local shard count given
    count = 1
    for i in view:
        if count == shard_cnt + 1:
            count = 1
        if count in new_shard:
            new_shard[count] = new_shard[count] + [i]
        else:
            new_shard[count] = [i]
        count += 1

    temp_dic_for_store_vc = {} #dictionary to store stores
    for i in shard:
        for socket in shard[i]:
            if socket in view:
                try:
                    #Ask node located on other shard to fulfil the request
                    #jsonDict = {'socket-address': newSocketAddr}
                    rsp = requests.get('http://' + socket + '/store-return', timeout=2)
                    data = rsp.json()
                    temp_dic_for_store_vc[i] = data['store']
                    break
                    #return rsp.text, rsp.status_code
                except:
                    continue

    #fill giant dic with ALL stores together
    for i in temp_dic_for_store_vc:
        for l in temp_dic_for_store_vc[i]:
            giant_dic[l] = temp_dic_for_store_vc[i][l]
    
    #below basically reshards each store
    new_shard_store = {}
    #set up new shard with the correct keys equal to []
    for i in range(1, shard_cnt + 1):
        new_shard_store[i] = {}
    
    print(giant_dic, flush = True) #is all stores merged together, big dictionary
    #this reshards each store to be sent
    for i in giant_dic:
        new_shard_store[calc_hash(i)][i] = giant_dic[i]
    print(new_shard_store, flush = True) # should contain number key for shard, then dictionary of correct store as value


    
    #send the stores to each replica for update, and send new shard list
    for socket in view:
        for i_shard in new_shard:
            if socket in new_shard[i_shard]:
                 json_update = json.dumps({'store': new_shard_store[i_shard], 'shard': new_shard, 'shard_id': i_shard, 'shard_count': shard_cnt})
        if(socket != s_address):
            try:
                requests.put('http://' + socket + '/key-value-store-shard/reshard-update' ,json = json_update , timeout=2)
            except Exception as e:
                print(e, flush = True)
    #update itself now
    shard = new_shard
    
    #update curr_shard
    for i_shard in new_shard:
        if s_address in new_shard[i_shard]:
            curr_shard = i_shard
    #update store
    store = new_shard_store[curr_shard]
    #VC
    VC = {} #we need to basically keep original vector clock values for each 
    for socket in shard[curr_shard]:
        VC[socket] = 0
    return jsonify({"message":"Resharding done successfully"}), 200

@app.route('/key-value-store-shard/reshard-update', methods=["PUT"])
def reshard_endpoint_update():
    global store, VC, shard, curr_shard, s_address, shard_cnt
    sharding_done = True
    loadDict = json.loads(request.get_json())
    #updates shard
    temp_shard = loadDict['shard']
    for i in temp_shard:
        shard[int(i)] = temp_shard[i]
    
    shard_cnt = loadDict['shard_count']
    store = loadDict['store']
    curr_shard = loadDict['shard_id']
    VC = {}
    for socket in shard[curr_shard]:
        VC[socket] = 0
    return jsonify({"message":"Resharding done successfully"}), 200



# ****************************************  This is the store return endpoint  ****************************************
@app.route('/store-return', methods=["GET"])
def store_return_endpoint():
    global store, VC, shard, curr_shard, s_address, view
    return jsonify(message = "Store retrieved successfully", store = store, VC = VC), 200


# ****************************************  This is the ping endpoint  ************************************************
@app.route('/ping', methods=["GET"])
def ping_endpoint():
    global store, VC, shard, curr_shard, s_address, view
    return jsonify(message = "View retrieved request successfully"), 200


# ****************************************  This is the view endpoint  ************************************************
@app.route('/key-value-store-view', methods=["GET", "PUT", "DELETE"])
def view_endpoint():
    #Initialize globals
    global store, VC, shard, curr_shard, s_address, view

    #------------------------------------Handles GET requests------------------------------------------------
    if request.method == 'GET':
        update_nodes()
        string_view = (',').join(view)
        return jsonify(message = "View retrieved successfully", view = string_view), 200

    #------------------------------------Handles PUT requests------------------------------------------------
    elif request.method == 'PUT':
        #Get socket of replica to be added
        new_replica = request.get_json(force=True)['s_address']

        if new_replica not in view:
            #Sets the socket address to '1' meaning it's running
            view.append(new_replica)
            #Sends OK response
            return jsonify(message = "Replica added successfully to the view"), 201
        #If replica already exists in view
        else:
            return jsonify(error = "Socket address already exists in the view", message = "Error in PUT"), 404
        
    #------------------------------------Handles DELETE requests------------------------------------------------
    elif request.method == 'DELETE':
        #Get ip of replica to be added
        removed_replica = request.get_json(force=True)['s_address']
         
        #This ensures that we will not have an infinite loop of put requests
        if removed_replica in view:
            #Remove replica from current view
            view.remove(removed_replica)
            return jsonify(message = "Replica deleted successfully from the view"), 200
        else:
            return jsonify(error = "Socket address does not exist in the view", message = "Error in DELETE"), 404


# ****************************************  This is the key/store endpoint  *******************************************
@app.route('/key-value-store/<key>', methods=["GET", "PUT", "DELETE"])
def key_val_store_endpoint(key=None):
    #maybe not the best but so we can access global variables, saying we do not plan on defining these
    global store, VC, shard, curr_shard, s_address, view, shard_cnt

    #Check if this is the first replica that recieved the request
    if request.get_json(silent=True) is not None and 'update' not in request.get_json(silent=True):
        update_nodes()
    
    #This is used for creating a shard list that only contains nodes that are currently running
    forward_shard = [i for i in shard[calc_hash(key)] if i in view]
    current_shard = [i for i in shard[curr_shard] if i in view]

    #---------------------------------------------- GET REQUESTS -----------------------------------
    #Handles GET request
    if request.method == "GET":
        #Used to forward to other shard if current shard does not have the key
        if(curr_shard != calc_hash(key)):
            for socket in forward_shard:
                try:
                    #Ask node located on other shard to fulfil the request
                    rsp = requests.get('http://' + socket + '/key-value-store/' + key)
                    rsp_json = rsp.json()
                    r_meta = rsp_json['causal-metadata']
                    r_val = rsp_json['value']
                    if rsp.status_code == 200:
                        return jsonify({'message':'Retrieved successfully', 'causal-metadata' : r_meta, 'value' : r_val}), 200
                    elif rsp.status_code == 404:
                        return jsonify(doesExist=False, error="Key does not exist", message="Error in GET"), 404
                except Exception as e:
                    print(e,flush=True) 
                    continue
               
            return jsonify(error="Other shard not available", message="Error in GET"), 500
        else:
            #Check if key is in our store
            if key not in store:
                return json.dumps({"doesExist":False, "error":"Key does not exist", "message":"Error in GET"}), 404
            return json.dumps({'message':'Retrieved successfully', 'causal-metadata' : json.dumps(VC), 'value' : store[key]}), 200

    #--------------------------------------------- PUT REQUESTS --------------------------------------------
    #Handles PUT request
    elif request.method == "PUT":
        #Ends if key is too long
        if len(key) > 50:
            return jsonify(error="Key is too long", message="Error in PUT"), 400

        #Used to forward to other shard if current shard does not have the key
        if(curr_shard != calc_hash(key)):
            #Forwarding request to other shard
            C_val = request.get_json(force=True)['value']
            C_meta = request.get_json(force=True)['causal-metadata']
            if 'shard-id' in request.get_json():
                C_shard = request.get_json(force=True)['shard-id']
            else:
                C_shard = curr_shard
            jsonForSend = {'value':C_val, 'causal-metadata':C_meta, 'update':False}
            for socket in forward_shard:
                try:                   
                    #Ask node located on other shard to fulfil the request
                    rsp = requests.put('http://' + socket + '/key-value-store/' + key, json=jsonForSend)
                    rsp_json = rsp.json()
                    r_meta = rsp_json['causal-metadata']
                    if rsp.status_code == 200:
                        return json.dumps({'message':'Updated successfully', 'causal-metadata':r_meta, 'shard-id':C_shard}), 200
                    elif rsp.status_code == 201:
                        return json.dumps({'message':'Added successfully', 'causal-metadata':r_meta, 'shard-id':C_shard}), 201
                    else:
                        return json.dumps({'error':'PUT failed', 'message':'Error in PUT'}), 500
                except Exception as e:
                    print(e,flush=True)
                    continue
            return jsonify(error="Other shard not available", message="Error in PUT"), 500
        else:
        #---------------------------------- PUT FROM REPLICA -----------------------------------------
            #If PUT is from other Replica, then VC, address,  included!
            
            if 'causal-metadata' in request.get_json() and 'address' in request.get_json() and 'value' in request.get_json():
                
                #taking VC string, address, causal-metadata, and value from the message from sending replica
                T_dict = json.loads(request.get_json())
                T_meta = T_dict['causal-metadata']
                T_value = T_dict['value']
                T_addr = T_dict['address']

                #If not valid VC keep looping
                while not VC_compare_replica(T_meta, T_addr):
                    time.sleep(1)

                #sets VC to senders, and updates meta
                VC = json.loads(T_meta)

                if key in store:
                #Store the JSON value into our store and replace old (key,val) pair with new val
                    store[key] = T_value
                    return jsonify(message="Updated successfully", replaced=True), 200
                else:
                    #Store the JSON value into our store and add new (key,val) pair
                    store[key] = T_value
                    return jsonify(message="Added successfully", replaced=False), 201 
            
            #------------------------------- PUT FROM CLIENT ------------------------------------------
            #If PUT is from client
            elif 'value' in request.get_json() and 'causal-metadata' in request.get_json():
                #simply extracting value and causal-metadata
                j_string = json.dumps(request.get_json())
                j_data = json.loads(j_string)
                C_val = j_data['value']
                C_meta = j_data['causal-metadata']
                
                while not VC_compare_client(C_meta):
                    for i in current_shard:     
                        if i != s_address:
                            store_rsp = requests.get('http://' + i + '/store-return', timeout=2)
                            VC_data = store_rsp.json()
                            #Comparing local VC to recieved VC
                            for socket in current_shard:
                                if VC[socket] < VC_data['VC'][socket]:   
                                    VC = VC_data['VC']
                                    store = VC_data['store']
                    time.sleep(1)

                #updates VC and then generates json to send to other requests
                VC[s_address] = VC[s_address] + 1
                jsonForSend = json.dumps({'value':C_val, 'causal-metadata':json.dumps(VC), 'address':s_address, 'update':False})

                
                #send to other nodes in shard!
                for i in current_shard:
                    if i != s_address:
                        requests.put('http://' + i + '/key-value-store/' + key, json = jsonForSend)

                #adding key and sending return message
                if key in store:
                    #Store the JSON value into our store and replace old (key,val) pair with new val
                    store[key] = C_val
                    return json.dumps({'message':'Updated successfully', 'causal-metadata':json.dumps(VC), 'shard-id':curr_shard}), 200
                else:
                    
                    #Store the JSON value into our store and add new (key,val) pair
                    store[key] = C_val
                    return json.dumps({'message':'Added successfully', 'causal-metadata':json.dumps(VC), 'shard-id':curr_shard}), 201 
                
            #runs if basically anything is missing
            
            else:
                return jsonify(error="Value is missing", message="Error in PUT"), 400
            

    #*************************************************** DELETE REQUESTS ***********************************************************
        #Handles DELETE request
    elif request.method == "DELETE":

        #Ends if key is too long
        if len(key) > 50:
            return jsonify(error="Key is too long", message="Error in PUT"), 400

        
        #Used to forward to other shard if current shard does not have the key
        if(curr_shard != calc_hash(key)):
            C_meta = request.get_json(force=True)['causal-metadata']
            C_shard = request.get_json(force=True)['shard-id']
            jsonForSend = {'causal-metadata':C_meta, 'update':False}
            for socket in forward_shard:
                try:
                    #Ask node located on other shard to fulfil the request
                    rsp = requests.delete('http://' + socket + '/key-value-store/' + key, json=jsonForSend, timeout=2)
                    rsp_json = rsp.json()
                    r_meta = rsp_json['causal-metadata']
                    if rsp.status_code == 200:
                        return json.dumps({'message':'Deleted successfully', 'causal-metadata':r_meta, 'shard-id':C_shard}), 200
                    elif rsp.status_code == 404:
                        return json.dumps({'doesExist':False, 'error':"Key does not exist", 'message':"Error in DELETE", 'shard-id':curr_shard}), 404
                except:
                    continue
            return jsonify(error="Other shard not available", message="Error in DELETE"), 500
        else:
            #--------------------------------- DELETE FROM REPLICA -----------------------------------------------
            #If DELETE is from other replica, then VC, address, causal-metadata, and value included!
            if 'causal-metadata' in request.get_json() and 'address' in request.get_json():

                #taking VC string, address, causal-metadata, and value from the message from sending replica
                T_dict = json.loads(request.get_json())
                T_meta = T_dict['causal-metadata']
                T_addr = T_dict['address']

                #if is valid VC sent, then put it into store, and update meta, if not keep looping
                while not VC_compare_replica(T_meta, T_addr):
                    time.sleep(1)

                #sets VC to senders, and updates meta
                VC = json.loads(T_meta)

                #Delete the key from store[]
                if key in store:
                #Success deleting!
                    store[key]=None
                    return jsonify(message="Deleted successfully", replaced=True), 200
                else:
                    #Error we cannot find the key
                    jsonify(doesExist=False, error="Key does not exist", message="Error in DELETE"), 404

            #------------------------------------ DELETE FROM CLIENT --------------------------------------------
            #If DELETE is from client
            elif 'causal-metadata' in request.get_json():
                #simply extracting value and causal-metadata
                j_string = json.dumps(request.get_json())
                j_data = json.loads(j_string)
                C_meta = j_data['causal-metadata']

                #if meta is present continue, if not loop until it is
                while not VC_compare_client(C_meta):
                    time.sleep(1)

                #updates VC and then generates json to send to other requests
                VC[s_address] = VC[s_address] + 1
                jsonForSend = json.dumps({'causal-metadata':json.dumps(VC),'address':s_address, 'update':False})
                
                #send to other nodes in the shard!
                for i in current_shard:
                    if i != s_address:
                        requests.delete('http://' + i + '/key-value-store/' + key, json = jsonForSend)

                #Delete the key from store[]
                if key in store:
                    #Success deleting!
                    store[key]=None
                    return jsonify({'message':'Deleted successfully', 'causal-metadata':json.dumps(VC)}), 200
                else:
                    #Error we cannot find the key
                    return jsonify(doesExist=False, error="Key does not exist", message="Error in DELETE"), 404

            #runs if basically anything is missing
    else:
        return jsonify(error="Value is missing", message="Error in request"), 400





#*************************************************** HELPER FUNCTIONS ***********************************************************

#essentially compares VC of recieved to VC of replica
def VC_compare_client(s):
    #maybe not the best but so we can access global variables, saying we do not plan on defining these
    global store, VC, shard, curr_shard, s_address, view

    #if metadata is '' then its good
    if s == '':
        return True
    
    if sharding_done:
        return True

    #this is the recieved VC string loaded to a dictionary
    T = json.loads(s)
    
    #Now we compare the VC's, if one is bigger than
    for i in VC.keys():
        if (i in T.keys()) and (T[i] > VC[i]):
            return False
    
    
    #true if no causal inconsistencies found in VC 
    return True

    
    
def VC_compare_replica(s, addr):
    #maybe not the best but so we can access global variables, saying we do not plan on defining these
    global store, VC, shard, curr_shard, s_address, view
    #if metadata is '' then its good
    if s == '':
        return True
    #this is the recieved VC string loaded to a dictionary
    T = json.loads(s)
    if sharding_done:
        return True

    #Now we compare the VC's, if one is bigger 
    for i in VC.keys():
        if i == addr:
            if i in T.keys() and T[i] != VC[i] + 1:
                return False
        else:
            if i in T.keys() and T[i] > VC[i]:
                return False
    #true if no causal inconsistencies found in VC 
    return True

#Getting current status of view
def update_nodes():
    #Use the global view as well as current working socket
    global store, VC, shard, curr_shard, s_address, view, full_view
    remove_list = []
    # Check if any replicas have crashed or failed 
    for replica_ip in full_view:
        if replica_ip != s_address:
            try:
                resp = requests.get('http://' + replica_ip + '/ping', timeout=0.5)
                # Won't get to this line if we get a timeout error
            except:
                if replica_ip in view:
                    view.remove(replica_ip)
                    remove_list.append(replica_ip)
                continue

    #Broadcast to other replicas that the view needs to be updated
    for replica_ip in view:
        if replica_ip != s_address:
            for r_ip in remove_list:
                requests.delete('http://' + replica_ip + '/key-value-store-view', json = {"s_address":r_ip}, timeout=5)

    #updates this current node's shard lists
    #update_shard()

    return None

# Calculates the hash of the key
# Returns the shard number that the key is located on
def calc_hash(key_str):
    global store, VC, shard, curr_shard, s_address, view, full_view, shard_cnt
    hashed = hashlib.sha256(key_str.encode())
    hashed_int = int(hashed.hexdigest(), 16)  
    
    return((hashed_int % shard_cnt) + 1)




    
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8085, debug=True)
