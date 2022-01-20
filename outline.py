# Imports
from collections import defaultdict
from flask import Flask, request, Response
import json, os, requests, sys, time
from consistenthash import *
# Create app
app = Flask(__name__)

# Storage data structures
# ---------------------------------------------------------------------------
key_dict = {}
replicas = []
socket = os.environ.get('SOCKET_ADDRESS') # Replica gets its own IP and port
vclock = defaultdict(int)
num_shards = None
hash_ring = None

if not socket:
    sys.exit('No socket address specified????')
if os.environ.get('VIEW'):
    replicas = os.environ.get('VIEW').split(',') # Replica gets other IPs and Ports

if os.environ.get('SHARD_COUNT'): # Note: not all nodes will have this value
    num_shards = int(os.environ.get('SHARD_COUNT'))
    hash_ring = HashRing(num_shards)

else:
    num_shards = 1 # TODO: default value. change later ? 
shard_ids = defaultdict(int) # Shard_ids dict of {replica: shard_id} pairings
shard_counter = 1 # Handles unique ids for shards
# Initialize shard_ids to -1 as an uninitialized, invalid value
for replica in replicas:
    shard_ids[replica] = -1


# Replica just created, broadcasts PUT request to all other replicas in view
# If timeout: assume timeout'd replica has gone down, broadcast DELETE 
# Random broadcasting
def broadcast_delete(down_replicas):
    global replicas
    while down_replicas:
        sock_to_del_json = {'socket-address': down_replicas.pop(0)}
        for replica in replicas:
            if replica == socket:
                continue
            try:
                requests.delete(f'http://{replica}/key-value-store-view', json = json.dumps(sock_to_del_json))
            except:
                print(f'Error, timeout deleting socket from {replica}')
                down_replicas.append(replica)
    # DEBUG
    print("printing replicas that are up")
    for replica in replicas:
        print(replica, " ")

    if down_replicas:
        print("UH OH! down_replicas in broadcast_delete:")
        for down_replica in down_replicas:
            print(down_replica)
            if down_replica in replicas:
                replicas.remove(down_replica)
        broadcast_delete(down_replicas)
    

def start_up_broadcast():
    global key_dict, replicas, shard_ids, vclock
    down_replicas = []

    # Add replica to a shard
    # Methodology: Replicas is an ordered list. We are given the SHARD_COUNT on initialization of each node.
    #              I am doing replica_order_in_Replicas MOD shard_count such that the initial replicas are
    #              dispersed evenly to each shard

    # base off view, number of nodes/num shards
    # This statement initially assigns the shard ids
    if os.environ.get('SHARD_COUNT'):
        #print(f'Assigning value { replicas.index(socket) % num_shards} to replica {socket}. Place in list: {replicas.index(socket)}')
        initial_replicas = os.environ.get('VIEW').split(',')
        initial_replicas.sort()
        offset = (len(initial_replicas) / num_shards)
        curr_id = 0
        counter = 0
        for replica in initial_replicas: # should work
            shard_ids[replica] = counter % num_shards
            counter += 1
            # if counter == offset:
            #     curr_id += 1
        #shard_ids[socket] = initial_replicas.index(socket) % num_shards

    for replica in replicas:
        print(f"Loop in start_up_broadcast for {replica}")
        if replica == socket:
            continue
        sock_to_add_json = {'socket-address': socket}

        try:
            # Add this replica to other replicas' views
            requests.put(f'http://{replica}/key-value-store-view', json = json.dumps(sock_to_add_json))
            
            # Retrieve key-value dictionary from a running replica
            # DEBUG: need to retrieve from a replica in the same shard! UPDATE: should work now? Not tested.
            if not key_dict:
                if shard_ids[replica] == shard_ids[socket]:
                    broadcast_get = requests.get(f'http://{replica}/key-value-store').json()
                    print(f'GET Response from {replica} during key value retrieval startup:{broadcast_get}')
                    key_dict = dict(broadcast_get['store'])
                    vclock = dict(broadcast_get['causal-metadata'])
        except:
            down_replicas.append(replica)
            continue

    for down_replica in down_replicas:
        if down_replica in replicas:
            replicas.remove(down_replica)
            print(f"{down_replica} removed from replicas")

    broadcast_delete(down_replicas)

# Start up broadcast
start_up_broadcast()

for replica in replicas:
    vclock[replica] = 0
print(vclock)

# NOTE: current thought: replicas are updated everytime a client sends a req, as a broadcast 
# is sent to all replicas to note of the change

# Key Value Store Endpoint
# Requests from Client to a Replica
# ----------------------------------------------------------------------------
@app.route('/key-value-store', methods=['GET'])
def main_path():
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    if request.method == 'GET':
        resp_json = {
            'store': key_dict,
            'causal-metadata': vclock
        }
        status = 200
    return Response(json.dumps(resp_json), status, mimetype="application/json")

# This may need to be reworked. I think keeping a record of previously received
# Causal meta data might be good.
def compare_vclocks(sender_clock, receiver_clock):
    # Check that for the keys the client has
        # The receiver has the client's keys
        # The receiver is allowed to have more keys than the client
    replicas_from_sender = list(sender_clock.keys())
    replicas_in_receiver = list(receiver_clock.keys())
    if not all(x in replicas_in_receiver for x in replicas_from_sender):
        return False
    # Check that for the matching key
    for replica in sender_clock:
        if not sender_clock[replica] <= receiver_clock[replica]:
            return False
    return True

#Given the sender's clock is ahead of the recvr's clock, return the list of replicas
#that may have the up to date key-value data.
#Precondition: compare_vclocks(sender, receiver) == False
def get_ahead_vclocks(sender_clock, receiver_clock):
    ret = []
    for replica in sender_clock:
        if replica not in receiver_clock.keys():
            ret.append(replica)
        elif sender_clock[replica] > receiver_clock[replica]:
            ret.append(replica)
    return ret

def merge_vclock(sender_clock):
    global vclock
    for replica in sender_clock:
        if replica in vclock:
            vclock[replica] = max(vclock[replica], sender_clock[replica])
        else:
            vclock[replica] = sender_clock[replica]

# Still need to generate causal meta data
@app.route('/key-value-store/<key>', methods=['GET', 'PUT', 'DELETE'])
def key_func(key):
    global key_dict, replicas, vclock

    try:
        req_json = request.get_json()
    except:
        req_json = None

    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    if req_json and req_json.get('causal-metadata'):
        caus_meta = req_json.get('causal-metadata')
        if (caus_meta == ""):
            caus_meta = dict()
        if not compare_vclocks(caus_meta, vclock):
            to_query = get_ahead_vclocks(caus_meta, vclock)
            #to_query are the replicas that have a vclock entry that indicates they have stuff we don't
            #this replica will now attempt to update
            for replica in to_query:
                try: 
                    # TODO: Need to get key_dict store ONLY from a replica in the same shard!!
                    shard_check = hash_ring.check(key)
                    if shard_ids[replica] != shard_ids[socket]:
                        continue
                    resp = requests.get(f'http://{replica}/key-value-store-consistency', timeout=20)
                    key_dict = resp.json()['store']
                    # vclock = resp.json()['causal-metadata'] #TODO maybe merge incoming caus_meta with existing vclock?
                    merge_vclock(resp.json()['causal-metadata'])
                except Exception as e: # need to broadcast delete
                    app.logger.info(f'{replica} is down?')
                    app.logger.info(e)
                if compare_vclocks(caus_meta, vclock):
                    #we are now up to date enough to handle the incoming request
                    break

    # NOTE: for GET, we do not need to communicate with other replicas
    if request.method == 'GET':
        # Check if key belongs to this shard or another one
        shard_with_key = hash_ring.check(key)
        if shard_with_key == shard_ids[socket]:
            # Key belongs to this shard
            if key in key_dict:
                resp_json = {
                    'message': 'Retrieved successfully', 
                    'value': key_dict[key],
                    'causal-metadata': vclock
                }
                status = 200
            else:
                resp_json = {
                    'error': 'Key does not exist', 
                    'message': 'Error in GET',
                    'causal-metadata': vclock
                    }
                status = 404
        else:
            # Forward request to a node in the correct shard
            node_in_correct_shard = None
            for replica in shard_ids:
                if shard_ids[replica] == shard_with_key:
                    node_in_correct_shard = replica
                    break
            if node_in_correct_shard != None:
                forwarded = requests.get(f'http://{node_in_correct_shard}/key-value-store/{key}', timeout=20)
                resp_json = forwarded.json()
                status = forwarded.status_code
            else:
                resp_json = {
                    'error': 'Key does not exist', 
                    'message': 'Error in GET',
                    'causal-metadata': vclock
                    }
                status = 404
        return Response(json.dumps(resp_json), status, mimetype='application/json')
    
    elif request.method == 'PUT':
        shard_check = hash_ring.check(key)
        if shard_check != shard_ids[socket]:
            replica_to_forward = None
            for replica, shard in shard_ids.items():
                if shard == shard_check:
                    replica_to_forward = replica
                    break
            shard_resp = requests.put(f'http://{replica_to_forward}/key-value-store/{key}', json=req_json, timeout=20)
            return Response(shard_resp.text, shard_resp.status_code, mimetype='application/json')
        down_replicas = []
        if req_json:
            if 'value' not in req_json or key is None: # This line MAY need to be changed
                resp_json = {
                    'error':'Value is missing',
                    'message':'Error in PUT'
                }
                status = 400
            elif len(key) > 50:
                resp_json = {
                    'error':'Key is too long',
                    'message':'Error in PUT'
                }
                status = 400
            elif key not in key_dict:
                key_dict[key] = req_json['value']
                vclock[socket] += 1
                resp_json = {
                    'message':'Added successfully',
                    'shard-id': shard_check,
                    'causal-metadata': vclock
                }
                status = 201
                for replica in replicas:
                    # update key_dict in all other replicas
                    if replica == socket:
                        continue
                    if shard_ids[replica] != shard_ids[socket]:
                        # Only broadcast PUT to nodes in the same shard. Ignore other shards
                        continue
                    try: 
                        requests.put(f'http://{replica}/key-value-store-consistency/{key}', 
                        json=json.dumps({
                            'value': req_json['value'], 
                            'causal-metadata': vclock,
                            'sender': socket
                            }), timeout=20)
                        
                    except: # need to broadcast delete
                        down_replicas.append(replica)
                for down_replica in down_replicas:
                    replicas.remove(down_replica)
                for down_replica in down_replicas: # A function and loop should be made to continually check that a replica is not down and handle exceptions
                    #replicas.remove(down_replica)  # This is a bandaid fix right now 
                    for replica in replicas:
                        requests.delete(f'http://{replica}/key-value-store-view',
                            json=json.dumps({
                                'socket-address': down_replica
                            }), timeout=20)
                        if down_replica in replicas:
                            replicas.remove(down_replica)
                
            else:
                key_dict[key] = req_json['value']
                vclock[socket] += 1
                # update key_dict in all other replicas and update vclock
                resp_json = {
                    'message':'Updated successfully',
                    'shard-id': shard_check, 
                    #'replaced': True
                    'causal-metadata': vclock
                }
                status = 200
                for replica in replicas:
                    # update key_dict in all other replicas and update vclock
                    if replica == socket:
                        continue
                    if shard_ids[replica] != shard_ids[socket]:
                        # Only broadcast PUT to nodes in the same shard. Ignore other shards
                        continue
                    try: 
                        requests.put(f'http://{replica}/key-value-store-consistency/{key}', 
                        json=json.dumps({
                            'value': req_json['value'], 
                            'causal-metadata': vclock,
                            'sender': socket
                            }), timeout=20)
                        
                    except: # need to broadcast delete
                        down_replicas.append(replica)
                for down_replica in down_replicas:
                    replicas.remove(down_replica)
                for down_replica in down_replicas:
                    for replica in replicas:
                        requests.delete(f'http://{replica}/key-value-store-view',
                            json=json.dumps({
                                'socket-address': down_replica
                            }), timeout=20)
                        if down_replica in replicas:
                            replicas.remove(down_replica)
    elif request.method == 'DELETE':
        # Initial check to see if this is the correct shard
        shard_check = hash_ring.check(key)
        if shard_check != shard_ids[socket]:
            replica_to_forward = None
            for replica, shard in shard_ids.items():
                if shard == shard_check:
                    replica_to_forward = replica
                    break
            shard_resp = requests.delete(f'http://{replica_to_forward}/key-value-store/{key}', json=json.dumps(req_json), timeout=20)
            return Response(shard_resp.text, shard_resp.status_code, mimetype='application/json')
            # May want to check if a rpelica is down

        if key in key_dict:
            del key_dict[key]
            vclock[socket] += 1
            resp_json = {
                'message'   : 'Deleted successfully',
                'shard-id': shard_check,
                'causal-metadata': vclock
            }
            status = 200
            for replica in replicas:
                # update key_dict in all other replicas and update vclock
                if replica == socket:
                    continue
                if shard_ids[replica] != shard_ids[socket]:
                    # Only broadcast PUT to nodes in the same shard. Ignore other shards
                    continue
                try: 
                    requests.delete(f'http://{replica}/key-value-store-consistency/{key}', 
                    json=json.dumps({'causal-metadata': vclock, 'sender': socket}), timeout=20)
                except: # need to broadcast delete
                    down_replicas.append(replica)
            for down_replica in down_replicas:
                replicas.remove(down_replica)
            for down_replica in down_replicas:
                for replica in replicas:
                    requests.delete(f'http://{replica}/key-value-store-view', json=json.dumps({'socket-address': down_replica}), timeout=20)
                    if down_replica in replicas:
                        replicas.remove(down_replica)
        else:
            resp_json = {
                'doesExist' : False,
                'error'     : 'Key does not exist',
                'message'   : 'Error in DELETE'
            }
            status = 404
        pass
    return Response(json.dumps(resp_json), status, mimetype='application/json')

# View Endpoint
# ----------------------------------------------------------------------------
@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def replica_view():
    global replicas
    try:
        req_json = request.get_json()
    except:
        req_json = {}
    resp_json = {'message': 'This method is unsupported.'}
    status = 405 
    if request.method == 'GET':
        resp_json = {
            'message': 'View retrieved successfully', 
            'view': ','.join(replicas)
        }
        status = 200
    elif request.method == 'PUT':
        if req_json:
            req_json = json.loads(req_json)
            if req_json.get('socket-address') in replicas:
                resp_json = {
                    'error': 'Socket address already exists in the view',
                    'message': 'Error in PUT'
                }
                status = 404
            elif req_json.get('socket-address'):
                replicas.append(req_json.get('socket-address'))
                if not req_json.get('socket-address') in vclock:
                    vclock[req_json.get('socket-address')] = 0
                replicas.sort()
                resp_json = {
                    'message': 'Replica added successfully to the view'
                }
                status = 201
    elif request.method == 'DELETE':
        if req_json:
            req_json = json.loads(req_json)
            try:
                replicas.remove(req_json.get('socket-address'))
                resp_json = {
                    'message': 'Replica deleted successfully from the view'
                }
                status = 200
            except ValueError:
                resp_json = {
                    'error': 'Socket address does not exist in the view',
                    'message': 'Error in DELETE'
                }
                status = 404
    return Response(json.dumps(resp_json), status, mimetype='application/json')
  
# Consistency Endpoint
# Handles key-dict-store requests from Replica to a Replica
# GET: get a replica's vclock and key_dict, 
# PUT: payload: vclock and key_dict, check own vclock on whether or not to 'deliver' i.e. update own key_dict and vector clock
# DELETE: payload: vclock and key_dict, check own vclock on whether or not to 'deliver' i.e. delete key in own key_dict and update vector clock
# ----------------------------------------------------------------------------
def internal_causal_check(sender_causal_metadata, sender_socket):   
    # Check if able to deliver based on vector clock values
    no_missing_msgs = True
    for replica in replicas:
        if replica == sender_socket:
            continue
        if not sender_causal_metadata[replica] <= vclock[replica]: # Key error
            no_missing_msgs = False
            break
    if no_missing_msgs and sender_causal_metadata[sender_socket] == vclock[sender_socket] + 1:
        # Set internal vclock to pointwise maximum
        for v in vclock: # where v is each key
            vclock[v] = max(vclock[v], sender_causal_metadata[v])
        return True
        
    return False

@app.route('/key-value-store-consistency/<key>', methods=['GET', 'PUT', 'DELETE'])
def replica_to_replica_consistency(key):
    global key_dict, replicas, vclock
    try:
        req_json = json.loads(request.get_json())
    except:
        req_json = None
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    count = 0
    if req_json and req_json.get('causal-metadata') and req_json.get('sender'):
        caus_meta = req_json.get('causal-metadata')
        if (caus_meta == ""):
            caus_meta = dict()
        sender = req_json.get('sender')
        while not internal_causal_check(caus_meta, sender): # Sleep/Busy wait until vclock is up to date, MAY need to add a timeout
            time.sleep(0.01)
            count += 1
            if count == (1000):
                return f'Busy wait fail vclock: {vclock}\n sender vclock: {caus_meta}'
    if request.method == 'PUT':
        req_json = dict(req_json)
        value = req_json['value']
        key_dict[key] = value
        resp_json = {
            'message': f'Successfully replicated PUT in replica {socket}'
        }
        status = 200
    elif request.method == 'DELETE':
        req_json = dict(req_json)
        if key in key_dict:
            del key_dict[key]
            resp_json = {
                'message': f'Successfully replicated DELETE in replica {socket}'
            }
            status = 200
        else:
            resp_json = {
                'message': f'Unable to replicate DELETE in replica {socket}',
                'error': 'No key found'
            }
            status = 404
    return Response(json.dumps(resp_json), status, mimetype='application/json')

@app.route('/key-value-store-consistency', methods=['GET'])
def replica_key_value_copy():
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    if request.method == 'GET':
        resp_json = {
            'message': f'Key-value data from replica {socket}',
            'store': key_dict,
            'causal-metadata': vclock,
            'shard_ids': shard_ids,
            'num_shards': num_shards,
            'shard': shard_ids[socket]
        }
        status = 200
    return Response(json.dumps(resp_json), status, mimetype='application/json')

# Causal consistency not needed for shard operations
# Node DO NOT automatically reshard

# Key-Value-Store-Shard Endpoints
# --------------------------------------------------------------------------------------------------
@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def return_all_shard_ids():
    # Return shard IDs of all shards in the store
    unique_ids = set(id for id in shard_ids.values()) 
    resp_json = {
        'message': f'Shard IDs retrieved successfully',
        'shard-ids': list(unique_ids), 
        'causal-metadata': vclock
    }
    status = 200
    return Response(json.dumps(resp_json), status, mimetype='application/json')

@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def return_node_shard_ids():
    # Return shard ID of this node
    resp_json = {
        'message': f'Shard ID of the node retrieved successfully',
        'shard-id': shard_ids[socket],
        'causal-metadata': vclock
    }
    status = 200
    return Response(json.dumps(resp_json), status, mimetype='application/json')

@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods=['GET'])
def return_members_of_shard_id(shard_id):
    # Return members of a shard given a shard id
    # Shard-id-members should be a json array of the sockets [(ip:port)] of the members
    resp_json = {
        'message': f'Members of shard ID retrieved successfully',
        'shard-id-members': [k for k in shard_ids.keys() if shard_ids[k] == int(shard_id)],
        'causal-metadata': vclock
    }
    status = 200
    return Response(json.dumps(resp_json), status, mimetype='application/json')

@app.route('/key-value-store-shard/shard-id-key-count/<shard_id>', methods=['GET'])
def return_number_of_keys_in_shard(shard_id):
    # Return the number of keys stored in the shard specified
    # if shard_id == this node's shard_id, calculate the stuff (maybe check # causmeta first?)
    # else forward to the proper node
    shard_id = int(shard_id)
    if shard_id == shard_ids[socket]:
        # TODO check causmeta and count keys
        # ^ Spec says we don't need to check metadata for shard operations
        resp_json = {
            'message': 'Key count of shard ID retrieved successfully',
            'shard-id-key-count': len(key_dict) # iterating through a dict only provides the keys
        }
        status = 200
    else: # nice
        for s in shard_ids.keys():
            if shard_ids[s] == shard_id:
                forwarded = requests.get(f'http://{s}/key-value-store-shard/shard-id-key-count/{shard_id}')
                resp_json = forwarded.json()
                status = forwarded.status_code
                break
    return Response(json.dumps(resp_json), status, mimetype='application/json')

@app.route('/key-value-store-shard/add-member/<shard_id>', methods=['PUT'])
def add_node_to_shard(shard_id):
    # Add a node to a shard, this will not have a shard count when started
    global shard_ids, key_dict, num_shards, hash_ring
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    req_json = request.get_json()
    #req_json['noprop'] = True
    if req_json is None:
        return Response(json.dumps(resp_json), status, mimetype='application/json')
    shard_id = int(shard_id)
    # Need to broadcast to other replicas the addition
    shard_ids[req_json.get('socket-address')] = shard_id
    if not(req_json.get('noprop')):
        req_json['noprop'] = True
        for replica in replicas:
            if replica == socket:
                continue
            response = requests.put(f'http://{replica}/key-value-store-shard/add-member/{shard_id}', json=req_json)
            if (response.status_code == 500):
                app.logger.info(response.content)
            # TODO: if statement: if response's shard is same shard, set this shard_ids to returned shard_ids from endpoint
            """
            if int(response.json()['shard']) == shard_id:
                shard_ids = dict(response.json()['shard_ids'])
                shard_ids[req_json.get('socket-address')] = shard_id
                num_shards = int(response.json()['num_shards'])
                hash_ring = HashRing(num_shards)
                """
            # TODO: Check for downed replica
    #if this is the replica, get key_dict from another node in the shard
    if req_json.get('socket-address') == socket:
        for replica in replicas:
            if replica == socket:
                continue
            response = requests.get(f'http://{replica}/key-value-store-consistency', timeout=20)
            if int(response.json()['shard']) == shard_id:
                shard_ids = dict(response.json()['shard_ids'])
                shard_ids[req_json.get('socket-address')] = shard_id
                num_shards = int(response.json()['num_shards'])
                hash_ring = HashRing(num_shards)
                req_json['flagshit'] = True
                break

        for replica in replicas:
            if replica == socket:
                continue
            if shard_ids[replica] == shard_id:
                resp = requests.get(f'http://{replica}/key-value-store-consistency', timeout=20)
                key_dict = dict(resp.json()['store'])
                break
    resp_json = {
        'shard': shard_ids[socket],
        'shard_ids': shard_ids,
        'num_shards': num_shards
    }
    status = 200
    app.logger.info(shard_ids)
    if (req_json.get('flagshit')):
        to_del = []
        for key in key_dict.keys():
            shardnum = hash_ring.check(key)
            if shardnum != shard_ids[socket]:
                #if the key belongs somewhere else
                for replica in shard_ids.keys():
                    if shard_ids[replica] == shardnum and key in key_dict:
                        key_update = {
                            'key': key,
                            'value': key_dict[key]
                        }
                        #requests.put(f'http://{replica}/key-value-store-reshard-transfer/', json=json.dumps(key_update), timeout=10)
                        to_del.append(key)
        for k in to_del:
            if k in key_dict:
                del key_dict[k]
    return Response(json.dumps(resp_json), status, mimetype='application/json')

@app.route('/key-value-store-shard/add-member-no-fwd/<shard_id>', methods=['PUT'])
def add_node_to_shard_no_fwd(shard_id):
    # Add a node to a shard, this will not have a shard count when started
    global key_dict
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    req_json = request.get_json()
    if req_json is None:
        return Response(json.dumps(resp_json), status, mimetype='application/json')
    req_json = json.loads(req_json)
    shard_id = int(shard_id)
    # Need to broadcast to other replicas the addition
    shard_ids[req_json.get('socket-address')] = shard_id
    #if this is the replica, get key_dict from another node in the shard
    if req_json.get('socket-address') == socket:
        for replica in replicas:
            if replica == socket:
                continue
            if shard_ids[replica] == shard_id:
                resp = requests.get(f'http://{replica}/key-value-store-consistency', timeout=20)
                key_dict = dict(resp.json()['store'])
                break
    resp_json = {
        'shard': shard_ids[socket],
        'shard_ids': shard_ids
    }
    status = 200
    return Response(json.dumps(resp_json), status, mimetype='application/json')

def clean_keys():
    to_del = []
    for key in key_dict.keys():
        if hash_ring.check(key) != shard_ids[socket]:
            to_del.append(key)
    for key in to_del:
        if key in key_dict.keys():
            del key_dict[key]


@app.route('/key-value-store-shard/reshard', methods=['PUT'])
def reshard():
    global shard_ids, key_dict, num_shards, hash_ring
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    req_json = request.get_json()
    if req_json is None:
        return Response(json.dumps(resp_json), status, mimetype='application/json')
    # try:
    #     req_json = json.loads(request.get_json())
    # except:
    #     return Response(json.dumps(resp_json), status, mimetype='application/json')

    if not(len(replicas) / int(req_json.get('shard-count')) >= 2):
        resp_json = {
            'message': 'Not enough nodes to provide fault-tolerance with the given shard count!'
        }
        status = 400
        return Response(json.dumps(resp_json), status, mimetype='application/json')
    # Reshard
    # Make sure it is possible to reshard before doing anything

    # This will probably need to be changed later
    num_shards = int(req_json.get('shard-count'))
    hash_ring = HashRing(num_shards)
    counter = 0
    shard_ids = defaultdict(int)
    initial_replicas = replicas.copy()
    initial_replicas.sort()
    #num_shards = req_json.get('shard-count')
    for replica in initial_replicas: # should work
        shard_ids[replica] = counter % num_shards
        counter += 1
    # Need to broadcast the reshard to other nodes
    if not(req_json.get('noprop')):
        # req_json['noprop'] = True
        for replica in initial_replicas:
            if replica == socket:
                continue
            send_json = {
                'shard_ids': shard_ids,
                'shard-count': num_shards,
                'noprop': True
            }
            response = requests.put(f'http://{replica}/key-value-store-shard/reshard', json=send_json, timeout=10)
            if (response.status_code != 200):
                # app.logger.info(response.content)
                resp_json = {"status": str(response.status_code), "message": str(response.content), "replica": replica}
                return Response(json.dumps(resp_json), status, mimetype='application/json')
    else:
        shard_ids = dict(req_json.get('shard_ids'))
    for key in key_dict.keys():
            place = int(hash_ring.check(key))
            if place != shard_ids[socket]:
                for replica in shard_ids.keys():
                    if shard_ids[replica] == place:
                        resp = requests.put(f'http://{replica}/key-value-store-transfer', json={
                                'key': key, 
                                'value': key_dict[key]
                            }, timeout=10)
    for replica in initial_replicas:
        if replica == socket:
            clean_keys()
            continue
        resp = requests.get(f'http://{replica}/key-value-store-reshard-clean', timeout=10)

    for replica in replicas:
        if replica == socket:
            continue
        response = requests.get(f'http://{replica}/fix-everything', timeout = 20)
    resp_json = {'message': 'Resharding done successfully.', 'shards': shard_ids}
    status = 200
    return Response(json.dumps(resp_json), status, mimetype='application/json')

@app.route('/key-value-store-reshard-transfer', methods=['PUT'])
def reshard_recv():
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    req_json = request.get_json()
    if req_json is None:
        return Response({'message': 'JSON not recvd'}, 400, mimetype='application/json')
    key_dict[req_json.get('key')] = req_json.get('value')
    return Response({'message':'Success'}, 200, mimetype='application/json')

@app.route('/key-value-store-reshard-clean', methods=['GET'])
def reshard_force_clean():
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    clean_keys()
    return Response({'message':'Success'}, 200, mimetype='application/json')

@app.route('/fix-everything')
def fixitnow():
    global key_dict, shard_ids, hash_ring
    if len(key_dict) == 0:
        for replica in replicas:
            if shard_ids[replica] == shard_ids[socket]:
                response = requests.get(f'http://{replica}/key-value-store-consistency', timeout = 20)
                key_dict.update(response.json()['store'])
                if len(key_dict) == 0:
                    continue
                else:
                    break
    for replica in replicas:
        if shard_ids[replica] == shard_ids[socket]:
            response = requests.get(f'http://{replica}/key-value-store-consistency', timeout = 20)
            key_dict.update(response.json()['store'])

# Main Code to run
if __name__ == "__main__":
    app.run(host = '0.0.0.0', port = 8085, debug = True, threaded=True, use_reloader=False)