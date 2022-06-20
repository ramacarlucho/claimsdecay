#import requests
import json
import sys
import evmos.claims.v1.query_pb2_grpc as queries
import evmos.claims.v1.query_pb2 as params
import cosmos.base.query.v1beta1.pagination_pb2 as pagination
import grpc
from protobuf_to_dict import protobuf_to_dict
import math
import os

#session = requests.Session()

#READ FROM ENV
initial_height = os.getenv('initial_height', default = 265401)
end_height = os.getenv('end_height', default = 265401)

grpc = os.getenv('grpc', default='0.0.0.0:9090')

# CLAIMS STATUS BEFORE DECAY GOT ACTIVATED 
initial_claims = {}

# CLAIMS STATUS AFTER DECAY GOT DEACTIVATED
final_claims = {}

# Heights at which each address perform a claim
heights = {}

# Get all claims records for every address for a specific height
def get_claims(height):
  with grpc.insecure_channel(grpc) as channel:
    stub = queries.QueryStub(channel)
    page = pagination.PageRequest(offset=0)
    response = stub.ClaimsRecords(params.QueryClaimsRecordsRequest(pagination=page), metadata=[('x-cosmos-block-height',str(height))])
    dic = protobuf_to_dict(response)
    yield dic

    num_pages = math.ceil(dic['pagination']['total'] / 200)
    for page in range(1, num_pages):
      # TODO rotating files backup
      page = pagination.PageRequest(offset=(200 * page))
      response = stub.ClaimsRecords(params.QueryClaimsRecordsRequest(pagination=page, metadata=[('x-cosmos-block-height',str(height))]))
      dic = protobuf_to_dict(response)
      yield next_page


# Get all claims record for address for a specific height
def get_claim(address, height):
  with grpc.insecure_channel(grpc) as channel:
    stub = queries.QueryStub(channel)
    response = stub.ClaimsRecord(request=params.QueryClaimsRecordRequest(address=address), metadata=[('x-cosmos-block-height',str(height))])
    dic = protobuf_to_dict(pb=response, including_default_value_fields=True)
    return dic
    

# Check which address performed a claim during decay
def get_claim_performed(initial, final):
  modified = []
  for address in initial.keys():
    i = initial[address]
    f = final[address]
    if i['actions_completed']!=f['actions_completed']:
      modified.append(address)
  return modified


# Check claim status at each height
# and store the changes
# we can use the heights and block time to calculate decay 
def save_height_claims(address, claim):
  actions = claim['actions_completed']
  with open('heights_claimed.json', 'a') as f:
    for height in range(initial_height+1, end_height):
        c = get_claim(address, str(height))
        arr = []
        for action in c['claims']:
          arr.append(action['completed'])

        if arr != actions:
          actions=arr
          heights[address].append(height)
        if actions == final_claims[address]['actions_completed']:
          # append to file
          json.dump({'address': address, 'heights': heights[address]}, f)
          f.write(',\n')
          return

if __name__ == "__main__":
  print ("Request Initial claims")
  for page in get_claims(str(initial_height)):
    print(page)
    for claim in page['claims']:
      initial_claims[claim['address']]={'initial_claimable_amount':claim['initial_claimable_amount'], 'actions_completed': claim['actions_completed']}
  
  
  print ("Storing Initial claims")
  # StoreToFile initial state
  with open('initial_claims.json', 'w') as f:
    json.dump(initial_claims, f)
  
  print ("Request final claims")
  final_claims = {}
  for page in get_claims(str(end_height)):
    for claim in page['claims']:
      final_claims[claim['address']]={'initial_claimable_amount':claim['initial_claimable_amount'], 'actions_completed': claim['actions_completed']}

  print ("Storing final claims")
  # StoreToFile final state
  with open('final_claims.json', 'w') as f:
    json.dump(final_claims, f)
  
  print ("Calculating differences")

  m = get_claim_performed(initial_claims, final_claims)
  
  print ("Storing differences")
  # StoreToFile claimed during decay
  with open('claims_during_decay.json', 'w') as f:
    json.dump(m, f)

  print ("Requesting claims")
  for address in m:
    print ("\n\nRequesting claims for address" + address)
    heights[address]=[]
    save_height_claims(address, initial_claims[address])
    with open('last_address.json', 'w') as f:
      json.dump(address, f)