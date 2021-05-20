import json
import requests
from string import Template
import os
from flask import Response

app_id = os.environ.get('app_id')
token = os.environ.get('token')


headers = {'App-ID': app_id, 'Auth-Token': token}
iconik_url = 'https://app.iconik.io/API/'

job_create_payload = """{"custom_type":"Podcast Folder Structure",
  "object_id":"$objectID",
  "object_type":"collections",
  "status":"STARTED",
  "title":"Bulk collection metadata update for: $CollectionName",
  "type":"CUSTOM"
}"""

job_update_payload = """{
  "id": "426da862-655c-11eb-8f50-0a580a3f8d19",
  "progress_processed": 30,
}"""

make_notification = """{
  "event_type": "finished","message_long": "Collection Metadata Updated!","message_short": "Collection Metadata Updated","object_id": "$job_id","object_type": "jobs","recipient_id": "$user_id","sender_id":"e414116e-9957-11ea-b502-0a580a3c65d8"}"""


#check to see if recieved webhook is from Spotify's Iconik instance
def check_validity(webhook):
    try:
        if webhook['system_domain_id'] != "562b6dc8-9c3e-11e9-93c5-0a580a3d829b":
            return False
    except:
        return False
    return True

def update_collections(request):
    inputdata = request.get_json()
    #inputdata = request

    if check_validity(inputdata):
      user_id = inputdata['user_id']

      try:
        top_collection = inputdata['collection_ids'][0]
      except:
        raise SystemError
      else: 
        print('collection ID exists, getting collection name and creating job')
        try:
          c = requests.get(iconik_url + 'assets/v1/collections/' + top_collection + '/', headers=headers)
        except requests.exceptions.HTTPError as err:
          print('Could not get top level collection info:',c.status_code)
          raise SystemError(err)
        else:  
          collectionname = c.json()['title']
          try:
            job_id = make_new_job(top_collection, collectionname)
          except:
            raise SystemError
          else:
            #get submitted collection ID, get all sub-collections recursively, append submitted collection to list 
            all_sub_collections = []
            all_sub_collections = collection_find(top_collection)
            #print(all_sub_collections)
            all_sub_collections.append(top_collection)

            #build metadata update payload 
            payload = metadata_values(inputdata)
            json_payload = json.dumps(payload, indent=4)

                    
            #attempt to push metadata to collections
            for collection in all_sub_collections:
              print('updating metadata on collection', collection)
              try:
                  r = requests.put(iconik_url + 'metadata/v1/collections/' + collection, data=json_payload, headers=headers)
                  #print(r.url)
                  r.raise_for_status()
              except requests.exceptions.HTTPError as err:
                  print('error updating metadata for collections', r.status_code)
                  print(r.text)
                  raise SystemError(err)
              else:
                print('updated ', collection, r.status_code)
            #complete job and notify user 
            try: 
              update_job(job_id, int('100'), 'FINISHED')
            except:
              raise SystemError
            else:
              try:
                print('attempting to notify user',user_id)
                notify_user(job_id, user_id)
              except:
                raise SystemError
              else:
                r = Response(response="Task Completed", status=200, mimetype="application/xml")
                r.headers["Content-Type"] = "text/xml; charset=utf-8"
                return r

def collection_find(top_collection):
        collection_list = []
        #get list of sub-collections
        params = {'per_page':100, 'object_types':'collections'}
        try:
            r = requests.get(iconik_url + 'assets/v1/collections/' + top_collection + '/contents/', params=params, headers=headers)
            #print(r.url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print('error getting list of sub-collections', r.status_code)
            raise SystemError(err)
        else:
            sub_collections = r.json()
            #print(sub_collections)
            if sub_collections['objects'] == None:
                print('no sub-collections in this collection')    
            else:
                for collection in sub_collections['objects']:
                    collection_list.append(collection['id'])
                    #call to get contents of each sub-collection from top level folder 
                    sub_collections = collection_find(collection['id'])
                    collection_list.extend(sub_collections)
            return collection_list

def metadata_values(webhook):
  inputdata = webhook

  values_to_add = {}
  values_to_add['metadata_values'] = {}
  for key, value in inputdata['metadata_values'].items():

    if value['field_values']:
      #ignore any values that are null or otherwise empty, since we are issuing an overwrite
      if value['field_values'][0]['value']:
        #print('value is', value)
        #value['mode'] = 'overwrite'
        #payload.append({"mode":"overwrite"})
        payload = value

        if not key in values_to_add.items():
            #print('value not in dict, adding entry', key)
            values_to_add['metadata_values'][key] = payload
        else:
            #print('appending entry to dict', key)
            values_to_add[key] = payload
  
  return values_to_add

def make_new_job(toplevelcollection, collectionname):
    payload = Template(job_create_payload)
    formatted_payload = payload.safe_substitute(objectID=toplevelcollection, CollectionName=collectionname)
    try:
      r = requests.post(iconik_url + 'jobs/v1/jobs/', data=formatted_payload, headers=headers)
      r.raise_for_status()
    except requests.exceptions.HTTPError as err:
      print('could not create job in Iconik')
      print(r.status_code)
      raise SystemError(err)
    else:  
      if r.status_code == 201:
        data = json.loads(r.content.decode('utf-8'))
        job_id = data['id']
        return job_id

def update_job(job_id, progress, status):
    try:
      job_patch = requests.patch(iconik_url + 'jobs/v1/jobs/' + job_id, headers=headers, json={"progress_processed": progress, "status": status})
      job_patch.raise_for_status()
    except requests.exceptions.HTTPError as err:
      print('could not update job in Iconik')
      print(job_patch.status_code)
      raise SystemError(err)
    else:  
      if job_patch.status_code == 201:
        print('updated job sucessfully')

def notify_user(job_id, user_id):
    payload = Template(make_notification)
    formatted_payload = payload.safe_substitute(job_id=job_id, user_id=user_id)
    try:
      notify = requests.post(iconik_url + 'users-notifications/v1/notifications/', headers=headers, data=formatted_payload)
      notify.raise_for_status()
    except requests.exceptions.HTTPError as err:
      print('could not create notification')
      print(notify.status_code)
      raise SystemError(err)
    else:  
      if notify.status_code == 201:
        print('notified user ID:',user_id)
