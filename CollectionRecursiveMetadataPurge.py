import json
import requests
from string import Template
import os
from flask import Response
import time

app_id = os.environ.get('app_id')
token = os.environ.get('token')


headers = {'App-ID': app_id, 'Auth-Token': token}
iconik_url = 'https://app.iconik.io/API/'

job_create_payload = """{"custom_type":"Podcast Folder Structure",
  "object_id":"$objectID",
  "object_type":"collections",
  "status":"STARTED",
  "title":"Metadata purge for: $CollectionName",
  "type":"CUSTOM"
}"""

job_update_payload = """{
  "id": "426da862-655c-11eb-8f50-0a580a3f8d19",
  "progress_processed": 30,
}"""

make_notification = """{
  "event_type": "finished","message_long": "Project metadata purged!","message_short": "Project metadata purged","object_id": "$job_id","object_type": "jobs","recipient_id": "$user_id","sender_id":"e414116e-9957-11ea-b502-0a580a3c65d8"}"""


#check to see if recieved webhook is from your Iconik instance
def check_validity(webhook):
    try:
        if webhook['system_domain_id'] != "INSERT SYSTEM DOMAIN ID HERE":
            return False
    except:
        return False
    return True

def metadata_purge(webhook):
    inputdata = webhook
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
            print('getting list of collections')
            #get submitted collection ID, get all sub-collections recursively
            all_sub_collections = []
            all_sub_collections = collection_find(top_collection)

            print('found', len(all_sub_collections), 'collections underneath top level collection')
            finalexportsfolders = []              
            #attempt to purge metadata from each sub-collection and purge metadata from every asset in the collection
            print('getting all collections for exclusion')
            for collection in all_sub_collections:
              #get collection name
              
              try:
                c = requests.get(iconik_url + 'assets/v1/collections/' + collection + '/', headers=headers)
              except requests.exceptions.HTTPError as err:
                print('Could not get sub-collection info:',c.status_code)
                raise SystemError(err)
              else: 
                time.sleep(c.elapsed.total_seconds())
                collectionname = c.json()['title']
                if collectionname == '02_Final_Exports' or collectionname == '03_Final_Deliverables':
                  finalexportsfolders.append(c.json()['id'])

            print('list of final exports folders', finalexportsfolders)

            for collection in all_sub_collections:
              #get collection name and parents 
              print('working in collection', collection)
              try:
                c = requests.get(iconik_url + 'assets/v1/collections/' + collection + '/', headers=headers)
              except requests.exceptions.HTTPError as err:
                print('Could not get sub-collection info:',c.status_code)
                raise SystemError(err)
              else: 
                time.sleep(c.elapsed.total_seconds())
                collectionname = c.json()['title']
                #parent_collections = c.json()['in_collections']
                print('collection name is:', collectionname)
                #print(c.text)

                #if collection doesn't have a parent ID of the 02_final_exports folder
                #print('list of final exports folders', finalexportsfolders)


                if any(elem in c.json()['parents'] for elem in finalexportsfolders):
                  print('assets are in Final Exports, skipping')

                else:
                  print('purging metadata for collection:', collection, 'and purging asset metadata')

                  #get all asssets inside collection, delete their metadata entries 
                  collection_assets = asset_find(collection, 0)
                  
                  if len(collection_assets) > 0:
                    position = 1
                    for asset in collection_assets:
                      #get metadata for asset, based on studios base metadata view 
                      print('working on asset ', position, 'of', len(collection_assets))
                      try:
                        r = requests.get(iconik_url + 'metadata/v1/assets/' + asset + '/views/39d06446-9c40-11e9-b295-0a580a3c104b/', headers=headers)
                        #print(r.url)
                        r.raise_for_status()
                      except requests.exceptions.HTTPError as err:
                        print('there was no metadata for asset, or error getting asset metadata', r.status_code)
                      else:
                        if r.status_code != 404: 
                          payload = metadata_values(r.json())
                          json_payload = json.dumps(payload, indent=4)
                          try:
                            r = requests.put(iconik_url + 'metadata/v1/assets/' + asset + '/views/39d06446-9c40-11e9-b295-0a580a3c104b/', data=json_payload, headers=headers)
                            #print(r.url)
                            r.raise_for_status()
                          except requests.exceptions.HTTPError as err:
                            print('error deleting metadata for asset', asset, r.status_code)
                            print(r.text)
                            raise SystemError(err)
                          else:
                            #time.sleep(r.elapsed.total_seconds())
                            print('purged metadata for ', asset, r.status_code)
                            position += 1

                  #after deleting asset metadata, check for any collection metadata and delete
                  #get metadata values for collection, if any 
                  try:
                      r = requests.get(iconik_url + 'metadata/v1/collections/' + collection + '/views/39d06446-9c40-11e9-b295-0a580a3c104b/', headers=headers)
                      #print(r.url)
                      r.raise_for_status()
                  except requests.exceptions.HTTPError as err:
                    print('there was no metadata values for collection, or error getting collection info', r.status_code)
                    #print(r.text)
                  else:
                    if r.status_code != 404:
                      time.sleep(r.elapsed.total_seconds())
                      payload = metadata_values(r.json())
                      json_payload = json.dumps(payload, indent=4)
                      try:
                        r = requests.put(iconik_url + 'metadata/v1/collections/' + collection + '/views/39d06446-9c40-11e9-b295-0a580a3c104b/', data=json_payload, headers=headers)
                        #print(r.url)
                        r.raise_for_status()
                      except requests.exceptions.HTTPError as err:
                        print('error purging metadata for collection', r.status_code)
                        print(r.text)
                        raise SystemError(err)
                      else:
                        print('purged metadata for ', collection, r.status_code)
                        time.sleep(r.elapsed.total_seconds())

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
                #time.sleep(r.elapsed.total_seconds())
                for collection in sub_collections['objects']:
                    collection_list.append(collection['id'])
                    #call to get contents of each sub-collection from top level folder 
                    sub_collections = collection_find(collection['id'])
                    collection_list.extend(sub_collections)
            return collection_list

def metadata_values(webhook):
  inputdata = webhook
  #print(inputdata['metadata_values'])
  values_to_delete = {}
  values_to_delete["metadata_values"] = {}
  for key, value in inputdata['metadata_values'].items():
    value["mode"] = "delete"
    #remove date created 
    del value['date_created']
    value['field_values'] = []

    payload = value
        
    if not key in values_to_delete.items():
        #print('value not in dict, adding entry', key)
        values_to_delete["metadata_values"][key] = payload
    else:
        #print('appending entry to dict', key)
        values_to_delete[key] = payload
  
  return values_to_delete


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

def asset_find(collection_id, page_number):
        asset_list = []
        #get list of assets in collection
        params = {}
        params['per_page'] = 100
        params['object_types'] = "assets"

        if page_number > 0:
          params['page'] = page_number 


        try:
            r = requests.get(iconik_url + 'assets/v1/collections/' + collection_id + '/contents/', params=params, headers=headers)
            #print(r.url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print(errh)
            print(r.json())
        except requests.exceptions.ConnectionError as errc:
            print(errc)
            print(r.json())
        except requests.exceptions.Timeout as errt:
            print(errt)
            print(r.json())
        except requests.exceptions.RequestException as err:
            print(err)
            print(r.json())

        else:
            time.sleep(r.elapsed.total_seconds())
            assets = r.json()
            #need to get total pages, and iterate through pages 
            #get total number of pages 
            total_pages = int(assets['pages'])
            current_page = assets['page']
            total_assets = assets['total']
            print('current page is', current_page)
            print('total pages is', total_pages)
            print('total assets is', total_assets)
            #print(assets)
            if total_pages != 0:
                for segment_list in assets['objects']:
                    asset_list.append(segment_list['id'])

            #go to next page, until last page is reached 
            if current_page < total_pages:
                next_list = asset_find(collection_id, current_page+1)
                for x in next_list:
                    asset_list.append(x)

            #return list of assets found 
            return asset_list
