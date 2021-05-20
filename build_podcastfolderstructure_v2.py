import requests
import json
import os
from string import Template
from flask import Response
import base64


app_id = os.environ.get('app_id')
token = os.environ.get('token')


headers = {'App-ID': app_id, 'Auth-Token': token}

iconik_url = 'https://app.iconik.io/API/'

collection_body = """{
  "parent_id": "$parent",
  "title": "$collectionname"
}"""

globalelementsfolders = ["01_Music", "02_VOX", "03_SFX", "04_Artwork", "05_Series_Socials"]
singleepisodefolders = ["01_WIP_DAW_Session", "02_VOX", "03_SFX", "04_Music", "05_Video_Convert", "06_WIP_Prints"]
#deliverableepisodefolders = ["01_Final_Mix", "02_Stems", "03_Socials", "04_Final_DAW_Session", "05_Transcripts", "06_Cue_Sheets", "07_Episodic_Artwork", "zOld"]

job_create_payload = """{"custom_type":"Podcast Folder Structure",
  "object_id":"$objectID",
  "object_type":"collections",
  "status":"STARTED",
  "title":"Build Folder Structure",
  "type":"CUSTOM"
}"""

job_update_payload = """{
  "id": "426da862-655c-11eb-8f50-0a580a3f8d19",
  "progress_processed": 30,
}"""

make_notification = """{
  "event_type": "finished","message_long": "Folder successfully Created!","message_short": "Folder successfully created!","object_id": "$job_id","object_type": "jobs","recipient_id": "$user_id","sender_id":"e414116e-9957-11ea-b502-0a580a3c65d8","status": "QUEUED","sub_object_id": "$toplevelcollection","sub_object_type": "collections"}"""



def process_message(event, context):
    #inputdata = webhook
    print("""This Function was triggered by messageId {} published at {}""".format(context.event_id, context.timestamp))

    if 'data' in event:
        payload = base64.b64decode(event['data']).decode('utf-8')
        json_payload = json.loads(payload)
        build_podcastfolderstructure(json_payload)
    else:
        print('could not process pubsub message')



def build_podcastfolderstructure(request):
    input_data = request

    parentcollection = input_data['collection_ids'][0]
    global user_id
    user_id = input_data['user_id']

    global toplevelcollection
    toplevelcollection = None

    #create job 
    print('Creating new job in iconik')
    try:
      global job_id
      job_id = make_new_job(parentcollection)
    except:
      raise SystemError
    else: 
      podcast_info = {}
      metadata_payload = {}
      metadata_payload['metadata_values'] = {}
      #build payload for project folder 
      if input_data['metadata_values']['ProjectCode']['field_values'][0]['value']:
          global project_id
          project_id = input_data['metadata_values']['ProjectCode']['field_values'][0]['value']
          #podcast_info['project_id'] = project_id
          metadata_payload['metadata_values']['ProjectCode'] = {}
          metadata_payload['metadata_values']['ProjectCode'].update(input_data['metadata_values']['ProjectCode'])
      else:
          print('no project code was submitted!')
          update_job(job_id, int('15'), 'FAILED')
          print('attempting to notify user', user_id)
          notify_user(job_id, user_id, toplevelcollection)
          #fail out, update job to failed with error message 
      if input_data['metadata_values']['ProjectTitle']['field_values'][0]['value']:
          global ProjectTitle
          ProjectTitle = input_data['metadata_values']['ProjectTitle']['field_values'][0]['value']
          ProjectTitle = ProjectTitle.strip()
          ProjectTitle = ProjectTitle.replace(" ", '_')
          podcast_info['ProjectTitle'] = ProjectTitle
          metadata_payload['metadata_values']['ProjectTitle'] = {}
          metadata_payload['metadata_values']['ProjectTitle'].update(input_data['metadata_values']['ProjectTitle'])
      else:
          print('no podcast title was submitted')
          #fail out, update job to failed with error message 
          update_job(job_id, int('15'), 'FAILED')
          print('attempting to notify user', user_id)
          notify_user(job_id, user_id, toplevelcollection)
      if input_data['metadata_values']['PodcastSeasonNumber']['field_values'][0]['value']:
          global season_num
          season_num = input_data['metadata_values']['PodcastSeasonNumber']['field_values'][0]['value']
          season_num = season_num.zfill(2)
          podcast_info['season_num'] = season_num
          metadata_payload['metadata_values']['PodcastSeasonNumber'] = {}
          metadata_payload['metadata_values']['PodcastSeasonNumber'].update(input_data['metadata_values']['PodcastSeasonNumber'])
      else:
          print('no podcast season number was submitted')
          update_job(job_id, int('15'), 'FAILED')
          print('attempting to notify user', user_id)
          notify_user(job_id, user_id, toplevelcollection)
          #fail out, update job to failed with error message 
      if input_data['metadata_values']['episodecount']['field_values'][0]['value']:
          global episodecount
          
          episodecount = input_data['metadata_values']['episodecount']['field_values'][0]['value']
          print('episode count is', episodecount)
          podcast_info['episodecount'] = episodecount 
      else:
          print('no episode count was submitted')
          update_job(job_id, int('15'), 'FAILED')
          print('attempting to notify user', user_id)
          notify_user(job_id, user_id, toplevelcollection)
          #fail out, update job to failed with error message 
      if input_data['metadata_values']['FolderStructureType']['field_values'][0]['value']:
        global folderstructuretype
        folderstructuretype = input_data['metadata_values']['FolderStructureType']['field_values'][0]['value']
        print('folder structure type is', folderstructuretype)
      else: 
          print('no folder structure type was submitted')
          update_job(job_id, int('15'), 'FAILED')
          print('attempting to notify user', user_id)
          notify_user(job_id, user_id, toplevelcollection)

      if len(input_data['metadata_values']['ExternalProductionPartner']['field_values']) > 0:
          global externalproductionpartner
          externalproductionpartner = input_data['metadata_values']['ExternalProductionPartner']['field_values'][0]['value']
          podcast_info['ExternalProductionPartner'] = externalproductionpartner
          metadata_payload['metadata_values']['ExternalProductionPartner'] = {}
          metadata_payload['metadata_values']['ExternalProductionPartner'].update(input_data['metadata_values']['ExternalProductionPartner'])
      else:
          print('no external production partner was submitted')

      if len(input_data['metadata_values']['Network']['field_values']) > 0:
          global network
          network = input_data['metadata_values']['Network']['field_values'][0]['value']
          podcast_info['Network'] = network
          metadata_payload['metadata_values']['Network'] = {}
          metadata_payload['metadata_values']['Network'].update(input_data['metadata_values']['Network'])
      else:
          print('no external production partner was submitted')

      
      project_collection_name = project_id + "_" + ProjectTitle + "_S" + season_num 


      #build project folder 
      payload = Template(collection_body)
      formatted_payload = payload.safe_substitute(parent=parentcollection, collectionname=project_collection_name)
      try: 
        r = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
        r.raise_for_status()
      except requests.exceptions.HTTPError as err:
        print('Failed to create project folder for', project_collection_name)
        raise SystemError(err)
      else:
        response = r.json()
        toplevelcollection = response['id']
        try:
          c = requests.get(iconik_url + 'assets/v1/collections/' + toplevelcollection + '/', headers=headers)
        except requests.exceptions.HTTPError as err:
          print('Could not get top level collection info:',c.status_code)
          raise SystemError(err)
        else:  
          
          payload = json.dumps(metadata_payload, indent=4)
          print('updating metadata on collection', toplevelcollection)
          try:
              #print(metadata_payload)
              r = requests.put(iconik_url + 'metadata/v1/collections/' + toplevelcollection + '/views/7a09f668-cad4-11ea-90de-0a580a3c8cb3/', data=payload, headers=headers)
              #print(r.url)
              r.raise_for_status()
          except requests.exceptions.HTTPError as err:
              print('error updating metadata for collections', r.status_code)
              print(r.text)
              raise SystemError(err)
          else:
            print('updated ', toplevelcollection, r.status_code)


            collectionname = c.json()['title']
            #make global elements structure and create job
            print('making podcast folder structure underneath', collectionname)
          try:
              make_global_elements_collection(toplevelcollection)
          except:
              raise SystemError
          else:
              if folderstructuretype == 'internal':
                try:
                  #make working episodes folder and update job progress to 33%
                  make_working_episodes_folder(toplevelcollection)
                except:
                  update_job(job_id, int('66'), 'FAILED')
                  raise SystemError

              try:
                update_job(job_id, int('66'), 'STARTED')
              except:
                raise SystemError
              else:
                
                try: 
                  final_deliverables = input_data['metadata_values']['RequiredDeliverables']['field_values']
                except:
                  raise SystemError
                else: 
                  try:
                    make_final_deliverables_folder(toplevelcollection, final_deliverables, metadata_payload)
                  except:
                    raise SystemError
                  else:
                    try:
                      update_job(job_id, int('100'), 'FINISHED')
                    except:
                      raise SystemError
                    else:
                      #notify user that the job is done
                      print('attempting to notify user', user_id)
                      try:
                        notify_user(job_id, user_id, toplevelcollection)
                      except:
                        raise SystemError
                      else:
                        return Response(status=200)


#build global elements collection underneath the receieved collection in webhook 
def make_global_elements_collection(toplevelcollection):
    payload = Template(collection_body)
    formatted_payload = payload.safe_substitute(parent=toplevelcollection, collectionname='02_Global_Elements')
    print('creating global elements folder')
    try:
      r = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
      r.raise_for_status()
    except requests.exceptions.HTTPError as err:
      print('global elements was not created successfully:',r.status_code)
      raise SystemError(err)
    else:  
      if r.status_code == 201:
          #verify newly created collection is underneath top level collection
          if 'parent_id' in r.json():
            if r.json()['parent_id'] == toplevelcollection:
              globalelementsid = r.json()['id']
              #build 5 subfolders         
              for folder in globalelementsfolders:
                formatted_payload = payload.safe_substitute(parent=globalelementsid, collectionname=folder)
                r = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                if r.status_code != 201:
                  print('error creating collection:', folder)
      else:
          print("Something went wrong with making collection", r.status_code)
     
#build working episodes collection underneath the receieved collection in webhook 
def make_working_episodes_folder(toplevelcollection):
    payload = Template(collection_body)
    formatted_payload = payload.safe_substitute(parent=toplevelcollection, collectionname='01_Working_Episodes')
    try: 
      r = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
      r.raise_for_status()
    except requests.exceptions.HTTPError as err:
      print('global elements was not created successfully')
      raise SystemError(err)
    else:
      if r.status_code == 201:
          #verify newly created collection is underneath top level collection
          if 'parent_id' in r.json():
            if r.json()['parent_id'] == toplevelcollection:
              working_episodes_folder = r.json()['id']

              #build each episode folder name S01E01, etc 
              for i in range(int(episodecount)):
                print('i is :', i)
                num = int(i)+1
                print('num is', num)
                #breakpoint()
                episode_num = str(num).zfill(2)
                #breakpoint()
                #season_num = season_numzfill(2)
                episode_folder_name = 'S' + season_num + 'E' + episode_num
                print('building episode', episode_folder_name)

                #build episode folders
                formatted_payload = payload.safe_substitute(parent=working_episodes_folder, collectionname=episode_folder_name)
                episodefolder = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                episodefolderid = episodefolder.json()['id']
                if episodefolder.status_code == 201:
                  #check to make sure we are working in the newly created Episode folder
                  if 'parent_id' in episodefolder.json():
                    if episodefolder.json()['parent_id'] == working_episodes_folder:
                      print('creating working episode subfolders')
                      #build second level subfolders inside episode folder
                      for folder in singleepisodefolders:
                        formatted_payload = payload.safe_substitute(parent=episodefolderid, collectionname=folder)
                        
                        #check to make sure subfolder was created successfully, and build subfolders if appropriate
                        try: 
                          subfolder = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                          subfolder.raise_for_status()
                        except requests.exceptions.HTTPError as err:
                          print('subfolder was not created successfully')
                          raise SystemError(err)
                        else:     
                          if folder == '02_VOX':
                            voxfolder = subfolder.json()['id']
                      
                            try:
                              formatted_payload = payload.safe_substitute(parent=voxfolder, collectionname='01_VO')
                              vo = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                              vo.raise_for_status()
                            except requests.exceptions.HTTPError as err:
                              print('VO subfolder was not created successfully')
                              raise SystemError(err)
                            
                            try:
                              formatted_payload = payload.safe_substitute(parent=voxfolder, collectionname='02_Interviews')
                              interviews = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                              interviews.raise_for_status()
                            except requests.exceptions.HTTPError as err:
                              print('VO subfolder was not created successfully')
                              raise SystemError(err)
                            
                          elif folder == '04_Music':
                            musicfolder = subfolder.json()['id']
                            try:
                              formatted_payload = payload.safe_substitute(parent=musicfolder, collectionname='01_Temp')
                              tmpfldr = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                              tmpfldr.raise_for_status()
                            except requests.exceptions.HTTPError as err:
                              print('Temp subfolder was not created successfully')
                              raise SystemError(err)
                            
                            try:
                              formatted_payload = payload.safe_substitute(parent=musicfolder, collectionname='02_Final')
                              final = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                              final.raise_for_status()
                            except requests.exceptions.HTTPError as err:
                              print('Final subfolder was not created successfully')
                              raise SystemError(err)

                          elif folder == '05_Video_Convert':
                            videoconvertfolder = subfolder.json()['id']
                            try:
                              formatted_payload = payload.safe_substitute(parent=videoconvertfolder, collectionname='01_Video')
                              vo = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                              vo.raise_for_status()
                            except requests.exceptions.HTTPError as err:
                              print('Video subfolder was not created successfully')
                              raise SystemError(err)
                            
                            try:
                              formatted_payload = payload.safe_substitute(parent=videoconvertfolder, collectionname='02_Converted_Audio')
                              interviews = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
                              interviews.raise_for_status()
                            except requests.exceptions.HTTPError as err:
                              print('ConvertedAudio subfolder was not created successfully')
                              raise SystemError(err)              
                  else:
                    print('parentID did not exist')
                else:
                  print('could not make episode folder')
      else:
          print("Something went wrong with making collection", r.status_code)

#build final deliverables collection underneath the recieved collection in Iconik
def make_final_deliverables_folder(toplevelcollection, final_deliverables, metadata_payload):
    payload = Template(collection_body)
    formatted_payload = payload.safe_substitute(parent=toplevelcollection, collectionname='03_Final_Deliverables')
    try: 
      r = requests.post(iconik_url + 'assets/v1/collections/', data=formatted_payload, headers=headers)
      r.raise_for_status()
    except requests.exceptions.HTTPError as err:
      print('final deliverables was not created successfully')
      raise SystemError(err)
    else:
      if r.status_code == 201:
          #verify newly created collection is underneath top level collection
          if 'parent_id' in r.json():
            if r.json()['parent_id'] == toplevelcollection :
              final_deliverables_folder = r.json()['id']
              print('building episode folders')
              #episodecount = int(podcast_info['episodecount'])
              print('number of episodes to build is', episodecount)

              
              metadata_payload['metadata_values']['EpisodeNumber'] = {}
              metadata_payload['metadata_values']['EpisodeNumber']['field_values'] = list()
              metadata_payload['metadata_values']['DeliverableType'] = {}
              metadata_payload['metadata_values']['DeliverableType']['field_values'] = list()
              
              metadata_payload['metadata_values']['FinalDeliverable'] = {}
              metadata_payload['metadata_values']['FinalDeliverable']['field_values'] = list()
              booldict = {}
              booldict["value"] = True
              metadata_payload['metadata_values']['FinalDeliverable']['field_values'].append(booldict)
              
              #build each episode folder name S01E01, etc 
              for i in range(int(episodecount)):
                print('i is :', i)
                num = int(i)+1
                print('num is', num)
                #breakpoint()
                episode_num = str(num).zfill(2)
                #breakpoint()
                #season_num = season_numzfill(2)
                episode_folder_name = 'S' + season_num + 'E' + episode_num
                print('building episode', episode_folder_name)

                #build episode folder, build placeholders, put placeholders in episode collection
                payload = Template(collection_body)
                episode_payload = payload.safe_substitute(parent=final_deliverables_folder, collectionname=episode_folder_name)
                print(episode_payload)
                #breakpoint()
                try: 
                  episodefolder = requests.post(iconik_url + 'assets/v1/collections/', data=episode_payload, headers=headers)
                except:
                  print('shit')
                else: 
                  episodefolderid = episodefolder.json()['id']
                  if episodefolder.status_code == 201:
                    #only build placeholder files if it's an external project structure 
                    if folderstructuretype == 'external':
                      #check to make sure we are working in the newly created Episode folder
                      if 'parent_id' in episodefolder.json():
                        if episodefolder.json()['parent_id'] == final_deliverables_folder:
                          print('creating placeholders for episode')
                          for deliverable in final_deliverables:
                            #loop through and build asset title based on deliverable, call build placeholder fuction
                            try: 
                              asset_title = project_id + "_" + ProjectTitle + '_S' + season_num + 'E' + episode_num + '_' + deliverable['value']
                              print('creating',asset_title)
                              placeholder = build_asset(asset_title, "PLACEHOLDER")
                            except:
                              print('could not create placeholder')
                              update_job(job_id, int('15'), 'FAILED')
                              print('attempting to notify user', user_id)
                              notify_user(job_id, user_id, toplevelcollection)
                            else: 
                              #add placeholder to episode collection
                              add_to_collection(episodefolderid, placeholder)
                              
                              #tag placeholder with metadata 
                              print('updating metadata on placeholder', asset_title)
                              #breakpoint()
                              try:
                                  #print(metadata_payload)
                                  #add episode number to metadata payload
                                  episodedict = {}
                                  episodedict["value"] = (i+1)
                                  #print(episodedict)
                                  
                                  metadata_payload['metadata_values']['EpisodeNumber']['field_values'].append(episodedict)
                                  #print(metadata_payload)
                                  #add deliverable type to metadata payload
                                  deliverdict = {}
                                  deliverdict['value'] = deliverable['value']
                                  metadata_payload['metadata_values']['DeliverableType']['field_values'].append(deliverdict)

                                  payload = json.dumps(metadata_payload, indent=4)
                                  #print('final payload is:', payload)
                                  #breakpoint()
                                  r = requests.put(iconik_url + 'metadata/v1/assets/' + placeholder + '/views/7a09f668-cad4-11ea-90de-0a580a3c8cb3/', data=payload, headers=headers)
                                  #print(r.url)
                                  r.raise_for_status()
                              except requests.exceptions.HTTPError as err:
                                  print('error updating metadata for asset', r.status_code)
                                  print(r.text)
                                  raise SystemError(err)
                              else:
                                print('updated metadata on', asset_title, r.status_code)
                                episodedict.clear()
                                metadata_payload['metadata_values']['EpisodeNumber']['field_values'].clear()
                                deliverdict.clear()
                                metadata_payload['metadata_values']['DeliverableType']['field_values'].clear()

                      else:
                        print('parentID did not exist')
                  else:
                    print('could not make episode folder')
      else:
          print("Something went wrong with making collection", r.status_code)

def make_new_job(toplevelcollection):
    payload = Template(job_create_payload)
    formatted_payload = payload.safe_substitute(objectID=toplevelcollection)
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

def notify_user(job_id, user_id, toplevelcollection):
    payload = Template(make_notification)
    formatted_payload = payload.safe_substitute(job_id=job_id, user_id=user_id, toplevelcollection=toplevelcollection)
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
        print(notify.text)

def build_asset(asset_title, asset_type):
    payload = {}
    payload['title'] = asset_title
    payload['type'] = asset_type
    #print(payload)
    #print('attempting to update build asset in Iconik')
    path = 'https://app.iconik.io/API/assets/v1/assets/'
    try:
        response = requests.post(path, headers=headers, data=json.dumps(payload))
        #print(response.url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print(errh)
        print(response.json())
        raise SystemError(errh)
    except requests.exceptions.ConnectionError as errc:
        print(errc)
        print(response.json())
        raise SystemError(errc)
    except requests.exceptions.Timeout as errt:
        print(errt)
        print(response.json())
        raise SystemError(errt)
    except requests.exceptions.RequestException as err:
        print(err)
        print(response.json())
        raise SystemError(err)
    else:
        print(response.status_code, 'sucessfully created asset')
        asset_info = response.json()
        asset_id = asset_info['id']        
        return asset_id 

def add_to_collection(collection_id, asset_id):
    payload = {}
    payload['object_id'] = asset_id
    payload['object_type'] = "assets"
    path = 'https://app.iconik.io/API/assets/v1/collections/{}/contents/'.format(collection_id)
    print('attempting to add asset to collection')
    try:
        response = requests.post(path, headers=headers, data=json.dumps(payload))
        #print(response.url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print(errh)
        print(response.json())
        raise SystemError(errh)
    except requests.exceptions.ConnectionError as errc:
        print(errc)
        print(response.json())
        raise SystemError(errc)
    except requests.exceptions.Timeout as errt:
        print(errt)
        print(response.json())
        raise SystemError(errt)
    except requests.exceptions.RequestException as err:
        print(err)
        print(response.json())
        raise SystemError(err)
    else:
        print(response.status_code, 'sucessfully added asset to collection')



