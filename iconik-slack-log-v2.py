import requests
import json
import os
from string import Template

app_id = os.environ.get('app_id')
token = os.environ.get('token')
slack_webhook = os.environ.get('slack_webhook')


headers = {'App-ID': app_id, 'Auth-Token': token}

iconik_url = 'https://app.iconik.io/API/'


slack_template = """{
	"attachments": [
		{
			"blocks": [
				{
					"type": "header",
					"text": {
						"type": "plain_text",
						"text": "New Share in Iconik",
						"emoji": true
					}
				},
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "$itemname has been shared - <$item_url|View in Iconik>"
					}
				},
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "Shared by <mailto:$email|$fname $lname>"
					}
				},
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "Shared to $sent_to_emails"
					}
				},
				{
					"type": "section",
					"text": {
						"type": "plain_text",
						"text": "Downloadable: $download",
						"emoji": true
					}
				}
			]
		}
	]
}"""


webhook = {
  "system_domain_id": "562b6dc8-9c3e-11e9-93c5-0a580a3d829b",
  "object_id": "ff93508c-8036-11eb-9776-0a580a3c0c42",
  "user_id": "e414116e-9957-11ea-b502-0a580a3c65d8",
  "realm": "shares",
  "operation": "create",
  "data": {
    "expires": "2021-03-15T17:52:06.936000+00:00",
    "id": "ff93508c-8036-11eb-9776-0a580a3c0c42",
    "object_id": "1cca7690-7e01-11eb-9145-0a580a3c0c42",
    "allow_download": false,
    "allow_download_proxy": false,
    "allow_comments": true,
    "metadata_views": [],
    "object_type": "assets",
    "date_created": "2021-03-08T17:52:06.936000+00:00",
    "owner_id": "e414116e-9957-11ea-b502-0a580a3c65d8",
    "allow_custom_actions": false,
    "allow_approving_comments": true,
    "allow_setting_approve_status": true,
    "message": null,
    "system_domain_id": "562b6dc8-9c3e-11e9-93c5-0a580a3d829b",
    "allow_view_versions": true,
    "allow_view_transcriptions": true
  },
  "request_id": "3d91fa8b50cddaaba1f4908858ffb6bb",
  "event_type": "assets"
}


def send_shareinfo_toslack(request):
    input_data = request.get_json()
    #input_data = request
    if check_validity(input_data):
        try:
            user_info = get_user_info(input_data)
        except Exception as e:
            print(e)
            raise SystemError
        else: 
            try:
                item_info = get_item_info(input_data)
            except Exception as e:
                print(e)
                raise SystemError
            else:
                try:
                    share_info = get_share_info(input_data)
                except Exception as e:
                    print(e)
                    raise SystemError
                else: 
                    message = Template(slack_template)
                    sharetype = input_data.get('data', {}).get('object_type')
                    downloadenabled = share_info[0]
                    sent_to_emails = share_info[1]


                    if sharetype == "assets":
                        formatted_message = message.safe_substitute(fname=user_info['first_name'], item_url='https://app.iconik.io/asset/' + input_data['data']['object_id'], lname=user_info['last_name'], email=user_info['email'], item_id=input_data['data']['object_id'], download=downloadenabled, itemname=item_info['title'], sent_to_emails=sent_to_emails)
                    elif sharetype == "collections":
                        formatted_message = message.safe_substitute(fname=user_info['first_name'], item_url='https://app.iconik.io/collection/' + input_data['data']['object_id'], lname=user_info['last_name'], email=user_info['email'], item_id=input_data['data']['object_id'], download=downloadenabled, itemname=item_info['title'], sent_to_emails=sent_to_emails)
                    print('Attempting to post to slack')
                    #print(formatted_message)
                    
                    try:
                        post_to_slack(formatted_message)
                    except Exception as e:
                        print(e)
                        raise SystemError
                    else:
                        return f'Successfully posted to slack'

#check to see if recieved webhook is from Spotify's Iconik instance
def check_validity(webhook):
    try:
        if webhook['system_domain_id'] != "562b6dc8-9c3e-11e9-93c5-0a580a3d829b":
            return False
    except Exception as e:
        print(e)
        return False
    return True

#get information from Iconik on the user who created the share
def get_user_info(webhook):
    try:
        r = requests.get(iconik_url + 'users/v1/users/' + webhook['data']['owner_id'] + '/', headers=headers)
    except requests.exceptions.HTTPError as err:
        print('could not get user info', r.status_code)
        raise SystemError(err)
    else: 
        return r.json()

#post formatted message to slack 
def post_to_slack(message):
    try: 
        r = requests.post(slack_webhook, headers={'content-type': 'application/json'}, data=message.encode('utf-8'))
    except requests.exceptions.HTTPError as err:
        print('could not post to slack', r.status_code)
        raise SystemError(err)

#get information about shared item from Iconik
def get_item_info(webhook):
    itemtype = webhook.get('data', {}).get('object_type')
    if itemtype == "assets":
        try:
            r = requests.get(iconik_url + 'assets/v1/assets/' + webhook['data']['object_id'] + '/', headers=headers)
        except requests.exceptions.HTTPError as err:
            print('could not get asset info', r.status_code)
            raise SystemError(err)
            
    elif itemtype == "collections":
        try:
            r = requests.get(iconik_url + 'assets/v1/collections/' + webhook['data']['object_id'] + '/', headers=headers)
        except requests.exceptions.HTTPError as err:
            print('could not get collection info', r.status_code)
            raise SystemError(err)

    if r.status_code == 200:
        return r.json()

#get information about the created share from Iconik
def get_share_info(webhook):
    sharetype = webhook.get('data', {}).get('object_type')
    print('successfully set share type to', sharetype)
    share_dict = {}
    try:
        if sharetype == "assets":
            #get share properties 
            try:
                r = requests.get(iconik_url + 'assets/v1/assets/' + webhook['data']['object_id'] + '/shares/' + webhook['object_id'], headers=headers)
            except requests.exceptions.HTTPError as err:
                print('could not get asset share info', r.status_code)
                raise SystemError(err)
            else: 
                try:
                    #get list of users the share was sent to 
                    print('attempting to get list of shared to emails')
                    payload = {'page': 0, 'per_page':100}
                    shared_to = requests.get(iconik_url + 'assets/v1/assets/' + webhook['data']['object_id'] + '/shares/' + webhook['object_id'] + "/users/", params=payload, headers=headers)
                except requests.exceptions.HTTPError as err:
                    print('could not get list of share users,', err)

        elif sharetype == "collections":
            #get share properties 
            try: 
                r = requests.get(iconik_url + 'assets/v1/collections/' + webhook['data']['object_id'] + '/shares/' + webhook['object_id'], headers=headers)
            except requests.exceptions.HTTPError as err:
                print('could not get collection share info', r.status_code)
                raise SystemError(err)
            else: 
                try:
                    #get list of users the share was sent to 
                    print('attempting to get list of shared to emails')
                    payload = {'page': 0, 'per_page':100}
                    shared_to = requests.get(iconik_url + 'assets/v1/collections/' + webhook['data']['object_id'] + '/shares/' + webhook['object_id'] + "/users/", params=payload, headers=headers)
                    share_objects = shared_to.json()
                except requests.exceptions.HTTPError as err:
                    print('could not get list of share users,', err)
    except Exception as e:
        print('could not get share info', e)
        raise SystemError
    else:
        #set first value of share_dict to download enabled status 
        share_response = r.json()
        share_dict[0] = share_response['allow_download']
        #print(json.dumps(shared_to.json()))
        share_objects = shared_to.json()

        #check response to see if any objects exist in json. 
        if not 'email' in share_objects['objects'][0]:
            print('this was a link generated share')
            share_dict[1] = 'no user email addresses. This was a generated link'
            return share_dict
        else:
            #get shared_to json file and put in a dict 
            shared_prep = []
            email_list = str
            #loop through list of objects, pull out email address, format for slack markdown, put in share_dict
            for objects in share_objects['objects']:
                shared_prep.append(str('<mailto:' + objects['email'] + '|' + objects['email'] + '> '  ))
                
            print('shared prep is', shared_prep)
            #concatenate dict to single string 
            email_list = ' '.join(map(str, shared_prep)) 
            #assign string to second key in dict     
            share_dict[1] = email_list
            return share_dict


