import json
import urllib.request as urllib
import urllib.parse
import boto3
import io
import gzip
import re
import os
import requests
from datetime import datetime
import dateutil.tz

s3 = boto3.client('s3')
sns = boto3.client('sns')
sns_arn = os.environ['SNS_TOPIC']
webhook_url = os.environ['SLACK_HOOK']
slack_channel = os.environ['SLACK_CHANNEL']

USER_AGENTS = {"console.amazonaws.com", "Coral/Jakarta", "Coral/Netty4"}
IGNORED_EVENTS = {"DownloadDBLogFilePortion", "TestScheduleExpression", "TestEventPattern", "LookupEvents",
                  "listDnssec", "Decrypt", "REST.GET.OBJECT_LOCK_CONFIGURATION", "ConsoleLogin"}

timezone = dateutil.tz.gettz('Asia/Jerusalem')
current_time = datetime.now(tz=timezone)
time = current_time.strftime("%I:%M %p, %m/%d/%Y")


def post_to_sns(user, event) -> None:
    message = f'Manual AWS Changed Detected:  {user} --> {event}'
    sns_publish(message)


def post_to_sns_details(message) -> None:
    message = {"Manual AWS Change Detected": message}
    sns_publish(message)
    

def sns_publish(message) -> None:
    sns.publish(
        TargetArn=sns_arn,
        Message=json.dumps({'default': json.dumps(message, indent=4, sort_keys=True, ensure_ascii=False, separators=(',', ': '))}),
        MessageStructure='json'
    )

###############
def post_to_slack(user, event, time, sns) -> None:
    pretext = f'<!channel>\n*Manual AWS Changed Detected*: \n `{user} --> {event}`'
    text = f'Detailed output was sent as AWS Notifications email to subscribers of *"{sns}"* topic at `{time}`'
    slack_publish(pretext, text)


def slack_publish(pretext, text) -> None:
    message = {
                "channel": slack_channel,
                "pretext": pretext,
                "text": text,
                "mrkdwn_in": ["pretext", "text"]
                }
    try:
        response = requests.post(webhook_url, data=json.dumps(message), headers={'Content-Type': 'application/json'})
        print('Response: ' + str(response.text) + "\n" + 'Response code: ' + str(response.status_code))
        print('Message posted to channel "' + slack_channel + '"')
    except urllib.error.HTTPError as e:
        text=e.reason
        status=e.code
        message = f"""
            Error sending message to Slack channel {slack_channel}
            Reason: {text}
            Status code: {status}
                """
        print(message)
        raise e
    except urllib.error.URLError as e:
        print('Server connection failed: ' + str(e.reason))


def check_regex(expr, txt) -> bool:
    match = re.search(expr, txt)
    return match is not None


def match_user_agent(txt) -> bool:
    if txt in USER_AGENTS:
        return True

    expressions = (
        "signin.amazonaws.com(.*)",
        "^S3Console",
        "^\[S3Console",
        "^Mozilla/",
        "^console(.*)amazonaws.com(.*)",
        "^aws-internal(.*)AWSLambdaConsole(.*)",
    )

    for expresion in expressions:
        if check_regex(expresion, txt):
            return True

    return False


def match_readonly_event_name(txt) -> bool:
    # starts with
    expressions = (
        "^Get",
        "^Describe",
        "^List",
        "^Head",
    )
    for expression in expressions:
        if check_regex(expression, txt):
            return True

    return False


def match_ignored_events(event_name) -> bool:
    return event_name in IGNORED_EVENTS


def filter_user_events(event) -> bool:
    is_match = match_user_agent(event['userAgent'])
    is_read_only = match_readonly_event_name(event['eventName'])
    is_ignored_event = match_ignored_events(event['eventName'])
    is_in_event = 'invokedBy' in event['userIdentity'] and event['userIdentity']['invokedBy'] == 'AWS Internal'

    status = is_match and not is_read_only and not is_ignored_event and not is_in_event

    return status


def get_user_email(principal_id) -> str:
    words = principal_id.split(':')
    if len(words) > 1:
        return words[1]
    return principal_id


def lambda_handler(event, context) -> None:
    message = json.loads(event['Records'][0]['Sns']['Message'])
    bucket = message['Records'][0]['s3']['bucket']['name']
    key = message['Records'][0]['s3']['object']['key']
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()

        with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as fh:
            event_json = json.load(fh)
            output_dict = [record for record in event_json['Records'] if filter_user_events(record)]
            if len(output_dict) > 0:
                post_to_sns_details(output_dict)
            for item in output_dict:
                post_to_slack(item['userIdentity']['principalId'], item['eventName'], time, sns_arn)

        return response['ContentType']
            
    except Exception as e:
        print(e)
        message = f"""
            Error getting object {key} from bucket {bucket}.
            Make sure they exist and your bucket is in the same region as this function.
        """
        print(message)
        raise e