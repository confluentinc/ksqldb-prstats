#!/usr/bin/python

from urllib2 import Request, urlopen, URLError
import sys
from argparse import ArgumentParser
import json
import re
import time
from kafka import KafkaProducer

MAX_RESULTS = 50
# {
#  "pull_id": "int",
#  "title": "string",
#  "user": "string", // this is "login" in the user field
#  "created_at": "date",
#  "updated_at": "date",
#  "closed_at": "date",
#  "emit_at": "long"
#}
#key : repo/pull_id

PRODUCER = KafkaProducer(bootstrap_servers="localhost:9092")

def get_pull_id(pattern, pull_request):
    result = pattern.search(pull_request["html_url"])
    return int(result.group(1))


def get_pulls_since(repo, token, last_timestamp):
    page = 0
    should_terminate = False
    next_timestamp = None
    pull_id_pattern = re.compile("^.*\/pull\/(\d+)$")

    while not should_terminate:
        url = "https://api.github.com/repos/%s/issues?page=%s&per_page=%s&since=%s&state=%s&sort=updated" % (repo, page, MAX_RESULTS, last_timestamp, 'all')
        request = Request(url)
        request.add_header("Accept", "application/vnd.github.v3+json")
        request.add_header("Authorization", "token " + token)

        try:
            response = urlopen(request)
        except URLError as e:
            if hasattr(e, 'reason'):
                print("We failed to to reach the API server. Reason: " + str(e.reason))
            elif hasattr(e, 'code'):
                print("The github server couldn't fulfill the request. Error code: " + str(e.code))
            sys.exit(1)

        issues = json.loads(response.read())
        pulls = []
        for issue in issues:
            if 'pull_request' in issue:
                pulls.append(issue)

        if 0 < len(pulls) and next_timestamp is None:
            next_timestamp = pulls[0]['updated_at']

        if len(issues) < MAX_RESULTS:
            should_terminate = True

        for pull_request in pulls:
            event = {}
            event['pull_id'] = int(pull_id_pattern.search(pull_request["html_url"]).group(1))
            event['title'] = pull_request['title']
            event['user'] = pull_request['user']['login']
            event['created_at'] = pull_request['created_at']
            event['updated_at'] = pull_request['updated_at']
            event['closed_at'] = pull_request['closed_at']
            event['emit_at'] =  long(time.time())
            event['repo'] = repo
            json_event = json.dumps(event)
            key = "%s-%s" % (repo, event['pull_id'])
            print json_event
            PRODUCER.send('apurva_test', key=key, value=json_event)

        page += 1

    return next_timestamp


def main():
    parser = ArgumentParser()
    parser.add_argument("-t", "--token", dest="github_token", required=True,
                        help="The github oauth token to use for authentication")
    args = parser.parse_args()
    next_timestamp = "2019-12-01T00:00:00Z"
    while True:
        next_timestamp = get_pulls_since("confluentinc/ksql", args.github_token, next_timestamp)
        print "sleeping ... "
        time.sleep(1)

if __name__ == "__main__":
    main()

