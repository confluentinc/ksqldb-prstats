#!/usr/bin/python

from urllib2 import Request, urlopen, URLError
import sys
from argparse import ArgumentParser
import json
import re
import time

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

def get_pull_id(pattern, pull_request):
    result = pattern.search(pull_request["html_url"])
    return int(result.group(1))


def get_pulls_since(repo, token, last_timestamp, open_pulls):
    page = 0
    should_terminate = False
    next_timestamp = last_timestamp
    pull_id_pattern = re.compile("^.*\/pull\/(\d+)$")
    while not should_terminate:
        state = "open" if open_pulls else "closed"
        url = "https://api.github.com/repos/%s/issues?page=%s&per_page=%s&since=%s&state=%s&sort=updated" % (repo, page, MAX_RESULTS, last_timestamp, state)
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

        if 0 < len(pulls):
            next_timestamp = pulls[0]['updated_at']

        if len(pulls) < MAX_RESULTS:
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
            json_event = json.dumps(event)
            print "key: %s/%s" % (repo, event['pull_id'])
            print "value: %s" % json_event

        return next_timestamp


def main():
    parser = ArgumentParser()
    parser.add_argument("-t", "--token", dest="github_token", required=True,
                        help="The github oauth token to use for authentication")
    args = parser.parse_args()
    next_timestamp_opened = "2019-12-05T00:00:00Z"
    next_timestamp_closed = "2019-12-05T00:00:00Z"
    while True:
        next_timestamp_opened = get_pulls_since("confluentinc/ksql", args.github_token, next_timestamp_opened, open_pulls=True)
        next_timestamp_closed = get_pulls_since("confluentinc/ksql", args.github_token, next_timestamp_closed, open_pulls=False)
        time.sleep(60)

if __name__ == "__main__":
    main()

