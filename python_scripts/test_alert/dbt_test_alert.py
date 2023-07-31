import datetime
import requests
import json
import sys
import os


def log_test_result():
    """Reads the run_results.json file and returns a dictionary of targeted test results"""

    filepath = "target/run_results.json"

    with open(filepath) as f:
        run = json.load(f)

    logs = []
    messages = {
        "fail": [],
        "warn": []
    }
    test_count = 0
    warn_count = 0
    fail_count = 0

    for test in run["results"]:
        test_count += 1
        if test["status"] != "pass":
            logs.append(test)

            message = f"{test['failures']} record failure(s) in {test['unique_id']}"

            if test["status"] == "warn":
                messages["warn"].append(message)
                warn_count += 1
            elif test["status"] == "fail":
                messages["fail"].append(message)
                fail_count += 1

    dbt_test_result = {
        "logs": logs,
        "messages": messages,
        "test_count": test_count,
        "warn_count": warn_count,
        "fail_count": fail_count,
        "elapsed_time": str(datetime.timedelta(seconds=run["elapsed_time"]))
    }

    return dbt_test_result


def create_message(**kwargs):
    messageBody = {
        "text": f"Hey{' <@here>' if len(kwargs['messages']['fail']) > 0 else ''}, new DBT test results for :{os.environ.get('DATABASE').split('_DEV')[0]}: {os.environ.get('DATABASE')}",
        "attachments": [
            {
                "color": kwargs["color"],
                "fields": [
                    {
                        "title": "Total Tests Run",
                        "value": kwargs["test_count"],
                        "short": True
                    },
                    {
                        "title": "Total Time Elapsed",
                        "value": kwargs["elapsed_time"],
                        "short": True
                    },
                    {
                        "title": "Number of Unsuccessful Tests",
                        "value": f"Fail: {kwargs['fail_count']}, Warn: {kwargs['warn_count']}",
                        "short": True
                    },
                    {
                        "title": "Failed Tests:",
                        "value": "\n".join(kwargs["messages"]["fail"]) if len(kwargs["messages"]["fail"]) > 0 else "None :)",
                        "short": False
                    }
                ],
                "actions": [

                    {
                        "type": "button",
                        "text": "View Warnings",
                        "style": "primary",
                        "url": "https://github.com/FlipsideCrypto/near-models/actions/workflows/dbt_test.yml",
                        "confirm": {
                            "title": f"{kwargs['warn_count']} Warnings",
                            "text": "\n".join(kwargs["messages"]["warn"]) if len(kwargs["messages"]["warn"]) > 0 else "None :)",
                            "ok_text": "Continue to GHA",
                            "dismiss_text": "Dismiss"
                        }
                    }
                ]
            }
        ]
    }

    return messageBody


def send_alert(webhook_url):
    """Sends a message to a slack channel"""

    url = webhook_url

    data = log_test_result()

    send_message = create_message(
        fail_count=data["fail_count"],
        warn_count=data["warn_count"],
        test_count=data["test_count"],
        messages=data["messages"],
        elapsed_time=data["elapsed_time"],
        color="#f44336" if data["fail_count"] > 0 else "#4CAF50"
    )

    x = requests.post(url, json=send_message)

    # test config to continue on error in workflow, so we want to exit with a non-zero code if there are any failures
    if data['fail_count'] > 0:
        sys.exit(1)


if __name__ == '__main__':

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    send_alert(webhook_url)
