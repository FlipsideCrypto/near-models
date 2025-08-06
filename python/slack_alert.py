import requests
import os
import sys
import argparse

def create_message(analysis_text=None):
    """Creates a failure notification message with optional AI analysis"""

    # Get GitHub environment variables
    repository = os.environ.get('GITHUB_REPOSITORY', 'Unknown repository')
    repo_name = repository.split('/')[-1] if '/' in repository else repository
    workflow_name = os.environ.get('GITHUB_WORKFLOW', 'Unknown workflow')
    run_id = os.environ.get('GITHUB_RUN_ID', '')
    server_url = os.environ.get('GITHUB_SERVER_URL', 'https://github.com')

    # Build the workflow URL
    workflow_url = f"{server_url}/{repository}/actions/runs/{run_id}"

    # Base attachment structure
    attachment = {
        "color": "#f44336",  # Red color for failures
        "fields": [
            {
                "title": "Repository",
                "value": repository,
                "short": True
            },
            {
                "title": "Workflow",
                "value": workflow_name,
                "short": True
            }
        ],
        "actions": [
            {
                "type": "button",
                "text": "View Workflow Run",
                "style": "primary",
                "url": workflow_url
            }
        ],
        "footer": "GitHub Actions"
    }

    # Add AI analysis if provided
    if analysis_text:
        attachment["text"] = analysis_text
        attachment["mrkdwn_in"] = ["text"]  # Enable markdown formatting

    message_body = {
        "text": f"Failure in {repo_name}",
        "attachments": [attachment]
    }

    return message_body

def send_alert(webhook_url, analysis_text=None):
    """Sends a failure notification to Slack"""

    message = create_message(analysis_text)

    try:
        response = requests.post(webhook_url, json=message)

        if response.status_code == 200:
            print("Successfully sent Slack notification")
        else:
            print(f"Failed to send Slack notification: {response.status_code} {response.text}")
            sys.exit(1)
    except Exception as e:
        print(f"Error sending Slack notification: {str(e)}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Send Slack failure notification")
    parser.add_argument(
        "--analysis-file", 
        help="Path to file containing AI analysis text"
    )
    parser.add_argument(
        "--analysis-text",
        help="Direct analysis text to include"
    )
    
    args = parser.parse_args()
    
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("ERROR: SLACK_WEBHOOK_URL environment variable is required")
        sys.exit(1)

    analysis_text = None
    
    # Load analysis from file if provided
    if args.analysis_file:
        try:
            with open(args.analysis_file, 'r') as f:
                analysis_text = f.read()
        except Exception as e:
            print(f"WARNING: Failed to read analysis file: {e}")
    
    # Use direct text if provided (overrides file)
    if args.analysis_text:
        analysis_text = args.analysis_text

    send_alert(webhook_url, analysis_text)

if __name__ == '__main__':
    main()
