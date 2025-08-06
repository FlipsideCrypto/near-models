#!/usr/bin/env python3
"""
Failure Assessor - POC Script for analyzing GitHub Actions workflow failures

This script analyzes failed GitHub Actions workflows using the Claude Code SDK
and posts enhanced Slack notifications with AI-generated analysis.

Usage:
    python failure_assessor.py --run-id GITHUB_RUN_ID --agent-file AGENT_FILE_PATH
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional, Tuple

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

try:
    from claude_code_sdk import query, ClaudeCodeOptions
    import asyncio
    logger.info("Successfully imported Claude Code SDK")
except ImportError as e:
    logger.error(f"Failed to import claude-code-sdk: {e}")
    print("ERROR: claude-code-sdk package not installed. Run: pip install claude-code-sdk")
    sys.exit(1)


class FailureAssessor:
    def __init__(self, run_id: str, agent_file: str, repo: str = None):
        logger.info(f"Initializing FailureAssessor with run_id: {run_id}")
        logger.info(f"Agent file: {agent_file}")
        logger.info(f"Repository: {repo}")
        
        self.run_id = run_id
        self.agent_file = agent_file
        # Use GitHub environment variable first, then CLI arg, then require explicit specification
        self.repo = os.environ.get("GITHUB_REPOSITORY") or repo
        if not self.repo:
            raise ValueError("Repository must be specified via --repo argument or GITHUB_REPOSITORY environment variable")
        
        logger.info(f"Using repository: {self.repo}")
        self.github_token = os.environ.get("GITHUB_TOKEN")
        self.slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
        
        logger.debug(f"GitHub token present: {'Yes' if self.github_token else 'No'}")
        logger.debug(f"Slack webhook present: {'Yes' if self.slack_webhook else 'No'}")
        
        if not self.github_token:
            logger.error("GITHUB_TOKEN environment variable is missing")
            raise ValueError("GITHUB_TOKEN environment variable is required")
        if not self.slack_webhook:
            logger.error("SLACK_WEBHOOK_URL environment variable is missing")
            raise ValueError("SLACK_WEBHOOK_URL environment variable is required")

    def run_command(self, cmd: str, capture_output: bool = True) -> Tuple[int, str, str]:
        """Run a shell command and return exit code, stdout, stderr"""
        logger.debug(f"Running command: {cmd}")
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=capture_output,
                text=True,
                timeout=60
            )
            logger.debug(f"Command exit code: {result.returncode}")
            logger.debug(f"Command stdout length: {len(result.stdout)} chars")
            logger.debug(f"Command stderr length: {len(result.stderr)} chars")
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            logger.error("Command timed out")
            return 1, "", "Command timed out"
        except Exception as e:
            logger.error(f"Command failed with exception: {e}")
            return 1, "", str(e)

    def gather_run_metadata(self) -> Dict:
        """Gather metadata about the failed run using gh CLI"""
        logger.info(f"Gathering metadata for run ID: {self.run_id}")
        
        # Get run details in JSON format
        json_fields = "name,status,conclusion,createdAt,headSha,jobs,workflowName,url"
        logger.debug(f"Requesting JSON fields: {json_fields}")
        exit_code, stdout, stderr = self.run_command(f"gh run view {self.run_id} --repo {self.repo} --json {json_fields}")
        
        if exit_code != 0:
            logger.error(f"Failed to get run details: {stderr}")
            print(f"ERROR: Failed to get run details: {stderr}")
            return {}
            
        logger.debug(f"Raw JSON response: {stdout[:500]}...")
        try:
            run_data = json.loads(stdout)
            logger.debug(f"Parsed JSON keys: {list(run_data.keys()) if run_data else 'None'}")
            # Use environment variables from GitHub Actions when available
            github_repo = os.environ.get("GITHUB_REPOSITORY", self.repo)
            github_workflow = os.environ.get("GITHUB_WORKFLOW", run_data.get("workflowName", "Unknown"))
            github_server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
            
            metadata = {
                "run_id": self.run_id,
                "workflow_name": github_workflow,
                "repository": github_repo.split('/')[-1] if '/' in github_repo else github_repo,
                "full_repository": github_repo,
                "status": run_data.get("status", "Unknown"),
                "conclusion": run_data.get("conclusion", "Unknown"),
                "created_at": run_data.get("createdAt", "Unknown"),
                "head_commit": "Unknown",  # Not available in basic fields
                "head_sha": run_data.get("headSha", "Unknown")[:8] if run_data.get("headSha") else "Unknown",
                "jobs": run_data.get("jobs", []),
                "url": run_data.get("url", f"{github_server_url}/{github_repo}/actions/runs/{self.run_id}")
            }
            logger.info(f"Successfully gathered metadata: workflow={metadata['workflow_name']}, status={metadata['status']}")
            return metadata
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse run JSON: {e}")
            logger.debug(f"Raw stdout that failed to parse: {stdout}")
            print(f"ERROR: Failed to parse run JSON: {stdout}")
            return {}

    def fetch_run_logs(self, max_lines: int = 3000) -> str:
        """Fetch and truncate run logs to control token usage"""
        logger.info(f"Fetching run logs for run ID: {self.run_id}")
        logger.debug(f"Max lines limit: {max_lines}")
        
        exit_code, stdout, stderr = self.run_command(f"gh run view {self.run_id} --repo {self.repo} --log")
        
        if exit_code != 0:
            logger.error(f"Failed to get run logs: {stderr}")
            print(f"ERROR: Failed to get run logs: {stderr}")
            return f"Failed to fetch logs: {stderr}"
        
        logger.debug(f"Raw logs length: {len(stdout)} characters")
        
        # Truncate logs to last N lines to control token usage
        lines = stdout.split('\n')
        logger.debug(f"Total log lines: {len(lines)}")
        
        if len(lines) > max_lines:
            logger.info(f"Truncating logs from {len(lines)} to {max_lines} lines")
            truncated_logs = '\n'.join(lines[-max_lines:])
            result = f"[LOG TRUNCATED - showing last {max_lines} lines]\n\n{truncated_logs}"
        else:
            logger.info(f"Using all {len(lines)} log lines (under limit)")
            result = stdout
            
        logger.debug(f"Final log output length: {len(result)} characters")
        return result

    def load_agent_instructions(self) -> str:
        """Load the agent instructions from the specified file"""
        logger.info(f"Loading agent instructions from: {self.agent_file}")
        try:
            with open(self.agent_file, 'r') as f:
                content = f.read()
                logger.debug(f"Agent file loaded successfully, length: {len(content)} characters")
                return content
        except FileNotFoundError:
            logger.error(f"Agent file not found: {self.agent_file}")
            print(f"ERROR: Agent file not found: {self.agent_file}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to read agent file: {e}")
            print(f"ERROR: Failed to read agent file: {e}")
            sys.exit(1)

    def build_analysis_prompt(self, metadata: Dict, logs: str) -> str:
        """Build the prompt for the Claude analysis"""
        agent_instructions = self.load_agent_instructions()
        
        # Extract key information for the prompt
        workflow_name = metadata.get("workflow_name", "Unknown")
        repository = metadata.get("repository", "Unknown")
        head_commit = metadata.get("head_commit", "Unknown")
        head_sha = metadata.get("head_sha", "Unknown")
        created_at = metadata.get("created_at", "Unknown")
        
        prompt = f"""
You are analyzing a failed GitHub Actions workflow. Please provide a concise analysis for a Slack notification.

## Context
- **Repository**: {repository}
- **Workflow**: {workflow_name}
- **Run ID**: {self.run_id}
- **Commit**: {head_sha} - {head_commit}
- **Timestamp**: {created_at}

## Agent Instructions
{agent_instructions}

## Failed Job Logs
```
{logs}
```

## Required Output Format
You MUST respond in this EXACT markdown format for Slack. Do not include any other text before or after this format:

**🔍 Failure Analysis**

**Root Cause:** 
[1-2 sentence summary of what caused the failure]

**Evidence:**
```
[Key error message or relevant log excerpt - keep under 10 lines]
```

**Immediate Actions:**
• [Action item 1]  
• [Action item 2]
• [Action item 3 if needed]
"""
        return prompt

    async def analyze_with_claude(self, prompt: str) -> str:
        """Send the prompt to Claude Code SDK and get analysis back"""
        logger.info("Starting analysis with Claude Code SDK...")
        
        try:
            # Load agent instructions and include them in the system prompt
            logger.debug("Loading agent instructions for system prompt...")
            agent_instructions = self.load_agent_instructions()
            
            # Create options for focused analysis - disable tools to prevent investigation workflow
            logger.debug("Creating ClaudeCodeOptions...")
            options = ClaudeCodeOptions(
                max_turns=1,
                system_prompt="""You are a GitHub Actions failure analysis assistant. You must analyze the provided logs and respond in the EXACT format requested. Do not use any tools or perform investigations - just analyze the logs provided and give a formatted response. You are NOT allowed to run commands, read files, or perform any investigations.""",
                allowed_tools=[]  # Disable all tools to prevent investigation workflow
            )
            logger.debug(f"Options created: max_turns={options.max_turns}")
            
            logger.info("Sending query to Claude Code SDK...")
            logger.debug(f"Prompt length: {len(prompt)} characters")
            
            analysis_parts = []
            message_count = 0
            
            async for message in query(prompt=prompt, options=options):
                message_count += 1
                logger.debug(f"Received message #{message_count}, type: {type(message).__name__}")
                
                if hasattr(message, 'content'):
                    logger.debug(f"Message has content with {len(message.content)} parts")
                    for i, content_block in enumerate(message.content):
                        logger.debug(f"Content block #{i}, type: {type(content_block).__name__}")
                        if hasattr(content_block, 'text'):
                            text_content = content_block.text
                            logger.debug(f"Text content length: {len(text_content)} chars")
                            analysis_parts.append(text_content)
                else:
                    logger.debug("Message has no content attribute")
            
            logger.info(f"Completed query iteration, received {message_count} messages")
            logger.info(f"Collected {len(analysis_parts)} analysis parts")
            
            if analysis_parts:
                analysis = '\n'.join(analysis_parts)
                logger.info(f"Generated analysis length: {len(analysis)} characters")
                logger.debug(f"Analysis preview: {analysis[:200]}...")
            else:
                analysis = "No analysis generated"
                logger.warning("No analysis parts collected from Claude Code SDK")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Claude Code SDK analysis failed: {e}", exc_info=True)
            print(f"ERROR: Failed to analyze with Claude Code SDK: {e}")
            return f"**Analysis Error**: Failed to analyze logs with AI: {str(e)}"

    def post_to_slack(self, analysis: str, metadata: Dict):
        """Post the enhanced failure notification to Slack"""
        print("Posting enhanced notification to Slack...")
        
        # Create temporary file with the analysis
        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(analysis)
            analysis_file = f.name
        
        try:
            # Call the updated slack_alert.py with analysis
            cmd = f"python {Path(__file__).parent}/slack_alert.py --analysis-file {analysis_file}"
            
            # Set up environment for slack_alert.py (preserve existing GitHub env vars)
            env = os.environ.copy()
            env.update({
                'SLACK_WEBHOOK_URL': self.slack_webhook,
                'GITHUB_REPOSITORY': metadata.get('full_repository', f"FlipsideCrypto/{metadata.get('repository', 'unknown')}"),
                'GITHUB_WORKFLOW': metadata.get('workflow_name', 'Unknown'),
                'GITHUB_RUN_ID': str(self.run_id),
                'GITHUB_SERVER_URL': os.environ.get('GITHUB_SERVER_URL', 'https://github.com')
            })
            
            result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"ERROR: Failed to send Slack notification: {result.stderr}")
                print(f"STDOUT: {result.stdout}")
                sys.exit(1)
            else:
                print("Successfully sent enhanced Slack notification")
                
        finally:
            # Clean up temporary file
            try:
                os.unlink(analysis_file)
            except:
                pass

    async def run(self):
        """Main execution flow"""
        logger.info("🔍 Starting failure assessment...")
        print("🔍 Starting failure assessment...")
        
        # Step 1: Gather run metadata
        logger.info("=== STEP 1: Gathering run metadata ===")
        metadata = self.gather_run_metadata()
        if not metadata:
            logger.error("Failed to gather run metadata, exiting")
            print("ERROR: Failed to gather run metadata")
            sys.exit(1)
        logger.info("✅ Step 1 complete")
        
        # Step 2: Fetch logs
        logger.info("=== STEP 2: Fetching run logs ===")
        logs = self.fetch_run_logs()
        if not logs or logs.strip() == "":
            logger.warning("No logs found - logs may have been purged or run is too old")
            print("WARNING: No logs found - using metadata-only analysis")
            logs = f"No logs available for run {self.run_id}. This may be because:\n- Logs have been purged due to retention policy\n- Run is too old\n- Logs were not generated\n\nMetadata indicates: {metadata.get('conclusion', 'Unknown')} conclusion for workflow '{metadata.get('workflow_name', 'Unknown')}'"
        logger.info("✅ Step 2 complete")
        
        # Step 3: Build prompt and analyze with Claude
        logger.info("=== STEP 3: Analyzing with Claude Code SDK ===")
        prompt = self.build_analysis_prompt(metadata, logs)
        logger.debug(f"Built prompt length: {len(prompt)} characters")
        
        analysis = await self.analyze_with_claude(prompt)
        logger.info("✅ Step 3 complete")
        
        # Step 4: Post to Slack
        logger.info("=== STEP 4: Posting to Slack ===")
        self.post_to_slack(analysis, metadata)
        logger.info("✅ Step 4 complete")
        
        logger.info("✅ Failure assessment complete!")
        print("✅ Failure assessment complete!")


async def main_async():
    parser = argparse.ArgumentParser(
        description="Analyze GitHub Actions workflow failures with Claude Code SDK"
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="GitHub run ID to analyze"
    )
    parser.add_argument(
        "--agent-file", 
        required=True,
        help="Path to Claude agent instructions file"
    )
    parser.add_argument(
        "--repo",
        help="GitHub repository in format 'owner/repo' (uses GITHUB_REPOSITORY env var if not specified)"
    )
    
    args = parser.parse_args()
    
    try:
        assessor = FailureAssessor(args.run_id, args.agent_file, args.repo)
        await assessor.run()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()