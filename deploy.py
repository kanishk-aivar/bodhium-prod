#!/usr/bin/env python3
"""
Bodhium Lambda Deployment Script

This script handles deployment of:
- Lambda code deployments (orchestrator, perplexity)
- ECR container deployments (aio, aim, chatgpt)
- Auto-tagging with version management
- Revert functionality
- Persistent configuration storage
"""

import os
import sys
import json
import boto3
import zipfile
import tempfile
import shutil
import subprocess
import argparse
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import hashlib
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Colors:
    """ANSI color codes for terminal output"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    PURPLE = '\033[0;35m'
    CYAN = '\033[0;36m'
    WHITE = '\033[1;37m'
    NC = '\033[0m'  # No Color

class DeploymentConfig:
    """Manages deployment configuration and persistent storage"""
    
    def __init__(self, config_file: str = "deployment_config.json"):
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Load configuration from file - REQUIRED, no defaults"""
        if not os.path.exists(self.config_file):
            logger.error(f"Configuration file not found: {self.config_file}")
            logger.error("Please create a configuration file. Use 'init-config' action to generate a template.")
            sys.exit(1)
        
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                logger.info(f"Loaded configuration from {self.config_file}")
                return config
        except Exception as e:
            logger.error(f"Failed to load config from {self.config_file}: {e}")
            sys.exit(1)
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2, default=str)
            logger.info(f"Configuration saved to {self.config_file}")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
    
    def create_template_config(self):
        """Create a template configuration file"""
        template_config = {
            "aws_region": "us-east-1",
            "ecr_registry": "127214200395.dkr.ecr.us-east-1.amazonaws.com",
            "lambdas": {
                "orchestrator": {
                    "function_name": "bodhium-llm-orchestrator",
                    "deployment_type": "code",
                    "source_path": "lambdas/llm-orchestrator",
                    "handler": "lambda_function.lambda_handler",
                    "runtime": "python3.9",
                    "timeout": 300,
                    "memory_size": 512
                },
                "perplexity": {
                    "function_name": "bodhium-llm-perplexityapi",
                    "deployment_type": "code",
                    "source_path": "lambdas/workers/perplexity",
                    "handler": "lambda_function.lambda_handler",
                    "runtime": "python3.9",
                    "timeout": 300,
                    "memory_size": 512
                },
                "aio": {
                    "function_name": "BodhiumAiOverview",
                    "deployment_type": "container",
                    "source_path": "lambdas/workers/aio",
                    "ecr_repo": "bodhium-aio-worker",
                    "timeout": 900,
                    "memory_size": 3008
                },
                "aim": {
                    "function_name": "bodhium-aimode-brightdata",
                    "deployment_type": "container",
                    "source_path": "lambdas/workers/aim",
                    "ecr_repo": "bodhium-aim-worker",
                    "timeout": 900,
                    "memory_size": 3008
                },
                "chatgpt": {
                    "function_name": "bodhium-llm-chatgpt-v1",
                    "deployment_type": "container",
                    "source_path": "lambdas/workers/ChatGPT",
                    "ecr_repo": "bodhium-chatgpt-worker",
                    "timeout": 900,
                    "memory_size": 3008
                }
            },
            "deployment_history": []
        }
        
        try:
            with open(self.config_file, 'w') as f:
                json.dump(template_config, f, indent=2)
            print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} Created template configuration: {self.config_file}")
            print(f"{Colors.YELLOW}[INFO]{Colors.NC} Please review and modify the configuration as needed.")
            return True
        except Exception as e:
            print(f"{Colors.RED}[ERROR]{Colors.NC} Failed to create template config: {e}")
            return False
    
    def get_lambda_config(self, lambda_name: str) -> Dict:
        """Get configuration for a specific lambda"""
        return self.config["lambdas"].get(lambda_name, {})
    
    def add_deployment_record(self, lambda_name: str, version: str, deployment_type: str, success: bool):
        """Add deployment record to history"""
        record = {
            "lambda_name": lambda_name,
            "version": version,
            "deployment_type": deployment_type,
            "timestamp": datetime.now().isoformat(),
            "success": success
        }
        self.config["deployment_history"].append(record)
        self.save_config()

class BodhiumDeployer:
    """Main deployment class"""
    
    def __init__(self):
        self.config = DeploymentConfig()
        self.aws_region = self.config.config["aws_region"]
        self.ecr_registry = self.config.config["ecr_registry"]
        
        # Initialize AWS clients
        self.lambda_client = boto3.client('lambda', region_name=self.aws_region)
        self.ecr_client = boto3.client('ecr', region_name=self.aws_region)
        self.sts_client = boto3.client('sts', region_name=self.aws_region)
        
        # Verify AWS credentials
        try:
            self.account_id = self.sts_client.get_caller_identity()['Account']
            logger.info(f"Using AWS Account: {self.account_id}")
        except Exception as e:
            logger.error(f"Failed to get AWS credentials: {e}")
            sys.exit(1)
    
    def print_status(self, message: str, color: str = Colors.BLUE):
        """Print colored status message"""
        print(f"{color}[INFO]{Colors.NC} {message}")
    
    def print_success(self, message: str):
        """Print success message"""
        print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {message}")
    
    def print_error(self, message: str):
        """Print error message"""
        print(f"{Colors.RED}[ERROR]{Colors.NC} {message}")
    
    def print_warning(self, message: str):
        """Print warning message"""
        print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {message}")
    
    def generate_version_tag(self) -> str:
        """Generate version tag based on timestamp and git commit"""
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        try:
            # Get git commit hash
            result = subprocess.run(
                ['git', 'rev-parse', '--short', 'HEAD'],
                capture_output=True, text=True, check=True
            )
            git_hash = result.stdout.strip()
            return f"v{timestamp}-{git_hash}"
        except subprocess.CalledProcessError:
            # Fallback if not in git repo
            return f"v{timestamp}"
    
    def ecr_login(self) -> bool:
        """Login to ECR"""
        try:
            self.print_status("Logging into AWS ECR...")
            
            # Get ECR login token
            response = self.ecr_client.get_authorization_token()
            token = response['authorizationData'][0]['authorizationToken']
            endpoint = response['authorizationData'][0]['proxyEndpoint']
            
            # Decode token
            import base64
            username, password = base64.b64decode(token).decode().split(':')
            
            # Docker login
            cmd = [
                'docker', 'login',
                '--username', username,
                '--password-stdin',
                endpoint
            ]
            
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(input=password.encode())
            
            if process.returncode == 0:
                self.print_success("Successfully logged into ECR")
                return True
            else:
                self.print_error(f"ECR login failed: {stderr.decode()}")
                return False
                
        except Exception as e:
            self.print_error(f"ECR login failed: {e}")
            return False
    
    def create_ecr_repository(self, repo_name: str) -> bool:
        """Create ECR repository if it doesn't exist"""
        try:
            self.ecr_client.describe_repositories(repositoryNames=[repo_name])
            logger.info(f"ECR repository {repo_name} already exists")
            return True
        except self.ecr_client.exceptions.RepositoryNotFoundException:
            try:
                self.print_status(f"Creating ECR repository: {repo_name}")
                self.ecr_client.create_repository(
                    repositoryName=repo_name,
                    imageScanningConfiguration={'scanOnPush': True}
                )
                self.print_success(f"Created ECR repository: {repo_name}")
                return True
            except Exception as e:
                self.print_error(f"Failed to create ECR repository {repo_name}: {e}")
                return False
        except Exception as e:
            self.print_error(f"Failed to check ECR repository {repo_name}: {e}")
            return False
    
    def build_and_push_container(self, lambda_name: str, version_tag: str) -> bool:
        """Build and push container to ECR"""
        lambda_config = self.config.get_lambda_config(lambda_name)
        source_path = lambda_config["source_path"]
        ecr_repo = lambda_config["ecr_repo"]
        
        if not os.path.exists(source_path):
            self.print_error(f"Source path does not exist: {source_path}")
            return False
        
        dockerfile_path = os.path.join(source_path, "Dockerfile")
        if not os.path.exists(dockerfile_path):
            self.print_error(f"Dockerfile not found: {dockerfile_path}")
            return False
        
        # Create ECR repository if needed
        if not self.create_ecr_repository(ecr_repo):
            return False
        
        # Build image
        self.print_status(f"Building Docker image for {lambda_name}...")
        image_tag = f"{ecr_repo}:{version_tag}"
        ecr_image_tag = f"{self.ecr_registry}/{image_tag}"
        
        try:
            cmd = [
                'docker', 'build',
                '--platform', 'linux/amd64',
                '--provenance', 'false',
                '-t', image_tag,
                '-t', ecr_image_tag,
                source_path
            ]
            
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.print_success(f"Successfully built image: {image_tag}")
            
        except subprocess.CalledProcessError as e:
            self.print_error(f"Docker build failed for {lambda_name}: {e.stderr}")
            return False
        
        # Push to ECR
        self.print_status(f"Pushing image to ECR: {ecr_image_tag}")
        try:
            cmd = ['docker', 'push', ecr_image_tag]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.print_success(f"Successfully pushed image: {ecr_image_tag}")
            return True
            
        except subprocess.CalledProcessError as e:
            self.print_error(f"Docker push failed for {lambda_name}: {e.stderr}")
            return False
    
    def create_lambda_zip(self, source_path: str) -> str:
        """Create ZIP file for Lambda deployment"""
        temp_dir = tempfile.mkdtemp()
        zip_path = os.path.join(temp_dir, "lambda.zip")
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(source_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, source_path)
                        zipf.write(file_path, arcname)
            
            return zip_path
            
        except Exception as e:
            logger.error(f"Failed to create ZIP file: {e}")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise
    
    def deploy_lambda_code(self, lambda_name: str, version_tag: str) -> bool:
        """Deploy Lambda function with code"""
        lambda_config = self.config.get_lambda_config(lambda_name)
        function_name = lambda_config["function_name"]
        source_path = lambda_config["source_path"]
        
        if not os.path.exists(source_path):
            self.print_error(f"Source path does not exist: {source_path}")
            return False
        
        try:
            # Create ZIP file
            self.print_status(f"Creating deployment package for {lambda_name}...")
            zip_path = self.create_lambda_zip(source_path)
            
            # Read ZIP file
            with open(zip_path, 'rb') as f:
                zip_content = f.read()
            
            # Update function code
            self.print_status(f"Updating Lambda function: {function_name}")
            response = self.lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_content
            )
            
            # Wait for update to complete before publishing version
            self.print_status("Waiting for function update to complete...")
            waiter = self.lambda_client.get_waiter('function_updated')
            waiter.wait(FunctionName=function_name)
            
            # Tag the version with retry logic
            self.print_status(f"Publishing version with tag: {version_tag}")
            max_retries = 3
            retry_delay = 5
            
            for attempt in range(max_retries):
                try:
                    version_response = self.lambda_client.publish_version(
                        FunctionName=function_name,
                        Description=f"Deployed on {datetime.now().isoformat()} - {version_tag}"
                    )
                    break
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceConflictException' and attempt < max_retries - 1:
                        self.print_warning(f"Function still updating, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise
            
            version_number = version_response['Version']
            
            # Create alias for the version
            alias_name = version_tag.replace('.', '-').replace(':', '-')
            try:
                self.lambda_client.create_alias(
                    FunctionName=function_name,
                    Name=alias_name,
                    FunctionVersion=version_number,
                    Description=f"Version {version_tag}"
                )
                self.print_success(f"Created alias: {alias_name} -> Version {version_number}")
            except self.lambda_client.exceptions.ResourceConflictException:
                # Update existing alias
                self.lambda_client.update_alias(
                    FunctionName=function_name,
                    Name=alias_name,
                    FunctionVersion=version_number
                )
                self.print_success(f"Updated alias: {alias_name} -> Version {version_number}")
            
            # Cleanup
            os.unlink(zip_path)
            os.rmdir(os.path.dirname(zip_path))
            
            self.print_success(f"Successfully deployed {lambda_name} (Version: {version_number})")
            return True
            
        except Exception as e:
            self.print_error(f"Failed to deploy {lambda_name}: {e}")
            return False
    
    def deploy_lambda_container(self, lambda_name: str, version_tag: str) -> bool:
        """Deploy Lambda function with container image"""
        lambda_config = self.config.get_lambda_config(lambda_name)
        function_name = lambda_config["function_name"]
        ecr_repo = lambda_config["ecr_repo"]
        
        image_uri = f"{self.ecr_registry}/{ecr_repo}:{version_tag}"
        
        try:
            # Update function code with container image
            self.print_status(f"Updating Lambda function with container: {function_name}")
            response = self.lambda_client.update_function_code(
                FunctionName=function_name,
                ImageUri=image_uri
            )
            
            # Wait for update to complete before publishing version
            self.print_status("Waiting for function update to complete...")
            waiter = self.lambda_client.get_waiter('function_updated')
            waiter.wait(FunctionName=function_name)
            
            # Publish version with retry logic
            self.print_status(f"Publishing version with tag: {version_tag}")
            max_retries = 3
            retry_delay = 5
            
            for attempt in range(max_retries):
                try:
                    version_response = self.lambda_client.publish_version(
                        FunctionName=function_name,
                        Description=f"Container deployed on {datetime.now().isoformat()} - {version_tag}"
                    )
                    break
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceConflictException' and attempt < max_retries - 1:
                        self.print_warning(f"Function still updating, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise
            
            version_number = version_response['Version']
            
            # Create alias for the version
            alias_name = version_tag.replace('.', '-').replace(':', '-')
            try:
                self.lambda_client.create_alias(
                    FunctionName=function_name,
                    Name=alias_name,
                    FunctionVersion=version_number,
                    Description=f"Container version {version_tag}"
                )
                self.print_success(f"Created alias: {alias_name} -> Version {version_number}")
            except self.lambda_client.exceptions.ResourceConflictException:
                # Update existing alias
                self.lambda_client.update_alias(
                    FunctionName=function_name,
                    Name=alias_name,
                    FunctionVersion=version_number
                )
                self.print_success(f"Updated alias: {alias_name} -> Version {version_number}")
            
            self.print_success(f"Successfully deployed {lambda_name} container (Version: {version_number})")
            return True
            
        except Exception as e:
            self.print_error(f"Failed to deploy {lambda_name} container: {e}")
            return False
    
    def deploy_lambda(self, lambda_name: str, version_tag: str = None) -> bool:
        """Deploy a single Lambda function"""
        if version_tag is None:
            version_tag = self.generate_version_tag()
        
        lambda_config = self.config.get_lambda_config(lambda_name)
        if not lambda_config:
            self.print_error(f"Lambda configuration not found: {lambda_name}")
            return False
        
        deployment_type = lambda_config["deployment_type"]
        
        self.print_status(f"Deploying {lambda_name} ({deployment_type})...")
        
        success = False
        if deployment_type == "code":
            success = self.deploy_lambda_code(lambda_name, version_tag)
        elif deployment_type == "container":
            # Build and push container first
            if self.build_and_push_container(lambda_name, version_tag):
                success = self.deploy_lambda_container(lambda_name, version_tag)
        else:
            self.print_error(f"Unknown deployment type: {deployment_type}")
        
        # Record deployment
        self.config.add_deployment_record(lambda_name, version_tag, deployment_type, success)
        
        return success
    
    def get_lambda_versions(self, function_name: str) -> List[Dict]:
        """Get all versions of a Lambda function"""
        try:
            response = self.lambda_client.list_versions_by_function(
                FunctionName=function_name
            )
            return response['Versions']
        except Exception as e:
            logger.error(f"Failed to get versions for {function_name}: {e}")
            return []
    
    def get_lambda_aliases(self, function_name: str) -> List[Dict]:
        """Get all aliases of a Lambda function"""
        try:
            response = self.lambda_client.list_aliases(
                FunctionName=function_name
            )
            return response['Aliases']
        except Exception as e:
            logger.error(f"Failed to get aliases for {function_name}: {e}")
            return []
    
    def revert_lambda(self, lambda_name: str, target_version: str = None) -> bool:
        """Revert Lambda function to a previous version"""
        lambda_config = self.config.get_lambda_config(lambda_name)
        if not lambda_config:
            self.print_error(f"Lambda configuration not found: {lambda_name}")
            return False
        
        function_name = lambda_config["function_name"]
        
        # Get available versions
        versions = self.get_lambda_versions(function_name)
        aliases = self.get_lambda_aliases(function_name)
        
        if not target_version:
            # Show available versions and let user choose
            print(f"\n{Colors.CYAN}Available versions for {lambda_name}:{Colors.NC}")
            print(f"{'Version':<10} {'Description':<50} {'Last Modified'}")
            print("-" * 80)
            
            for version in versions:
                if version['Version'] != '$LATEST':
                    print(f"{version['Version']:<10} {version.get('Description', 'N/A'):<50} {version['LastModified']}")
            
            print(f"\n{Colors.CYAN}Available aliases:{Colors.NC}")
            for alias in aliases:
                print(f"{alias['Name']} -> Version {alias['FunctionVersion']}")
            
            target_version = input(f"\nEnter version or alias to revert to: ").strip()
        
        if not target_version:
            self.print_error("No target version specified")
            return False
        
        try:
            # Update $LATEST to point to the target version
            self.print_status(f"Reverting {lambda_name} to version/alias: {target_version}")
            
            # Get the function configuration for the target version
            response = self.lambda_client.get_function(
                FunctionName=function_name,
                Qualifier=target_version
            )
            
            # Update function code to match the target version
            code_config = response['Code']
            if 'ImageUri' in code_config:
                # Container-based function
                self.lambda_client.update_function_code(
                    FunctionName=function_name,
                    ImageUri=code_config['ImageUri']
                )
            else:
                # ZIP-based function - we need to get the code
                self.print_warning("ZIP-based revert requires re-uploading code from version")
                # For now, just update the alias to point to the target version
                try:
                    self.lambda_client.update_alias(
                        FunctionName=function_name,
                        Name='current',
                        FunctionVersion=target_version
                    )
                except self.lambda_client.exceptions.ResourceNotFoundException:
                    self.lambda_client.create_alias(
                        FunctionName=function_name,
                        Name='current',
                        FunctionVersion=target_version,
                        Description=f"Reverted to version {target_version}"
                    )
            
            self.print_success(f"Successfully reverted {lambda_name} to version {target_version}")
            
            # Record revert action
            revert_tag = f"revert-to-{target_version}"
            self.config.add_deployment_record(lambda_name, revert_tag, "revert", True)
            
            return True
            
        except Exception as e:
            self.print_error(f"Failed to revert {lambda_name}: {e}")
            return False
    
    def deploy_all(self, version_tag: str = None) -> bool:
        """Deploy all Lambda functions"""
        if version_tag is None:
            version_tag = self.generate_version_tag()
        
        self.print_status(f"Starting deployment of all Lambdas with version: {version_tag}")
        
        # ECR login for container deployments
        if not self.ecr_login():
            return False
        
        lambdas_to_deploy = list(self.config.config["lambdas"].keys())
        success_count = 0
        
        for lambda_name in lambdas_to_deploy:
            self.print_status(f"\n{'='*60}")
            self.print_status(f"Deploying {lambda_name.upper()}")
            self.print_status(f"{'='*60}")
            
            if self.deploy_lambda(lambda_name, version_tag):
                success_count += 1
            else:
                self.print_error(f"Failed to deploy {lambda_name}")
        
        self.print_status(f"\n{'='*60}")
        if success_count == len(lambdas_to_deploy):
            self.print_success(f"All {len(lambdas_to_deploy)} Lambda functions deployed successfully!")
        else:
            self.print_warning(f"Deployed {success_count}/{len(lambdas_to_deploy)} Lambda functions")
        self.print_status(f"{'='*60}")
        
        return success_count == len(lambdas_to_deploy)
    
    def show_deployment_history(self):
        """Show deployment history"""
        history = self.config.config.get("deployment_history", [])
        
        if not history:
            print("No deployment history found.")
            return
        
        print(f"\n{Colors.CYAN}Deployment History:{Colors.NC}")
        print(f"{'Timestamp':<20} {'Lambda':<15} {'Version':<20} {'Type':<10} {'Status'}")
        print("-" * 80)
        
        for record in history[-20:]:  # Show last 20 records
            status_color = Colors.GREEN if record['success'] else Colors.RED
            status_text = "SUCCESS" if record['success'] else "FAILED"
            
            print(f"{record['timestamp'][:19]:<20} "
                  f"{record['lambda_name']:<15} "
                  f"{record['version']:<20} "
                  f"{record['deployment_type']:<10} "
                  f"{status_color}{status_text}{Colors.NC}")

def main():
    parser = argparse.ArgumentParser(
        description="Bodhium Lambda Deployment Script - Deploy orchestrator and workers with auto-tagging and revert capabilities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s init-config                           # Initialize configuration file (required first step)
  %(prog)s deploy --lambda orchestrator         # Deploy single Lambda function
  %(prog)s deploy-all                           # Deploy all Lambda functions
  %(prog)s revert --lambda aio                  # Revert single Lambda to previous version
  %(prog)s revert --multiple aio aim chatgpt   # Revert multiple Lambdas
  %(prog)s history                              # Show deployment history
  %(prog)s config                               # Display current configuration

Lambda Functions:
  orchestrator  - LLM Orchestrator (code deployment)
  perplexity    - Perplexity API worker (code deployment)  
  aio           - AI Overview worker (container deployment)
  aim           - AI Mode worker (container deployment)
  chatgpt       - ChatGPT worker (container deployment)

Configuration:
  Requires deployment_config.json file. Use 'init-config' to create template.
        """
    )
    
    parser.add_argument('action', 
                       choices=['deploy', 'deploy-all', 'revert', 'history', 'config', 'init-config'], 
                       help='Action to perform')
    
    parser.add_argument('--lambda', '-l', 
                       metavar='NAME',
                       choices=['orchestrator', 'perplexity', 'aio', 'aim', 'chatgpt'],
                       help='Lambda function name (orchestrator, perplexity, aio, aim, chatgpt)')
    
    parser.add_argument('--version', '-v', 
                       metavar='TAG',
                       help='Version tag (auto-generated if not provided, format: vYYYYMMDD-HHMMSS-githash)')
    
    parser.add_argument('--target-version', '-t', 
                       metavar='VERSION',
                       help='Target version or alias for revert operation')
    
    parser.add_argument('--multiple', '-m', 
                       nargs='+', 
                       metavar='NAME',
                       choices=['orchestrator', 'perplexity', 'aio', 'aim', 'chatgpt'],
                       help='Multiple lambda names for batch operations')
    
    args = parser.parse_args()
    
    # Handle init-config action before creating deployer (which requires config file)
    if args.action == 'init-config':
        # Create config manager without loading config (for init-config only)
        config_manager = DeploymentConfig.__new__(DeploymentConfig)
        config_manager.config_file = "deployment_config.json"
        success = config_manager.create_template_config()
        sys.exit(0 if success else 1)
    
    deployer = BodhiumDeployer()
    
    if args.action == 'deploy':
        lambda_name = getattr(args, 'lambda')
        if not lambda_name:
            print("Error: --lambda is required for deploy action")
            sys.exit(1)
        
        success = deployer.deploy_lambda(lambda_name, args.version)
        sys.exit(0 if success else 1)
    
    elif args.action == 'deploy-all':
        success = deployer.deploy_all(args.version)
        sys.exit(0 if success else 1)
    
    elif args.action == 'revert':
        lambda_name = getattr(args, 'lambda')
        if args.multiple:
            # Revert multiple lambdas
            success_count = 0
            for lambda_name in args.multiple:
                if deployer.revert_lambda(lambda_name, args.target_version):
                    success_count += 1
            
            print(f"\nReverted {success_count}/{len(args.multiple)} Lambda functions")
            sys.exit(0 if success_count == len(args.multiple) else 1)
        
        elif lambda_name:
            # Revert single lambda
            success = deployer.revert_lambda(lambda_name, args.target_version)
            sys.exit(0 if success else 1)
        
        else:
            print("Error: --lambda or --multiple is required for revert action")
            sys.exit(1)
    
    elif args.action == 'history':
        deployer.show_deployment_history()
    
    elif args.action == 'config':
        print(f"\n{Colors.CYAN}Current Configuration:{Colors.NC}")
        print(json.dumps(deployer.config.config, indent=2, default=str))

if __name__ == "__main__":
    main()
