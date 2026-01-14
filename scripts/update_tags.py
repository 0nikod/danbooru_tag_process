import argparse
import os
import subprocess
import sys
from huggingface_hub import hf_hub_download, list_repo_commits

REPO_ID = "deepghs/site_tags"
# Using absolute paths since this script runs from root or scripts/
# We assume CWD is project root when running via 'uv run scripts/update_tags.py'
TAGS_FILE = "tags.parquet"
ALIAS_FILE = "tag_alias.csv"

def get_latest_commit_sha(repo_id):
    commits = list_repo_commits(repo_id=repo_id)
    if commits:
        return commits[0].commit_id
    return None

def download_data(repo_id, filename):
    print(f"Downloading {filename} from {repo_id}...")
    return hf_hub_download(repo_id=repo_id, filename=filename, repo_type="dataset", local_dir=".")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-name", help="Current GitHub Repo Name (unused, but kept for compatibility)", default=None)
    args = parser.parse_args()

    # 1. Check HF Commit (Optional but useful for output)
    try:
        latest_sha = get_latest_commit_sha(REPO_ID)
        print(f"Latest HF Commit: {latest_sha}")
    except Exception as e:
        print(f"Warning: Could not fetch HF commit: {e}")
        latest_sha = "unknown"

    # 2. Download Files
    try:
        download_data(REPO_ID, "tags.parquet")
        # tag_alias.csv is local, so we don't download it.
    except Exception as e:
        print(f"Error downloading data: {e}")
        sys.exit(1)

    # 3. Run Processing
    print("Running main processing script...")
    # Assuming main.py is in the current directory (project root)
    if os.path.exists("main.py"):
        subprocess.check_call([sys.executable, "main.py"])
    else:
        print("Error: main.py not found in current directory.")
        sys.exit(1)

    # 4. Set GitHub Outputs
    # Logic: If successful, we assume we should release.
    # We can check if output file changed, but for now let's just assume yes.
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"should_release=true\n")
            f.write(f"hf_sha={latest_sha}\n")
            f.write(f"release_tag={latest_sha[:7]}\n")
        print("GitHub outputs set.")
    else:
        print("Not running in GitHub Actions, skipping output setting.")

if __name__ == "__main__":
    main()
