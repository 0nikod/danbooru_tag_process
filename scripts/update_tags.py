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

from huggingface_hub import hf_hub_download, list_repo_commits, list_repo_files

REPO_ID = "deepghs/site_tags"
# ...

def get_latest_commit_sha(repo_id):
    # Fix: Specify repo_type="dataset"
    commits = list_repo_commits(repo_id=repo_id, repo_type="dataset")
    if commits:
        return commits[0].commit_id
    return None

def download_data(repo_id, filename):
    # Default to danbooru if just filename provided, or accept full path
    # But for this script, let's just specific the remote filename vs local filename
    print(f"Downloading {filename} from {repo_id}...")
    try:
        # We want to download 'danbooru.donmai.us/tags.parquet' but save as 'tags.parquet'
        # The hf_hub_download 'filename' argument is the REMOTE path.
        # We can rename it after download or rely on uv/cache, but hf_hub_download downloads to cache and returns path.
        # Wait, if we use local_dir=".", it maintains structure. 
        # So 'danbooru.donmai.us/tags.parquet' will be saved to './danbooru.donmai.us/tags.parquet'.
        # We need to move it or change logic.
        
        file_path = hf_hub_download(repo_id=repo_id, filename="danbooru.donmai.us/tags.parquet", repo_type="dataset", local_dir=".")
        
        # Move to root tags.parquet if needed, or just return path
        import shutil
        target = "tags.parquet"
        shutil.move(file_path, target)
        return target
    except Exception as e:
        print(f"Failed to download {filename}. Error: {e}")
        print("Listing available files in repo to help debug...")
        try:
            files = list_repo_files(repo_id=repo_id, repo_type="dataset")
            print(f"Files in {repo_id}: {files}")
        except Exception as list_e:
            print(f"Could not list files: {list_e}")
        raise e

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
