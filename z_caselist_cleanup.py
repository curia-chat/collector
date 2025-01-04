import os
from datetime import datetime

# Function to clean up old files in the 'caselist_csv' folder
def cleanup_old_files(directory, max_files=60):
    # Get a list of all files in the directory
    files = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    
    # If the number of files is less than or equal to the max_files limit, no cleanup is necessary
    if len(files) <= max_files:
        print(f"No cleanup needed. There are only {len(files)} files.")
        return

    # Sort files by their modification time (oldest first)
    files.sort(key=lambda x: os.path.getmtime(x))

    # Calculate how many files need to be deleted
    files_to_delete = len(files) - max_files

    # Delete the oldest files
    for i in range(files_to_delete):
        print(f"Deleting file: {files[i]}")
        os.remove(files[i])

    print(f"{files_to_delete} old files have been deleted. Now there are {len(files) - files_to_delete} files remaining.")

# Example usage
directory = "caselist_csv"  # Directory to clean up
cleanup_old_files(directory, max_files=60)
