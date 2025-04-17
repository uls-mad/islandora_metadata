#!/bin/python3 

""" Modules """

# Import standard modules
import os
import sys
import argparse
import traceback
import zipfile
import shutil
try:
    import tkinter as tk
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False

# Import local modules
from file_utils import get_directory


""" Functions """

def parse_arguments():
    """
    Parse command-line arguments to retrieve the batch directory.

    Returns:
        tuple: batch_path (str | None)
    """
    parser = argparse.ArgumentParser(
        description="Process a batch directory by removing media files, zipping the remaining contents, and deleting the original directory."
    )
    parser.add_argument(
        "--batch_path",
        type=str,
        default=None,
        help="Path to a batch directory (default: will prompt if not provided)."
    )
    args = parser.parse_args()
    return args.batch_path


def prompt_to_delete(media_path):
    """
    Prompts the user to confirm whether they are sure they want to delete all media files in the directory,
    after notifying them of the files that will be deleted. If confirmed, deletes the files except those
    with extensions .csv or .txt, or deletes the whole directory if it's named "media".

    Args:
        media_path (str): The path to the media directory where the files are located.
    
    Returns:
        None
    """
    # Get the count of each file type in the media directory
    print("Analyzing media files...")
    file_counts = {}
    for root, dirs, files in os.walk(media_path):
        for file in files:
            file_ext = os.path.splitext(file)[1].lower().lstrip('.')
            if file_ext in ['csv', 'txt']:  # Skip .csv and .txt files
                continue
            file_counts[file_ext] = file_counts.get(file_ext, 0) + 1
    
    # Print the list of files to be deleted
    media_dir = os.path.basename(media_path.rstrip(os.sep))
    print(f"\nThe following files will be deleted in the {media_dir} directory:")
    for ext, count in file_counts.items():
        print(f"{ext.upper()}: {count}")
    
    # Prompt for confirmation to proceed
    response = input("\nDo you want to proceed? (y/n): ").strip().lower()
    if response == 'y':
        # Remove the media files or the media directory
        print("\nDeleting media files...")
        if media_dir == "media":
            # Delete the directory
            shutil.rmtree(media_path) 
            print(f"Deleted {media_dir} directory.")
        else:
            # Delete files in import directory, except .csv and .txt files
            for root, dirs, files in os.walk(media_path, topdown=False):
                for file in files:
                    file_ext = os.path.splitext(file)[1].lower().lstrip('.')
                    if file_ext not in ['csv', 'txt']: 
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                        # print(f"Deleted file: {file_path}")
            print(f"Deleted media files in the {media_dir} directory.")
    else:
        print("\nBatch cleanup will not be performed. Exiting.")
        sys.exit()


def zip_batch_directory(batch_path):
    """
    Zips the remaining contents of the batch directory (excluding the media directory).
    If the zip file already exists, it skips the zipping step.

    Args:
        batch_path (str): The path to the batch directory to be zipped.
    
    Returns:
        str: The filename of the created or existing zip file.
    """
    # Define the zip filename
    zip_filename = f"{batch_path}.zip"
    
    # Check if the zip file already exists
    if os.path.exists(zip_filename):
        print(f"\nZIP file already exists: {zip_filename}. Skipping zipping step.")
        return zip_filename
    
    # If the zip file doesn't exist, create it
    print("\nZipping batch directory...")
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(batch_path):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, batch_path))
    
    print(f"Created ZIP file: {zip_filename}")
    return zip_filename


def move_zip_to_done(batch_path: str, zip_filename: str):
    """
    Moves the created zip file to the 'Done' directory one level up from the batch directory if it exists.
    If the zip file already exists in the destination, it skips the move or renames the file.

    Args:
        batch_path (str): The path to the batch directory.
        zip_filename (str): The path to the zip file to be moved.
    
    Returns:
        None
    """
    # Set the done_dir to be one level up from batch_path and append 'Done'
    done_dir = os.path.join(os.path.dirname(batch_path), 'Done')
    
    # Check if the done_dir exists
    if not os.path.exists(done_dir):
        print(f"\n{done_dir} does not exist. Skipping move.")
        return

    # Define the destination path
    destination_path = os.path.join(done_dir, os.path.basename(zip_filename))

    # Check if the destination file already exists
    if os.path.exists(destination_path):
        print(f"\nDestination file {destination_path} already exists.")
        return
    
    # Move the zip file to the 'Done' directory
    shutil.move(zip_filename, destination_path)
    print(f"\nMoved ZIP file to {destination_path}.")


def delete_directory_contents(batch_path):
    """
    Recursively deletes all files and subdirectories in a directory.
    
    Args:
        batch_path (str): The path to the directory to be cleaned.
    
    Returns:
        None
    """
    # print(f"\nDeleting contents of batch directory: {batch_path}")
    for root, dirs, files in os.walk(batch_path, topdown=False):
        for file in files:
            try:
                file_path = os.path.join(root, file)
                os.remove(file_path)
            except Exception as e:
                print(f"Failed to delete file {file_path}: {e}")
        
        for dir in dirs:
            try:
                dir_path = os.path.join(root, dir)
                os.rmdir(dir_path)
                print(f"Deleted directory: {dir_path}")
            except Exception as e:
                print(f"Failed to remove directory {dir_path}: {e}")


def delete_batch_directory(batch_path):
    """
    Deletes the contents of a batch directory, then removes the batch directory itself.

    Args:
        batch_path (str): The path to the batch directory to be deleted.

    Returns:
        None
    """
    delete_directory_contents(batch_path)

    try:
        os.rmdir(batch_path)  
        print(f"Deleted batch directory: {batch_path}")
        return True
    except OSError as e:
        print(f"Error deleting batch directory {batch_path}: {e}")
        return False


def main():
    """
    Main function to handle the batch cleanup process. Prompts the user to select a batch directory, 
    removes media files, zips the remaining directory contents, moves the zip file to the Done directory,
    and deletes the original batch directory.

    Returns:
        None
    """
    batch_path = parse_arguments()
    
    try:
        if TK_AVAILABLE:
            # Set up tkinter window for GUI
            root = tk.Tk()
            root.withdraw()
            input_prompt = 'Select Batch directory for Cleanup'
        else:
            input_prompt = 'Enter Batch directory for Cleanup'

        # Get batch directory path
        if batch_path is None:
            batch_path = get_directory('input', input_prompt, TK_AVAILABLE)
        print(f"\nProcessing batch directory: {batch_path}\n")
        
        # Define media directory path
        media_dir = os.path.join(batch_path, "import", "media")
        import_dir = os.path.join(batch_path, "import")

        # Check if media directory exists, otherwise check for the import directory
        if os.path.exists(media_dir):
            prompt_to_delete(media_dir)
        elif os.path.exists(import_dir):
            prompt_to_delete(import_dir)
        else:
            print("No import or media directory found. Skipping media file deletion step.")
        
        # ZIP the remaining contents of the batch directory
        zip_filename = zip_batch_directory(batch_path)
        
        # Move the ZIP file to Done directory if it exists
        move_zip_to_done(batch_path, zip_filename)
        
        # Delete the batch directory
        deleted = delete_batch_directory(batch_path)

        if deleted:
            # Report completion
            print("\nBatch cleanup complete.")
        else:
            print("\nBatch cleanup did not complete.")      
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        sys.exit(1)

    finally:
        if TK_AVAILABLE:
            # Close GUI window
            root.destroy()

        sys.exit(0)


if __name__ == "__main__":
    main()
