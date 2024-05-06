# External packages
import re


# Extract object PID from MODS filename
def get_pid(file=str):
    pid = file.replace("pitt_", "").replace("_MODS", "").replace(".xml", "")
    return pid


# Remove newline characters, trailing whitespaces, and multiple spaces from text
def remove_whitespaces(text):
    if isinstance(text, str):
        new_text = text.replace('\n    ', ' ').replace('\n', '').strip()
        new_text = re.sub(r'\s+', ' ', new_text)
        return new_text.strip()
    return ''


