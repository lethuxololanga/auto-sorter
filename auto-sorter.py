import os
import shutil
import json
import logging
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# =========================
# CONFIG PATHS & FOLDERS
# =========================
DOWNLOADS_FOLDER = os.path.expanduser("~/Downloads")
DESTINATION_FOLDERS = {
    "Documents": os.path.expanduser("~/Documents"),
    "Pictures": os.path.expanduser("~/Pictures"),
    "Videos": os.path.expanduser("~/Videos"),
    "Music": os.path.expanduser("~/Music")
}

# Ensure destination folders exist
for folder in DESTINATION_FOLDERS.values():
    os.makedirs(folder, exist_ok=True)

# Extensions config file
CONFIG_PATH = os.path.expanduser("~/.config/download_sorter.json")
DEFAULT_CONFIG = {
    "Documents": ["pdf", "docx", "txt", "xlsx", "pptx"],
    "Pictures": ["jpg", "jpeg", "png", "gif"],
    "Videos": ["mp4", "mkv", "avi", "mov"],
    "Music": ["mp3", "wav", "flac"]
}

# Create default config if missing
if not os.path.exists(CONFIG_PATH):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(DEFAULT_CONFIG, f, indent=4)

with open(CONFIG_PATH) as f:
    EXTENSIONS = json.load(f)

# =========================
# LOGGING SETUP
# =========================
LOG_DIR = os.path.expanduser("~/.Script_Logs")
LOG_FILE = os.path.join(LOG_DIR, "download_sorter.log")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def log_message(message, level="info"):
    """Logs and prints messages."""
    print(message)
    getattr(logging, level)(message)

# =========================
# HELPER FUNCTIONS
# =========================
def is_file_complete(file_path, wait_time=1):
    """Check if file size has stopped changing."""
    try:
        initial_size = os.path.getsize(file_path)
        time.sleep(wait_time)
        return os.path.getsize(file_path) == initial_size
    except FileNotFoundError:
        return False

def safe_move(src, dest_folder):
    """Move file without overwriting existing ones."""
    base, ext = os.path.splitext(os.path.basename(src))
    counter = 1
    dest_path = os.path.join(dest_folder, base + ext)

    while os.path.exists(dest_path):
        dest_path = os.path.join(dest_folder, f"{base}({counter}){ext}")
        counter += 1

    shutil.move(src, dest_path)
    return dest_path

# =========================
# MAIN SORTER
# =========================
class DownloadSorter(FileSystemEventHandler):
    def on_created(self, event):
        """Triggered when a new file appears in Downloads."""
        if event.is_directory:
            return
        self.sort_file(event.src_path)

    def sort_file(self, file_path):
        """Sort file into correct folder."""
        if not os.path.isfile(file_path):
            return

        filename = os.path.basename(file_path)
        file_ext = filename.split(".")[-1].lower()

        # Skip incomplete files
        if filename.endswith((".crdownload", ".part", ".tmp")):
            log_message(f"⏳ Skipping incomplete file: {filename}", "warning")
            return

        # Wait until file finishes downloading
        if not is_file_complete(file_path, wait_time=2):
            log_message(f"⏳ Waiting for {filename} to finish downloading", "warning")
            return

        # Determine destination
        destination = None
        for category, extensions in EXTENSIONS.items():
            if file_ext in extensions:
                destination = DESTINATION_FOLDERS.get(category)
                break

        if not destination:
            log_message(f"⚠️ Unknown file type: {filename}", "warning")
            return

        # Move file
        try:
            start_time = time.time()
            final_path = safe_move(file_path, destination)
            duration = round(time.time() - start_time, 2)
            file_size = round(os.path.getsize(final_path) / 1024, 2)
            log_message(f"✅ Moved: {filename} ({file_size} KB) → {destination} in {duration}s")
        except PermissionError:
            log_message(f"❌ Permission denied: {filename}", "error")
        except Exception as e:
            log_message(f"❌ Error moving {filename}: {e}", "error")

# =========================
# RUN
# =========================
if __name__ == "__main__":
    log_message("🚀 Monitoring Downloads folder... Press Ctrl+C to stop.")
    event_handler = DownloadSorter()
    observer = Observer()
    observer.schedule(event_handler, DOWNLOADS_FOLDER, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        log_message("🛑 Stopped monitoring.")

    observer.join()

