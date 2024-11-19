from flask import Flask, render_template, request, jsonify
from queue import Queue
import threading
import requests
from datetime import datetime
import sqlite3
from collections import deque
import threading
from tqdm.auto import tqdm
import sys
from io import StringIO
import subprocess
from pathlib import Path

app = Flask(__name__)

# Queue to store crawl tasks
crawl_queue = Queue()

# Store crawl status
crawl_status = {}

# Database path
DB_PATH = "../links.db"

# Add at the top with other globals
log_buffer = deque(maxlen=1000)  # Store last 1000 log entries
log_lock = threading.Lock()

def create_tables():
    """Create necessary tables for the dashboard"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Create tasks table
    c.execute('''CREATE TABLE IF NOT EXISTS crawl_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL,
        depth INTEGER,
        same_domain BOOLEAN,
        stealth_mode BOOLEAN,
        status TEXT,
        created_at TIMESTAMP,
        completed_at TIMESTAMP
    )''')
    
    conn.commit()
    conn.close()

# Add log capture function
def capture_log(message):
    with log_lock:
        log_buffer.append(f"{datetime.now().strftime('%H:%M:%S')} {message}")

# Add this class for progress capture
class ProgressCapture:
    def __init__(self):
        self.output = StringIO()
        self._original_stdout = sys.stdout
        self.pbar = None

    def write(self, message):
        capture_log(message.strip())
        self._original_stdout.write(message)

    def flush(self):
        self._original_stdout.flush()

# Modify background_crawler function
def background_crawler():
    """Background thread to process the crawl queue"""
    while True:
        task = crawl_queue.get()
        task_id = task['id']
        
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            c.execute('''UPDATE crawl_tasks 
                        SET status = 'running' 
                        WHERE id = ?''', (task_id,))
            conn.commit()
            
            # Setup progress capture
            progress_capture = ProgressCapture()
            sys.stdout = progress_capture
            
            # Import and run crawler
            from web import crawl
            session = requests.Session()
            saved_urls = set()
            
            crawl(task['url'], task['depth'], session, task['stealth_mode'], 
                 saved_urls=saved_urls, same_domain=task['same_domain'])
            
            # Restore stdout
            sys.stdout = progress_capture._original_stdout
            
            c.execute('''UPDATE crawl_tasks 
                        SET status = 'completed',
                        completed_at = ? 
                        WHERE id = ?''', (datetime.now().isoformat(), task_id))
            conn.commit()
            
        except Exception as e:
            capture_log(f"Error: {str(e)}")
            if 'progress_capture' in locals():
                sys.stdout = progress_capture._original_stdout
            c.execute('''UPDATE crawl_tasks 
                        SET status = ?,
                        completed_at = ? 
                        WHERE id = ?''', (f'failed: {str(e)}', datetime.now().isoformat(), task_id))
            conn.commit()
        finally:
            conn.close()
            crawl_queue.task_done()

def reset_running_tasks():
    """Set the state of any 'running' tasks to 'failed'"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''UPDATE crawl_tasks 
                 SET status = 'failed'
                 WHERE status = 'running' ''')
    
    conn.commit()
    conn.close()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/add_task', methods=['POST'])
def add_task():
    task = request.json
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''INSERT INTO crawl_tasks 
                 (url, depth, same_domain, stealth_mode, status, created_at)
                 VALUES (?, ?, ?, ?, 'pending', ?)''',
              (task['url'], task['depth'], task['same_domain'], 
               task['stealth_mode'], datetime.now().isoformat()))
    
    task_id = c.lastrowid
    conn.commit()
    conn.close()
    
    # Add task to queue
    task['id'] = task_id
    crawl_queue.put(task)
    
    return jsonify({'success': True})

@app.route('/tasks')
def get_tasks():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    c.execute('''SELECT * FROM crawl_tasks 
                 ORDER BY created_at DESC 
                 LIMIT 15''')
    
    tasks = [dict(row) for row in c.fetchall()]
    conn.close()
    
    return jsonify(tasks)

# Add new route for logs
@app.route('/logs')
def get_logs():
    with log_lock:
        return jsonify(list(log_buffer))
    
@app.route('/update_stale', methods=['POST'])
def update_stale():
    """Run resultupdater.py directly and capture output"""
    try:
        resultupdater_path = Path(__file__).parent / 'resultupdater.py'
        process = subprocess.Popen(['python3', str(resultupdater_path)],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 text=True)
        
        for line in process.stdout:
            with log_lock:
                log_buffer.append(line)
        
        process.wait()
        return jsonify({'success': True})
    except Exception as e:
        with log_lock:
            log_buffer.append(f"Error running resultupdater: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    create_tables()
    reset_running_tasks()  # Reset running tasks to failed
    # Start background crawler thread
    crawler_thread = threading.Thread(target=background_crawler, daemon=True)
    crawler_thread.start()
    app.run(debug=False)