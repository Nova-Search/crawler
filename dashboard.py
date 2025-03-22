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
import time

app = Flask(__name__)

# Queue to store crawl tasks
crawl_queue = Queue()

# Store crawl status
crawl_status = {}

canceled_tasks = set()

# Database path
DB_PATH = "../links.db"

# Add at the top with other globals
log_buffer = deque(maxlen=1000)  # Store last 1000 log entries
log_lock = threading.Lock()

# Modify create_tables() to add task_type column
def create_tables():
    """Create necessary tables for the dashboard"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS crawl_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NULL,
        depth INTEGER NULL,
        same_domain BOOLEAN NULL,
        stealth_mode BOOLEAN NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        completed_at TIMESTAMP,
        task_type TEXT DEFAULT 'crawl'
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
    """Background thread to process the crawl queue."""
    while True:
        task = crawl_queue.get()
        task_id = task['id']
        
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Check if the task has been canceled
            c.execute('SELECT status FROM crawl_tasks WHERE id = ?', (task_id,))
            status = c.fetchone()
            if status and status[0] == 'canceled':
                capture_log(f"Task {task_id} has been canceled.")
                conn.close()
                crawl_queue.task_done()
                continue
            
            c.execute('''UPDATE crawl_tasks 
                         SET status = 'running' 
                         WHERE id = ?''', (task_id,))
            conn.commit()
            
            if task.get('task_type') == 'stale_update':
                # Run stale update
                resultupdater_path = Path(__file__).parent / 'resultupdater.py'
                process = subprocess.Popen(['python3', str(resultupdater_path)],
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT,
                                           text=True)
                
                for line in process.stdout:
                    if task_id in canceled_tasks:
                        capture_log(f"Task {task_id} has been canceled mid-execution.")
                        process.terminate()
                        break
                    capture_log(f"Stale Update: {line.strip()}")
                
                process.wait()
            else:
                # Regular crawl task
                progress_capture = ProgressCapture()
                sys.stdout = progress_capture
                
                from web import crawl
                session = requests.Session()
                saved_urls = set()
                
                # Pass a callback to check for cancellation
                def is_canceled():
                    return task_id in canceled_tasks
                
                crawl(task['url'], task['depth'], session, task['stealth_mode'], 
                      saved_urls=saved_urls, same_domain=task['same_domain'], 
                      is_canceled=is_canceled)
                
                sys.stdout = progress_capture._original_stdout
            
            if task_id in canceled_tasks:
                capture_log(f"Task {task_id} was canceled mid-execution.")
                c.execute('''UPDATE crawl_tasks 
                             SET status = 'canceled',
                             completed_at = ? 
                             WHERE id = ?''', (datetime.now().isoformat(), task_id))
            else:
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
            # Remove the task ID from the canceled_tasks set
            canceled_tasks.discard(task_id)

def reset_running_tasks():
    """Set the state of any 'running' tasks to 'failed'"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''UPDATE crawl_tasks 
                 SET status = 'failed'
                 WHERE status IN ('running', 'pending')''')
    
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

@app.route('/cancel_task/<int:task_id>', methods=['POST'])
def cancel_task(task_id):
    """Cancel a task by marking its status as 'canceled'."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''UPDATE crawl_tasks 
                 SET status = 'canceled' 
                 WHERE id = ? AND status IN ('pending', 'running')''', (task_id,))
    conn.commit()
    conn.close()
    
    # Add the task ID to the canceled_tasks set
    canceled_tasks.add(task_id)
    
    return jsonify({'success': True, 'task_id': task_id})

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

# Modify periodic_stale_update()
def periodic_stale_update():
    """Create stale update tasks every hour"""
    while True:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''INSERT INTO crawl_tasks 
                     (task_type, status, created_at, url)
                     VALUES ('stale_update', 'pending', ?, NULL)''',
                  (datetime.now().isoformat(),))
        
        task_id = c.lastrowid
        conn.commit()
        conn.close()
        
        # Add task to queue
        crawl_queue.put({
            'id': task_id,
            'task_type': 'stale_update'
        })
        
        time.sleep(1800)

def migrate_database():
    """Recreate table with nullable columns"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Backup existing data
    c.execute("SELECT * FROM crawl_tasks")
    existing_data = c.fetchall()
    
    # Drop and recreate table
    c.execute("DROP TABLE IF EXISTS crawl_tasks")
    
    # Create new table
    c.execute('''CREATE TABLE crawl_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NULL,
        depth INTEGER NULL,
        same_domain BOOLEAN NULL,
        stealth_mode BOOLEAN NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        completed_at TIMESTAMP,
        task_type TEXT DEFAULT 'crawl'
    )''')
    
    # Restore data if any
    if existing_data:
        c.executemany('''INSERT INTO crawl_tasks VALUES (?,?,?,?,?,?,?,?,?)''', existing_data)
    
    conn.commit()
    conn.close()

# Modify the if __name__ == '__main__': block to:
if __name__ == '__main__':
    create_tables()
    migrate_database()  # Add this line
    reset_running_tasks()
    
    # Start background crawler thread
    crawler_thread = threading.Thread(target=background_crawler, daemon=True)
    crawler_thread.start()
    
    # Start periodic stale update thread
    stale_thread = threading.Thread(target=periodic_stale_update, daemon=True)
    stale_thread.start()
    
    app.run(debug=False, port=5001)