from flask import Flask, render_template, request, jsonify
from queue import Queue
import threading
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import os

app = Flask(__name__)

# Queue to store crawl tasks
crawl_queue = Queue()

# Store crawl status
crawl_status = {}

# Database path
DB_PATH = "../links.db"

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

def background_crawler():
    """Background thread to process the crawl queue"""
    while True:
        task = crawl_queue.get()
        task_id = task['id']
        
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Update status to running
            c.execute('''UPDATE crawl_tasks 
                        SET status = 'running' 
                        WHERE id = ?''', (task_id,))
            conn.commit()
            
            # Call the existing crawler with the task parameters
            from web import crawl
            session = requests.Session()
            saved_urls = set()
            
            crawl(task['url'], task['depth'], session, task['stealth_mode'], 
                 saved_urls=saved_urls, same_domain=task['same_domain'])
            
            # Update status to completed
            c.execute('''UPDATE crawl_tasks 
                        SET status = 'completed',
                        completed_at = ? 
                        WHERE id = ?''', (datetime.now().isoformat(), task_id))
            conn.commit()
            
        except Exception as e:
            # Update status to failed
            c.execute('''UPDATE crawl_tasks 
                        SET status = ?,
                        completed_at = ? 
                        WHERE id = ?''', (f'failed: {str(e)}', datetime.now().isoformat(), task_id))
            conn.commit()
        finally:
            conn.close()
            crawl_queue.task_done()

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

if __name__ == '__main__':
    create_tables()
    # Start background crawler thread
    crawler_thread = threading.Thread(target=background_crawler, daemon=True)
    crawler_thread.start()
    app.run(debug=False)