import sqlite3
import os
import json
import shutil
from datetime import datetime, timedelta

DB_PATH = '/data/site.db' if os.path.exists('/data') else 'site.db'
BACKUP_DIR = '/data/backups' if os.path.exists('/data') else 'data/backups'


def get_db():
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else '.', exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            price TEXT DEFAULT '',
            price_numeric REAL DEFAULT 0,
            url TEXT DEFAULT '',
            image TEXT DEFAULT '',
            category TEXT DEFAULT '',
            seller TEXT DEFAULT '',
            rating REAL DEFAULT 0,
            batch TEXT DEFAULT '',
            retail_price TEXT DEFAULT '',
            review_count INTEGER DEFAULT 0,
            tags TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS categories (
            slug TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            icon TEXT DEFAULT '',
            description TEXT DEFAULT '',
            sort_order INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS clicks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            product_name TEXT DEFAULT '',
            category TEXT DEFAULT '',
            element_type TEXT DEFAULT '',
            page TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            user_ip TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            country TEXT DEFAULT '',
            clicked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS daily_stats (
            date TEXT PRIMARY KEY,
            total_clicks INTEGER DEFAULT 0,
            unique_visitors INTEGER DEFAULT 0,
            top_product TEXT DEFAULT '',
            top_category TEXT DEFAULT '',
            page_views INTEGER DEFAULT 0,
            signup_clicks INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_clicks_product ON clicks(product_id);
        CREATE INDEX IF NOT EXISTS idx_clicks_date ON clicks(clicked_at);
        CREATE INDEX IF NOT EXISTS idx_clicks_category ON clicks(category);
        CREATE INDEX IF NOT EXISTS idx_clicks_page ON clicks(page);
        CREATE INDEX IF NOT EXISTS idx_clicks_element ON clicks(element_type);
    ''')
    # Add tags column if not exists (migration for existing DBs)
    try:
        conn.execute('ALTER TABLE products ADD COLUMN tags TEXT DEFAULT ""')
    except Exception:
        pass
    conn.close()


def get_products(category=None):
    conn = get_db()
    if category:
        rows = conn.execute(
            'SELECT * FROM products WHERE category = ? ORDER BY created_at DESC',
            (category,)
        ).fetchall()
    else:
        rows = conn.execute('SELECT * FROM products ORDER BY created_at DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_product(product_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM products WHERE id = ?', (product_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def add_product(product):
    conn = get_db()
    conn.execute('''
        INSERT OR REPLACE INTO products (id, name, price, price_numeric, url, image, category, seller, rating, batch, retail_price, review_count, tags, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ''', (
        product.get('id', ''),
        product.get('name', ''),
        product.get('price', ''),
        float(product.get('price_numeric', 0) or 0),
        product.get('url', ''),
        product.get('image', ''),
        product.get('category', ''),
        product.get('seller', ''),
        float(product.get('rating', 0) or 0),
        product.get('batch', ''),
        product.get('retail_price', ''),
        int(product.get('review_count', 0) or 0),
        product.get('tags', ''),
    ))
    conn.commit()
    conn.close()


def add_products_bulk(products):
    conn = get_db()
    for p in products:
        conn.execute('''
            INSERT OR REPLACE INTO products (id, name, price, price_numeric, url, image, category, seller, rating, batch, retail_price, review_count, tags, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            p.get('id', ''),
            p.get('name', ''),
            p.get('price', ''),
            float(p.get('price_numeric', 0) or 0),
            p.get('url', ''),
            p.get('image', ''),
            p.get('category', ''),
            p.get('seller', ''),
            float(p.get('rating', 0) or 0),
            p.get('batch', ''),
            p.get('retail_price', ''),
            int(p.get('review_count', 0) or 0),
            p.get('tags', ''),
        ))
    conn.commit()
    conn.close()


def update_product(product_id, updates):
    conn = get_db()
    allowed = ['name', 'price', 'price_numeric', 'url', 'image', 'category', 'seller', 'rating', 'batch', 'retail_price', 'tags']
    sets = []
    vals = []
    for key in allowed:
        if key in updates:
            sets.append(f'{key} = ?')
            vals.append(updates[key])
    if not sets:
        conn.close()
        return
    sets.append('updated_at = CURRENT_TIMESTAMP')
    vals.append(product_id)
    conn.execute(f'UPDATE products SET {", ".join(sets)} WHERE id = ?', vals)
    conn.commit()
    conn.close()


def delete_product(product_id):
    conn = get_db()
    conn.execute('DELETE FROM products WHERE id = ?', (product_id,))
    conn.commit()
    conn.close()


def search_products(query):
    conn = get_db()
    q = f'%{query}%'
    rows = conn.execute('''
        SELECT *,
            CASE
                WHEN name LIKE ? THEN 3
                WHEN seller LIKE ? THEN 2
                WHEN tags LIKE ? THEN 1
                ELSE 0
            END as relevance
        FROM products
        WHERE name LIKE ? OR seller LIKE ? OR tags LIKE ? OR category LIKE ?
        ORDER BY relevance DESC, created_at DESC
    ''', (q, q, q, q, q, q, q)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_categories():
    conn = get_db()
    rows = conn.execute('SELECT * FROM categories ORDER BY sort_order').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_category(slug, name, icon='', description='', sort_order=0):
    conn = get_db()
    conn.execute('''
        INSERT OR REPLACE INTO categories (slug, name, icon, description, sort_order)
        VALUES (?, ?, ?, ?, ?)
    ''', (slug, name, icon, description, sort_order))
    conn.commit()
    conn.close()


def record_click(data):
    conn = get_db()
    conn.execute('''
        INSERT INTO clicks (product_id, product_name, category, element_type, page, referrer, user_ip, user_agent, country)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data.get('product_id', ''),
        data.get('product_name', ''),
        data.get('category', ''),
        data.get('element_type', 'click'),
        data.get('page', ''),
        data.get('referrer', ''),
        data.get('user_ip', ''),
        data.get('user_agent', ''),
        data.get('country', ''),
    ))
    conn.commit()
    conn.close()


def get_analytics(days=30):
    conn = get_db()
    since = (datetime.now() - timedelta(days=days)).isoformat()

    total = conn.execute(
        'SELECT COUNT(*) as c FROM clicks WHERE clicked_at >= ?', (since,)
    ).fetchone()['c']

    unique = conn.execute(
        'SELECT COUNT(DISTINCT user_ip) as c FROM clicks WHERE clicked_at >= ?', (since,)
    ).fetchone()['c']

    top_products = conn.execute('''
        SELECT product_name, COUNT(*) as clicks FROM clicks
        WHERE clicked_at >= ? AND product_name != ''
        GROUP BY product_name ORDER BY clicks DESC LIMIT 10
    ''', (since,)).fetchall()

    top_categories = conn.execute('''
        SELECT category, COUNT(*) as clicks FROM clicks
        WHERE clicked_at >= ? AND category != ''
        GROUP BY category ORDER BY clicks DESC LIMIT 10
    ''', (since,)).fetchall()

    top_pages = conn.execute('''
        SELECT page, COUNT(*) as views FROM clicks
        WHERE clicked_at >= ? AND page != ''
        GROUP BY page ORDER BY views DESC LIMIT 10
    ''', (since,)).fetchall()

    element_types = conn.execute('''
        SELECT element_type, COUNT(*) as clicks FROM clicks
        WHERE clicked_at >= ?
        GROUP BY element_type ORDER BY clicks DESC
    ''', (since,)).fetchall()

    daily = conn.execute('''
        SELECT DATE(clicked_at) as day, COUNT(*) as clicks, COUNT(DISTINCT user_ip) as visitors
        FROM clicks WHERE clicked_at >= ?
        GROUP BY DATE(clicked_at) ORDER BY day
    ''', (since,)).fetchall()

    signup_clicks = conn.execute(
        "SELECT COUNT(*) as c FROM clicks WHERE clicked_at >= ? AND element_type = 'signup'", (since,)
    ).fetchone()['c']

    conn.close()
    return {
        'total_clicks': total,
        'unique_visitors': unique,
        'signup_clicks': signup_clicks,
        'top_products': [dict(r) for r in top_products],
        'top_categories': [dict(r) for r in top_categories],
        'top_pages': [dict(r) for r in top_pages],
        'element_types': [dict(r) for r in element_types],
        'daily': [dict(r) for r in daily],
    }


def backup_database():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    backup_path = os.path.join(BACKUP_DIR, f'backup_{timestamp}.db')
    shutil.copy2(DB_PATH, backup_path)

    # Clean backups older than 30 days
    cutoff = datetime.now() - timedelta(days=30)
    for f in os.listdir(BACKUP_DIR):
        fpath = os.path.join(BACKUP_DIR, f)
        if os.path.isfile(fpath):
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
            if mtime < cutoff:
                os.remove(fpath)

    return backup_path


def check_auto_backup():
    """Run backup if last one was > 24hrs ago."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    backups = sorted([f for f in os.listdir(BACKUP_DIR) if f.startswith('backup_')])
    if not backups:
        return backup_database()

    latest = os.path.join(BACKUP_DIR, backups[-1])
    mtime = datetime.fromtimestamp(os.path.getmtime(latest))
    if datetime.now() - mtime > timedelta(hours=24):
        return backup_database()
    return None
