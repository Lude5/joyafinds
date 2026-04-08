import os
import json
import secrets
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, session, url_for, send_file
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))
CORS(app)

ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'changeme123')
JOYABUY_AFFILIATE = os.environ.get('JOYABUY_AFFILIATE_CODE', '')
DATA_DIR = '/data' if os.path.exists('/data') else 'data'

try:
    from database import (
        init_db, get_products, get_product, add_product, add_products_bulk,
        update_product, delete_product, search_products, get_categories, add_category,
        record_click, get_analytics, backup_database, check_auto_backup
    )
    init_db()
    check_auto_backup()

    if not get_products():
        products_file = os.path.join(os.path.dirname(__file__), 'static', 'products.json')
        if os.path.exists(products_file):
            with open(products_file, 'r', encoding='utf-8') as _f:
                _products = json.load(_f)
            add_products_bulk(_products)
            print(f"Loaded {len(_products)} products from products.json")

        CATS = [
            {'slug': 'shoes', 'name': 'Shoes', 'icon': '', 'sort_order': 1},
            {'slug': 'shirts', 'name': 'Shirts', 'icon': '', 'sort_order': 2},
            {'slug': 'hoodies', 'name': 'Hoodies', 'icon': '', 'sort_order': 3},
            {'slug': 'pants', 'name': 'Pants', 'icon': '', 'sort_order': 4},
            {'slug': 'accessories', 'name': 'Accessories', 'icon': '', 'sort_order': 5},
            {'slug': 'bags', 'name': 'Bags', 'icon': '', 'sort_order': 6},
            {'slug': 'jackets', 'name': 'Jackets', 'icon': '', 'sort_order': 7},
            {'slug': 'tech', 'name': 'Tech', 'icon': '', 'sort_order': 8},
            {'slug': 'womens', 'name': 'Womens', 'icon': '', 'sort_order': 9},
            {'slug': 'trending', 'name': 'Trending', 'icon': '', 'sort_order': 0},
        ]
        for c in CATS:
            add_category(c['slug'], c['name'], c['icon'], '', c['sort_order'])
        print("Categories seeded")
except Exception as e:
    print(f"DB init warning: {e}")


def is_admin():
    return session.get('admin_logged_in', False)


# --- Public Routes ---

@app.route('/')
def home():
    products = get_products()
    categories = get_categories()
    import random
    conveyor = products[:60]
    random.shuffle(conveyor)
    return render_template('home.html', products=products, conveyor=conveyor[:40], categories=categories)


@app.route('/shop')
def shop():
    category = request.args.get('category', '')
    sort = request.args.get('sort', 'newest')
    q = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 40

    if q:
        all_products = search_products(q)
        if category:
            all_products = [p for p in all_products if p.get('category') == category]
    else:
        all_products = get_products(category if category else None)

    if sort == 'price_low':
        all_products.sort(key=lambda p: p.get('price_numeric', 0))
    elif sort == 'price_high':
        all_products.sort(key=lambda p: p.get('price_numeric', 0), reverse=True)

    total = len(all_products)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    products = all_products[(page - 1) * per_page : page * per_page]

    categories = get_categories()
    return render_template('shop.html', products=products, categories=categories,
        current_category=category, current_sort=sort, search_query=q,
        page=page, total_pages=total_pages, total=total)


@app.route('/link-converter')
def link_converter():
    return render_template('link_converter.html')


@app.route('/privacy')
def privacy():
    return render_template('privacy.html')


@app.route('/terms')
def terms():
    return render_template('terms.html')


@app.route('/go/<product_id>')
def affiliate_redirect(product_id):
    product = get_product(product_id)
    if not product:
        return redirect(url_for('shop'))
    try:
        record_click({
            'product_id': product_id, 'product_name': product.get('name', ''),
            'category': product.get('category', ''), 'element_type': 'affiliate',
            'page': 'redirect', 'referrer': request.referrer or '',
            'user_ip': request.headers.get('X-Forwarded-For', request.remote_addr or ''),
            'user_agent': request.headers.get('User-Agent', ''), 'country': '',
        })
    except Exception:
        pass
    url = product.get('url', '')
    if url:
        from urllib.parse import quote
        joya_url = f"https://joyabuy.com/product/?url={quote(url)}"
        if JOYABUY_AFFILIATE:
            joya_url += f"&affcode={JOYABUY_AFFILIATE}"
        return redirect(joya_url)
    return redirect(url_for('shop'))


# --- API ---

@app.route('/api/products')
def api_products():
    limit = request.args.get('limit', 50, type=int)
    products = get_products()
    import random
    sampled = random.sample(products, min(limit, len(products)))
    return jsonify([{'id': p['id'], 'name': p['name'], 'image': p['image'], 'price': p['price'], 'category': p['category']} for p in sampled])


@app.route('/api/click', methods=['POST'])
def api_click():
    data = request.get_json(silent=True) or {}
    data['user_ip'] = request.headers.get('X-Forwarded-For', request.remote_addr or '')
    data['user_agent'] = request.headers.get('User-Agent', '')
    try:
        record_click(data)
    except Exception:
        pass
    return jsonify({'ok': True})


@app.route('/api/track-pageview', methods=['POST'])
def api_pageview():
    data = request.get_json(silent=True) or {}
    data['element_type'] = 'pageview'
    data['user_ip'] = request.headers.get('X-Forwarded-For', request.remote_addr or '')
    data['user_agent'] = request.headers.get('User-Agent', '')
    try:
        record_click(data)
    except Exception:
        pass
    return jsonify({'ok': True})


# --- Admin ---

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['admin_logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        return render_template('admin_login.html', error='Wrong password')
    return render_template('admin_login.html')


@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('home'))


@app.route('/admin')
def admin_dashboard():
    if not is_admin():
        return redirect(url_for('admin_login'))
    stats = get_analytics(30)
    products = get_products()
    return render_template('admin_dashboard.html', stats=stats, products=products)


@app.route('/admin/products/add', methods=['POST'])
def admin_add_product():
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    if not data.get('name'):
        return jsonify({'error': 'Name required'}), 400
    data['id'] = data.get('id', f"p{secrets.token_hex(4)}")
    add_product(data)
    return jsonify({'ok': True, 'id': data['id']})


@app.route('/admin/products/update/<pid>', methods=['POST'])
def admin_update_product(pid):
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    if not data:
        return jsonify({'error': 'No data'}), 400
    if 'price' in data:
        data['price_numeric'] = float(data['price'] or 0)
    update_product(pid, data)
    return jsonify({'ok': True})


@app.route('/admin/products/bulk', methods=['POST'])
def admin_bulk():
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    products = data.get('products', [])
    if not products:
        return jsonify({'error': 'No products'}), 400
    for p in products:
        if not p.get('id'):
            p['id'] = f"p{secrets.token_hex(4)}"
    add_products_bulk(products)
    return jsonify({'ok': True, 'count': len(products)})


@app.route('/admin/products/delete/<pid>', methods=['DELETE'])
def admin_delete(pid):
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    delete_product(pid)
    return jsonify({'ok': True})


@app.route('/admin/products/delete-batch', methods=['POST'])
def admin_delete_batch():
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    ids = data.get('ids', [])
    for pid in ids:
        delete_product(pid)
    return jsonify({'ok': True, 'count': len(ids)})


@app.route('/admin/products')
def admin_products():
    if not is_admin():
        return redirect(url_for('admin_login'))
    products = get_products()
    categories = get_categories()
    return render_template('admin_products.html', products=products, categories=categories)


@app.route('/admin/analytics')
def admin_analytics():
    if not is_admin():
        return redirect(url_for('admin_login'))
    days = request.args.get('days', 30, type=int)
    stats = get_analytics(days)
    return render_template('admin_analytics.html', stats=stats, days=days)


@app.route('/admin/analytics/api')
def admin_analytics_api():
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    days = request.args.get('days', 30, type=int)
    return jsonify(get_analytics(days))


@app.route('/admin/categories/add', methods=['POST'])
def admin_add_category():
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    if not data.get('slug') or not data.get('name'):
        return jsonify({'error': 'Slug and name required'}), 400
    add_category(data['slug'], data['name'], data.get('icon', ''), data.get('description', ''), data.get('sort_order', 0))
    return jsonify({'ok': True})


@app.route('/admin/scrape', methods=['POST'])
def admin_scrape():
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    url = data.get('url', '').strip()
    category = data.get('category', '')
    if not url:
        return jsonify({'error': 'No URL provided'}), 400
    try:
        from scraper import scrape_listing
        result = scrape_listing(url, category=category, affiliate_code=JOYABUY_AFFILIATE)
        if 'error' in result:
            return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/admin/scrape/import', methods=['POST'])
def admin_scrape_import():
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    products = data.get('products', [])
    if not products:
        return jsonify({'error': 'No products'}), 400
    add_products_bulk(products)
    return jsonify({'ok': True, 'count': len(products)})


@app.route('/admin/backup', methods=['POST'])
def admin_backup():
    if not is_admin():
        return jsonify({'error': 'Unauthorized'}), 401
    path = backup_database()
    return jsonify({'ok': True, 'path': path})


@app.route('/admin/backup/download')
def admin_download_backup():
    if not is_admin():
        return redirect(url_for('admin_login'))
    path = backup_database()
    return send_file(path, as_attachment=True)


@app.errorhandler(404)
def not_found(e):
    return redirect(url_for('home'))


if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    app.run(debug=True, port=5002)
