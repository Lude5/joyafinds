"""
Product scraper for Weidian, Taobao, and 1688 listings.
Extracts all variant images, names, and prices from a single listing URL.
Uses GPT-5.4 Nano vision to auto-identify products from images.
"""
import os
import re
import json
import secrets
import urllib.parse
import requests
import base64


HEADERS_MOBILE = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.43",
    "Accept": "application/json, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

HEADERS_DESKTOP = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

CNY_TO_USD = 0.14
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')


CATEGORY_OPTIONS = ['shoes', 'shirts', 'hoodies', 'pants', 'accessories', 'bags', 'watches', 'tech']


_listing_context_cache = {}


def get_listing_context(listing_name):
    """Use GPT to translate the listing title and identify the exact brand + model.
    One cheap text call per listing (not per variant), cached."""
    if not listing_name or not OPENAI_API_KEY:
        return ''
    if listing_name in _listing_context_cache:
        return _listing_context_cache[listing_name]

    try:
        resp = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {OPENAI_API_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'model': 'gpt-4o-mini',
                'messages': [
                    {'role': 'system', 'content': '''You are a fashion product identifier. Given a Chinese/mixed product listing title from Weidian/Taobao, identify:
1. The exact fashion brand (full name)
2. The specific model/product name
3. English translation of Chinese descriptors

Known product line → brand mappings (use these):
- Alaska / Skiwear boots → Balenciaga
- Dunk / Air Force / Air Max / Shox / Tech Fleece / Nocta → Nike
- Yeezy / Foam Runner / 350 / 500 / Slide → Yeezy/Adidas
- Jordan 1-14 / Retro → Jordan/Nike
- Nuptse / Puffer → The North Face
- Maya / Badge puffer → Moncler
- Chilliwack / Expedition → Canada Goose
- Speedhunter / Triple S / Track / Defender / 3XL → Balenciaga
- Keepall / Neverfull / Trainer → Louis Vuitton
- Marmont / Ace → Gucci
- Samba / Campus / Gazelle → Adidas
- Ramones / DRKSHDW → Rick Owens
- Tabi / GAT / Future → Maison Margiela
- Reverend / Distressed tee → ERD / Enfants Riches Déprimés

Common rep brands: Nike, Adidas, Jordan, Balenciaga, Louis Vuitton, Gucci, Prada, Chrome Hearts, Dior, Moncler, Canada Goose, The North Face, Yeezy, Off-White, Amiri, ERD, No Faith Studios, Represent, Essentials/FOG, Stussy, Supreme, Bape, Gallery Dept, Trapstar, Corteiz, Maison Margiela, Rick Owens, Kapital.

If you can identify the brand from the product line name or Chinese text, DO IT. Only say "Brand: Unknown" if truly unidentifiable.

Respond like: "Brand: Balenciaga. Product: Alaska Snow Boot. Padded, fur-lined."'''},
                    {'role': 'user', 'content': f'Identify the brand and product: "{listing_name}"'}
                ],
                'max_tokens': 80,
                'temperature': 0.1,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            context = resp.json()['choices'][0]['message']['content'].strip()
            print(f"[CONTEXT] '{listing_name[:40]}' -> {context[:100]}")
            _listing_context_cache[listing_name] = context
            return context
    except Exception as e:
        print(f"[CONTEXT] Failed: {e}")

    _listing_context_cache[listing_name] = ''
    return ''


def ai_identify_product(image_url, listing_name=''):
    """Use GPT vision to identify a product and its category from its image.
    Returns {'name': '...', 'category': '...'} or None."""
    if not OPENAI_API_KEY:
        print("[AI] No OPENAI_API_KEY set, skipping identification")
        return None
    if not image_url:
        return None

    categories_str = ', '.join(CATEGORY_OPTIONS)
    system_prompt = f"""You identify replica fashion products. Respond with ONLY valid JSON.
Format: {{"name": "Brand Model Colorway", "brand": "Brand", "category": "category", "tags": "search keywords"}}
Rules:
- name: Brand + specific model name + color. MAX 6 words. Use the REAL model name (e.g. "Reverend Hoodie" not just "Hoodie", "Alaska Boot" not just "Boot", "Dunk Low" not just "Sneaker"). No generic descriptions.
- brand: the FASHION HOUSE / COMPANY that makes it. NOT the model name. Common brands: Nike, Adidas, Jordan, Balenciaga, Louis Vuitton, Gucci, Prada, Chrome Hearts, Dior, Moncler, Canada Goose, The North Face, Yeezy, Off-White, Amiri, Represent, Essentials, Stussy, Supreme, Bape, Gallery Dept, Trapstar, Corteiz. If unsure, look at logos/text on the product.
- category: one of [{categories_str}]
- tags: detailed search keywords, lowercase, space separated. Include brand, model, color, material, style, silhouette, visual features. Put most searchable terms first. Think what someone would type to find this.
- Use the common/popular name people use, not the official long name
- Color goes at the end, one word (Black, White, Grey, Pink, etc.)

Good examples:
{{"name": "Balenciaga Alaska Boots Black", "brand": "Balenciaga", "category": "shoes", "tags": "balenciaga alaska boots black fur snow padded nylon lace up chunky winter"}}
{{"name": "Nike Dunk Low Panda", "brand": "Nike", "category": "shoes", "tags": "nike dunk low panda black white leather sneaker sb skateboard"}}
{{"name": "Chrome Hearts Zip Hoodie Black", "brand": "Chrome Hearts", "category": "hoodies", "tags": "chrome hearts zip hoodie black cross logo heavyweight cotton streetwear"}}
{{"name": "Jordan 4 Military Black", "brand": "Jordan", "category": "shoes", "tags": "jordan 4 retro military black grey white nike air sneaker basketball"}}

BAD names (too long):
"Alaska Skiwear Snow Boot Lace-Up Black Nylon" — too wordy
"Padded Lace-Up Winter Boots" — missing brand"""

    # Get web search context for better identification (cached per listing)
    web_context = get_listing_context(listing_name) if listing_name else ''

    # Build the user message with all available context
    user_text = 'Identify this product.'
    if listing_name:
        user_text += f'\nListing title: "{listing_name}"'
    if web_context:
        user_text += f'\nListing context: {web_context}'
        user_text += '\nThis context is from the listing title — it MAY be wrong about the brand. Look at the actual product in the image (logos, design, silhouette) to determine the real brand. Trust what you SEE over what the listing says.'

    # Try gpt-5.4-nano first, fall back to gpt-4o-mini
    for model in ['gpt-5.4-nano', 'gpt-4o-mini']:
        try:
            body = {
                'model': model,
                'messages': [
                    {'role': 'system', 'content': system_prompt},
                    {
                        'role': 'user',
                        'content': [
                            {'type': 'text', 'text': user_text},
                            {'type': 'image_url', 'image_url': {'url': image_url, 'detail': 'low'}},
                        ]
                    }
                ],
                'temperature': 0.2,
            }
            # gpt-5.4-nano uses max_completion_tokens, older models use max_tokens
            if '5.4' in model:
                body['max_completion_tokens'] = 60
            else:
                body['max_tokens'] = 60

            resp = requests.post(
                'https://api.openai.com/v1/chat/completions',
                headers={
                    'Authorization': f'Bearer {OPENAI_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json=body,
                timeout=20,
            )

            if resp.status_code == 200:
                data = resp.json()
                content = data['choices'][0]['message']['content'].strip()
                # Strip markdown code blocks if present
                if content.startswith('```'):
                    content = content.split('\n', 1)[-1].rsplit('```', 1)[0].strip()
                # Parse JSON response
                try:
                    parsed = json.loads(content)
                    name = parsed.get('name', '').strip().strip('"\'')
                    brand = parsed.get('brand', '').strip().strip('"\'')
                    category = parsed.get('category', '').strip().lower()
                    tags = parsed.get('tags', '').strip().lower()
                    if category not in CATEGORY_OPTIONS:
                        category = ''
                    if name and len(name) > 2:
                        print(f"[AI] {model}: '{name}' brand='{brand}' -> {category} tags='{tags[:50]}'")
                        return {'name': name, 'brand': brand, 'category': category, 'tags': tags}
                except json.JSONDecodeError:
                    # Try to extract name from plain text response
                    content = content.strip('"\'')
                    if content and len(content) > 2 and len(content) < 100:
                        print(f"[AI] {model} (plain text): '{content}'")
                        return {'name': content, 'category': ''}
            else:
                print(f"[AI] {model} returned {resp.status_code}: {resp.text[:200]}")
                continue  # Try next model

        except Exception as e:
            print(f"[AI] {model} failed for {image_url[:60]}: {e}")
            continue

    print(f"[AI] All models failed for {image_url[:60]}")
    return None


def detect_platform(url):
    """Parse URL and return (platform, item_id) or (None, None)."""
    # Unwrap KakoBuy links
    if 'kakobuy.com' in url:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        inner = params.get('url', [''])[0]
        if inner:
            url = inner

    if 'weidian.com' in url:
        m = re.search(r'itemID[=](\d+)', url)
        return ('weidian', m.group(1)) if m else (None, None)
    elif 'taobao.com' in url or 'tmall.com' in url:
        m = re.search(r'[?&]id=(\d+)', url)
        return ('taobao', m.group(1)) if m else (None, None)
    elif '1688.com' in url:
        m = re.search(r'/offer/(\d+)', url)
        return ('1688', m.group(1)) if m else (None, None)

    return (None, None)


def scrape_listing(url, category='', affiliate_code=''):
    """
    Scrape a product listing and return a list of products (one per variant).
    Each product dict has: id, name, price, price_numeric, image, url, category.
    """
    platform, item_id = detect_platform(url)
    if not platform:
        return {'error': 'Could not parse URL. Supports Weidian, Taobao, 1688.'}

    result = {
        'platform': platform,
        'item_id': item_id,
        'name': '',
        'price': '',
        'images': [],
        'variants': [],
    }

    if platform == 'weidian':
        result = _scrape_weidian(item_id, result)
    elif platform == 'taobao':
        result = _scrape_taobao(item_id, result)
    elif platform == '1688':
        result = _scrape_1688(item_id, result)

    # Build source URL for affiliate link
    if platform == 'weidian':
        source = f"https://weidian.com/item.html?itemID={item_id}"
    elif platform == 'taobao':
        source = f"https://item.taobao.com/item.htm?id={item_id}"
    else:
        source = f"https://detail.1688.com/offer/{item_id}.html"

    kakobuy_url = f"https://www.kakobuy.com/item/details?url={urllib.parse.quote(source, safe='')}"
    if affiliate_code:
        kakobuy_url += f"&affcode={affiliate_code}"

    # Convert CNY price to USD
    price_cny = result.get('price', '')
    try:
        price_usd = round(float(price_cny) * CNY_TO_USD, 2) if price_cny else 0
    except (ValueError, TypeError):
        price_usd = 0

    # Build product list — one per variant, or one for the whole listing
    products = []
    base_name = result.get('name', '') or f'{platform.title()} Item {item_id}'

    if result.get('variants'):
        for v in result['variants']:
            img = v.get('image', '') or (result['images'][0] if result['images'] else '')
            # Per-variant price if available, otherwise use listing base price
            v_price_cny = v.get('price_cny') or float(price_cny or 0)
            try:
                v_price_usd = round(float(v_price_cny) * CNY_TO_USD, 2)
            except (ValueError, TypeError):
                v_price_usd = price_usd
            # AI identify each variant — returns {name, category}
            ai_result = ai_identify_product(img, base_name) if img else None
            ai_name = ai_result.get('name', '') if ai_result else ''
            ai_brand = ai_result.get('brand', '') if ai_result else ''
            ai_category = ai_result.get('category', '') if ai_result else ''
            ai_tags = ai_result.get('tags', '') if ai_result else ''
            prod = {
                'id': f"{platform[0]}{item_id}_{secrets.token_hex(3)}",
                'name': ai_name or f"{base_name} - {v.get('name', '')}".strip(' -'),
                'name_original': v.get('name', ''),
                'name_ai': ai_name,
                'brand': ai_brand,
                'tags': ai_tags,
                'price': str(v_price_usd) if v_price_usd else str(v_price_cny),
                'price_cny': str(v_price_cny),
                'price_numeric': v_price_usd or v_price_cny,
                'url': source,
                'image': img,
                'category': category or ai_category,
                'category_ai': ai_category,
                'seller': ai_brand,
                'batch': '',
                'retail_price': '',
                'review_count': 0,
                'kakobuy_link': kakobuy_url,
            }
            products.append(prod)
    else:
        # No variants — create one product per image
        images = result.get('images', [])
        if not images:
            images = ['']
        for i, img in enumerate(images):
            ai_result = ai_identify_product(img, base_name) if img else None
            ai_name = ai_result.get('name', '') if ai_result else ''
            ai_brand = ai_result.get('brand', '') if ai_result else ''
            ai_category = ai_result.get('category', '') if ai_result else ''
            ai_tags = ai_result.get('tags', '') if ai_result else ''
            prod = {
                'id': f"{platform[0]}{item_id}_{secrets.token_hex(3)}",
                'name': ai_name or (base_name if i == 0 else f"{base_name} ({i+1})"),
                'name_original': base_name,
                'name_ai': ai_name,
                'brand': ai_brand,
                'tags': ai_tags,
                'price': str(price_usd) if price_usd else price_cny,
                'price_numeric': price_usd or float(price_cny or 0),
                'url': source,
                'image': img,
                'category': category or ai_category,
                'category_ai': ai_category,
                'seller': ai_brand,
                'batch': '',
                'retail_price': '',
                'review_count': 0,
                'kakobuy_link': kakobuy_url,
            }
            products.append(prod)

    return {
        'products': products,
        'listing_name': base_name,
        'listing_price_cny': price_cny,
        'listing_price_usd': str(price_usd),
        'total_variants': len(products),
        'platform': platform,
        'item_id': item_id,
        'kakobuy_link': kakobuy_url,
        'ai_enabled': bool(OPENAI_API_KEY),
    }


def _scrape_weidian_raw(item_id):
    """Return raw Thor API response for debugging."""
    try:
        param_str = json.dumps({"itemId": item_id}, separators=(',', ':'))
        api_url = f"https://thor.weidian.com/detail/getItemSkuInfo/1.0?param={urllib.parse.quote(param_str)}"
        resp = requests.get(api_url, headers={
            **HEADERS_MOBILE,
            "Referer": f"https://shop.weidian.com/item.html?itemID={item_id}",
            "Origin": "https://shop.weidian.com",
        }, timeout=10)
        data = resp.json()
        r = data.get('result', {})

        # Also try the detail endpoint for more pricing data
        param_str2 = json.dumps({"itemId": item_id}, separators=(',', ':'))
        detail_url = f"https://thor.weidian.com/detail/getItemDetail/1.0?param={urllib.parse.quote(param_str2)}"
        resp2 = requests.get(detail_url, headers={
            **HEADERS_MOBILE,
            "Referer": f"https://shop.weidian.com/item.html?itemID={item_id}",
            "Origin": "https://shop.weidian.com",
        }, timeout=10)
        detail_data = resp2.json()

        return {
            'sku_api': {
                'keys': list(r.keys()) if isinstance(r, dict) else str(type(r)),
                'skuList': r.get('skuList'),
                'skuMap': r.get('skuMap'),
                'attrList_count': len(r.get('attrList', [])),
                'attrList_sample': r.get('attrList', [])[:2],
                'price_fields': {
                    'price': r.get('price'),
                    'itemDiscountLowPrice': r.get('itemDiscountLowPrice'),
                    'itemDiscountHighPrice': r.get('itemDiscountHighPrice'),
                    'itemOriginalLowPrice': r.get('itemOriginalLowPrice'),
                    'itemOriginalHighPrice': r.get('itemOriginalHighPrice'),
                },
            },
            'detail_api': {
                'keys': list(detail_data.get('result', {}).keys()) if isinstance(detail_data.get('result'), dict) else str(type(detail_data.get('result'))),
                'skuList': detail_data.get('result', {}).get('skuList'),
                'skus': detail_data.get('result', {}).get('skus'),
                'skuMap': detail_data.get('result', {}).get('skuMap'),
            }
        }
    except Exception as e:
        return {'error': str(e)}


def _scrape_weidian(item_id, result):
    """Scrape Weidian using Thor API + HTML fallbacks."""

    # Method 1: Thor API (getItemSkuInfo)
    try:
        param_str = json.dumps({"itemId": item_id}, separators=(',', ':'))
        api_url = f"https://thor.weidian.com/detail/getItemSkuInfo/1.0?param={urllib.parse.quote(param_str)}"

        resp = requests.get(api_url, headers={
            **HEADERS_MOBILE,
            "Referer": f"https://shop.weidian.com/item.html?itemID={item_id}",
            "Origin": "https://shop.weidian.com",
        }, timeout=10)

        if resp.status_code == 200:
            data = resp.json()
            r = data.get('result') or {}

            result['name'] = r.get('itemTitle') or r.get('title') or r.get('itemName') or ''

            low = r.get('itemDiscountLowPrice') or r.get('itemOriginalLowPrice')
            if low and isinstance(low, int) and low > 100:
                result['price'] = str(low / 100)
            elif r.get('price'):
                result['price'] = str(r['price'])

            main_pic = r.get('itemMainPic', '')
            if main_pic:
                result['image'] = main_pic
                result['images'] = [main_pic]

            # Build SKU price map: colorAttrId -> lowest price (in yuan)
            # Data lives in "skuInfos" — each entry has attrIds[colorId, sizeId]
            # and skuInfo.discountPrice in fen
            sku_prices = {}
            sku_infos = r.get('skuInfos') or r.get('skuList') or r.get('skuMap') or []
            if isinstance(sku_infos, dict):
                sku_infos = list(sku_infos.values())
            for sku in sku_infos:
                # Price can be nested in skuInfo or at top level
                info = sku.get('skuInfo', sku)
                sku_price = info.get('discountPrice') or info.get('originalPrice') or info.get('price') or 0
                sku_price = float(sku_price) if sku_price else 0
                # Convert fen to yuan (prices > 100 are likely fen)
                if sku_price > 500:
                    sku_price = sku_price / 100
                if sku_price <= 0:
                    continue
                # attrIds is a list [colorId, sizeId] or a string "colorId;sizeId"
                attr_ids = sku.get('attrIds') or info.get('attrIds') or ''
                if isinstance(attr_ids, list):
                    # First ID = color/style, use that as the key
                    color_id = str(attr_ids[0]) if attr_ids else ''
                elif isinstance(attr_ids, str):
                    color_id = attr_ids.split(';')[0].strip()
                else:
                    continue
                if color_id:
                    if color_id not in sku_prices or sku_price < sku_prices[color_id]:
                        sku_prices[color_id] = sku_price

            print(f"[SCRAPE] SKU prices found: {len(sku_prices)} unique color prices: {dict(list(sku_prices.items())[:5])}")

            # Extract variants from attrList
            attr_list = r.get('attrList') or []
            variants = []
            for group in attr_list:
                group_title = group.get('attrTitle', '')
                is_size = any(kw in group_title.lower() for kw in ['size', '尺码', '码', '尺寸', '号'])
                is_color = any(kw in group_title.lower() for kw in ['色', 'color', '配色', '款式', '款', 'style', '版本'])

                if is_size and not is_color:
                    continue

                for val in group.get('attrValues', []):
                    v_name = val.get('attrValue', '')
                    v_img = val.get('img', '')
                    v_id = str(val.get('attrId', ''))
                    if v_name:
                        v = {'name': v_name, 'group': group_title, 'id': v_id}
                        # Attach per-variant price from SKU map
                        if v_id and v_id in sku_prices:
                            v['price_cny'] = sku_prices[v_id]
                            print(f"[SCRAPE] Matched variant '{v_name}' -> ¥{sku_prices[v_id]}")
                        if v_img:
                            v['image'] = v_img
                            if v_img not in result['images']:
                                result['images'].append(v_img)
                        variants.append(v)

            result['variants'] = variants

            # If no per-variant prices found from skuList, try the detail API
            if variants and not any(v.get('price_cny') for v in variants):
                try:
                    param2 = json.dumps({"itemId": item_id}, separators=(',', ':'))
                    detail_url = f"https://thor.weidian.com/detail/getItemDetail/1.0?param={urllib.parse.quote(param2)}"
                    resp2 = requests.get(detail_url, headers={
                        **HEADERS_MOBILE,
                        "Referer": f"https://shop.weidian.com/item.html?itemID={item_id}",
                        "Origin": "https://shop.weidian.com",
                    }, timeout=10)
                    if resp2.status_code == 200:
                        d2 = resp2.json().get('result', {})
                        # Look for skuList/skus in detail response
                        detail_skus = d2.get('skuList') or d2.get('skus') or d2.get('skuMap') or []
                        if isinstance(detail_skus, dict):
                            detail_skus = list(detail_skus.values())
                        for sku in detail_skus:
                            sp = sku.get('price') or sku.get('discountPrice') or sku.get('salePrice') or 0
                            if isinstance(sp, int) and sp > 100:
                                sp = sp / 100
                            sp = float(sp) if sp else 0
                            if sp <= 0:
                                continue
                            attr_ids = str(sku.get('attrIds') or sku.get('attrIdStr') or sku.get('attrs') or '')
                            for aid in attr_ids.replace(',', ';').split(';'):
                                aid = aid.strip()
                                if aid:
                                    for v in variants:
                                        if v.get('id') == aid and not v.get('price_cny'):
                                            v['price_cny'] = sp
                        print(f"[SCRAPE] Detail API added prices to {sum(1 for v in variants if v.get('price_cny'))} variants")
                except Exception as e2:
                    print(f"[SCRAPE] Detail API failed: {e2}")

            if result['name']:
                return result
    except Exception as e:
        print(f"[SCRAPE] Thor API failed: {e}")

    # Method 2: HTML scrape
    try:
        page_url = f"https://weidian.com/item.html?itemID={item_id}"
        resp = requests.get(page_url, headers=HEADERS_DESKTOP, timeout=15)
        html = resp.text

        imgs = re.findall(r'(https?://si\.geilicdn\.com/[^\s"\'\\<>]+?\.(?:jpg|png|webp))(?:[?"\s][^\s]*)?', html)
        product_imgs = []
        seen = set()
        for img in imgs:
            clean = re.split(r'[?"]', img)[0]
            if clean not in seen:
                dim = re.search(r'_(\d+)_(\d+)', clean)
                if dim and int(dim.group(1)) >= 500:
                    seen.add(clean)
                    product_imgs.append(clean)

        if product_imgs:
            result['images'] = product_imgs[:12]
            result['image'] = product_imgs[0]

        for pat in [r'"itemName"\s*:\s*"([^"]+)"', r'"title"\s*:\s*"([^"]{10,200})"', r'"goodsName"\s*:\s*"([^"]+)"']:
            m = re.search(pat, html)
            if m and len(m.group(1)) > 5:
                result['name'] = m.group(1).strip()
                break

        for pat in [r'"price"\s*:\s*"?(\d+\.?\d{0,2})"?', r'"minPrice"\s*:\s*"?(\d+\.?\d{0,2})"?', r'[¥￥]\s*(\d+\.?\d{0,2})']:
            m = re.search(pat, html)
            if m and float(m.group(1)) > 0:
                result['price'] = m.group(1)
                break
    except Exception as e:
        print(f"[SCRAPE] HTML failed: {e}")

    return result


def _extract_json_block(html, key):
    """Try to find and parse a JSON object containing a specific key from page HTML."""
    # Look for patterns like "skuBase":{ ... } or "skuCore":{ ... }
    patterns = [
        rf'"{key}"\s*:\s*(\{{[^{{}}]*(?:\{{[^{{}}]*\}}[^{{}}]*)*\}})',  # nested 1 level
        rf'"{key}"\s*:\s*(\[.*?\])',  # array
    ]
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            try:
                return json.loads(m.group(1))
            except (json.JSONDecodeError, ValueError):
                pass
    return None


def _scrape_taobao(item_id, result):
    """Scrape Taobao product data with SKU parsing."""
    try:
        for base_url in [
            f"https://h5.m.taobao.com/awp/core/detail.htm?id={item_id}",
            f"https://item.taobao.com/item.htm?id={item_id}",
        ]:
            resp = requests.get(base_url, headers=HEADERS_MOBILE, timeout=15, allow_redirects=True)
            html = resp.text

            # Title
            for pat in [r'"title"\s*:\s*"([^"]{10,200})"', r'"subject"\s*:\s*"([^"]+)"', r'"itemTitle"\s*:\s*"([^"]+)"']:
                m = re.search(pat, html)
                if m and not result.get('name'):
                    result['name'] = m.group(1).strip()
                    break

            # Base price
            for pat in [r'"price"\s*:\s*"?(\d+\.?\d{0,2})"?', r'"reservePrice"\s*:\s*"?(\d+\.?\d{0,2})"?']:
                m = re.search(pat, html)
                if m and float(m.group(1)) > 0 and not result.get('price'):
                    result['price'] = m.group(1)
                    break

            # Images
            imgs = re.findall(r'(https?://(?:img|gw)\.alicdn\.com/[^\s"\'\\<>]+?\.(?:jpg|png|webp))', html)
            clean_imgs = []
            seen = set()
            for img in imgs:
                clean = img.split('?')[0]
                if clean not in seen and len(clean) > 30:
                    seen.add(clean)
                    clean_imgs.append(clean)
            if clean_imgs and not result.get('images'):
                result['images'] = clean_imgs[:12]
                result['image'] = clean_imgs[0]

            # --- SKU Parsing ---
            # Taobao embeds SKU data in various JSON structures in page source
            # Look for skuBase/skuCore with props (variant definitions) and sku list with prices
            variants = []

            # Method 1: Find "skuItem" or "skuList" with price info
            sku_prices = {}  # skuId -> price
            for sku_key in ['skuItem', 'skuList', 'skuMap']:
                sku_match = re.search(rf'"{sku_key}"\s*:\s*\{{(.*?)\}}\s*[,\}}]', html, re.DOTALL)
                if sku_match:
                    # Extract individual SKU entries with prices
                    price_entries = re.findall(r'"(\d+(?:;\d+)*)"\s*:\s*\{[^}]*"price"\s*:\s*"?(\d+\.?\d{0,2})"?', sku_match.group(1))
                    for sku_id, price in price_entries:
                        sku_prices[sku_id] = float(price)
                    if sku_prices:
                        break

            # Method 2: Find prop definitions (color/style variant names + images)
            # Taobao uses "prop" structures like: "pid:vid" -> {name, image}
            prop_entries = re.findall(
                r'"(\d+:\d+)"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"(?:[^}]*"image"\s*:\s*"([^"]*)")?',
                html
            )

            # Build variant map: vid -> {name, image}
            prop_map = {}
            for prop_id, name, image in prop_entries:
                prop_map[prop_id] = {'name': name, 'image': image or ''}

            # Match SKU prices to prop names
            if sku_prices and prop_map:
                for sku_id, price in sku_prices.items():
                    parts = sku_id.split(';')
                    # Find the color/style prop (skip size props)
                    for part in parts:
                        if part in prop_map:
                            p = prop_map[part]
                            # Check if this is likely a color/style (has image) vs size (no image)
                            v = {
                                'name': p['name'],
                                'price_cny': price,
                            }
                            if p.get('image'):
                                img_url = p['image']
                                if not img_url.startswith('http'):
                                    img_url = 'https:' + img_url
                                v['image'] = img_url
                                if img_url not in result.get('images', []):
                                    result.setdefault('images', []).append(img_url)
                            # Only add if we haven't seen this variant name
                            if not any(ev['name'] == v['name'] for ev in variants):
                                variants.append(v)
                            break

            # Fallback: just parse prop images as variants without prices
            if not variants and prop_map:
                for prop_id, p in prop_map.items():
                    if p.get('image'):
                        img_url = p['image']
                        if not img_url.startswith('http'):
                            img_url = 'https:' + img_url
                        v = {'name': p['name'], 'image': img_url}
                        if img_url not in result.get('images', []):
                            result.setdefault('images', []).append(img_url)
                        if not any(ev['name'] == v['name'] for ev in variants):
                            variants.append(v)

            if variants:
                result['variants'] = variants
                print(f"[SCRAPE] Taobao SKU: {len(variants)} variants, {len(sku_prices)} prices")

            if result.get('name'):
                break

        print(f"[SCRAPE] Taobao: name='{result.get('name','')[:50]}', price={result.get('price','')}, imgs={len(result.get('images',[]))}, variants={len(result.get('variants',[]))}")

    except Exception as e:
        print(f"[SCRAPE] Taobao failed: {e}")

    return result


def _scrape_1688(item_id, result):
    """Scrape 1688 product data with SKU parsing."""
    try:
        page_url = f"https://detail.1688.com/offer/{item_id}.html"
        resp = requests.get(page_url, headers=HEADERS_DESKTOP, timeout=15)
        html = resp.text

        # Title
        for pat in [r'"subject"\s*:\s*"([^"]+)"', r'"title"\s*:\s*"([^"]{10,200})"', r'<title>([^<]+?)\s*[-–]']:
            m = re.search(pat, html)
            if m:
                result['name'] = m.group(1).replace('-阿里巴巴', '').strip()
                break

        # Base price
        for pat in [r'"price"\s*:\s*"?(\d+\.?\d{0,2})"?', r'"priceRange"\s*:\s*"(\d+\.?\d{0,2})']:
            m = re.search(pat, html)
            if m and float(m.group(1)) > 0:
                result['price'] = m.group(1)
                break

        # Images
        imgs = re.findall(r'(https?://cbu\d*\.alicdn\.com/[^\s"\'\\<>]+?\.(?:jpg|png|webp))', html)
        if imgs:
            clean = list(dict.fromkeys(img.split('?')[0] for img in imgs))[:12]
            result['images'] = clean
            result['image'] = clean[0]

        # --- SKU Parsing ---
        # 1688 embeds skuProps (variant definitions) and skuInfoMap (prices) in page source
        variants = []

        # Find SKU properties — variant names and images
        # Format: "skuProps":[{"prop":"颜色","value":[{"name":"黑色","imageUrl":"..."},...]},...]
        sku_props_match = re.search(r'"skuProps"\s*:\s*(\[.*?\])\s*[,\}]', html, re.DOTALL)
        if sku_props_match:
            try:
                sku_props = json.loads(sku_props_match.group(1))
                for group in sku_props:
                    prop_name = group.get('prop', '')
                    is_size = any(kw in prop_name.lower() for kw in ['size', '尺码', '码', '尺寸', '号', '大小'])
                    if is_size:
                        continue
                    for val in group.get('value', []):
                        v_name = val.get('name', '')
                        v_img = val.get('imageUrl', '')
                        if v_name:
                            v = {'name': v_name}
                            if v_img:
                                if not v_img.startswith('http'):
                                    v_img = 'https:' + v_img
                                v['image'] = v_img
                                if v_img not in result.get('images', []):
                                    result.setdefault('images', []).append(v_img)
                            variants.append(v)
            except (json.JSONDecodeError, ValueError) as e:
                print(f"[SCRAPE] 1688 skuProps parse error: {e}")

        # Find SKU price map — maps prop combos to prices
        # Format: "skuInfoMap":{"propPath":"value":{"price":123,...},...}
        sku_map_match = re.search(r'"skuInfoMap"\s*:\s*(\{.*?\})\s*[,\}]', html, re.DOTALL)
        if sku_map_match and variants:
            try:
                sku_map = json.loads(sku_map_match.group(1))
                # Map first prop value to its price
                for key, info in sku_map.items():
                    price = info.get('price') or info.get('discountPrice', 0)
                    if price:
                        price = float(price)
                        # key is like "0" or "0>1" — first number = color index
                        idx = int(key.split('>')[0]) if '>' in key else int(key)
                        if idx < len(variants) and 'price_cny' not in variants[idx]:
                            variants[idx]['price_cny'] = price
            except (json.JSONDecodeError, ValueError, IndexError) as e:
                print(f"[SCRAPE] 1688 skuInfoMap parse error: {e}")

        if variants:
            result['variants'] = variants
            print(f"[SCRAPE] 1688 SKU: {len(variants)} variants")

        print(f"[SCRAPE] 1688: name='{result.get('name','')[:50]}', price={result.get('price','')}, imgs={len(result.get('images',[]))}, variants={len(result.get('variants',[]))}")

    except Exception as e:
        print(f"[SCRAPE] 1688 failed: {e}")

    return result
