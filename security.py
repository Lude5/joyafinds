"""Drop-in security hardening for the finds sites + master admin.

Usage: call ``harden(app)`` once, right after you create the Flask app.
Stdlib-only, no extra dependencies, and it makes NO behavioural change to your
routes — so it's safe to add to a live site.

What it does:
  * Session cookie: HttpOnly + SameSite=Lax always, and Secure in production
    (Render sets the RENDER env var). SameSite=Lax also stops the session cookie
    from being sent into a cross-site <iframe>, which neutralises clickjacking of
    authenticated admin actions without needing X-Frame-Options (so it won't
    break the master-admin studio, which legitimately frames each site).
  * Safe response headers: nosniff, Referrer-Policy, Permissions-Policy, and
    HSTS in production.
  * Brute-force throttle on the login form: at most ``max_attempts`` POSTs to a
    login path per client IP per 15-minute window, then a temporary 429.
"""
import os
import time
from collections import defaultdict

from flask import request, abort

_ATTEMPTS = defaultdict(list)          # ip -> [timestamps]
_WINDOW_SECONDS = 900                   # 15 minutes
_DEFAULT_MAX = 12                       # login POSTs / IP / window before a block
_DEFAULT_LOGIN_PATHS = ('/admin/login', '/login')


def _client_ip():
    fwd = request.headers.get('X-Forwarded-For', '')
    if fwd:
        return fwd.split(',')[0].strip()
    return request.remote_addr or 'unknown'


def harden(app, login_paths=_DEFAULT_LOGIN_PATHS, max_attempts=_DEFAULT_MAX):
    """Apply cookie hardening, security headers, and login throttling to ``app``."""
    in_prod = bool(os.environ.get('RENDER') or os.environ.get('FLASK_SECURE_COOKIES'))
    app.config.setdefault('SESSION_COOKIE_HTTPONLY', True)
    app.config.setdefault('SESSION_COOKIE_SAMESITE', 'Lax')
    app.config['SESSION_COOKIE_SECURE'] = in_prod

    norm_paths = tuple(p.rstrip('/') for p in login_paths)

    @app.before_request
    def _throttle_login():
        if request.method != 'POST':
            return
        if (request.path.rstrip('/') or '/') not in norm_paths:
            return
        ip = _client_ip()
        now = time.time()
        recent = [t for t in _ATTEMPTS[ip] if now - t < _WINDOW_SECONDS]
        _ATTEMPTS[ip] = recent
        if len(recent) >= max_attempts:
            abort(429, description='Too many login attempts. Please wait a few minutes.')
        _ATTEMPTS[ip].append(now)

    @app.after_request
    def _security_headers(resp):
        resp.headers.setdefault('X-Content-Type-Options', 'nosniff')
        resp.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
        resp.headers.setdefault('Permissions-Policy', 'geolocation=(), microphone=(), camera=()')
        if in_prod:
            resp.headers.setdefault('Strict-Transport-Security', 'max-age=15552000')
        return resp

    return app
