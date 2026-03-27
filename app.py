#!/usr/bin/env python3
"""
DM Cvent Integration - Attendee lookup and order data (Cvent only)
"""

import os
import json
import time
import threading
import hashlib
import requests
from typing import Optional
from flask import Flask, render_template, request, jsonify, redirect
from dotenv import load_dotenv
from itsdangerous import URLSafeTimedSerializer

load_dotenv()

app = Flask(__name__, static_folder="static", template_folder="templates")


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error"}), 500


CV_CLIENT_ID = os.getenv("CV_CLIENT_ID")
CV_CLIENT_SECRET = os.getenv("CV_CLIENT_SECRET")
# API base: api-platform.cvent.com (NA) or api-platform-eur.cvent.com (EU)
CV_API_BASE = os.getenv("CV_API_BASE", "https://api-platform.cvent.com")

HUBSPOT_TOKEN = os.getenv("CustomCode")
HUBSPOT_ATTENDEE_OBJECT = "2-44005420"
HUBSPOT_EVENTS_OBJECT = "2-43992149"
HUBSPOT_FESTIVALS_OBJECT = "2-52852059"
HUBSPOT_SPONSORS_OBJECT = "2-45742771"
HUBSPOT_REG_QUESTIONS_GROUP = "registration_questions"  # Internal name; display: Registration Questions
HUBSPOT_EVENT_ADMISSION_PROP = "cvent_admission_item_ids"
HUBSPOT_EVENT_CODE_PROP = "event_code"
_hubspot_prop_names_cache = None

# Attendee → Event association labels (USER_DEFINED)
ASSOC_LABEL_PAYING_DELEGATE = 111
ASSOC_LABEL_DEALMAKERS_GUEST = 117
ASSOC_LABEL_SPEAKER_NON_SPONSOR = 113
ASSOC_LABEL_SPEAKER_SPONSOR = 115
ASSOC_LABEL_SPONSOR_CLIENT = 109
ASSOC_LABEL_SPONSOR_EXECUTIVE = 107
ASSOC_LABEL_UNKNOWN = 143

# Paying Delegate: only these registration types (attendee types) get Paying Delegate. We do not use registration path.
PAYING_DELEGATE_TYPES = frozenset({
    "Academic/Student",
    "Advisor/Broker",
    "Analyst/Researcher",
    "Government Agency",
    "Insurance Services",
    "Intellectual Property Investor",
    "Investment Bank",
    "IP Licensing Company",
    "Law Firm",
    "Limited Partner Investor",
    "Litigation Finance Firm",
    "Multinational Corporation",
    "Patent Pool/Platform",
    "Press/Media",
    "Research Institution/University",
    "Service Provider",
    "Small and Midsize Enterprise",
    "Traditional Bank",
})

# Dealmakers Guest: only these registration types get the Dealmakers Guest association label.
DEALMAKERS_GUEST_TYPES = frozenset({"Dealmakers Guest"})

# Deal creation (workflow step 20)
HUBSPOT_DEAL_PIPELINE = "726721932"
HUBSPOT_DEAL_STAGE = "1191309199"

# App auth (email one-time code), aligned with Kinly pattern.
SESSION_SECRET = os.getenv("SESSION_SECRET", "").strip() or None
SMTP_HOST = os.getenv("SMTP_HOST", "").strip() or None
try:
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587").strip() or "587")
except (TypeError, ValueError):
    SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER", "").strip() or None
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip() or None
EMAIL_FROM = os.getenv("EMAIL_FROM", "").strip() or None
ALLOWED_EMAILS_STR = os.getenv("ALLOWED_EMAILS", "").strip() or None
ALLOWED_EMAILS = [e.strip().lower() for e in (ALLOWED_EMAILS_STR or "").split(",") if e.strip()] or None

SESSION_COOKIE_NAME = "dm_cvent_session"
SESSION_MAX_AGE_SECONDS = 24 * 3600
EMAIL_OTP_ENABLED = bool(SESSION_SECRET and SMTP_HOST and EMAIL_FROM)
AUTH_ENABLED = bool(SESSION_SECRET and EMAIL_OTP_ENABLED)
OTP_CODE_EXPIRY_SECONDS = 15 * 60
OTP_RATE_EMAIL_SECONDS = 2 * 60
OTP_RATE_IP_MAX = 10
OTP_RATE_IP_WINDOW = 15 * 60
_otp_store = {}
_otp_lock = threading.Lock()
_otp_email_last_send = {}
_otp_ip_sends = {}


def _session_serializer():
    if not SESSION_SECRET:
        return None
    return URLSafeTimedSerializer(SESSION_SECRET, salt="dm-cvent-login")


def _mask_email_for_logs(email: str) -> str:
    """Log-safe email reference (never logs raw address)."""
    if not email:
        return "unknown"
    digest = hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()
    return f"sha256:{digest[:12]}"


def _verify_session_cookie():
    if not AUTH_ENABLED:
        return True
    val = request.cookies.get(SESSION_COOKIE_NAME)
    if not val:
        return False
    ser = _session_serializer()
    if not ser:
        return False
    try:
        ser.loads(val, max_age=SESSION_MAX_AGE_SECONDS)
        return True
    except Exception:
        return False


def _set_session_cookie(response):
    ser = _session_serializer()
    if not ser:
        return response
    token = ser.dumps("authenticated")
    response.set_cookie(
        SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        secure=True,
        samesite="Lax",
        path="/",
    )
    return response


def _send_otp_email(to_email: str, code: str) -> None:
    import ssl
    import smtplib
    from datetime import datetime, timezone
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    subject = f"DM Cvent verification code (expires in 15 minutes) {datetime.now(timezone.utc).strftime('%d%m%y - %H:%M')}"
    body = f"""Hi there,

Here's your one-time sign-in code for the DM Cvent app:

{code}

This code will expire in 15 minutes.

If you didn't request this sign-in code, you can safely ignore this email.
"""
    # Prefer SendGrid HTTP API when configured. This avoids SMTP socket/network issues.
    if SMTP_HOST and "sendgrid" in SMTP_HOST.lower() and SMTP_PASSWORD:
        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": EMAIL_FROM},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        }
        try:
            r = requests.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={
                    "Authorization": f"Bearer {SMTP_PASSWORD}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=15,
            )
            if r.status_code in (200, 201, 202):
                return
            detail = r.text[:500] if r.text else f"status {r.status_code}"
            raise RuntimeError(f"SendGrid API send failed: {detail}")
        except requests.RequestException as e:
            raise RuntimeError(f"SendGrid API request failed: {str(e)}") from e

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = to_email
    msg.attach(MIMEText(body, "plain"))
    context = ssl.create_default_context()
    try:
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=15) as smtp:
                if SMTP_USER and SMTP_PASSWORD:
                    smtp.login(SMTP_USER, SMTP_PASSWORD)
                smtp.sendmail(EMAIL_FROM, [to_email], msg.as_string())
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
                smtp.starttls(context=context)
                if SMTP_USER and SMTP_PASSWORD:
                    smtp.login(SMTP_USER, SMTP_PASSWORD)
                smtp.sendmail(EMAIL_FROM, [to_email], msg.as_string())
    except smtplib.SMTPAuthenticationError as e:
        raise RuntimeError("SMTP authentication failed. Check SMTP_USER/SMTP_PASSWORD.") from e
    except smtplib.SMTPException as e:
        raise RuntimeError(f"SMTP error while sending code: {str(e)}") from e
    except OSError as e:
        raise RuntimeError(f"SMTP connection failed: {str(e)}") from e


@app.before_request
def _require_auth():
    if not AUTH_ENABLED:
        return None
    path = request.path.rstrip("/") or "/"
    if path in (
        "/login",
        "/api/auth/methods",
        "/api/auth/send-code",
        "/api/auth/verify-code",
        "/api/logout",
        "/api/health",
        "/api/ping",
    ):
        return None
    if path.startswith("/static/"):
        return None
    if _verify_session_cookie():
        return None
    if path == "/" or path == "/events":
        return redirect("/login")
    if path.startswith("/api/"):
        return jsonify({"error": "Unauthorized"}), 401
    return None


@app.route("/login")
def login_page():
    if AUTH_ENABLED and _verify_session_cookie():
        return redirect("/events")
    return render_template("login.html")


@app.route("/api/auth/methods", methods=["GET"])
def auth_methods():
    return jsonify({"email_code": EMAIL_OTP_ENABLED})


@app.route("/api/auth/send-code", methods=["POST"])
def send_code():
    if not EMAIL_OTP_ENABLED:
        return jsonify({"error": "Email sign-in is not configured"}), 400
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"error": "Valid email required"}), 400
    if ALLOWED_EMAILS and email not in ALLOWED_EMAILS:
        app.logger.warning("auth.send_code blocked email=%s", _mask_email_for_logs(email))
        return jsonify({"error": "This email is not allowed to sign in"}), 403
    now = time.time()
    with _otp_lock:
        last = _otp_email_last_send.get(email, 0)
        if now - last < OTP_RATE_EMAIL_SECONDS:
            app.logger.warning("auth.send_code rate_limited_email email=%s", _mask_email_for_logs(email))
            return jsonify({"error": "Please wait a few minutes before requesting another code"}), 429
        ip = request.remote_addr or "unknown"
        window_start = now - OTP_RATE_IP_WINDOW
        _otp_ip_sends.setdefault(ip, [])
        _otp_ip_sends[ip] = [t for t in _otp_ip_sends[ip] if t > window_start]
        if len(_otp_ip_sends[ip]) >= OTP_RATE_IP_MAX:
            app.logger.warning("auth.send_code rate_limited_ip ip=%s", ip)
            return jsonify({"error": "Too many requests; try again later"}), 429
        _otp_ip_sends[ip].append(now)
        import random
        code = "".join(str(random.randint(0, 9)) for _ in range(6))
        _otp_store[email] = {"code": code, "expires_at": now + OTP_CODE_EXPIRY_SECONDS}
        _otp_email_last_send[email] = now
    try:
        _send_otp_email(email, code)
        app.logger.info("auth.send_code success email=%s", _mask_email_for_logs(email))
    except Exception as e:
        with _otp_lock:
            _otp_store.pop(email, None)
        app.logger.error("auth.send_code failed email=%s error=%s", _mask_email_for_logs(email), str(e))
        return jsonify({"error": str(e) or "Failed to send email; try again later"}), 500
    return jsonify({"ok": True, "message": "Check your email for the code"})


@app.route("/api/auth/verify-code", methods=["POST"])
def verify_code():
    if not EMAIL_OTP_ENABLED:
        return jsonify({"error": "Email sign-in is not configured"}), 400
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    code = (data.get("code") or "").strip()
    if not email or not code:
        return jsonify({"error": "Email and code required"}), 400
    now = time.time()
    with _otp_lock:
        entry = _otp_store.get(email)
        if not entry:
            app.logger.warning("auth.verify_code missing email=%s", _mask_email_for_logs(email))
            return jsonify({"error": "Invalid or expired code"}), 401
        if now > entry["expires_at"]:
            del _otp_store[email]
            app.logger.warning("auth.verify_code expired email=%s", _mask_email_for_logs(email))
            return jsonify({"error": "Code has expired; request a new one"}), 401
        if entry["code"] != code:
            app.logger.warning("auth.verify_code invalid email=%s", _mask_email_for_logs(email))
            return jsonify({"error": "Invalid code"}), 401
        del _otp_store[email]
    app.logger.info("auth.verify_code success email=%s", _mask_email_for_logs(email))
    resp = jsonify({"ok": True})
    return _set_session_cookie(resp)


@app.route("/api/logout", methods=["POST"])
def logout():
    resp = jsonify({"ok": True})
    resp.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return resp


@app.route("/api/ping")
@app.route("/ping")
def ping():
    return jsonify({"ping": "pong", "app": "dm-cvent"})


@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "app": "dm-cvent",
        "auth_enabled": AUTH_ENABLED,
        "otp_enabled": EMAIL_OTP_ENABLED,
    })


def _get_speaker_event_answer(attendee: dict) -> str:
    """
    Get answer to "Which event are you participating as a speaker?".
    Returns event name (matched to one of the events), "both", or "".
    We match by question text (wording), not question ID: the wording is stable across events
    while the question ID may change per event.
    """
    answers = attendee.get("answers") or []
    for a in answers:
        qtext = (a.get("question_text") or "").strip().lower()
        if "participating as a speaker" in qtext or "which event" in qtext:
            val = a.get("value")
            if isinstance(val, list):
                val = " ".join(str(x) for x in val if x).strip()
            else:
                val = (val or "").strip()
            if not val:
                return ""
            if val.lower() == "both":
                return "both"
            return val
    return ""


def _build_deal_plan(
    attendee: dict,
    order: dict,
    event_associations: list,
    attendee_exists: bool = False,
    sponsor_associations: list = None,
    training: bool = False,
    quantity_item_product_mappings: dict = None,
) -> dict:
    """
    Determine if deal(s) would be created and build plan (1 deal per event, revenue split when multiple).
    Returns {
        "registration_status": str,
        "deal_conditions_met": bool,
        "deal_conditions": { "is_accepted", "has_positive_amount", "ref_no_delsale" },
        "deal_plan": [ { "event_id", "event_name", "amount", "dealname", "properties" }, ... ],
    }
    """
    reg_status = (attendee.get("registration_status") or "").strip()
    is_accepted = reg_status.lower() == "accepted"

    def _to_float(v, default: float = 0.0) -> float:
        if v in ("", None):
            return default
        try:
            return float(str(v).replace(",", ""))
        except (TypeError, ValueError):
            return default

    # Quantity + tax allocation inputs (provided by hubspot_sync_attendee step builder).
    quantity_items_current = order.get("quantity_items_current") or {}
    quantity_amount_ordered = _to_float(order.get("quantity_amount_ordered"), 0.0)
    tax_admission_current = _to_float(order.get("tax_admission_current"), 0.0)
    tax_admission_total = _to_float(order.get("tax_admission_total"), 0.0)
    tax_quantity_current_total = 0.0
    if isinstance(quantity_items_current, dict):
        for _qi_id, _qi in (quantity_items_current or {}).items():
            if isinstance(_qi, dict):
                tax_quantity_current_total += _to_float(_qi.get("tax"), 0.0)

    amount_ordered_raw = order.get("amount_ordered") or ""
    amount_ordered = None
    if amount_ordered_raw not in ("", None):
        try:
            n = float(str(amount_ordered_raw).replace(",", ""))
            if n == n:  # not NaN
                amount_ordered = n
        except (TypeError, ValueError):
            pass
    orders_amounts = order.get("orders_amounts") or []
    orders_count = len(orders_amounts) or int((order.get("orders_count") or 0))
    total_amount_ordered = order.get("total_amount_ordered")
    if total_amount_ordered is None:
        try:
            total_amount_ordered = float(order.get("total_amount_ordered", 0) or 0)
        except (TypeError, ValueError):
            total_amount_ordered = amount_ordered or 0
    if (not total_amount_ordered or total_amount_ordered == 0) and orders_amounts:
        total_amount_ordered = sum(orders_amounts)

    has_positive_amount = (
        (amount_ordered is not None and amount_ordered > 0)
        or (quantity_amount_ordered > 0)
    )

    reference_id = (attendee.get("reference_id") or "").strip()
    ref_no_delsale = "delsale" not in reference_id.lower()

    all_met = is_accepted and has_positive_amount and ref_no_delsale
    first = (attendee.get("first_name") or "").strip()
    last = (attendee.get("last_name") or "").strip()
    full_name = f"{first} {last}".strip() or "Unknown Attendee"

    reg_type = (attendee.get("registration_type") or "").strip()
    reg_path = (attendee.get("registration_path") or "").strip()
    combined_type = f"{reg_type} {reg_path}".strip() or reg_type or reg_path
    is_speaker = (
        "Speaker" in combined_type and "Non Sponsor" in combined_type
    ) or ("Speaker Path" in combined_type and "Internal" in combined_type)
    is_sponsor_type = any(
        ea.get("label_name") in ("Sponsor Executive", "Sponsor Client")
        for ea in (event_associations or [])
    )

    deal_plan = []
    deal_scenario = "standard"
    speaker_upgrade_event_labels = None  # list of { event_id, event_name, label_id, label_name } when speaker/sponsor upgrade

    if all_met and event_associations:
        n_events = len(event_associations)

        # Paying delegate upgrade: already has attendee + deal for one event, now combi (2 events) + 2+ transactions
        if attendee_exists and n_events == 2 and orders_count >= 2 and not is_speaker and not is_sponsor_type:
            deal_scenario = "paying_delegate_upgrade"
            total = total_amount_ordered if total_amount_ordered else (amount_ordered or 0) * 2
            half = round(total / 2, 2) if total else None
            for i, ea in enumerate(event_associations):
                event_id = ea.get("event_id")
                event_name = (ea.get("full_name") or "").strip() or f"Event {event_id}"
                dealname = f"{full_name} - {event_name}"
                props = {
                    "dealname": dealname,
                    "pipeline": HUBSPOT_DEAL_PIPELINE,
                    "dealstage": HUBSPOT_DEAL_STAGE,
                    "company_name": (attendee.get("company_name") or "").strip(),
                    "country": (attendee.get("attendee_country") or "").strip(),
                    "cvent_admission_item": (attendee.get("admission_item") or "").strip(),
                    "cvent_cancelled": (order.get("cancelled") or "false").strip().lower(),
                    "cvent_invoice_number": (order.get("invoice_number") or "").strip(),
                    "cvent_reference_id": reference_id,
                    "primary_organization_type": (attendee.get("primary_organisation_type") or "").strip(),
                }
                if half is not None:
                    props["amount"] = half
                props = {k: v for k, v in props.items() if v not in ("", None)}
                deal_plan.append({
                    "event_id": event_id,
                    "event_name": event_name,
                    "amount": half,
                    "dealname": dealname,
                    "properties": props,
                    "action": "update_existing" if i == 0 else "create",
                })

        # Speaker upgrade: speaker type, already associated to one event, combi + additional transaction (paid or free)
        elif (
            attendee_exists
            and n_events == 2
            and is_speaker
            and orders_count >= 2
            and len(orders_amounts) >= 2
        ):
            # Use THIS step's amount (last in list); with phantom transactions the paid upgrade may be step 3 or 4, not step 2
            additional_amount = (orders_amounts[-1] or 0) if orders_amounts else 0
            deal_scenario = "speaker_upgrade"
            speaker_answer = _get_speaker_event_answer(attendee).strip()
            # Check Non Sponsor first so we never assign Speaker - Sponsor to a non-sponsor (e.g. type "Speaker Path" with path "Non Sponsor").
            if "Speaker" in combined_type and "Non Sponsor" in combined_type:
                speaker_label = (ASSOC_LABEL_SPEAKER_NON_SPONSOR, "Speaker - Non Sponsor")
            elif "Speaker Path" in combined_type and "Internal" in combined_type:
                speaker_label = (ASSOC_LABEL_SPEAKER_SPONSOR, "Speaker - Sponsor")
            else:
                speaker_label = (ASSOC_LABEL_SPEAKER_NON_SPONSOR, "Speaker - Non Sponsor")

            speaker_upgrade_event_labels = []
            for ea in event_associations:
                event_name = (ea.get("full_name") or "").strip() or f"Event {ea.get('event_id')}"
                # Speaker association: "both" → speaker on both; one event name → that event speaker, other = guest.
                if speaker_answer.lower() == "both":
                    speaker_upgrade_event_labels.append({
                        "event_id": ea.get("event_id"),
                        "event_name": event_name,
                        "label_id": speaker_label[0],
                        "label_name": speaker_label[1],
                    })
                elif speaker_answer and (speaker_answer.lower() in event_name.lower() or event_name.lower() in speaker_answer.lower()):
                    speaker_upgrade_event_labels.append({
                        "event_id": ea.get("event_id"),
                        "event_name": event_name,
                        "label_id": speaker_label[0],
                        "label_name": speaker_label[1],
                    })
                else:
                    # Not speaking at this event: speaker upgrade paid → Paying Delegate; else Guest (simple rule)
                    try:
                        paid = float(additional_amount or 0) > 0
                    except (TypeError, ValueError):
                        paid = False
                    if paid:
                        label_id, label_name = ASSOC_LABEL_PAYING_DELEGATE, "Paying Delegate"
                    else:
                        label_id, label_name = ASSOC_LABEL_DEALMAKERS_GUEST, "Dealmakers Guest"
                    speaker_upgrade_event_labels.append({
                        "event_id": ea.get("event_id"),
                        "event_name": event_name,
                        "label_id": label_id,
                        "label_name": label_name,
                    })

            # Deal should belong to the event where attendee becomes Paying Delegate.
            paying_delegate_label_id = ASSOC_LABEL_PAYING_DELEGATE
            paid_event = next(
                (
                    e for e in (speaker_upgrade_event_labels or [])
                    if e.get("label_id") == paying_delegate_label_id
                ),
                None,
            )
            if paid_event:
                event_id = paid_event.get("event_id")
                event_name = (paid_event.get("event_name") or "").strip() or f"Event {event_id}"
            else:
                # Fallback to previous behavior when labels couldn't be resolved.
                new_event = event_associations[1] if len(event_associations) > 1 else event_associations[0]
                event_id = new_event.get("event_id")
                event_name = (new_event.get("full_name") or "").strip() or f"Event {event_id}"
            dealname = f"{full_name} - {event_name}"
            # Only create a new deal when the additional transaction has positive amount
            amt = round(additional_amount, 2) if additional_amount > 0 else None
            props = {
                "dealname": dealname,
                "pipeline": HUBSPOT_DEAL_PIPELINE,
                "dealstage": HUBSPOT_DEAL_STAGE,
                "company_name": (attendee.get("company_name") or "").strip(),
                "country": (attendee.get("attendee_country") or "").strip(),
                "cvent_admission_item": (attendee.get("admission_item") or "").strip(),
                "cvent_cancelled": (order.get("cancelled") or "false").strip().lower(),
                "cvent_invoice_number": (order.get("invoice_number") or "").strip(),
                "cvent_reference_id": reference_id,
                "primary_organization_type": (attendee.get("primary_organisation_type") or "").strip(),
            }
            if amt is not None:
                props["amount"] = amt
            props = {k: v for k, v in props.items() if v not in ("", None)}
            deal_plan.append({
                "event_id": event_id,
                "event_name": event_name,
                "amount": amt,
                "dealname": dealname,
                "properties": props,
                "action": "create",
            })

        # Sponsor upgrade: Sponsor Client/Executive paid for admission item. Sponsor's event = Sponsor Exec/Client;
        # event sponsor is NOT associated to = Paying Delegate. Deal goes to the Paying Delegate event.
        elif (
            attendee_exists
            and n_events == 2
            and is_sponsor_type
            and orders_count >= 2
            and len(orders_amounts) >= 2
        ):
            # Use THIS step's amount (last in list); with phantom transactions the paid upgrade may be step 3+, not step 2
            additional_amount = (orders_amounts[-1] or 0) if orders_amounts else 0
            try:
                paid = float(additional_amount or 0) > 0
            except (TypeError, ValueError):
                paid = False
            if not paid:
                additional_amount = None  # skip deal creation below
            deal_scenario = "sponsor_upgrade"
            pay_label = (ASSOC_LABEL_PAYING_DELEGATE, "Paying Delegate")
            # Get sponsor label from first event that has it; collect events the sponsor(s) are linked to
            sponsor_label_ea = next(
                (ea for ea in event_associations if ea.get("label_name") in ("Sponsor Executive", "Sponsor Client")),
                None,
            )
            sponsor_label = (
                (sponsor_label_ea["label_id"], sponsor_label_ea["label_name"])
                if sponsor_label_ea
                else pay_label
            )
            # Rule 1: event sponsor IS associated to → sponsor label. Rule 2: event sponsor NOT associated to + paid → Paying Delegate.
            all_event_ids_build = {str(ea.get("event_id")) for ea in event_associations if ea.get("event_id")}
            api_event_ids_build: set = set()
            if training:
                if event_associations and sponsor_associations:
                    api_event_ids_build.add(str(event_associations[0].get("event_id", "")))
            else:
                for s in (sponsor_associations or []):
                    sid = s.get("id")
                    if not sid or str(sid).startswith("training-sponsor-"):
                        continue
                    for eid in _hubspot_events_for_sponsor(str(sid)):
                        api_event_ids_build.add(str(eid))
            sponsor_linked_event_ids_build = set(api_event_ids_build)
            if sponsor_linked_event_ids_build.intersection(all_event_ids_build):
                # Same as 2b: use complement so sponsor-linked = our events minus API set.
                sponsor_linked_event_ids_build = all_event_ids_build - api_event_ids_build
            speaker_upgrade_event_labels = []
            paying_delegate_event_ea = None  # deal goes to this event when paid
            if not api_event_ids_build:
                # Sponsor has no linked events; all events get sponsor label (no Paying Delegate, no deal).
                for ea in event_associations:
                    event_id_val = ea.get("event_id")
                    event_name_val = (ea.get("full_name") or "").strip() or f"Event {event_id_val}"
                    speaker_upgrade_event_labels.append({
                        "event_id": event_id_val,
                        "event_name": event_name_val,
                        "label_id": sponsor_label[0],
                        "label_name": sponsor_label[1],
                    })
            else:
                for ea in event_associations:
                    event_id_val = ea.get("event_id")
                    event_id_str = str(event_id_val)
                    event_name_val = (ea.get("full_name") or "").strip() or f"Event {event_id_val}"
                    if event_id_str in sponsor_linked_event_ids_build:
                        label_id, label_name = sponsor_label[0], sponsor_label[1]
                    else:
                        if paid:
                            label_id, label_name = pay_label[0], pay_label[1]
                            paying_delegate_event_ea = ea
                        else:
                            label_id, label_name = sponsor_label[0], sponsor_label[1]
                    speaker_upgrade_event_labels.append({
                        "event_id": event_id_val,
                        "event_name": event_name_val,
                        "label_id": label_id,
                        "label_name": label_name,
                    })
            # Deal only when they paid; deal is for the event they are Paying Delegate for
            if paid and paying_delegate_event_ea:
                new_event = paying_delegate_event_ea
                event_id = new_event.get("event_id")
                event_name = (new_event.get("full_name") or "").strip() or f"Event {event_id}"
                dealname = f"{full_name} - {event_name}"
                amt = round(additional_amount, 2) if additional_amount is not None else None
                props = {
                    "dealname": dealname,
                    "pipeline": HUBSPOT_DEAL_PIPELINE,
                    "dealstage": HUBSPOT_DEAL_STAGE,
                    "company_name": (attendee.get("company_name") or "").strip(),
                    "country": (attendee.get("attendee_country") or "").strip(),
                    "cvent_admission_item": (attendee.get("admission_item") or "").strip(),
                    "cvent_cancelled": (order.get("cancelled") or "false").strip().lower(),
                    "cvent_invoice_number": (order.get("invoice_number") or "").strip(),
                    "cvent_reference_id": reference_id,
                    "primary_organization_type": (attendee.get("primary_organisation_type") or "").strip(),
                }
                if amt is not None:
                    props["amount"] = amt
                props = {k: v for k, v in props.items() if v not in ("", None)}
                deal_plan.append({
                    "event_id": event_id,
                    "event_name": event_name,
                    "amount": amt,
                    "dealname": dealname,
                    "properties": props,
                    "action": "create",
                })

        # Standard: split across all events
        else:
            n = n_events
            amount_each = (amount_ordered / n) if n and amount_ordered is not None else None
            for ea in event_associations:
                event_id = ea.get("event_id")
                event_name = (ea.get("full_name") or "").strip() or f"Event {event_id}"
                dealname = f"{full_name} - {event_name}"
                props = {
                    "dealname": dealname,
                    "pipeline": HUBSPOT_DEAL_PIPELINE,
                    "dealstage": HUBSPOT_DEAL_STAGE,
                    "company_name": (attendee.get("company_name") or "").strip(),
                    "country": (attendee.get("attendee_country") or "").strip(),
                    "cvent_admission_item": (attendee.get("admission_item") or "").strip(),
                    "cvent_cancelled": (order.get("cancelled") or "false").strip().lower(),
                    "cvent_invoice_number": (order.get("invoice_number") or "").strip(),
                    "cvent_reference_id": reference_id,
                    "primary_organization_type": (attendee.get("primary_organisation_type") or "").strip(),
                }
                if amount_each is not None:
                    props["amount"] = round(amount_each, 2)
                props = {k: v for k, v in props.items() if v not in ("", None)}
                deal_plan.append({
                    "event_id": event_id,
                    "event_name": event_name,
                    "amount": amount_each,
                    "dealname": dealname,
                    "properties": props,
                    "action": "create",
                })

    # Allocate tax across planned admission deals, then create quantity deals (net amounts only).
    tax_breakdown = {
        "tax_admission_current": tax_admission_current,
        "tax_admission_total": tax_admission_total,
        "tax_quantity_current_total": tax_quantity_current_total,
    }

    if all_met and event_associations:
        def _allocate_by_ratio(total, ratios):
            # ratios should sum to 1; allocate with simple rounding remainder to the last bucket.
            total = float(total or 0.0)
            allocations = []
            running = 0.0
            for i, r in enumerate(ratios):
                r = float(r or 0.0)
                if i == len(ratios) - 1:
                    allocations.append(round(total - running, 2))
                else:
                    a = round(total * r, 2)
                    allocations.append(a)
                    running += a
            return allocations

        # Admission deals are whatever the scenario logic added to deal_plan at this point.
        admission_deals = [d for d in deal_plan if (d.get("component") != "quantity")]
        admission_tax_to_allocate = tax_admission_total if deal_scenario == "paying_delegate_upgrade" else tax_admission_current

        admission_net_total = sum(
            (float(d.get("amount") or 0.0) if d.get("amount") is not None else 0.0)
            for d in admission_deals
        )

        if admission_deals:
            # If all admission amounts are 0, still split by event/deal count to avoid losing tax.
            admission_ratios = []
            if admission_net_total > 0:
                for d in admission_deals:
                    admission_ratios.append((float(d.get("amount") or 0.0) / admission_net_total) if admission_net_total else 0.0)
            else:
                admission_ratios = [1.0 / len(admission_deals) for _ in admission_deals]

            tax_allocs = _allocate_by_ratio(admission_tax_to_allocate, admission_ratios)
            for i, d in enumerate(admission_deals):
                tax_amt = tax_allocs[i] if i < len(tax_allocs) else 0.0
                d["tax_amount"] = tax_amt
                d["component"] = "admission"
                d.setdefault("properties", {})
                d["properties"]["cvent_tax_amount"] = tax_amt

        # Quantity deals (separate deals per Cvent quantity item).
        quantity_items_map = quantity_items_current if isinstance(quantity_items_current, dict) else {}
        if quantity_items_map:
            # Build event allocations based on admission deal amounts (fallback uniform).
            admission_deals_net_for_ratio = sum((float(d.get("amount") or 0.0) for d in admission_deals), 0.0) if admission_deals else 0.0
            event_allocs = []
            if admission_deals and admission_deals_net_for_ratio > 0:
                for d in admission_deals:
                    amt = float(d.get("amount") or 0.0)
                    ratio = (amt / admission_deals_net_for_ratio) if admission_deals_net_for_ratio else 0.0
                    event_allocs.append({
                        "event_id": d.get("event_id"),
                        "event_name": d.get("event_name"),
                        "ratio": ratio,
                    })
            elif event_associations:
                n = len(event_associations)
                event_allocs = [
                    {
                        "event_id": ea.get("event_id"),
                        "event_name": (ea.get("full_name") or "").strip() or f"Event {ea.get('event_id')}",
                        "ratio": 1.0 / n if n else 0.0,
                    }
                    for ea in event_associations
                ]

            quantity_deals = []
            for qi_id, qi in quantity_items_map.items():
                try:
                    qi_net = float(qi.get("amount") or 0.0)
                except (TypeError, ValueError):
                    qi_net = 0.0
                try:
                    qi_tax = float(qi.get("tax") or 0.0)
                except (TypeError, ValueError):
                    qi_tax = 0.0
                if qi_net <= 0 and qi_tax <= 0:
                    continue

                qi_name = (qi.get("name") or "").strip() or f"Quantity {qi_id}"
                prod = (quantity_item_product_mappings or {}).get(str(qi_id)) if quantity_item_product_mappings else None
                product_id = str((prod or {}).get("id") or "").strip() if prod else ""
                product_name = (prod or {}).get("name") or "" if prod else ""
                product_sku = (prod or {}).get("sku") or "" if prod else ""
                if not product_id:
                    # Quantity item must be mapped in the UI to create an associated product deal.
                    continue

                ratios = [ea.get("ratio") for ea in event_allocs] if event_allocs else []
                net_allocs = _allocate_by_ratio(qi_net, ratios) if ratios else [qi_net]
                tax_allocs = _allocate_by_ratio(qi_tax, ratios) if ratios else [qi_tax]

                for j, ea in enumerate(event_allocs):
                    ev_amount = net_allocs[j] if j < len(net_allocs) else 0.0
                    ev_tax = tax_allocs[j] if j < len(tax_allocs) else 0.0
                    if ev_amount <= 0 and ev_tax <= 0:
                        continue
                    event_id = ea.get("event_id")
                    event_name = ea.get("event_name") or f"Event {event_id}"
                    dealname = f"{full_name} - {event_name} - {qi_name} (QI:{qi_id})"
                    props = {
                        "dealname": dealname,
                        "pipeline": HUBSPOT_DEAL_PIPELINE,
                        "dealstage": HUBSPOT_DEAL_STAGE,
                        "company_name": (attendee.get("company_name") or "").strip(),
                        "country": (attendee.get("attendee_country") or "").strip(),
                        "cvent_admission_item": (attendee.get("admission_item") or "").strip(),
                        "cvent_cancelled": (order.get("cancelled") or "false").strip().lower(),
                        "cvent_invoice_number": (order.get("invoice_number") or "").strip(),
                        "cvent_reference_id": reference_id,
                        "primary_organization_type": (attendee.get("primary_organisation_type") or "").strip(),
                        "cvent_tax_amount": ev_tax,
                    }
                    if ev_amount > 0:
                        props["amount"] = round(ev_amount, 2)
                    props = {k: v for k, v in props.items() if v not in ("", None)}

                    quantity_deals.append({
                        "event_id": event_id,
                        "event_name": event_name,
                        "amount": round(ev_amount, 2) if ev_amount > 0 else None,
                        "tax_amount": ev_tax,
                        "dealname": dealname,
                        "properties": props,
                        "action": "create",
                        "component": "quantity",
                        "product_id": product_id,
                        "product_name": product_name,
                        "product_sku": product_sku,
                    })

            # Append quantity deals after admission deals.
            deal_plan.extend(quantity_deals)

    return {
        "registration_status": reg_status,
        "deal_conditions_met": all_met,
        "deal_conditions": {
            "is_accepted": is_accepted,
            "has_positive_amount": has_positive_amount,
            "ref_no_delsale": ref_no_delsale,
        },
        "deal_plan": deal_plan,
        "deal_scenario": deal_scenario,
        "tax_breakdown": tax_breakdown,
        "speaker_upgrade_event_labels": speaker_upgrade_event_labels,
        "speaker_event_answer": _get_speaker_event_answer(attendee) if is_speaker else "",
    }


def lookup_attendee_cvent(cvent_event_id: str, cvent_attendee_id: str, access_token: str) -> dict:
    """
    Look up attendee from Cvent GET /ea/attendees with event.id filter.
    Requires event/attendees:read scope.
    """
    import urllib.parse
    api_base = f"{CV_API_BASE.rstrip('/')}/ea"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}

    try:
        filter_expr = urllib.parse.quote(f"event.id eq '{cvent_event_id}'")
        url = f"{api_base}/attendees?limit=200&filter={filter_expr}"
        r = requests.get(url, headers=headers, timeout=30)
        data = r.json() if r.text else {}
        items = data.get("data", []) if isinstance(data.get("data"), list) else []

        # Fetch event-questions for question text
        question_text_by_id = {}
        try:
            filt = urllib.parse.quote(f"event.id eq '{cvent_event_id}'")
            eq_r = requests.get(
                f"{api_base}/event-questions?limit=100&filter={filt}",
                headers=headers,
                timeout=15,
            )
            if eq_r.ok:
                eq_data = (eq_r.json() or {}).get("data") or []
                for eq in eq_data:
                    if isinstance(eq, dict) and eq.get("id"):
                        question_text_by_id[eq["id"]] = (eq.get("text") or "").strip()
        except Exception:
            pass

        # Question IDs used by HubSpot workflow step 6 (Cvent API TEST)
        Q_HEAR_ABOUT = "7f8e887d-b23b-40d3-8641-8d27341b197d"
        Q_PRIMARY_ORG = "5805ecce-a386-4254-96ce-5ce9f9d3751f"
        Q_COUNTRY = "894e0543-a6d7-413e-9d23-45c8a6b9d89b"
        Q_SPECIAL_REQ = "950aab16-3ad7-4dcc-8de3-eb58ea6f507f"

        def get_answer(answers_list, qid):
            for a in answers_list:
                if str((a.get("question") or {}).get("id", "")) == str(qid):
                    v = a.get("value")
                    if isinstance(v, list):
                        return "; ".join(str(x) for x in v if x)
                    return str(v) if v else ""
            return ""

        for item in items:
            if str((item or {}).get("id")) == str(cvent_attendee_id):
                contact = (item or {}).get("contact") or {}
                name = " ".join(filter(None, [contact.get("firstName"), contact.get("lastName")]))
                email = (contact.get("email") or "").strip()
                first_name = (contact.get("firstName") or "").strip()
                last_name = (contact.get("lastName") or "").strip()
                company = (contact.get("company") or "").strip()
                job_title = (contact.get("title") or "").strip()
                mobile_phone = (contact.get("mobilePhone") or "").strip()
                links = contact.get("_links") or {}
                linkedin_url = (links.get("linkedInUrl") or {}).get("href", "") or ""
                reg_type = (item.get("registrationType") or {}).get("name", "")
                reg_path = (item.get("registrationPath") or {}).get("name", "")
                adm_item_obj = item.get("admissionItem") or {}
                adm_item = adm_item_obj.get("name", "")
                adm_item_id = str(adm_item_obj.get("id", "")) if adm_item_obj.get("id") else ""
                answers_raw = item.get("answers") or []
                answers = []
                for a in answers_raw:
                    q = a.get("question") or {}
                    qid = q.get("id", "")
                    answers.append({
                        "question_id": qid,
                        "question_text": question_text_by_id.get(qid, ""),
                        "value": ", ".join(a.get("value") or []) if isinstance(a.get("value"), list) else str(a.get("value", "")),
                    })
                last_mod = item.get("lastModified") or item.get("attendeeLastModified") or ""
                created = item.get("created") or ""
                registered_at = item.get("registeredAt") or ""
                reference_id = (item.get("referenceId") or "").strip()
                confirmation_number = (item.get("confirmationNumber") or "").strip()
                return {
                    "attendee_exists": "Yes",
                    "matching_attendee_id": str(item.get("id")),
                    "attendee_lookup_count": 1,
                    "attendee_lookup_error": "",
                    "cvent_event_id": cvent_event_id,
                    "cvent_event_code": "",
                    "attendee_name": name or "",
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "company_name": company,
                    "job_title": job_title,
                    "mobile_phone": mobile_phone,
                    "linkedin_url": linkedin_url,
                    "registration_type": reg_type,
                    "registration_path": reg_path,
                    "admission_item": adm_item,
                    "admission_item_id": adm_item_id,
                    "registration_status": (item.get("status") or "").strip(),
                    "checked_in": "Yes" if item.get("checkedIn") is True else ("No" if item.get("checkedIn") is False else ""),
                    "reference_id": reference_id,
                    "confirmation_number": confirmation_number,
                    "registered_at": registered_at,
                    "hear_about_us": get_answer(answers_raw, Q_HEAR_ABOUT),
                    "primary_organisation_type": get_answer(answers_raw, Q_PRIMARY_ORG),
                    "attendee_country": get_answer(answers_raw, Q_COUNTRY),
                    "special_requirements": get_answer(answers_raw, Q_SPECIAL_REQ),
                    "answers": answers,
                    "last_modified": last_mod,
                    "created": created,
                    "source": "cvent",
                    "debug": "",
                }

        return {
            "attendee_exists": "No",
            "matching_attendee_id": "",
            "attendee_lookup_count": 0,
            "attendee_lookup_error": "Attendee not found in Cvent",
            "cvent_event_id": cvent_event_id,
            "attendee_name": "",
            "email": "",
            "first_name": "",
            "last_name": "",
            "company_name": "",
            "job_title": "",
            "mobile_phone": "",
            "linkedin_url": "",
            "registration_type": "",
            "registration_path": "",
            "admission_item": "",
            "admission_item_id": "",
            "registration_status": "",
            "checked_in": "",
            "reference_id": "",
            "confirmation_number": "",
            "registered_at": "",
            "hear_about_us": "",
            "primary_organisation_type": "",
            "attendee_country": "",
            "special_requirements": "",
            "answers": [],
            "last_modified": "",
            "created": "",
            "source": "cvent",
            "debug": "",
        }
    except Exception as e:
        return {
            "attendee_exists": "No",
            "matching_attendee_id": "",
            "attendee_lookup_count": 0,
            "attendee_lookup_error": str(e)[:500],
            "cvent_event_id": cvent_event_id,
            "attendee_name": "",
            "email": "",
            "first_name": "",
            "last_name": "",
            "company_name": "",
            "job_title": "",
            "mobile_phone": "",
            "linkedin_url": "",
            "registration_type": "",
            "registration_path": "",
            "admission_item": "",
            "admission_item_id": "",
            "registration_status": "",
            "checked_in": "",
            "reference_id": "",
            "confirmation_number": "",
            "registered_at": "",
            "hear_about_us": "",
            "primary_organisation_type": "",
            "attendee_country": "",
            "special_requirements": "",
            "answers": [],
            "last_modified": "",
            "created": "",
            "source": "cvent",
            "debug": str(e)[:2000],
        }


def fetch_cvent_token() -> str:
    """Get OAuth2 access token from Cvent."""
    if not CV_CLIENT_ID or not CV_CLIENT_SECRET:
        raise ValueError("Missing secrets: CV_CLIENT_ID / CV_CLIENT_SECRET")

    import base64
    token_url = f"{CV_API_BASE.rstrip('/')}/ea/oauth2/token"
    basic = base64.b64encode(f"{CV_CLIENT_ID}:{CV_CLIENT_SECRET}".encode()).decode()

    # Cvent requires grant_type and client_id in body; scope is optional
    res = requests.post(
        token_url,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic}",
        },
        data={
            "grant_type": "client_credentials",
            "client_id": CV_CLIENT_ID,
        },
        timeout=30,
    )
    text = res.text
    if not res.ok:
        raise RuntimeError(f"Token request failed ({res.status_code}): {text}")

    data = res.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("No access_token returned from Cvent.")
    return token


def fetch_order_data(cvent_event_id: str, cvent_attendee_id: str) -> dict:
    """
    Replicates workflow 2: Fetch order data from Cvent API.
    """
    if not cvent_event_id or not cvent_attendee_id:
        return {
            "error": "Both cvent_event_id and cvent_attendee_id are required for order lookup.",
            "user_journey": [],
            "discount_codes": [],
            "order_id": "",
            "order_number": "",
            "invoice_number": "",
            "order_type": "",
            "payment_method": "",
            "reference_number": "",
            "cancelled": "false",
            "amount_ordered": "",
            "amount_paid": "",
            "amount_due": "",
            "admission_item_name": "",
            "admission_item_amount_paid": "",
            "tax_name": "",
            "tax_amount_paid": "",
            "orders_count": "0",
            "order_items_count": "0",
            "orders": [],
            "order_items": [],
            "orders_json": "[]",
            "order_items_json": "[]",
            "total_amount_ordered": 0,
            "orders_amounts": [],
            "orders_admission_amounts": [],
            "orders_tax_amounts": [],
            "orders_quantity_items": [],
            "orders_quantity_net_amounts": [],
        }

    try:
        access_token = fetch_cvent_token()
    except Exception as e:
        return {
            "error": str(e),
            "user_journey": [],
            "discount_codes": [],
            "order_id": "",
            "order_number": "",
            "invoice_number": "",
            "order_type": "",
            "payment_method": "",
            "reference_number": "",
            "cancelled": "false",
            "amount_ordered": "",
            "amount_paid": "",
            "amount_due": "",
            "admission_item_name": "",
            "admission_item_amount_paid": "",
            "tax_name": "",
            "tax_amount_paid": "",
            "orders_count": "0",
            "order_items_count": "0",
            "orders": [],
            "order_items": [],
            "orders_json": "[]",
            "order_items_json": "[]",
            "total_amount_ordered": 0,
            "orders_amounts": [],
        }

    api_base = f"{CV_API_BASE.rstrip('/')}/ea"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}

    def fetch_json(url):
        r = requests.get(url, headers=headers, timeout=30)
        try:
            j = r.json() if r.text else {}
        except json.JSONDecodeError:
            j = {"raw": r.text}
        if not r.ok:
            raise RuntimeError(f"Cvent API failed ({r.status_code}) {url}: {json.dumps(j)}")
        return j

    def paginate(first_url, max_pages=200):
        all_data = []
        url = first_url
        for _ in range(max_pages):
            j = fetch_json(url)
            data = j.get("data", []) if isinstance(j.get("data"), list) else []
            all_data.extend(data)
            url = (j.get("paging") or {}).get("_links", {}).get("next", {}).get("href") or None
            if not url:
                break
        return all_data

    try:
        orders_url = f"{api_base}/events/{cvent_event_id}/orders?limit=50&filter=attendee.id%20eq%20%27{cvent_attendee_id}%27"
        items_url = f"{api_base}/events/{cvent_event_id}/orders/items?limit=50&filter=attendee.id%20eq%20%27{cvent_attendee_id}%27"

        orders = paginate(orders_url)
        items = paginate(items_url)

        order = orders[0] if orders else None
        # Collect discount codes from all orders (attendee may have multiple)
        discount_codes = []
        for o in orders:
            for d in (o or {}).get("discounts") or []:
                code = (d.get("code") or "").strip()
                name = (d.get("name") or "").strip()
                if code and code not in [x.get("code") for x in discount_codes]:
                    discount_codes.append({"code": code, "name": name or code})
        admission_item = next((i for i in items if (i or {}).get("product", {}).get("type") == "AdmissionItem"), None)
        tax_item = next((i for i in items if (i or {}).get("product", {}).get("type") == "Tax"), None)

        def _fmt_amt(val):
            if val is None:
                return ""
            try:
                n = float(val)
                return f"{n:,.2f}" if n != int(n) else f"{int(n):,}"
            except (TypeError, ValueError):
                return str(val) if val else ""

        def _fmt_date(iso_str):
            if not iso_str:
                return ""
            try:
                from datetime import datetime
                d = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
                return d.strftime("%d/%m/%Y %H:%M")
            except Exception:
                return str(iso_str)[:19]

        # Build user journey: Order 1, Order 2, ... with products and amounts.
        # Exclude cancelled orders so we only show active transactions (avoids odd counts when Cvent has amendments/cancellations).
        orders_sorted = sorted(orders, key=lambda o: (o or {}).get("created") or "")
        orders_active = [o for o in orders_sorted if not (o or {}).get("cancelled")]
        order = orders_active[0] if orders_active else (orders[0] if orders else None)
        total_amount_ordered = 0
        # Gross amounts (may include tax/quantity). Used for UI + phantom-detection.
        orders_amounts = []
        # Net breakdown used for deal creation (tax excluded; admission excludes quantity items).
        orders_admission_amounts = []
        orders_tax_amounts = []
        orders_quantity_items = []  # list (per order) of [{id,name,amount}]
        orders_quantity_net_amounts = []
        for o in orders_active:
            # Use amountPaid when present so paid upgrades are detected (amountOrdered can be 0 for comps)
            amt = (o or {}).get("amountPaid") or (o or {}).get("amountOrdered")
            try:
                n = float(amt) if amt not in (None, "") else 0
            except (TypeError, ValueError):
                n = 0
            orders_amounts.append(n)
            total_amount_ordered += n
        items_by_order = {}
        for it in items:
            raw_order = (it or {}).get("order")
            oid = (raw_order.get("id") if isinstance(raw_order, dict) else str(raw_order) if raw_order else None)
            if oid:
                items_by_order.setdefault(str(oid), []).append(it)
        order_num_by_id = {str((o or {}).get("id")): (o or {}).get("number", "") for o in orders}
        user_journey = []
        for idx, o in enumerate(orders_active, 1):
            oid = (o or {}).get("id")
            order_items = items_by_order.get(str(oid) if oid else "", [])
            products = []
            admission_item_for_order = None

            # Net breakdown for this order (tax excluded).
            admission_net_amount = 0.0
            tax_amount = 0.0
            quantity_items_map = {}  # qi_id -> {"id":..., "name":..., "amount": float}

            for it in order_items:
                amt_paid = (it or {}).get("amountPaid")
                amt_ordered = (it or {}).get("amountOrdered")
                amt_val = amt_paid if amt_paid is not None else amt_ordered
                ptype = ((it or {}).get("product") or {}).get("type") or ""
                ptype_norm = (ptype or "").strip().lower()
                # Best-effort numeric conversion.
                try:
                    amt_num = float(amt_val) if amt_val not in (None, "") else 0.0
                except (TypeError, ValueError):
                    amt_num = 0.0

                products.append({
                    "name": (it or {}).get("name", ""),
                    "type": ptype,
                    "amount": _fmt_amt(amt_val) or "",
                })

                if ptype == "Tax" or ptype_norm == "tax":
                    tax_amount += amt_num
                    continue

                if ptype == "QuantityItem" or ptype_norm == "quantity item":
                    qi_id = str(
                        (((it or {}).get("product") or {}).get("id"))
                        or ((it or {}).get("quantityItem") or {}).get("id")
                        or (it or {}).get("productId")
                        or (it or {}).get("id")
                        or ""
                    ).strip()
                    qi_name = ((it or {}).get("name") or (it or {}).get("product", {}).get("name") or "").strip()
                    if qi_id:
                        ex = quantity_items_map.get(qi_id) or {"id": qi_id, "name": qi_name, "amount": 0.0}
                        ex["name"] = ex["name"] or qi_name
                        ex["amount"] = (ex["amount"] or 0.0) + amt_num
                        quantity_items_map[qi_id] = ex
                    continue

                # Admission item + any other non-tax non-quantity items count towards admission-net.
                admission_net_amount += amt_num
                if ptype == "AdmissionItem" or ptype_norm == "admission item":
                    admission_item_for_order = it

            o_amount_paid = (o or {}).get("amountPaid")
            o_amount_ordered = (o or {}).get("amountOrdered")
            order_discounts = (o or {}).get("discounts") or []
            discount_str = ", ".join(
                f"{d.get('code', '')} ({d.get('name', '')})" if d.get("name") else str(d.get("code", ""))
                for d in order_discounts if d.get("code")
            ) or ""

            adm_id = ""
            adm_name = ""
            if admission_item_for_order:
                adm_id = str(
                    (admission_item_for_order.get("product") or {}).get("id")
                    or (admission_item_for_order.get("admissionItem") or {}).get("id")
                    or admission_item_for_order.get("id")
                    or ""
                )
                adm_name = (admission_item_for_order.get("name") or "").strip()

            quantity_net_amount = sum((v or {}).get("amount", 0.0) or 0.0 for v in quantity_items_map.values())
            quantity_lines = list(quantity_items_map.values())

            user_journey.append({
                "order_number": idx,
                "order_ref": (o or {}).get("number", ""),
                "order_id": oid or "",
                "created": _fmt_date((o or {}).get("created", "")),
                "amount_ordered": _fmt_amt(o_amount_ordered) if o_amount_ordered is not None else "",
                "amount_paid": _fmt_amt(o_amount_paid) if o_amount_paid is not None else "",
                "discount_codes": discount_str,
                "products": products,
                "admission_item_id": adm_id,
                "admission_item_name": adm_name,
                # For phantom detection + tax allocation.
                "quantity_net_amount": round(quantity_net_amount, 2),
                "tax_amount": round(tax_amount, 2),
            })

            orders_admission_amounts.append(round(admission_net_amount, 2))
            orders_tax_amounts.append(round(tax_amount, 2))
            orders_quantity_net_amounts.append(round(quantity_net_amount, 2))
            orders_quantity_items.append(quantity_lines)

        # Structured transactional data for display (include discount codes and admission item per order)
        orders_structured = []
        for o in orders_active:
            oo = o or {}
            oid = oo.get("id")
            order_items = items_by_order.get(str(oid) if oid else "", [])
            def _is_admission_item(it):
                ptype = ((it or {}).get("product") or {}).get("type") or ""
                return ptype == "AdmissionItem" or (ptype or "").strip().lower() == "admission item"
            admission_item_for_order = next((it for it in order_items if _is_admission_item(it)), None)
            adm_id = ""
            adm_name = ""
            if admission_item_for_order:
                adm_id = str(
                    (admission_item_for_order.get("product") or {}).get("id")
                    or (admission_item_for_order.get("admissionItem") or {}).get("id")
                    or admission_item_for_order.get("id")
                    or ""
                )
                adm_name = (admission_item_for_order.get("name") or "").strip()
            order_discounts = oo.get("discounts") or []
            discount_codes_str = ", ".join(
                f"{d.get('code', '')} ({d.get('name', '')})" if d.get("name") else str(d.get("code", ""))
                for d in order_discounts if d.get("code")
            ) or ""
            orders_structured.append({
                "id": oo.get("id", ""),
                "number": oo.get("number", ""),
                "invoice_number": oo.get("invoiceNumber", ""),
                "cancelled": oo.get("cancelled", False),
                "amount_ordered": _fmt_amt(oo.get("amountOrdered")),
                "amount_paid": _fmt_amt(oo.get("amountPaid")),
                "amount_due": _fmt_amt(oo.get("amountDue")),
                "created": _fmt_date(oo.get("created", "")),
                "discount_codes": discount_codes_str,
                "admission_item_id": adm_id,
                "admission_item_name": adm_name,
            })
        # Phantom transactions: same amount + same admission item as an earlier transaction (Cvent duplicate).
        # Mark duplicates so we ignore them for sync; first occurrence is kept, rest are phantom.
        seen_amount_admission = set()
        for i in range(len(orders_active)):
            amt = orders_amounts[i] if i < len(orders_amounts) else 0
            adm_id_uj = (user_journey[i].get("admission_item_id") or "").strip() if i < len(user_journey) else ""
            qty_net = (user_journey[i].get("quantity_net_amount") or 0) if i < len(user_journey) else 0
            tax_amt = (user_journey[i].get("tax_amount") or 0) if i < len(user_journey) else 0
            key = (round(float(amt or 0), 2), adm_id_uj, round(float(qty_net or 0), 2), round(float(tax_amt or 0), 2))
            phantom = key in seen_amount_admission
            if not phantom:
                seen_amount_admission.add(key)
            user_journey[i]["phantom"] = phantom
            if i < len(orders_structured):
                orders_structured[i]["phantom"] = phantom
        order_items_structured = []
        for it in items:
            i = it or {}
            oid = (i.get("order") or {}).get("id")
            order_items_structured.append({
                "order_id": oid or "",
                "order_number": order_num_by_id.get(str(oid), ""),
                "name": i.get("name", ""),
                "product_type": (i.get("product") or {}).get("type", ""),
                "quantity": i.get("quantity"),
                "amount_paid": _fmt_amt(i.get("amountPaid")),
                "amount_ordered": _fmt_amt(i.get("amountOrdered")),
            })

        total_admission_net_amount = sum(orders_admission_amounts or [])
        total_tax_amount_paid = sum(orders_tax_amounts or [])

        return {
            "error": "",
            "user_journey": user_journey,
            "discount_codes": discount_codes,
            "order_id": order.get("id", "") if order else "",
            "order_number": order.get("number", "") if order else "",
            "invoice_number": order.get("invoiceNumber", "") if order else "",
            "order_type": order.get("type", "") if order else "",
            "payment_method": order.get("paymentMethod", "") if order else "",
            "reference_number": order.get("referenceNumber", "") if order else "",
            "cancelled": "true" if order and order.get("cancelled") else "false",
            "amount_ordered": str(order.get("amountOrdered", "")) if order else "",
            "amount_paid": str(order.get("amountPaid", "")) if order else "",
            "amount_due": str(order.get("amountDue", "")) if order else "",
            "admission_item_name": admission_item.get("name", "") if admission_item else "",
            # Net admission amount (tax excluded; quantity items excluded from this bucket).
            "admission_item_amount_paid": f"{total_admission_net_amount:.2f}",
            "tax_name": tax_item.get("name", "") if tax_item else "",
            "tax_amount_paid": f"{total_tax_amount_paid:.2f}",
            "orders_count": str(len(orders_active)),
            "order_items_count": str(len(items)),
            "total_amount_ordered": total_amount_ordered,
            "orders_amounts": orders_amounts,
            "orders_admission_amounts": orders_admission_amounts,
            "orders_tax_amounts": orders_tax_amounts,
            "orders_quantity_items": orders_quantity_items,
            "orders_quantity_net_amounts": orders_quantity_net_amounts,
            "orders": orders_structured,
            "order_items": order_items_structured,
            "orders_json": json.dumps(orders)[:50000],
            "order_items_json": json.dumps(items)[:50000],
        }
    except Exception as e:
        return {
            "error": str(e),
            "user_journey": [],
            "discount_codes": [],
            "order_id": "",
            "order_number": "",
            "invoice_number": "",
            "order_type": "",
            "payment_method": "",
            "reference_number": "",
            "cancelled": "false",
            "amount_ordered": "",
            "amount_paid": "",
            "amount_due": "",
            "admission_item_name": "",
            "admission_item_amount_paid": "",
            "tax_name": "",
            "tax_amount_paid": "",
            "orders_count": "0",
            "order_items_count": "0",
            "orders": [],
            "order_items": [],
            "orders_json": "[]",
            "order_items_json": "[]",
            "total_amount_ordered": 0,
            "orders_amounts": [],
        }


@app.route("/")
@app.route("/events")
def events_page():
    return render_template("events.html")


@app.route("/api/events")
def list_events():
    """List all events from Cvent. Requires event/events:read scope."""
    try:
        token = fetch_cvent_token()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    api_base = f"{CV_API_BASE.rstrip('/')}/ea"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    def fetch_json(url):
        r = requests.get(url, headers=headers, timeout=30)
        try:
            j = r.json() if r.text else {}
        except json.JSONDecodeError:
            j = {"raw": r.text}
        if not r.ok:
            raise RuntimeError(f"Cvent API failed ({r.status_code}): {json.dumps(j)}")
        return j

    all_events = []
    url = f"{api_base}/events?limit=100"
    for _ in range(50):
        j = fetch_json(url)
        data = j.get("data", []) if isinstance(j.get("data"), list) else []
        all_events.extend(data)
        paging = j.get("paging") or {}
        links = paging.get("_links") or {}
        next_link = (links.get("next") or {}).get("href") if isinstance(links.get("next"), dict) else None
        if not next_link:
            break
        url = next_link

    return jsonify({"events": all_events, "count": len(all_events)})


@app.route("/api/account/questions")
def list_account_questions():
    """List all unique question codes across all events in the account."""
    try:
        token = fetch_cvent_token()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    api_base = f"{CV_API_BASE.rstrip('/')}/ea"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    def fetch_json(url):
        r = requests.get(url, headers=headers, timeout=30)
        try:
            j = r.json() if r.text else {}
        except json.JSONDecodeError:
            j = {"raw": r.text}
        if not r.ok:
            raise RuntimeError(f"Cvent API failed ({r.status_code}): {json.dumps(j)}")
        return j

    try:
        import urllib.parse
        # Get events
        all_events = []
        url = f"{api_base}/events?limit=100"
        for _ in range(20):
            j = fetch_json(url)
            data = j.get("data", []) if isinstance(j.get("data"), list) else []
            all_events.extend(data)
            paging = j.get("paging") or {}
            links = paging.get("_links") or {}
            next_link = (links.get("next") or {}).get("href") if isinstance(links.get("next"), dict) else None
            if not next_link:
                break
            url = next_link

        questions_seen = {}
        for ev in all_events[:30]:
            event_id = ev.get("id")
            if not event_id:
                continue
            try:
                filter_expr = urllib.parse.quote(f"event.id eq '{event_id}'")
                url = f"{api_base}/attendees?limit=50&filter={filter_expr}"
                j = fetch_json(url)
                data = j.get("data", []) if isinstance(j.get("data"), list) else []
                for a in data:
                    for ans in (a.get("answers") or []):
                        q = ans.get("question") or {}
                        qcode = (q.get("code") or q.get("questionCode") or "").strip()
                        if not qcode:
                            continue
                        if qcode not in questions_seen:
                            questions_seen[qcode] = {"code": qcode}
            except Exception:
                continue

        questions_list = sorted(questions_seen.values(), key=lambda x: x.get("code", ""))
        return jsonify({"questions": questions_list})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/question-sample")
def debug_question_sample():
    """Return raw question objects from attendees to inspect API structure. Tries multiple events until answers found."""
    try:
        token = fetch_cvent_token()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    api_base = f"{CV_API_BASE.rstrip('/')}/ea"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    try:
        import urllib.parse
        j = requests.get(f"{api_base}/events?limit=10", headers=headers, timeout=15).json()
        events = j.get("data") or []
        if not events:
            return jsonify({"message": "No events", "samples": [], "raw_attendee_sample": None})
        samples = []
        raw_attendee_sample = None
        events_tried = []
        for ev in events[:10]:
            event_id = ev.get("id")
            if not event_id:
                continue
            f = urllib.parse.quote(f"event.id eq '{event_id}'")
            j2 = requests.get(f"{api_base}/attendees?limit=20&filter={f}", headers=headers, timeout=15).json()
            attendees = j2.get("data") or []
            events_tried.append({"event_id": event_id, "event_name": ev.get("name", "")[:50], "attendee_count": len(attendees)})
            for a in attendees[:5]:
                answers = a.get("answers") or []
                if raw_attendee_sample is None and answers:
                    raw_attendee_sample = {"id": a.get("id"), "answers_count": len(answers), "answers": answers[:3]}
                for ans in answers[:5]:
                    q = ans.get("question") or {}
                    samples.append({"raw_question": q, "question_keys": list(q.keys()) if q else []})
            if samples:
                break
        return jsonify({
            "events_tried": events_tried,
            "samples": samples[:15],
            "raw_attendee_sample": raw_attendee_sample,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/event-questions/<event_id>")
def debug_event_questions(event_id):
    """Try List Event Questions API to see if it returns questions with codes."""
    try:
        token = fetch_cvent_token()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    api_base = f"{CV_API_BASE.rstrip('/')}/ea"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    import urllib.parse
    filt = urllib.parse.quote(f"event.id eq '{event_id}'")
    url = f"{api_base}/event-questions?limit=50&filter={filt}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        j = r.json() if r.text else {}
        data = j.get("data") or []
        all_keys = set()
        for q in data:
            if isinstance(q, dict):
                all_keys.update(q.keys())
        return jsonify({
            "event_id": event_id,
            "status": r.status_code,
            "count": len(data),
            "all_keys_in_response": sorted(all_keys),
            "sample_questions": data[:5],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/events/<event_id>/attendees")
def list_event_attendees(event_id):
    """List all attendees for an event. Uses GET /ea/attendees with event.id filter. Requires event/attendees:read scope.
    Quantity items for setup also use GET /ea/events/{id}/quantity-items when event/quantity-items:read is granted."""
    try:
        token = fetch_cvent_token()
    except Exception as e:
        return jsonify({"error": f"Token: {e}"}), 500

    api_base = f"{CV_API_BASE.rstrip('/')}/ea"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    def fetch_json(url):
        r = requests.get(url, headers=headers, timeout=30)
        try:
            j = r.json() if r.text else {}
        except json.JSONDecodeError:
            j = {"raw": r.text[:500]}
        if not r.ok:
            raise RuntimeError(f"Cvent API ({r.status_code}): {json.dumps(j)}")
        return j

    try:
        import urllib.parse
        filter_expr = urllib.parse.quote(f"event.id eq '{event_id}'")
        all_attendees = []
        url = f"{api_base}/attendees?limit=100&filter={filter_expr}"
        for _ in range(100):
            j = fetch_json(url)
            data = j.get("data", []) if isinstance(j.get("data"), list) else []
            all_attendees.extend(data)
            paging = j.get("paging") or {}
            links = paging.get("_links") or {}
            next_info = links.get("next")
            next_link = next_info.get("href") if isinstance(next_info, dict) else (next_info if isinstance(next_info, str) else None)
            if not next_link:
                break
            url = next_link

        # Fetch orders and order items to enrich with orders_count and admission_item
        orders_by_attendee = {}
        admission_by_attendee = {}
        all_order_items = []  # all line items across event (for admission items list)
        try:
            for endpoint, key in [("orders", "orders_by_attendee"), ("orders/items", "admission_by_attendee")]:
                all_data = []
                url = f"{api_base}/events/{event_id}/{endpoint}?limit=100"
                for _ in range(100):
                    j = fetch_json(url)
                    data = j.get("data", []) if isinstance(j.get("data"), list) else []
                    all_data.extend(data)
                    paging = j.get("paging") or {}
                    links = paging.get("_links") or {}
                    next_info = links.get("next")
                    next_link = next_info.get("href") if isinstance(next_info, dict) else (next_info if isinstance(next_info, str) else None)
                    if not next_link:
                        break
                    url = next_link

                if endpoint == "orders":
                    for o in all_data:
                        aid = (o.get("attendee") or {}).get("id")
                        if aid:
                            orders_by_attendee[aid] = orders_by_attendee.get(aid, 0) + 1
                else:
                    all_order_items.extend(all_data)
                    for item in all_data:
                        aid = (item.get("attendee") or {}).get("id")
                        if aid and (item.get("product") or {}).get("type") == "AdmissionItem":
                            admission_by_attendee[aid] = item.get("name", "")
        except Exception:
            pass

        for a in all_attendees:
            aid = a.get("id")
            a["_orders_count"] = orders_by_attendee.get(aid, 0)
            a["_admission_item"] = admission_by_attendee.get(aid, "")

        def _is_quantity_item_type(ptype: str) -> bool:
            t = (ptype or "").strip().lower()
            return t == "quantityitem" or t == "quantity item"

        # 1) Try to fetch ALL admission items / quantity items / products for the event from Cvent (event config, not just used by attendees)
        admission_items = {}
        quantity_items = {}
        event_filter = "event.id eq '" + event_id + "'"
        for try_url in [
            f"{api_base}/events/{event_id}/products?limit=100",
            f"{api_base}/admission-items?limit=100&filter={urllib.parse.quote(event_filter)}",
        ]:
            try:
                url = try_url
                for _ in range(50):
                    r = requests.get(url, headers=headers, timeout=15)
                    if not r.ok:
                        break
                    j = r.json() if r.text else {}
                    data = j.get("data", []) if isinstance(j.get("data"), list) else []
                    for p in data:
                        pid = (p.get("id") or p.get("productId") or "").strip()
                        ptype = (p.get("type") or (p.get("product") or {}).get("type") or "").strip()
                        if not pid:
                            continue
                        if ptype == "AdmissionItem" or (ptype or "").lower() == "admission item":
                            admission_items[str(pid)] = {
                                "id": str(pid),
                                "name": (p.get("name") or p.get("label") or (p.get("product") or {}).get("name") or "").strip(),
                                "code": (p.get("code") or (p.get("product") or {}).get("code") or "").strip(),
                            }
                        elif ptype == "QuantityItem" or _is_quantity_item_type(ptype):
                            quantity_items[str(pid)] = {
                                "id": str(pid),
                                "name": (p.get("name") or p.get("label") or (p.get("product") or {}).get("name") or "").strip(),
                                "code": (p.get("code") or (p.get("product") or {}).get("code") or "").strip(),
                                "product_type": "QuantityItem",
                            }
                    paging = j.get("paging") or {}
                    next_info = (paging.get("_links") or {}).get("next") or paging.get("next")
                    next_link = next_info.get("href") if isinstance(next_info, dict) else (next_info if isinstance(next_info, str) else None)
                    if not next_link:
                        break
                    url = next_link
                if admission_items:
                    break
            except Exception:
                pass

        # 1b) List Quantity Items API (OpenAPI: GET /events/{id}/quantity-items, scope event/quantity-items:read).
        #     Authoritative catalog for optional quantity products; merged with products + order lines below.
        try:
            qurl = f"{api_base}/events/{event_id}/quantity-items?limit=100"
            for _ in range(50):
                qr = requests.get(qurl, headers=headers, timeout=15)
                if not qr.ok:
                    break
                qj = qr.json() if qr.text else {}
                qdata = qj.get("data", []) if isinstance(qj.get("data"), list) else []
                for qi in qdata:
                    if not isinstance(qi, dict):
                        continue
                    pid = (qi.get("id") or "").strip()
                    if not pid:
                        continue
                    qname = (qi.get("name") or "").strip()
                    qcode = (qi.get("code") or "").strip()
                    if str(pid) not in quantity_items:
                        quantity_items[str(pid)] = {
                            "id": str(pid),
                            "name": qname,
                            "code": qcode,
                            "product_type": "QuantityItem",
                        }
                    else:
                        ex = quantity_items[str(pid)]
                        if not ex.get("name") and qname:
                            ex["name"] = qname
                        if not ex.get("code") and qcode:
                            ex["code"] = qcode
                qpaging = qj.get("paging") or {}
                next_token = qpaging.get("nextToken")
                next_info = (qpaging.get("_links") or {}).get("next")
                next_link = next_info.get("href") if isinstance(next_info, dict) else (next_info if isinstance(next_info, str) else None)
                if next_token:
                    qurl = f"{api_base}/events/{event_id}/quantity-items?limit=100&token={urllib.parse.quote(str(next_token))}"
                elif next_link:
                    if next_link.startswith("http://") or next_link.startswith("https://"):
                        qurl = next_link
                    elif next_link.startswith("/"):
                        qurl = f"{CV_API_BASE.rstrip('/')}{next_link}"
                    else:
                        qurl = next_link
                else:
                    break
        except Exception:
            pass

        # 2) Add admission items from attendees (current registration)
        for a in all_attendees:
            ai = (a.get("admissionItem") or {})
            if ai.get("id"):
                aid = str(ai["id"])
                if aid not in admission_items:
                    admission_items[aid] = {"id": aid, "name": ai.get("name", ""), "code": ai.get("code", "")}
                else:
                    if not admission_items[aid].get("name") and ai.get("name"):
                        admission_items[aid]["name"] = ai.get("name", "")
                    if not admission_items[aid].get("code") and ai.get("code"):
                        admission_items[aid]["code"] = ai.get("code", "")

        # 3) Add admission items from any order line item (so we show all that have been purchased, even if attendee list is partial)
        for item in all_order_items:
            prod = (item or {}).get("product") or {}
            ptype = (prod.get("type") or "").strip()
            if ptype == "AdmissionItem" or (ptype or "").lower() == "admission item":
                pid = str(prod.get("id") or item.get("productId") or item.get("id") or "")
                if pid and pid not in admission_items:
                    admission_items[pid] = {
                        "id": pid,
                        "name": (item.get("name") or prod.get("name") or "").strip(),
                        "code": (prod.get("code") or "").strip(),
                    }
                elif pid and admission_items.get(pid) and not admission_items[pid].get("name") and item.get("name"):
                    admission_items[pid]["name"] = (item.get("name") or "").strip()
            elif ptype == "QuantityItem" or _is_quantity_item_type(ptype):
                pid = str(prod.get("id") or item.get("productId") or item.get("id") or "")
                if pid and pid not in quantity_items:
                    quantity_items[pid] = {
                        "id": pid,
                        "name": (item.get("name") or prod.get("name") or "").strip(),
                        "code": (prod.get("code") or "").strip(),
                        "product_type": "QuantityItem",
                    }
                elif pid and quantity_items.get(pid) and not quantity_items[pid].get("name") and item.get("name"):
                    quantity_items[pid]["name"] = (item.get("name") or "").strip()

        admission_items_list = sorted(
            admission_items.values(),
            key=lambda x: (x.get("code") or "", x.get("name") or ""),
        )
        quantity_items_list = sorted(
            quantity_items.values(),
            key=lambda x: (x.get("code") or "", x.get("name") or ""),
        )

        # Collect ALL registration questions from event-questions API (not just answered ones)
        # Then enrich with sample_values from attendee answers
        registration_questions = {}
        try:
            filt = urllib.parse.quote(f"event.id eq '{event_id}'")
            eq_r = requests.get(
                f"{api_base}/event-questions?limit=200&filter={filt}",
                headers=headers,
                timeout=15,
            )
            if eq_r.ok:
                eq_data = (eq_r.json() or {}).get("data") or []
                for eq in eq_data:
                    if isinstance(eq, dict) and eq.get("id"):
                        qid = eq["id"]
                        registration_questions[qid] = {
                            "id": qid,
                            "key": qid,
                            "text": (eq.get("text") or "").strip(),
                            "code": (eq.get("code") or eq.get("questionCode") or "").strip(),
                            "sample_values": [],
                        }
        except Exception:
            pass

        # Add sample values from attendee answers
        for a in all_attendees:
            for ans in (a.get("answers") or []):
                q = ans.get("question") or {}
                qid = q.get("id")
                if not qid:
                    continue
                if qid not in registration_questions:
                    registration_questions[qid] = {
                        "id": qid,
                        "key": qid,
                        "text": "",
                        "code": "",
                        "sample_values": [],
                    }
                val = ans.get("value")
                vals = val if isinstance(val, list) else ([val] if val else [])
                val_str = ", ".join(str(v) for v in vals) if vals else ""
                if val_str and val_str not in registration_questions[qid]["sample_values"]:
                    registration_questions[qid]["sample_values"].append(val_str)
        for q in registration_questions.values():
            q["sample_values"] = q["sample_values"][:10]
        registration_questions_list = sorted(
            registration_questions.values(),
            key=lambda x: (x.get("text") or x.get("id") or ""),
        )

        # Collect registration types and registration paths separately (from event config + any on attendees)
        # Cvent may return data under "data", "results", or a named key; support multiple shapes and pagination
        def _extract_name(item: dict, code_fallback: bool = True) -> str:
            name = (
                (item.get("name") or item.get("label") or item.get("displayName") or "")
                or ((item.get("registrationType") or {}).get("name"))
                or ((item.get("type") or {}).get("name"))
                or ""
            )
            name = (name or "").strip()
            if name:
                return name
            if code_fallback:
                code = (item.get("code") or item.get("registrationTypeCode") or "").strip()
                if code and code != "—":
                    return code
            return ""

        def _fetch_paginated_list(endpoint_suffix: str) -> set:
            out = set()
            try:
                url = f"{api_base}/events/{event_id}/{endpoint_suffix}?limit=100"
                for _ in range(50):
                    r = requests.get(url, headers=headers, timeout=15)
                    if not r.ok:
                        break
                    j = r.json() if r.text else {}
                    data = j.get("data")
                    if not isinstance(data, list):
                        data = j.get("results")
                    if not isinstance(data, list):
                        data = j.get("registrationTypes") or j.get("registrationPaths")
                    if not isinstance(data, list):
                        data = []
                    for item in data:
                        if isinstance(item, dict):
                            name = _extract_name(item)
                            if name:
                                out.add(name)
                    paging = j.get("paging") or {}
                    links = paging.get("_links") or {}
                    next_info = links.get("next") or paging.get("next")
                    next_link = next_info.get("href") if isinstance(next_info, dict) else (next_info if isinstance(next_info, str) else None)
                    if not next_link:
                        break
                    if next_link.startswith("/"):
                        next_link = f"{CV_API_BASE.rstrip('/')}{next_link}"
                    url = next_link
            except Exception:
                pass
            return out

        registration_types_set = _fetch_paginated_list("registration-types")
        registration_paths_set = _fetch_paginated_list("registration-paths")
        for a in all_attendees:
            rt = (a.get("registrationType") or {}).get("name", "").strip()
            rp = (a.get("registrationPath") or {}).get("name", "").strip()
            if rt:
                registration_types_set.add(rt)
            if rp:
                registration_paths_set.add(rp)
        registration_types_list = sorted(registration_types_set)
        registration_paths_list = sorted(registration_paths_set)

        return jsonify({
            "attendees": all_attendees,
            "count": len(all_attendees),
            "admission_items": admission_items_list,
            "quantity_items": quantity_items_list,
            "registration_questions": registration_questions_list,
            "registration_types": registration_types_list,
            "registration_paths": registration_paths_list,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _hubspot_mappable_property_types(p):
    """Include string (text, textarea) and enumeration (dropdown, select, etc.) for question mapping."""
    t = p.get("type")
    if t == "string":
        return True
    if t == "enumeration":
        return True
    return False


def _hubspot_text_property_names() -> list:
    """Get list of mappable property names for attendee object (string + enumeration). Cached."""
    global _hubspot_prop_names_cache
    if not HUBSPOT_TOKEN:
        return []
    if _hubspot_prop_names_cache is not None:
        return _hubspot_prop_names_cache
    try:
        r = requests.get(
            f"https://api.hubapi.com/crm/v3/properties/{HUBSPOT_ATTENDEE_OBJECT}",
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
            timeout=10,
        )
        if not r.ok:
            return []
        results = r.json().get("results", [])
        names = [p["name"] for p in results if _hubspot_mappable_property_types(p)]
        _hubspot_prop_names_cache = names
        return names
    except Exception:
        return []


def fetch_hubspot_attendee(cvent_attendee_id: str) -> dict:
    """Fetch HubSpot attendee by cvent_attendee_id. Returns properties dict or empty."""
    if not HUBSPOT_TOKEN or not cvent_attendee_id:
        return {}
    try:
        prop_names = _hubspot_text_property_names()
        if not prop_names:
            prop_names = ["cvent_attendee_id", "attendee_name", "company_name"]
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_ATTENDEE_OBJECT}/search",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "cvent_attendee_id",
                        "operator": "EQ",
                        "value": cvent_attendee_id,
                    }]
                }],
                "limit": 1,
                "properties": prop_names,
            },
            timeout=15,
        )
        if not r.ok:
            return {}
        data = r.json()
        results = data.get("results", [])
        if not results:
            return {}
        return (results[0].get("properties") or {})
    except Exception:
        return {}


@app.route("/api/hubspot/attendee-properties")
def hubspot_attendee_properties():
    """Return mappable properties for HubSpot Attendee object (string + enumeration e.g. dropdown)."""
    if not HUBSPOT_TOKEN:
        return jsonify({"error": "HubSpot token (CustomCode) not configured"}), 500
    try:
        r = requests.get(
            f"https://api.hubapi.com/crm/v3/properties/{HUBSPOT_ATTENDEE_OBJECT}",
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
            timeout=15,
        )
        if not r.ok:
            return jsonify({"error": f"HubSpot API: {r.status_code}"}), 502
        data = r.json()
        results = data.get("results", [])
        props = [
            {"name": p.get("name"), "label": p.get("label", p.get("name", ""))}
            for p in results
            if _hubspot_mappable_property_types(p)
        ]
        props.sort(key=lambda x: (x.get("label") or "").lower())
        return jsonify({"properties": props})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _ensure_registration_questions_group() -> bool:
    """Create Registration Questions group if it doesn't exist. Returns True if group exists."""
    try:
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/properties/{HUBSPOT_ATTENDEE_OBJECT}/groups",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"name": HUBSPOT_REG_QUESTIONS_GROUP, "label": "Registration Questions"},
            timeout=10,
        )
        return r.ok or (r.status_code == 409)
    except Exception:
        return False


@app.route("/api/hubspot/create-property", methods=["POST"])
def hubspot_create_property():
    """Create a single-line text property on HubSpot Attendee object in Registration Questions group."""
    if not HUBSPOT_TOKEN:
        return jsonify({"error": "HubSpot token (CustomCode) not configured"}), 500
    data = request.get_json() or {}
    label = (data.get("label") or "").strip()
    if not label:
        return jsonify({"error": "label is required"}), 400
    import re
    base = re.sub(r"[^a-zA-Z0-9]+", "_", label).strip("_").lower() or "property"
    if not base[0].isalpha():
        base = "prop_" + base
    name = f"reg_{base}"
    try:
        global _hubspot_prop_names_cache
        _ensure_registration_questions_group()
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/properties/{HUBSPOT_ATTENDEE_OBJECT}",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "name": name,
                "label": label,
                "groupName": HUBSPOT_REG_QUESTIONS_GROUP,
                "type": "string",
                "fieldType": "text",
            },
            timeout=15,
        )
        if not r.ok:
            err = r.json() if r.text else {}
            msg = err.get("message", err.get("error", r.text or str(r.status_code)))
            return jsonify({"error": f"HubSpot API: {msg}"}), 400
        _hubspot_prop_names_cache = None
        created = r.json()
        return jsonify({
            "name": created.get("name", name),
            "label": created.get("label", label),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _ensure_event_admission_property() -> bool:
    """Create cvent_admission_item_ids property on Events object if it doesn't exist."""
    try:
        r = requests.get(
            f"https://api.hubapi.com/crm/v3/properties/{HUBSPOT_EVENTS_OBJECT}",
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
            timeout=10,
        )
        if r.ok:
            for p in r.json().get("results", []):
                if p.get("name") == HUBSPOT_EVENT_ADMISSION_PROP:
                    return True
        r2 = requests.post(
            f"https://api.hubapi.com/crm/v3/properties/{HUBSPOT_EVENTS_OBJECT}",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "name": HUBSPOT_EVENT_ADMISSION_PROP,
                "label": "Cvent Admission Item IDs",
                "groupName": "events_information",
                "type": "string",
                "fieldType": "textarea",
            },
            timeout=10,
        )
        return r2.ok or (r2.status_code == 409)
    except Exception:
        return False


@app.route("/api/hubspot/events")
def hubspot_events():
    """List HubSpot events sorted by start_date, with full_name and cvent_admission_item_ids."""
    if not HUBSPOT_TOKEN:
        return jsonify({"error": "HubSpot token (CustomCode) not configured"}), 500
    _ensure_event_admission_property()
    try:
        all_events = []
        after = None
        for _ in range(20):
            body = {
                "limit": 100,
                "sorts": [{"propertyName": "start_date", "direction": "ASCENDING"}],
                "properties": ["full_name", "start_date", HUBSPOT_EVENT_ADMISSION_PROP],
            }
            if after:
                body["after"] = after
            r = requests.post(
                f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_EVENTS_OBJECT}/search",
                headers={
                    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=15,
            )
            if not r.ok:
                return jsonify({"error": f"HubSpot API: {r.status_code}"}), 502
            data = r.json()
            results = data.get("results", [])
            for rec in results:
                props = rec.get("properties", {})
                adm_ids_raw = props.get(HUBSPOT_EVENT_ADMISSION_PROP) or ""
                adm_ids = [x.strip() for x in adm_ids_raw.split(",") if x.strip()]
                all_events.append({
                    "id": rec.get("id"),
                    "full_name": props.get("full_name", ""),
                    "start_date": props.get("start_date", ""),
                    "admission_item_ids": adm_ids,
                })
            paging = data.get("paging", {})
            after = (paging.get("next", {}) or {}).get("after")
            if not after:
                break
        return jsonify({"events": all_events})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/hubspot/products")
def hubspot_list_products():
    """List HubSpot CRM products (name, SKU), sorted alphabetically by name.
    Requires HubSpot scope: crm.objects.products.read (or legacy e-commerce product read)."""
    if not HUBSPOT_TOKEN:
        return jsonify({"error": "HubSpot token (CustomCode) not configured"}), 500
    out = []
    try:
        after = None
        for _ in range(200):
            params = {"limit": 100, "properties": "name,hs_sku"}
            if after:
                params["after"] = after
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/products",
                headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
                params=params,
                timeout=20,
            )
            if not r.ok:
                err_text = (r.text or "")[:800]
                hint = ""
                if r.status_code == 403:
                    hint = " Ensure the private app / token includes crm.objects.products.read."
                return (
                    jsonify(
                        {
                            "error": f"HubSpot products API: {r.status_code} {err_text}",
                            "hint": hint.strip(),
                            "products": out,
                        }
                    ),
                    502,
                )
            data = r.json()
            for rec in data.get("results", []) or []:
                props = rec.get("properties") or {}
                sku = (props.get("hs_sku") or props.get("sku") or "").strip()
                name = (props.get("name") or "").strip()
                out.append(
                    {
                        "id": str(rec.get("id", "")),
                        "name": name,
                        "sku": sku,
                    }
                )
            paging = data.get("paging") or {}
            next_info = paging.get("next")
            after = next_info.get("after") if isinstance(next_info, dict) else None
            if not after:
                break
        out.sort(key=lambda x: ((x.get("name") or "").lower(), (x.get("sku") or "").lower()))
        return jsonify({"products": out})
    except Exception as e:
        return jsonify({"error": str(e), "products": out}), 500


@app.route("/api/hubspot/associate-admission-item", methods=["POST"])
def hubspot_associate_admission_item():
    """Associate a Cvent admission item with HubSpot events. event_ids = list of HubSpot event IDs to associate."""
    if not HUBSPOT_TOKEN:
        return jsonify({"error": "HubSpot token (CustomCode) not configured"}), 500
    data = request.get_json() or {}
    admission_item_id = (data.get("admission_item_id") or "").strip()
    event_ids = data.get("event_ids", [])
    if not admission_item_id:
        return jsonify({"error": "admission_item_id is required"}), 400
    if not isinstance(event_ids, list):
        event_ids = [event_ids] if event_ids else []
    event_ids = set(str(x).strip() for x in event_ids if str(x).strip())
    try:
        _ensure_event_admission_property()
        all_events = []
        after = None
        for _ in range(20):
            params = {"limit": 100, "properties": HUBSPOT_EVENT_ADMISSION_PROP}
            if after:
                params["after"] = after
            r = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_EVENTS_OBJECT}",
                headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
                params=params,
                timeout=15,
            )
            if not r.ok:
                return jsonify({"error": f"HubSpot API: {r.status_code}"}), 502
            data = r.json()
            all_events.extend(data.get("results", []))
            after = (data.get("paging", {}).get("next", {}) or {}).get("after")
            if not after:
                break
        for rec in all_events:
            eid = rec.get("id")
            adm_raw = (rec.get("properties") or {}).get(HUBSPOT_EVENT_ADMISSION_PROP) or ""
            adm_list = [x.strip() for x in adm_raw.split(",") if x.strip()]
            if eid in event_ids:
                if admission_item_id not in adm_list:
                    adm_list.append(admission_item_id)
                else:
                    continue
            else:
                if admission_item_id in adm_list:
                    adm_list.remove(admission_item_id)
                else:
                    continue
            pr = requests.patch(
                f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_EVENTS_OBJECT}/{eid}",
                headers={
                    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={"properties": {HUBSPOT_EVENT_ADMISSION_PROP: ",".join(adm_list)}},
                timeout=10,
            )
            if not pr.ok:
                return jsonify({"error": f"HubSpot API update: {pr.text}"}), 400
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _parse_exec_client_discount_code(code: str) -> tuple:
    """
    Parse discount code for EXEC/CLIENT pattern: {ABC}{EXEC|CLIENT}{EVENTORFESTIVALCODE}
    Returns (role, event_or_festival_code) or (None, None) if not matching.
    """
    import re
    code = (code or "").strip().upper()
    if not code:
        return (None, None)
    m = re.match(r"^(.+?)(EXEC|CLIENT)(.+)$", code, re.IGNORECASE)
    if not m:
        return (None, None)
    role = m.group(2).upper()
    event_festival_code = (m.group(3) or "").strip()
    return (role, event_festival_code) if event_festival_code else (None, None)


def _hubspot_search_festival_by_code(festival_code: str) -> dict:
    """Search HubSpot festivals by festival_code. Returns first match or empty dict."""
    if not HUBSPOT_TOKEN or not festival_code:
        return {}
    try:
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_FESTIVALS_OBJECT}/search",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "festival_code",
                        "operator": "EQ",
                        "value": festival_code,
                    }]
                }],
                "limit": 1,
                "properties": ["festival_code"],
            },
            timeout=15,
        )
        if not r.ok:
            return {}
        data = r.json()
        results = data.get("results", [])
        return results[0] if results else {}
    except Exception:
        return {}


def _hubspot_search_event_by_event_code(event_code: str) -> dict:
    """Search HubSpot events by event_code. Returns first match or empty dict."""
    if not HUBSPOT_TOKEN or not event_code:
        return {}
    try:
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_EVENTS_OBJECT}/search",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "filterGroups": [{
                    "filters": [{
                        "propertyName": HUBSPOT_EVENT_CODE_PROP,
                        "operator": "EQ",
                        "value": event_code,
                    }]
                }],
                "limit": 1,
                "properties": ["full_name", HUBSPOT_EVENT_CODE_PROP],
            },
            timeout=15,
        )
        if not r.ok:
            return {}
        data = r.json()
        results = data.get("results", [])
        return results[0] if results else {}
    except Exception:
        return {}


def _hubspot_festivals_for_event(event_id: str) -> list:
    """Get HubSpot festivals associated to an event (event → festival). Returns list of {id, festival_code}."""
    if not HUBSPOT_TOKEN or not event_id:
        return []
    try:
        r = requests.get(
            f"https://api.hubapi.com/crm/v4/objects/{HUBSPOT_EVENTS_OBJECT}/{event_id}/associations/{HUBSPOT_FESTIVALS_OBJECT}",
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
            timeout=15,
        )
        if not r.ok:
            return []
        data = r.json()
        results = data.get("results", [])
        festival_ids = []
        seen = set()
        for a in results:
            for fid in [a.get("toObjectId"), a.get("id")]:
                if fid and str(fid) not in seen:
                    seen.add(str(fid))
                    festival_ids.append(str(fid))
            for t in (a.get("to") or []):
                tid = t.get("toObjectId") or t.get("id")
                if tid and str(tid) not in seen:
                    seen.add(str(tid))
                    festival_ids.append(str(tid))
        festivals = []
        for fid in festival_ids[:20]:
            fr = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_FESTIVALS_OBJECT}/{fid}",
                headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
                params={"properties": "festival_code"},
                timeout=10,
            )
            if fr.ok:
                j = fr.json()
                festivals.append({
                    "id": j.get("id"),
                    "festival_code": (j.get("properties") or {}).get("festival_code", ""),
                })
        return festivals
    except Exception:
        return []


def _hubspot_events_for_festival(festival_id: str) -> list:
    """Get HubSpot events associated to a festival. Returns list of {id, full_name}."""
    if not HUBSPOT_TOKEN or not festival_id:
        return []
    try:
        r = requests.get(
            f"https://api.hubapi.com/crm/v4/objects/{HUBSPOT_FESTIVALS_OBJECT}/{festival_id}/associations/{HUBSPOT_EVENTS_OBJECT}",
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
            timeout=15,
        )
        if not r.ok:
            return []
        data = r.json()
        results = data.get("results", [])
        event_ids = []
        seen = set()
        for a in results:
            for eid in [a.get("toObjectId"), a.get("id")]:
                if eid and str(eid) not in seen:
                    seen.add(str(eid))
                    event_ids.append(str(eid))
            for t in (a.get("to") or []):
                tid = t.get("toObjectId") or t.get("id")
                if tid and str(tid) not in seen:
                    seen.add(str(tid))
                    event_ids.append(str(tid))
        if not event_ids:
            return []
        # Fetch event details
        events = []
        for eid in event_ids[:50]:
            er = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_EVENTS_OBJECT}/{eid}",
                headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
                params={"properties": "full_name"},
                timeout=10,
            )
            if er.ok:
                j = er.json()
                events.append({"id": j.get("id"), "full_name": (j.get("properties") or {}).get("full_name", "")})
        return events
    except Exception:
        return []


def _hubspot_search_sponsor_by_discount_code(discount_code: str, prop: str = "exec_discount_code") -> dict:
    """
    Search sponsors where prop (exec_discount_code or client_discount_code) contains the full discount code.
    Field may be comma or space separated. Returns first match or empty dict.
    """
    if not HUBSPOT_TOKEN or not discount_code:
        return {}
    try:
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_SPONSORS_OBJECT}/search",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "filterGroups": [{
                    "filters": [{
                        "propertyName": prop,
                        "operator": "CONTAINS_TOKEN",
                        "value": discount_code,
                    }]
                }],
                "limit": 1,
                "properties": ["name", prop],
            },
            timeout=15,
        )
        if not r.ok:
            return {}
        data = r.json()
        results = data.get("results", [])
        return results[0] if results else {}
    except Exception:
        return {}


def _hubspot_events_for_sponsor(sponsor_id: str) -> list:
    """
    Get HubSpot event IDs associated to a sponsor (sponsor → events).
    Returns list of event id strings.
    """
    if not HUBSPOT_TOKEN or not sponsor_id:
        return []
    try:
        r = requests.get(
            f"https://api.hubapi.com/crm/v4/objects/{HUBSPOT_SPONSORS_OBJECT}/{sponsor_id}/associations/{HUBSPOT_EVENTS_OBJECT}",
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
            timeout=15,
        )
        if not r.ok:
            return []
        data = r.json()
        results = data.get("results", [])
        event_ids = []
        seen = set()
        for a in results:
            for eid in [a.get("toObjectId"), a.get("id")]:
                if eid and str(eid) not in seen:
                    seen.add(str(eid))
                    event_ids.append(str(eid))
            for t in (a.get("to") or []):
                tid = t.get("toObjectId") or t.get("id")
                if tid and str(tid) not in seen:
                    seen.add(str(tid))
                    event_ids.append(str(tid))
        return event_ids
    except Exception:
        return []


def _resolve_association_label_and_events(
    attendee: dict,
    order: dict,
    admission_item_id: str,
    training: bool = False,
    current_transaction_amount: Optional[float] = None,
) -> dict:
    """
    Resolve attendee→event associations with labels.
    When current_transaction_amount > 0, Sponsor/Speaker types get Paying Delegate (not guest/sponsor)
    on the "other" event (upgrade scenario).
    Returns {
        "event_associations": [{"event_id", "full_name", "label_id", "label_name"}],
        "festival_associations": [{"id", "festival_code"}],
        "sponsor_associations": [{"id", "name"}],
        "base_label": {"id", "name"},
        "warnings": [str],
    }
    """
    reg_type = (attendee.get("registration_type") or "").strip()
    warnings: list = []

    # Base label from attendee type only (we do not use registration path).
    # Paying Delegate only for the explicit PAYING_DELEGATE_TYPES list.
    reg_type_lc = reg_type.lower()
    if reg_type in PAYING_DELEGATE_TYPES:
        base_label = (ASSOC_LABEL_PAYING_DELEGATE, "Paying Delegate")
    elif reg_type in DEALMAKERS_GUEST_TYPES:
        base_label = (ASSOC_LABEL_DEALMAKERS_GUEST, "Dealmakers Guest")
    elif ("sponsor" in reg_type_lc and "executive" in reg_type_lc) or reg_type_lc in {"sponsor exec"}:
        base_label = (ASSOC_LABEL_SPONSOR_EXECUTIVE, "Sponsor Executive")
    elif ("sponsor" in reg_type_lc and "client" in reg_type_lc):
        base_label = (ASSOC_LABEL_SPONSOR_CLIENT, "Sponsor Client")
    elif "Speaker Path" in reg_type and "Internal" in reg_type:
        base_label = (ASSOC_LABEL_SPEAKER_SPONSOR, "Speaker - Sponsor")
    elif "Speaker" in reg_type and "Non Sponsor" in reg_type:
        base_label = (ASSOC_LABEL_SPEAKER_NON_SPONSOR, "Speaker - Non Sponsor")
    else:
        base_label = (ASSOC_LABEL_UNKNOWN, "Unknown")
        if reg_type:
            warnings.append(f"Registration type \"{reg_type}\" is not in known mappings; defaulting to Unknown.")

    event_by_id = {}
    festival_assocs = []
    sponsor_assocs = []

    # 1. Events from admission item (use base label)
    for e in _hubspot_events_for_admission_item(admission_item_id):
        eid = e.get("id")
        if eid:
            event_by_id[str(eid)] = {
                "event_id": eid,
                "full_name": e.get("full_name", ""),
                "label_id": base_label[0],
                "label_name": base_label[1],
            }

    # 2. EXEC/CLIENT discount codes:
    #    - still used to associate attendee to sponsor record
    #    - and to decide Sponsor-vs-Guest label on events from admission item
    #      (we no longer use sponsor→event associations to decide labels)
    discount_codes = order.get("discount_codes") or []
    discount_code_strs = [dc.get("code", "") for dc in discount_codes if dc.get("code")]

    sponsor_main_label_id = None
    sponsor_main_label_name = None
    has_exec_role = False
    has_client_role = False
    sponsor_by_festival_code = False
    sponsor_event_ids_from_code = set()

    for code in discount_code_strs:
        role, event_festival_code = _parse_exec_client_discount_code(code)
        if not role or not event_festival_code:
            continue

        if role == "EXEC":
            label_id, label_name = ASSOC_LABEL_SPONSOR_EXECUTIVE, "Sponsor Executive"
            sponsor_prop = "exec_discount_code"
            has_exec_role = True
        else:
            label_id, label_name = ASSOC_LABEL_SPONSOR_CLIENT, "Sponsor Client"
            sponsor_prop = "client_discount_code"
            has_client_role = True

        sponsor_main_label_id = label_id
        sponsor_main_label_name = label_name

        # Resolve code type: festival code means sponsor label on all admitted events;
        # event code means sponsor label only on matching admitted event(s), others guest.
        festival = _hubspot_search_festival_by_code(event_festival_code)
        if festival:
            sponsor_by_festival_code = True
            fid = festival.get("id")
            if fid:
                festival_assocs.append({
                    "id": fid,
                    "festival_code": (festival.get("properties") or {}).get("festival_code", ""),
                })
        else:
            event = _hubspot_search_event_by_event_code(event_festival_code)
            if event:
                eid = event.get("id")
                if eid:
                    sponsor_event_ids_from_code.add(str(eid))

        # Search sponsor by discount code (in training assume always found)
        sponsor = _hubspot_search_sponsor_by_discount_code(code, prop=sponsor_prop)
        if sponsor:
            sid = sponsor.get("id")
            if sid and not any(s.get("id") == sid for s in sponsor_assocs):
                sponsor_assocs.append({
                    "id": sid,
                    "name": (sponsor.get("properties") or {}).get("name", ""),
                })
        elif training:
            # Training: assume sponsor always found; use placeholder (associated to one event only, picked in deal plan)
            fake_id = f"training-sponsor-{code[:20]}" if code else "training-sponsor-1"
            if not any(s.get("id") == fake_id for s in sponsor_assocs):
                sponsor_assocs.append({"id": fake_id, "name": f"Sponsor (simulated for {code or 'code'})"})

    # 2b. Apply Sponsor-vs-Guest event labels using ONLY:
    #     - admission item events (already in event_by_id)
    #     - discount code type (festival vs event code)
    # Guest fallback requested by business rule.
    if sponsor_main_label_id is not None and event_by_id:
        guest_label = (ASSOC_LABEL_DEALMAKERS_GUEST, "Dealmakers Guest")
        if sponsor_by_festival_code:
            # Festival code => sponsor on all admitted events.
            for ev in event_by_id.values():
                ev["label_id"] = sponsor_main_label_id
                ev["label_name"] = sponsor_main_label_name
        else:
            # Event code => sponsor only on matching admitted event; other admitted events = guest.
            # If no admitted event matches code, all admitted events become guest.
            matched_any = False
            for eid_str, ev in list(event_by_id.items()):
                eid_val = str(ev.get("event_id", eid_str))
                if eid_val in sponsor_event_ids_from_code:
                    ev["label_id"] = sponsor_main_label_id
                    ev["label_name"] = sponsor_main_label_name
                    matched_any = True
                else:
                    ev["label_id"] = guest_label[0]
                    ev["label_name"] = guest_label[1]
            if not matched_any and sponsor_event_ids_from_code:
                warnings.append("EXEC/CLIENT event code did not match admitted event(s); using Dealmakers Guest for admitted event association(s).")

    # 2d. Speaker question (all-access ticket = 2+ events): "Which event are you participating as a speaker?"
    # There are only two speaker types: Speaker - Sponsor and Speaker - Non Sponsor (no generic "Speaker").
    # Only apply this logic when we have identified the attendee as one of these two from type + path.
    # - Answer "both" → associate as that speaker type on both events.
    # - Answer = one event name → that event = that speaker type, other event(s) = Dealmakers Guest.
    speaker_answer_raw = _get_speaker_event_answer(attendee).strip()
    if speaker_answer_raw and len(event_by_id) >= 2:
        reg_path = (attendee.get("registration_path") or "").strip()
        combined_type = f"{reg_type} {reg_path}".strip()
        # Check Non Sponsor first so we never assign Speaker - Sponsor to a non-sponsor.
        if "Speaker" in combined_type and "Non Sponsor" in combined_type:
            speaker_label = (ASSOC_LABEL_SPEAKER_NON_SPONSOR, "Speaker - Non Sponsor")
        elif "Speaker Path" in combined_type and "Internal" in combined_type:
            speaker_label = (ASSOC_LABEL_SPEAKER_SPONSOR, "Speaker - Sponsor")
        else:
            speaker_label = None  # Not a recognised speaker type; do not apply speaker-question logic
        if speaker_label is not None:
            speaker_answer = speaker_answer_raw.lower()
            # Simple rule: speaker upgrade paid (this transaction amount > 0) → "other" event = Paying Delegate; else Guest.
            try:
                paid = float(current_transaction_amount or 0) > 0
            except (TypeError, ValueError):
                paid = False
            other_event_label = (
                (ASSOC_LABEL_PAYING_DELEGATE, "Paying Delegate")
                if paid
                else (ASSOC_LABEL_DEALMAKERS_GUEST, "Dealmakers Guest")
            )
            if speaker_answer == "both":
                for ev in event_by_id.values():
                    ev["label_id"], ev["label_name"] = speaker_label[0], speaker_label[1]
            else:
                # Match answer to one event by name (first event whose full_name contains or is contained in answer)
                speaking_event_id = None
                for eid_str, ev in event_by_id.items():
                    event_name = (ev.get("full_name") or "").strip().lower()
                    if not event_name:
                        continue
                    if speaker_answer in event_name or event_name in speaker_answer:
                        speaking_event_id = eid_str
                        break
                if speaking_event_id is not None:
                    for eid_str, ev in event_by_id.items():
                        if eid_str == speaking_event_id:
                            ev["label_id"], ev["label_name"] = speaker_label[0], speaker_label[1]
                        else:
                            ev["label_id"], ev["label_name"] = other_event_label[0], other_event_label[1]

    # 3. For each event, get festivals the event is associated to; add attendee→festival and show in output
    festival_ids_seen = {str(f.get("id")) for f in festival_assocs if f.get("id")}
    event_associations = []
    for eid, ev in event_by_id.items():
        festivals_for_ev = _hubspot_festivals_for_event(str(ev.get("event_id", "")))
        ev["festivals"] = festivals_for_ev
        for f in festivals_for_ev:
            fid = str(f.get("id")) if f.get("id") else None
            if fid and fid not in festival_ids_seen:
                festival_ids_seen.add(fid)
                festival_assocs.append({"id": f.get("id"), "festival_code": f.get("festival_code", "")})
        event_associations.append(ev)

    return {
        "event_associations": event_associations,
        "festival_associations": festival_assocs,
        "sponsor_associations": sponsor_assocs,
        "base_label": {"id": base_label[0], "name": base_label[1]},
        "warnings": warnings,
    }


def _hubspot_events_for_admission_item(admission_item_id: str) -> list:
    """
    Find HubSpot events that have this Cvent admission item ID in their cvent_admission_item_ids.
    Returns list of {id, full_name}.
    """
    if not HUBSPOT_TOKEN or not admission_item_id:
        return []
    try:
        _ensure_event_admission_property()
        events = []
        after = None
        for _ in range(20):
            params = {"limit": 100, "properties": "full_name," + HUBSPOT_EVENT_ADMISSION_PROP}
            if after:
                params["after"] = after
            r = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_EVENTS_OBJECT}",
                headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
                params=params,
                timeout=15,
            )
            if not r.ok:
                return []
            data = r.json()
            for rec in data.get("results", []):
                props = rec.get("properties", {})
                adm_raw = props.get(HUBSPOT_EVENT_ADMISSION_PROP) or ""
                adm_ids = [x.strip() for x in adm_raw.split(",") if x.strip()]
                if admission_item_id in adm_ids:
                    events.append({
                        "id": rec.get("id"),
                        "full_name": props.get("full_name", ""),
                    })
            after = (data.get("paging", {}).get("next", {}) or {}).get("after")
            if not after:
                break
        return events
    except Exception:
        return []


def _hubspot_search_contact_by_email(email: str) -> dict:
    """Search HubSpot for contact by email. Returns first match or empty dict."""
    if not HUBSPOT_TOKEN or not email:
        return {}
    try:
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts/search",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email,
                    }]
                }],
                "limit": 1,
                "properties": ["email", "firstname", "lastname"],
            },
            timeout=15,
        )
        if not r.ok:
            return {}
        data = r.json()
        results = data.get("results", [])
        return results[0] if results else {}
    except Exception:
        return {}


def _hubspot_search_attendee_by_cvent_id(cvent_attendee_id: str) -> dict:
    """Search HubSpot for attendee by cvent_attendee_id. Returns first match or empty dict."""
    if not HUBSPOT_TOKEN or not cvent_attendee_id:
        return {}
    try:
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_ATTENDEE_OBJECT}/search",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "cvent_attendee_id",
                        "operator": "EQ",
                        "value": cvent_attendee_id,
                    }]
                }],
                "limit": 1,
                "properties": ["cvent_attendee_id", "attendee_name"],
            },
            timeout=15,
        )
        if not r.ok:
            return {}
        data = r.json()
        results = data.get("results", [])
        return results[0] if results else {}
    except Exception:
        return {}


def _hubspot_create_contact(properties: dict) -> dict:
    """Create a HubSpot contact. Returns created object or empty dict on failure."""
    if not HUBSPOT_TOKEN or not properties:
        return {}
    try:
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"properties": {k: str(v) for k, v in properties.items() if v not in ("", None)}},
            timeout=15,
        )
        if not r.ok:
            return {}
        return r.json()
    except Exception:
        return {}


def _hubspot_create_attendee(properties: dict) -> dict:
    """Create a HubSpot attendee (custom object). Returns created object or empty dict on failure."""
    if not HUBSPOT_TOKEN or not properties:
        return {}
    try:
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_ATTENDEE_OBJECT}",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"properties": {k: str(v) for k, v in properties.items() if v not in ("", None)}},
            timeout=15,
        )
        if not r.ok:
            return {}
        return r.json()
    except Exception:
        return {}


def _hubspot_create_attendee_with_error(properties: dict) -> tuple:
    """
    Create attendee and return (created_obj, error_message).
    error_message includes HubSpot validation details when available.
    """
    if not HUBSPOT_TOKEN or not properties:
        return ({}, "Missing HubSpot token or attendee properties")
    try:
        payload_props = {k: str(v) for k, v in properties.items() if v not in ("", None)}
        r = requests.post(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_ATTENDEE_OBJECT}",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"properties": payload_props},
            timeout=15,
        )
        if r.ok:
            return (r.json(), "")

        msg = ""
        try:
            j = r.json() if r.text else {}
            msg = (j.get("message") or "").strip()
            errs = j.get("errors") or []
            parts = []
            for e in errs[:8]:
                if not isinstance(e, dict):
                    continue
                en = (e.get("name") or "").strip()
                em = (e.get("message") or "").strip()
                ctx = e.get("context") or {}
                if isinstance(ctx, dict) and ctx:
                    ctx_str = ", ".join(f"{k}={v}" for k, v in ctx.items())
                    em = f"{em} ({ctx_str})" if em else ctx_str
                if en and em:
                    parts.append(f"{en}: {em}")
                elif em:
                    parts.append(em)
            if parts:
                msg = f"{msg} | " + " | ".join(parts) if msg else " | ".join(parts)
        except Exception:
            msg = (r.text or "")[:500]
        if not msg:
            msg = f"HTTP {r.status_code}"
        return ({}, f"HubSpot attendee create failed: {msg}")
    except Exception as e:
        return ({}, f"HubSpot attendee create exception: {str(e)}")


def _hubspot_update_attendee(attendee_id: str, properties: dict) -> bool:
    """Update HubSpot attendee properties. Returns True on success."""
    if not HUBSPOT_TOKEN or not attendee_id or not properties:
        return False
    try:
        r = requests.patch(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_ATTENDEE_OBJECT}/{attendee_id}",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"properties": {k: str(v) for k, v in properties.items() if v not in ("", None)}},
            timeout=15,
        )
        return r.ok
    except Exception:
        return False


def _hubspot_update_attendee_with_error(attendee_id: str, properties: dict) -> tuple:
    """
    Update attendee and return (ok, error_message).
    error_message includes HubSpot validation details when available.
    """
    if not HUBSPOT_TOKEN or not attendee_id or not properties:
        return (False, "Missing HubSpot token, attendee ID, or properties")
    try:
        payload_props = {k: str(v) for k, v in properties.items() if v not in ("", None)}
        r = requests.patch(
            f"https://api.hubapi.com/crm/v3/objects/{HUBSPOT_ATTENDEE_OBJECT}/{attendee_id}",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"properties": payload_props},
            timeout=15,
        )
        if r.ok:
            return (True, "")

        msg = ""
        try:
            j = r.json() if r.text else {}
            msg = (j.get("message") or "").strip()
            errs = j.get("errors") or []
            parts = []
            for e in errs[:8]:
                if not isinstance(e, dict):
                    continue
                en = (e.get("name") or "").strip()
                em = (e.get("message") or "").strip()
                ctx = e.get("context") or {}
                if isinstance(ctx, dict) and ctx:
                    ctx_str = ", ".join(f"{k}={v}" for k, v in ctx.items())
                    em = f"{em} ({ctx_str})" if em else ctx_str
                if en and em:
                    parts.append(f"{en}: {em}")
                elif em:
                    parts.append(em)
            if parts:
                msg = f"{msg} | " + " | ".join(parts) if msg else " | ".join(parts)
        except Exception:
            msg = (r.text or "")[:500]
        if not msg:
            msg = f"HTTP {r.status_code}"
        return (False, f"HubSpot attendee update failed: {msg}")
    except Exception as e:
        return (False, f"HubSpot attendee update exception: {str(e)}")


def _hubspot_put_association(
    from_type: str, from_id: str, to_type: str, to_id: str, association_type_id: int = None
) -> bool:
    """
    Create association between two objects. If association_type_id is set (e.g. attendee-event label),
    use labeled endpoint; else use default (unlabeled) association. Returns True on success.
    """
    if not HUBSPOT_TOKEN or not from_id or not to_id:
        return False
    try:
        if association_type_id is not None:
            url = f"https://api.hubapi.com/crm/v4/objects/{from_type}/{from_id}/associations/{to_type}/{to_id}"
            payload = [{
                "associationCategory": "USER_DEFINED",
                "associationTypeId": association_type_id,
            }]
            r = requests.put(
                url,
                headers={
                    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=15,
            )
            # Compatibility fallback: some portals may accept/require type id only.
            if not (r.ok or r.status_code in (200, 201)):
                r = requests.put(
                    url,
                    headers={
                        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json=[{"associationTypeId": association_type_id}],
                    timeout=15,
                )
        else:
            url = f"https://api.hubapi.com/crm/v4/objects/{from_type}/{from_id}/associations/default/{to_type}/{to_id}"
            r = requests.put(
                url,
                headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
                timeout=15,
            )
        return r.ok or r.status_code in (200, 201)
    except Exception:
        return False


def _hubspot_get_object_associations(from_type: str, from_id: str, to_type: str) -> list:
    """
    Get existing associations from one object to another type.
    Returns list of {"to_id": str, "association_type_id": int or None}.
    """
    if not HUBSPOT_TOKEN or not from_id or not to_type:
        return []
    try:
        url = f"https://api.hubapi.com/crm/v4/objects/{from_type}/{from_id}/associations/{to_type}"
        r = requests.get(url, headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"}, timeout=15)
        if not r.ok:
            return []
        data = r.json()
        results = data.get("results", [])
        out = []
        for item in results:
            to_id = item.get("toObjectId") or item.get("id")
            if not to_id and isinstance(item.get("to"), dict):
                to_id = item["to"].get("id")
            if not to_id:
                continue
            to_id = str(to_id)
            types = item.get("associationTypes") or item.get("types") or []
            if types:
                for t in types:
                    type_id = t.get("associationTypeId") or t.get("typeId")
                    out.append({"to_id": to_id, "association_type_id": type_id})
            else:
                out.append({"to_id": to_id, "association_type_id": None})
        return out
    except Exception:
        return []


def _hubspot_create_deal(properties: dict) -> dict:
    """Create a HubSpot deal. Returns created object or empty dict on failure."""
    if not HUBSPOT_TOKEN or not properties:
        return {}
    try:
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/deals",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "properties": {k: str(v) if v is not None else "" for k, v in properties.items()}
            },
            timeout=15,
        )
        if not r.ok:
            return {}
        return r.json()
    except Exception:
        return {}


def _hubspot_update_deal(deal_id: str, properties: dict) -> bool:
    """Update HubSpot deal properties. Returns True on success."""
    if not HUBSPOT_TOKEN or not deal_id or not properties:
        return False
    try:
        r = requests.patch(
            f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
            headers={
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"properties": {k: str(v) if v is not None else "" for k, v in properties.items()}},
            timeout=15,
        )
        return r.ok
    except Exception:
        return False


def _hubspot_search_deals_for_contact(contact_id: str, limit: int = 20) -> list:
    """Return list of deals associated to a contact (for finding existing deal to update)."""
    if not HUBSPOT_TOKEN or not contact_id:
        return []
    try:
        r = requests.get(
            f"https://api.hubapi.com/crm/v4/objects/contacts/{contact_id}/associations/deals",
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
            timeout=15,
        )
        if not r.ok:
            return []
        results = r.json().get("results", [])
        deal_ids = []
        for a in results:
            did = a.get("toObjectId") or a.get("id")
            if did and str(did) not in deal_ids:
                deal_ids.append(str(did))
        if not deal_ids:
            return []
        out = []
        for did in deal_ids[:limit]:
            dr = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/deals/{did}",
                headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
                params={"properties": "dealname,amount"},
                timeout=10,
            )
            if dr.ok:
                j = dr.json()
                out.append({
                    "id": j.get("id"),
                    "dealname": (j.get("properties") or {}).get("dealname", ""),
                    "amount": (j.get("properties") or {}).get("amount", ""),
                })
        return out
    except Exception:
        return []


def _admission_from_journey_entry(entry: dict) -> tuple:
    """
    Get (admission_item_name, admission_item_id) from a user_journey entry.
    Uses the same .products list the Purchase Journey displays, so step logic matches what the user sees.
    """
    if not entry:
        return ("", "")
    products = entry.get("products") or []
    adm_type_ok = lambda t: (t or "").strip().lower() in ("admissionitem", "admission item")
    adm_product = next((p for p in products if adm_type_ok(p.get("type"))), None)
    name = (adm_product.get("name") or "").strip() if adm_product else (entry.get("admission_item_name") or "").strip()
    aid = (entry.get("admission_item_id") or "").strip()
    return (name, aid)


def _build_attendee_properties(attendee: dict, order: dict, admission_item_override: str = None) -> dict:
    """
    Build HubSpot attendee properties matching workflow step 6 (Cvent API TEST).
    Returns dict of property name -> value (excluding empty/null).
    If admission_item_override is set (e.g. per-transaction admission item), use it for cvent_admission_item.
    """
    from datetime import datetime, timezone

    def _to_hubspot_date_ms(raw_val):
        """
        Convert Cvent date/time values into HubSpot date long (epoch ms at midnight UTC).
        HubSpot date properties reject non-midnight timestamps.
        """
        if not raw_val:
            return None
        s = str(raw_val).strip()
        if not s:
            return None
        try:
            # ISO datetime from Cvent, e.g. 2026-03-16T10:23:00Z
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc)
            y, m, d = dt.year, dt.month, dt.day
            return int(datetime(y, m, d, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        except Exception:
            pass
        try:
            # Date-only fallback, e.g. 03/16/2026
            dt = datetime.strptime(s, "%m/%d/%Y").replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None

    first = (attendee.get("first_name") or "").strip()
    last = (attendee.get("last_name") or "").strip()
    email = (attendee.get("email") or "").strip()
    cvent_id = (attendee.get("matching_attendee_id") or "").strip()
    attendee_name = f"{first} {last}".strip() or email or cvent_id

    reg_date_raw = attendee.get("registered_at") or ""
    cvent_reg_date = _to_hubspot_date_ms(reg_date_raw)

    amount_due_raw = (order.get("amount_due") or "").strip()
    cvent_amount_due = None
    if amount_due_raw:
        try:
            n = float(amount_due_raw.replace(",", ""))
            if n != 0:
                cvent_amount_due = n
        except (TypeError, ValueError):
            pass

    # Use only transaction-derived admission item when override is provided (even if ""); never attendee current for steps
    if admission_item_override is not None:
        cvent_admission_item = (admission_item_override or "").strip()
    else:
        cvent_admission_item = (attendee.get("admission_item") or "").strip()
    props = {
        "attendee_name": attendee_name,
        "company_name": (attendee.get("company_name") or "").strip(),
        "country": (attendee.get("attendee_country") or "").strip(),
        "cvent_admission_item": cvent_admission_item,
        "cvent_amount_due": cvent_amount_due,
        "cvent_attendee_id": cvent_id,
        "cvent_cancelled": (order.get("cancelled") or "false").strip().lower(),
        "cvent_confirmation_number": (attendee.get("confirmation_number") or "").strip(),
        "cvent_invoice_number": (order.get("invoice_number") or "").strip(),
        "cvent_reference_id": (attendee.get("reference_id") or "").strip(),
        "cvent_reg_date": cvent_reg_date,
        "cvent_reg_status": (attendee.get("registration_status") or "").strip(),
        "cvent_registration_type": (attendee.get("registration_type") or "").strip(),
        "email": email,
        "first_name": first,
        "last_name": last,
        "job_title": (attendee.get("job_title") or "").strip(),
        "linkedin_url": (attendee.get("linkedin_url") or "").strip(),
        "phone_number": (attendee.get("mobile_phone") or "").strip(),
        "how_did_you_hear": (attendee.get("hear_about_us") or "").strip(),
        "primary_organization_type": (attendee.get("primary_organisation_type") or "").strip(),
        "special_requirements": (attendee.get("special_requirements") or "").strip(),
    }
    # Remove empty strings and None
    return {k: v for k, v in props.items() if v not in ("", None)}


def _execute_sync_step(
    email: str,
    first_name: str,
    last_name: str,
    cvent_attendee_id: str,
    attendee_properties: dict,
    event_associations: list,
    festival_associations: list,
    sponsor_associations: list,
    deal_plan: list,
) -> dict:
    """
    Execute one sync step: ensure contact and attendee exist, update attendee props,
    create associations (attendee→event with label, attendee→festival, attendee→sponsor),
    create/update deals and associate contact→deal and attendee→deal.
    Returns {"contact_id", "attendee_id", "created_contact", "created_attendee", "actions", "errors"}.
    """
    result = {
        "contact_id": None,
        "attendee_id": None,
        "created_contact": False,
        "created_attendee": False,
        "actions": [],
        "errors": [],
    }
    if not email or not cvent_attendee_id:
        result["errors"].append("Missing email or cvent_attendee_id")
        return result

    # 1. Contact
    contact = _hubspot_search_contact_by_email(email)
    if contact:
        result["contact_id"] = str(contact.get("id", ""))
        result["actions"].append("Contact found in HubSpot")
    else:
        created = _hubspot_create_contact({
            "email": email,
            "firstname": first_name or "",
            "lastname": last_name or "",
        })
        if not created or not created.get("id"):
            result["errors"].append("Failed to create HubSpot contact")
            return result
        result["contact_id"] = str(created["id"])
        result["created_contact"] = True
        result["actions"].append("Created HubSpot contact")

    # 2. Attendee
    attendee_rec = _hubspot_search_attendee_by_cvent_id(cvent_attendee_id)
    if attendee_rec:
        result["attendee_id"] = str(attendee_rec.get("id", ""))
        result["actions"].append("Attendee record found in HubSpot")
        if attendee_properties and result["attendee_id"]:
            ok, err = _hubspot_update_attendee_with_error(result["attendee_id"], attendee_properties)
            if ok:
                result["actions"].append("Updated attendee properties")
            else:
                result["errors"].append(err or "Failed to update attendee properties")
    else:
        props = dict(attendee_properties) if attendee_properties else {}
        props["cvent_attendee_id"] = cvent_attendee_id
        if not props.get("attendee_name") and (first_name or last_name or email):
            props["attendee_name"] = f"{first_name} {last_name}".strip() or email
        created, create_err = _hubspot_create_attendee_with_error(props)
        if not created or not created.get("id"):
            result["errors"].append(create_err or "Failed to create HubSpot attendee")
            return result
        result["attendee_id"] = str(created["id"])
        result["created_attendee"] = True
        result["actions"].append("Created HubSpot attendee")
        if result["contact_id"]:
            _hubspot_put_association(
                "contacts", result["contact_id"],
                HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"],
                association_type_id=None,
            )
            result["actions"].append("Associated contact to attendee")

    if not result["attendee_id"]:
        return result

    existing_event_assocs = _hubspot_get_object_associations(
        HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"], HUBSPOT_EVENTS_OBJECT
    )
    existing_event_set = {(a["to_id"], a.get("association_type_id")) for a in existing_event_assocs}
    existing_festival_ids = {
        a["to_id"] for a in _hubspot_get_object_associations(
            HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"], HUBSPOT_FESTIVALS_OBJECT
        )
    }
    existing_sponsor_ids = {
        a["to_id"] for a in _hubspot_get_object_associations(
            HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"], HUBSPOT_SPONSORS_OBJECT
        )
    }

    # 3. Attendee → Events (with label); skip if already associated with same label
    for ea in event_associations:
        eid = str(ea.get("event_id", ""))
        label_id = ea.get("label_id")
        if not eid:
            continue
        if (eid, label_id) in existing_event_set:
            result["actions"].append(f"Already associated to event {ea.get('full_name', eid)} ({ea.get('label_name', '')}) – skipped")
            continue
        if _hubspot_put_association(
            HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"],
            HUBSPOT_EVENTS_OBJECT, eid,
            association_type_id=label_id,
        ):
            result["actions"].append(f"Associated attendee to event {ea.get('full_name', eid)} ({ea.get('label_name', '')})")
        else:
            result["errors"].append(
                f"Failed to associate attendee to event {ea.get('full_name', eid)}"
                f" ({ea.get('label_name', '')}, label_id={label_id})"
            )

    # 4. Attendee → Festivals; skip if already associated
    for f in festival_associations:
        fid = str(f.get("id", ""))
        if not fid:
            continue
        if fid in existing_festival_ids:
            result["actions"].append(f"Already associated to festival {f.get('festival_code', fid)} – skipped")
            continue
        if _hubspot_put_association(
            HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"],
            HUBSPOT_FESTIVALS_OBJECT, fid,
            association_type_id=None,
        ):
            result["actions"].append(f"Associated attendee to festival {f.get('festival_code', fid)}")

    # 5. Attendee → Sponsors; skip if already associated
    for s in sponsor_associations:
        sid = str(s.get("id", ""))
        if not sid or str(sid).startswith("training-sponsor-"):
            continue
        if sid in existing_sponsor_ids:
            result["actions"].append(f"Already associated to sponsor {s.get('name', sid)} – skipped")
            continue
        if _hubspot_put_association(
            HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"],
            HUBSPOT_SPONSORS_OBJECT, sid,
            association_type_id=None,
        ):
            result["actions"].append(f"Associated attendee to sponsor {s.get('name', sid)}")

    # 6. Deals
    if (not result["contact_id"] and not result["attendee_id"]) or not deal_plan:
        return result
    contact_deals = _hubspot_search_deals_for_contact(result["contact_id"])
    for item in deal_plan:
        action = item.get("action", "create")
        event_id = str((item.get("event_id") or "")).strip()
        event_name = (item.get("event_name") or "").strip()
        dealname = item.get("dealname") or ""
        props = item.get("properties") or {}
        amount = item.get("amount")
        tax_amount = item.get("tax_amount")
        product_id = str((item.get("product_id") or "")).strip()
        if action == "update_existing":
            existing = next(
                (d for d in contact_deals if event_name and (event_name in (d.get("dealname") or ""))),
                None,
            )
            if existing:
                update_props = {}
                if amount is not None:
                    update_props["amount"] = str(amount)
                if tax_amount is not None:
                    update_props["cvent_tax_amount"] = str(tax_amount)
                if update_props:
                    if _hubspot_update_deal(str(existing["id"]), update_props):
                        result["actions"].append(f"Updated deal for {event_name} (amount/tax)")
                if result["attendee_id"]:
                    if _hubspot_put_association(HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"], "deals", str(existing["id"]), association_type_id=None):
                        result["actions"].append("Associated attendee to deal")
                    else:
                        result["errors"].append("Failed to associate attendee to deal")
                # Ensure deal is associated to the relevant event object.
                if event_id:
                    if _hubspot_put_association("deals", str(existing["id"]), HUBSPOT_EVENTS_OBJECT, event_id, association_type_id=None):
                        result["actions"].append(f"Associated deal to event {event_name or event_id}")
                    else:
                        result["errors"].append(f"Failed to associate deal to event {event_name or event_id}")
                # Ensure product association for quantity deals, if provided.
                if product_id:
                    if _hubspot_put_association("deals", str(existing["id"]), "products", product_id, association_type_id=None):
                        result["actions"].append(f"Associated deal to product {product_id}")
                    else:
                        result["errors"].append(f"Failed to associate deal to product {product_id}")
                continue
            # If we couldn't find an existing deal, fall back to creation.
            action = "create"
        created = _hubspot_create_deal(props)
        if not created or not created.get("id"):
            result["errors"].append(f"Failed to create deal for {event_name}")
            continue
        deal_id = str(created["id"])
        result["actions"].append(f"Created deal for {event_name}")
        if result["contact_id"]:
            _hubspot_put_association(
                "contacts", result["contact_id"],
                "deals", deal_id,
                association_type_id=None,
            )
        if result["attendee_id"]:
            if _hubspot_put_association(HUBSPOT_ATTENDEE_OBJECT, result["attendee_id"], "deals", deal_id, association_type_id=None):
                result["actions"].append("Associated attendee to deal")
            else:
                result["errors"].append("Failed to associate attendee to deal")
        if event_id:
            if _hubspot_put_association("deals", deal_id, HUBSPOT_EVENTS_OBJECT, event_id, association_type_id=None):
                result["actions"].append(f"Associated deal to event {event_name or event_id}")
            else:
                result["errors"].append(f"Failed to associate deal to event {event_name or event_id}")
        if product_id:
            if _hubspot_put_association("deals", deal_id, "products", product_id, association_type_id=None):
                result["actions"].append(f"Associated deal to product {product_id}")
            else:
                result["errors"].append(f"Failed to associate deal to product {product_id}")
    return result


@app.route("/api/hubspot/sync-attendee", methods=["POST"])
def hubspot_sync_attendee():
    """
    Sync Cvent attendee to HubSpot. training=True: dry run with report and transaction_steps.
    training=False: run live per transaction (1 then 2 then …).
    """
    if not HUBSPOT_TOKEN:
        return jsonify({"error": "HubSpot token (CustomCode) not configured in .env"}), 500
    data = request.get_json() or {}
    training = data.get("training", True)
    cvent_attendee_id = (data.get("cvent_attendee_id") or "").strip()
    cvent_event_id = (data.get("cvent_event_id") or "").strip()
    quantity_item_product_mappings = data.get("quantity_item_product_mappings") or {}
    if not isinstance(quantity_item_product_mappings, dict):
        quantity_item_product_mappings = {}
    if not cvent_attendee_id or not cvent_event_id:
        return jsonify({"error": "cvent_attendee_id and cvent_event_id are required"}), 400

    try:
        token = fetch_cvent_token()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    attendee = lookup_attendee_cvent(cvent_event_id, cvent_attendee_id, token)
    if attendee.get("attendee_exists") != "Yes":
        return jsonify({"error": "Attendee not found in Cvent"}), 404

    email = (attendee.get("email") or "").strip()
    if not email:
        return jsonify({
            "error": "Attendee has no email address - cannot search/create HubSpot contact",
        }), 400

    order = fetch_order_data(cvent_event_id, cvent_attendee_id)
    first_name = (attendee.get("first_name") or "").strip()
    last_name = (attendee.get("last_name") or "").strip()
    attendee_properties_full = _build_attendee_properties(attendee, order)

    admission_item_id = (attendee.get("admission_item_id") or "").strip()
    assoc_result = _resolve_association_label_and_events(
        attendee, order, admission_item_id, training=training
    )
    event_associations = assoc_result.get("event_associations", [])
    festival_associations = assoc_result.get("festival_associations", [])
    sponsor_associations = assoc_result.get("sponsor_associations", [])
    base_label = assoc_result.get("base_label", {})

    orders_gross_amounts = order.get("orders_amounts") or []
    orders_admission_amounts = order.get("orders_admission_amounts") or []
    orders_tax_amounts = order.get("orders_tax_amounts") or []
    orders_quantity_items = order.get("orders_quantity_items") or []
    orders_quantity_net_amounts = order.get("orders_quantity_net_amounts") or []
    user_journey = order.get("user_journey") or []
    orders_structured = order.get("orders") or []
    num_steps = max(1, len(orders_gross_amounts)) if orders_gross_amounts else 1

    contact_result = _hubspot_search_contact_by_email(email)
    attendee_result = _hubspot_search_attendee_by_cvent_id(cvent_attendee_id)

    if training:
        deal_result = _build_deal_plan(
            attendee, order, event_associations,
            attendee_exists=bool(attendee_result),
            sponsor_associations=sponsor_associations,
            training=True,
            quantity_item_product_mappings=quantity_item_product_mappings,
        )
        speaker_labels = deal_result.get("speaker_upgrade_event_labels")
        if speaker_labels:
            by_id = {e["event_id"]: e for e in speaker_labels}
            for ea in event_associations:
                ov = by_id.get(ea.get("event_id"))
                if ov:
                    ea["label_id"] = ov["label_id"]
                    ea["label_name"] = ov["label_name"]
        reg_status = deal_result.get("registration_status", "")
        report = {
            "training_mode": True,
            "message": "No changes made. This is a dry run showing what would be done.",
            "cvent_attendee_id": cvent_attendee_id,
            "cvent_event_id": cvent_event_id,
            "attendee_name": attendee.get("attendee_name", ""),
            "email": email,
            "registration_status": reg_status,
            "is_accepted": reg_status.lower() == "accepted",
            "admission_item": attendee.get("admission_item", ""),
            "admission_item_id": admission_item_id,
            "hubspot_events": [{"id": e["event_id"], "full_name": e["full_name"]} for e in event_associations],
            "event_associations": event_associations,
            "festival_associations": festival_associations,
            "sponsor_associations": sponsor_associations,
            "base_label": base_label,
            "deal_conditions_met": deal_result.get("deal_conditions_met", False),
            "deal_conditions": deal_result.get("deal_conditions", {}),
            "deal_plan": deal_result.get("deal_plan", []),
            "deal_scenario": deal_result.get("deal_scenario", "standard"),
            "tax_breakdown": deal_result.get("tax_breakdown"),
            "speaker_upgrade_event_labels": deal_result.get("speaker_upgrade_event_labels"),
            "speaker_event_answer": deal_result.get("speaker_event_answer", ""),
            "contact": {"found": bool(contact_result), "id": contact_result.get("id") if contact_result else None, "email": (contact_result.get("properties") or {}).get("email", "") if contact_result else ""} if contact_result else {"found": False, "id": None, "properties": {"firstname": first_name, "lastname": last_name, "email": email}},
            "attendee": {"found": bool(attendee_result), "id": attendee_result.get("id") if attendee_result else None} if attendee_result else {"found": False, "id": None},
            "actions": [],
            "attendee_properties": attendee_properties_full,
        }
        if contact_result:
            report["actions"].append("Contact found in HubSpot (no create needed)")
        else:
            report["actions"].append("Would create new HubSpot contact (firstname, lastname, email from Cvent)")
        if attendee_result:
            report["actions"].append("Attendee record found in HubSpot (no create needed)")
        else:
            report["actions"].append("Would create new HubSpot attendee record with cvent_attendee_id")
            if not contact_result:
                report["actions"].append("Would create contact first, then create attendee and associate")
            else:
                report["actions"].append("Would associate contact to attendee")
        report["actions"].append("Would set attendee properties (see below)")
        if event_associations:
            by_label = {}
            for ea in event_associations:
                ln = ea.get("label_name", "Paying Delegate")
                by_label.setdefault(ln, []).append(ea.get("full_name") or f"Event {ea.get('event_id')}")
            parts = [f"{label}: {', '.join(names)}" for label, names in by_label.items()]
            report["actions"].append(f"Would associate attendee to events with labels: {'; '.join(parts)}")
            if festival_associations:
                fnames = [f.get("festival_code") or f"Festival {f.get('id')}" for f in festival_associations]
                report["actions"].append(f"Would associate attendee to festival(s): {', '.join(fnames)}")
            if sponsor_associations:
                snames = [s.get("name") or f"Sponsor {s.get('id')}" for s in sponsor_associations]
                report["actions"].append(f"Would associate attendee to sponsor(s): {', '.join(snames)}")
        elif admission_item_id:
            report["actions"].append("No HubSpot event paired with this admission item in settings – would not associate attendee to any event")
        else:
            report["actions"].append("Attendee has no admission item – would not associate to any HubSpot event")
        dr = deal_result
        if dr.get("deal_conditions_met") and dr.get("deal_plan"):
            scenario = dr.get("deal_scenario", "standard")
            plan = dr["deal_plan"]
            if scenario == "paying_delegate_upgrade":
                report["actions"].append("Would amend existing deal amount to half of total and create one new deal for the other event (total from both transactions split 50/50)")
            elif scenario == "speaker_upgrade":
                report["actions"].append("Would create 1 deal for the event they paid to attend and associate with correct speaker/paying delegate labels per question")
            elif scenario == "sponsor_upgrade":
                report["actions"].append("Would create 1 deal for the event they are not yet associated to, associate attendee to that event (sponsor or paying delegate label depending on sponsor–event association)")
            else:
                n = len(plan)
                if n == 1:
                    report["actions"].append("Would create 1 deal and associate to that event")
                else:
                    report["actions"].append(f"Would create {n} deals (one per event), revenue split {100 // n}% each")
        elif not dr.get("deal_conditions_met"):
            report["actions"].append("Would not create deal (conditions not met: need Accepted, amount > 0, reference_id without DelSale)")
        elif not event_associations:
            report["actions"].append("Would not create deal (no events to associate to)")

        transaction_steps = []
        for k in range(1, num_steps + 1):
            is_phantom = (
                (k <= len(user_journey) and user_journey[k - 1].get("phantom"))
                or (k <= len(orders_structured) and orders_structured[k - 1].get("phantom"))
            )
            if is_phantom:
                step_date = user_journey[k - 1].get("created", "") if k <= len(user_journey) else ""
                transaction_steps.append({
                    "step": k,
                    "date": step_date,
                    "report": {
                        "training_mode": True,
                        "message": report["message"],
                        "transaction_step_number": k,
                        "transaction_step_intro": f"Transaction {k}: ignored (phantom transaction – same amount and admission item as an earlier transaction).",
                        "phantom_ignored": True,
                        "cvent_attendee_id": cvent_attendee_id,
                        "cvent_event_id": cvent_event_id,
                        "attendee_name": report["attendee_name"],
                        "email": email,
                    },
                })
                continue
            # Build per-transaction inputs for deal planning.
            idx = k - 1
            admission_net_current = float(orders_admission_amounts[idx]) if idx < len(orders_admission_amounts) else 0.0
            quantity_net_current = float(orders_quantity_net_amounts[idx]) if idx < len(orders_quantity_net_amounts) else 0.0
            tax_current = float(orders_tax_amounts[idx]) if idx < len(orders_tax_amounts) else 0.0

            denom_current = admission_net_current + quantity_net_current
            tax_admission_current = (tax_current * (admission_net_current / denom_current)) if denom_current > 0 else 0.0

            quantity_lines_current = orders_quantity_items[idx] if idx < len(orders_quantity_items) else []
            quantity_items_current = {}
            for line in (quantity_lines_current or []):
                qi_id = str(line.get("id") or "").strip()
                if not qi_id:
                    continue
                qi_net = float(line.get("amount") or 0.0)
                qi_name = (line.get("name") or "").strip()
                qi_tax = (tax_current * (qi_net / denom_current)) if denom_current > 0 else 0.0
                quantity_items_current[qi_id] = {
                    "id": qi_id,
                    "name": qi_name,
                    "amount": round(qi_net, 2),
                    "tax": round(qi_tax, 2),
                }

            # Cumulative (non-phantom) totals for admission tax + admission revenue.
            effective_indices = []
            for i0 in range(k):
                uj_ph = bool(i0 < len(user_journey) and user_journey[i0].get("phantom"))
                ord_ph = bool(i0 < len(orders_structured) and orders_structured[i0].get("phantom"))
                if not (uj_ph or ord_ph):
                    effective_indices.append(i0)

            orders_amounts_k = [float(orders_admission_amounts[i]) for i in effective_indices if i < len(orders_admission_amounts)]
            total_k = sum(orders_amounts_k)

            tax_admission_total = 0.0
            for i0 in effective_indices:
                admission_i = float(orders_admission_amounts[i0]) if i0 < len(orders_admission_amounts) else 0.0
                quantity_i = float(orders_quantity_net_amounts[i0]) if i0 < len(orders_quantity_net_amounts) else 0.0
                tax_i = float(orders_tax_amounts[i0]) if i0 < len(orders_tax_amounts) else 0.0
                denom_i = admission_i + quantity_i
                if denom_i > 0:
                    tax_admission_total += tax_i * (admission_i / denom_i)

            order_k = {
                "total_amount_ordered": total_k,
                "orders_amounts": orders_amounts_k,
                "orders_count": len(orders_amounts_k) or k,
                "amount_ordered": admission_net_current,
                "quantity_amount_ordered": quantity_net_current,
                "tax_admission_current": round(tax_admission_current, 2),
                "tax_admission_total": round(tax_admission_total, 2),
                "quantity_items_current": quantity_items_current,
                "cancelled": order.get("cancelled"),
                "invoice_number": orders_structured[k - 1].get("invoice_number", order.get("invoice_number")) if k <= len(orders_structured) else order.get("invoice_number"),
                "reference_number": order.get("reference_number"),
                "amount_due": order.get("amount_due"),
            }
            # Admission item from same source as Purchase Journey (user_journey[k].products) so step logic matches UI
            if k <= len(user_journey):
                journey_entry = user_journey[k - 1]
                admission_item_name_k, admission_item_id_k = _admission_from_journey_entry(journey_entry)
                if not admission_item_id_k:
                    admission_item_id_k = (orders_structured[k - 1].get("admission_item_id") or "").strip() if k <= len(orders_structured) else admission_item_id
            else:
                admission_item_id_k = admission_item_id
                admission_item_name_k = ""
            # Paid detection for associations (speaker/sponsor logic) uses net paid amount (tax-excluded).
            _amt = denom_current
            try:
                current_amt = float(_amt) if _amt not in (None, "") else None
            except (TypeError, ValueError):
                current_amt = None
            assoc_result_k = _resolve_association_label_and_events(
                attendee, order, admission_item_id_k, training=True, current_transaction_amount=current_amt
            )
            event_associations_step = list(assoc_result_k.get("event_associations", []))
            festival_associations_step = list(assoc_result_k.get("festival_associations", []))
            sponsor_associations_step = list(assoc_result_k.get("sponsor_associations", []))
            base_label_step = assoc_result_k.get("base_label", base_label)
            attendee_exists_k = k > 1
            deal_result_k = _build_deal_plan(
                attendee, order_k, event_associations_step,
                attendee_exists=attendee_exists_k,
                sponsor_associations=sponsor_associations_step,
                training=True,
                quantity_item_product_mappings=quantity_item_product_mappings,
            )
            speaker_labels_k = deal_result_k.get("speaker_upgrade_event_labels")
            if speaker_labels_k:
                by_id = {e["event_id"]: e for e in speaker_labels_k}
                for ea in event_associations_step:
                    ov = by_id.get(ea.get("event_id"))
                    if ov:
                        ea["label_id"] = ov["label_id"]
                        ea["label_name"] = ov["label_name"]
            step_intro = (
                "When transaction 1 is processed: the sync would search for contact and attendee by email / Cvent ID; create them if not found, then set properties, associate to events (and festivals/sponsors), and create deal(s) as below."
                if k == 1 else
                f"When transaction {k} is processed: the sync would find the existing contact and attendee, then apply only the updates below (property refresh, event associations, and deal create/amend as needed)."
            )
            deal_plan_step = [dict(item) for item in deal_result_k.get("deal_plan", [])]
            if k > 1:
                for item in deal_plan_step:
                    if item.get("action") == "update_existing":
                        item["simulated_from_step_1"] = True
            # Use only transaction-derived admission item for this step (never fall back to attendee current)
            attendee_properties_k = _build_attendee_properties(
                attendee, order_k, admission_item_override=admission_item_name_k
            )
            step_report = {
                "training_mode": True,
                "message": report["message"],
                "transaction_step_number": k,
                "transaction_step_intro": step_intro,
                "phantom_ignored": False,
                "cvent_attendee_id": cvent_attendee_id,
                "cvent_event_id": cvent_event_id,
                "attendee_name": report["attendee_name"],
                "email": email,
                "registration_status": deal_result_k.get("registration_status", ""),
                "is_accepted": report["is_accepted"],
                "admission_item": admission_item_name_k,  # only what was in this transaction
                "admission_item_id": admission_item_id_k or admission_item_id,
                "hubspot_events": [{"id": e.get("event_id"), "full_name": e.get("full_name")} for e in event_associations_step],
                "event_associations": event_associations_step,
                "festival_associations": festival_associations_step,
                "sponsor_associations": sponsor_associations_step,
                "base_label": base_label_step,
                "deal_conditions_met": deal_result_k.get("deal_conditions_met", False),
                "deal_conditions": deal_result_k.get("deal_conditions", {}),
                "deal_plan": deal_plan_step,
                "deal_scenario": deal_result_k.get("deal_scenario", "standard"),
                "tax_breakdown": deal_result_k.get("tax_breakdown"),
                "speaker_upgrade_event_labels": deal_result_k.get("speaker_upgrade_event_labels"),
                "speaker_event_answer": deal_result_k.get("speaker_event_answer", ""),
                "contact": (
                    {"found": True, "id": "simulated-after-step-1", "email": email}
                    if k > 1 else report["contact"]
                ),
                "attendee": {"found": True, "id": "simulated-after-step-1", "cvent_attendee_id": cvent_attendee_id} if k > 1 else report["attendee"],
                "actions": [],
                "attendee_properties": attendee_properties_k,
                "warnings": list(assoc_result_k.get("warnings") or []),
            }
            if k == 1:
                if contact_result:
                    step_report["actions"].append("Contact found in HubSpot (no create needed)")
                else:
                    step_report["actions"].append("Would create new HubSpot contact (firstname, lastname, email from Cvent)")
                if attendee_result:
                    step_report["actions"].append("Attendee record found in HubSpot (no create needed)")
                else:
                    step_report["actions"].append("Would create new HubSpot attendee record with cvent_attendee_id")
                    if not contact_result:
                        step_report["actions"].append("Would create contact first, then create attendee and associate")
                    else:
                        step_report["actions"].append("Would associate contact to attendee")
                step_report["actions"].append("Would set attendee properties (see below)")
            else:
                step_report["actions"].append("Contact found in HubSpot (no create needed)")
                step_report["actions"].append("Attendee record found in HubSpot (no create needed) – simulated from step 1")
                step_report["actions"].append("Would update attendee properties (see below)")
            if event_associations_step:
                by_label = {}
                for ea in event_associations_step:
                    ln = ea.get("label_name", "Paying Delegate")
                    by_label.setdefault(ln, []).append(ea.get("full_name") or f"Event {ea.get('event_id')}")
                parts = [f"{label}: {', '.join(names)}" for label, names in by_label.items()]
                step_report["actions"].append(f"Would associate attendee to events with labels: {'; '.join(parts)}")
                if festival_associations_step:
                    fnames = [f.get("festival_code") or f"Festival {f.get('id')}" for f in festival_associations_step]
                    step_report["actions"].append(f"Would associate attendee to festival(s): {', '.join(fnames)}")
                if sponsor_associations_step:
                    snames = [s.get("name") or f"Sponsor {s.get('id')}" for s in sponsor_associations_step]
                    step_report["actions"].append(f"Would associate attendee to sponsor(s): {', '.join(snames)}")
            elif admission_item_id_k:
                step_report["actions"].append("No HubSpot event paired with this admission item in settings – would not associate attendee to any event")
            else:
                step_report["actions"].append("Attendee has no admission item for this transaction – would not associate to any HubSpot event")
            drk = deal_result_k
            if drk.get("deal_conditions_met") and drk.get("deal_plan"):
                scenario = drk.get("deal_scenario", "standard")
                plan = step_report["deal_plan"]
                if scenario == "paying_delegate_upgrade":
                    step_report["actions"].append("Would amend existing deal amount to half of total and create one new deal for the other event (total from both transactions split 50/50)")
                elif scenario == "speaker_upgrade":
                    step_report["actions"].append("Would create 1 deal for the event they paid to attend and associate with correct speaker/paying delegate labels per question")
                elif scenario == "sponsor_upgrade":
                    step_report["actions"].append("Would create 1 deal for the event they are not yet associated to, associate attendee to that event (sponsor or paying delegate label depending on sponsor–event association)")
                else:
                    n = len(plan)
                    if n == 1:
                        step_report["actions"].append("Would create 1 deal and associate to that event")
                    else:
                        step_report["actions"].append(f"Would create {n} deals (one per event), revenue split {100 // n}% each")
                for item in plan:
                    if item.get("action") == "update_existing" and item.get("simulated_from_step_1"):
                        en = item.get("event_name") or "event"
                        amt = item.get("amount")
                        step_report["actions"].append(f"Would amend existing deal (simulated from step 1) for {en} to £{amt}")
            elif not drk.get("deal_conditions_met"):
                step_report["actions"].append("Would not create deal (conditions not met: need Accepted, amount > 0, reference_id without DelSale)")
            elif not event_associations_step:
                step_report["actions"].append("Would not create deal (no events to associate to)")
            step_date = user_journey[k - 1].get("created", "") if k <= len(user_journey) else ""
            transaction_steps.append({"step": k, "date": step_date, "report": step_report})
        report["transaction_steps"] = transaction_steps
        return jsonify(report)

    all_actions = []
    all_errors = []
    step_results = []
    contact_id = None
    attendee_id = None

    for k in range(1, num_steps + 1):
        is_phantom = (
            (k <= len(user_journey) and user_journey[k - 1].get("phantom"))
            or (k <= len(orders_structured) and orders_structured[k - 1].get("phantom"))
        )
        if is_phantom:
            step_date = user_journey[k - 1].get("created", "") if k <= len(user_journey) else ""
            step_results.append({
                "step": k,
                "date": step_date,
                "actions": ["Ignored (phantom transaction – same amount and admission item as an earlier transaction)."],
                "errors": [],
                "created_contact": False,
                "created_attendee": False,
            })
            continue
        # Build per-transaction inputs for deal planning.
        idx = k - 1
        admission_net_current = float(orders_admission_amounts[idx]) if idx < len(orders_admission_amounts) else 0.0
        quantity_net_current = float(orders_quantity_net_amounts[idx]) if idx < len(orders_quantity_net_amounts) else 0.0
        tax_current = float(orders_tax_amounts[idx]) if idx < len(orders_tax_amounts) else 0.0

        denom_current = admission_net_current + quantity_net_current
        tax_admission_current = (tax_current * (admission_net_current / denom_current)) if denom_current > 0 else 0.0

        quantity_lines_current = orders_quantity_items[idx] if idx < len(orders_quantity_items) else []
        quantity_items_current = {}
        for line in (quantity_lines_current or []):
            qi_id = str(line.get("id") or "").strip()
            if not qi_id:
                continue
            qi_net = float(line.get("amount") or 0.0)
            qi_name = (line.get("name") or "").strip()
            qi_tax = (tax_current * (qi_net / denom_current)) if denom_current > 0 else 0.0
            quantity_items_current[qi_id] = {
                "id": qi_id,
                "name": qi_name,
                "amount": round(qi_net, 2),
                "tax": round(qi_tax, 2),
            }

        # Cumulative (non-phantom) totals for admission tax + admission revenue.
        effective_indices = []
        for i0 in range(k):
            uj_ph = bool(i0 < len(user_journey) and user_journey[i0].get("phantom"))
            ord_ph = bool(i0 < len(orders_structured) and orders_structured[i0].get("phantom"))
            if not (uj_ph or ord_ph):
                effective_indices.append(i0)

        orders_amounts_k = [float(orders_admission_amounts[i]) for i in effective_indices if i < len(orders_admission_amounts)]
        total_k = sum(orders_amounts_k)

        tax_admission_total = 0.0
        for i0 in effective_indices:
            admission_i = float(orders_admission_amounts[i0]) if i0 < len(orders_admission_amounts) else 0.0
            quantity_i = float(orders_quantity_net_amounts[i0]) if i0 < len(orders_quantity_net_amounts) else 0.0
            tax_i = float(orders_tax_amounts[i0]) if i0 < len(orders_tax_amounts) else 0.0
            denom_i = admission_i + quantity_i
            if denom_i > 0:
                tax_admission_total += tax_i * (admission_i / denom_i)

        order_k = {
            "total_amount_ordered": total_k,
            "orders_amounts": orders_amounts_k,
            "orders_count": len(orders_amounts_k) or k,
            "amount_ordered": admission_net_current,
            "quantity_amount_ordered": quantity_net_current,
            "tax_admission_current": round(tax_admission_current, 2),
            "tax_admission_total": round(tax_admission_total, 2),
            "quantity_items_current": quantity_items_current,
            "cancelled": order.get("cancelled"),
            "invoice_number": orders_structured[k - 1].get("invoice_number", order.get("invoice_number")) if k <= len(orders_structured) else order.get("invoice_number"),
            "reference_number": order.get("reference_number"),
            "amount_due": order.get("amount_due"),
        }
        # Admission item from same source as Purchase Journey (user_journey[k].products)
        if k <= len(user_journey):
            journey_entry = user_journey[k - 1]
            admission_item_name_k, admission_item_id_k = _admission_from_journey_entry(journey_entry)
            if not admission_item_id_k and k <= len(orders_structured):
                admission_item_id_k = (orders_structured[k - 1].get("admission_item_id") or "").strip()
            if not admission_item_id_k:
                admission_item_id_k = admission_item_id
        else:
            admission_item_id_k = admission_item_id
            admission_item_name_k = ""
        # Paid detection for associations (speaker/sponsor logic) uses net paid amount (tax-excluded).
        _amt = denom_current
        try:
            current_amt = float(_amt) if _amt not in (None, "") else None
        except (TypeError, ValueError):
            current_amt = None
        assoc_result_k = _resolve_association_label_and_events(
            attendee, order, admission_item_id_k, training=False, current_transaction_amount=current_amt
        )
        event_associations_step = list(assoc_result_k.get("event_associations", []))
        festival_associations_step = list(assoc_result_k.get("festival_associations", []))
        sponsor_associations_step = list(assoc_result_k.get("sponsor_associations", []))
        # Use only transaction-derived admission item for this step (never fall back to attendee current)
        attendee_properties_k = _build_attendee_properties(
            attendee, order_k, admission_item_override=admission_item_name_k
        )
        attendee_exists_k = k > 1
        deal_result_k = _build_deal_plan(
            attendee,
            order_k,
            event_associations_step,
            attendee_exists=attendee_exists_k,
            sponsor_associations=sponsor_associations_step,
            training=False,
            quantity_item_product_mappings=quantity_item_product_mappings,
        )
        speaker_labels_k = deal_result_k.get("speaker_upgrade_event_labels")
        if speaker_labels_k:
            by_id = {e["event_id"]: e for e in speaker_labels_k}
            for ea in event_associations_step:
                ov = by_id.get(ea.get("event_id"))
                if ov:
                    ea["label_id"] = ov["label_id"]
                    ea["label_name"] = ov["label_name"]

        deal_plan_k = deal_result_k.get("deal_plan", []) if deal_result_k.get("deal_conditions_met") else []

        step_result = _execute_sync_step(
            email=email,
            first_name=first_name,
            last_name=last_name,
            cvent_attendee_id=cvent_attendee_id,
            attendee_properties=attendee_properties_k,
            event_associations=event_associations_step,
            festival_associations=festival_associations_step,
            sponsor_associations=sponsor_associations_step,
            deal_plan=deal_plan_k,
        )
        step_date = user_journey[k - 1].get("created", "") if k <= len(user_journey) else ""
        step_results.append({
            "step": k,
            "date": step_date,
            "actions": step_result.get("actions", []),
            "errors": step_result.get("errors", []),
            "created_contact": step_result.get("created_contact", False),
            "created_attendee": step_result.get("created_attendee", False),
        })
        all_actions.extend(step_result.get("actions", []))
        all_errors.extend(step_result.get("errors", []))
        contact_id = step_result.get("contact_id") or contact_id
        attendee_id = step_result.get("attendee_id") or attendee_id

    report = {
        "message": "Sync completed (per transaction: 1 then 2 then …).",
        "cvent_attendee_id": cvent_attendee_id,
        "cvent_event_id": cvent_event_id,
        "attendee_name": attendee.get("attendee_name", ""),
        "email": email,
        "contact_id": contact_id,
        "attendee_id": attendee_id,
        "steps": step_results,
        "actions": all_actions,
        "errors": all_errors,
    }

    if all_errors:
        return jsonify({**report, "error": "; ".join(all_errors)}), 207
    return jsonify(report)


@app.route("/api/lookup", methods=["POST"])
def lookup():
    data = request.get_json() or {}
    cvent_attendee_id = (data.get("cvent_attendee_id") or "").strip()
    cvent_event_id = (data.get("cvent_event_id") or "").strip()

    if not cvent_attendee_id:
        return jsonify({"error": "Attendee ID is required"}), 400
    if not cvent_event_id:
        return jsonify({"error": "Event ID is required"}), 400

    try:
        token = fetch_cvent_token()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    attendee_result = lookup_attendee_cvent(cvent_event_id, cvent_attendee_id, token)
    order_result = fetch_order_data(cvent_event_id, cvent_attendee_id)
    hubspot_attendee = fetch_hubspot_attendee(cvent_attendee_id)

    return jsonify({
        "attendee": attendee_result,
        "order": order_result,
        "hubspot_attendee": hubspot_attendee,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    debug = os.getenv("FLASK_DEBUG", "").strip().lower() in ("1", "true", "yes")
    app.run(host="0.0.0.0", port=port, debug=debug)
