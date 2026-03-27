# DM Cvent Integration

Integration between Cvent and HubSpot for DM. Look up attendee information from HubSpot and order data from Cvent.

## Setup

1. Copy `.env.example` to `.env` and fill in your secrets:
   - `CustomCode` – HubSpot Private App token (for attendee lookup, events UI, etc.). For **Event setup → quantity items → HubSpot Product** mapping, the token needs **`crm.objects.products.read`** (standard CRM Products).
   - `CV_CLIENT_ID` – Cvent API client ID
   - `CV_CLIENT_SECRET` – Cvent API client secret

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the app:
   ```bash
   python app.py
   ```

4. Open http://localhost:5001 in your browser.

## Railway Deployment

This project is now ready for Railway:

- `Procfile` is included for production startup with Gunicorn.
- App listens on `PORT` in production.
- Optional email OTP login can be enforced using environment variables.

### Required Railway Variables

Core integration:

- `CustomCode` - HubSpot private app token
- `CV_CLIENT_ID` - Cvent API client ID
- `CV_CLIENT_SECRET` - Cvent API client secret
- `CV_API_BASE` - optional (`https://api-platform.cvent.com` or EU base)

Email login (OTP) - required if you want protected access:

- `SESSION_SECRET` - long random secret used to sign auth cookie
- `SMTP_HOST` - SMTP server host (for SendGrid SMTP: `smtp.sendgrid.net`)
- `SMTP_PORT` - SMTP port (usually `587`)
- `SMTP_USER` - SMTP username (for SendGrid SMTP: `apikey`)
- `SMTP_PASSWORD` - SMTP password / API key
- `EMAIL_FROM` - sender email address
- `ALLOWED_EMAILS` - optional comma-separated allow-list (for example `a@x.com,b@y.com`)

Auth behavior:

- If `SESSION_SECRET`, `SMTP_HOST`, and `EMAIL_FROM` are all set, login is enabled and users must sign in via email code.
- If not set, the app behaves as open access (no login wall), which is convenient for local development.

### Recommended Railway Hardening

- Set Railway healthcheck path to `/api/health`.
- Keep `ALLOWED_EMAILS` restricted to your team only.
- Rotate `SESSION_SECRET` and SMTP/API credentials periodically.
- Check Railway logs for auth events:
  - `auth.send_code success|failed|rate_limited_*`
  - `auth.verify_code success|invalid|expired|missing`
  - Email addresses are logged as SHA-256 hashes (not raw addresses).

## Usage

1. Enter a **Cvent Attendee ID** (required).
2. Optionally enter **Event ID** or **Event Code** if you have them.
3. If the attendee record in HubSpot has `cvent_event_id` or `cvent_event_code`, those are used automatically for the Cvent order lookup.
4. Click **Lookup** to see attendee and order results.

## Workflow Mapping

- **Attendee lookup** – Replicates the HubSpot workflow that searches the Attendee custom object (`2-44005420`) by `cvent_attendee_id`.
- **Order data** – Replicates the Cvent workflow that fetches orders and order items for the given event and attendee.
