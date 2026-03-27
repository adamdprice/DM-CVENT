# Cvent → HubSpot sync: how it works

This document explains how the DM Cvent Integration creates and updates HubSpot records from Cvent: **attendees** (and contacts), **associations** to events, festivals and sponsors, and **deals**, including when deals are created and how revenue is split.

---

## 1. How attendees (and contacts) are created

- **Trigger:** Sync runs per Cvent attendee, per **transaction step** (each order/transaction is processed in sequence: 1, then 2, then …). Cancelled orders are excluded. Some transactions are treated as **phantom** (see below) and are skipped.

- **Contact**
  - We look up a HubSpot **contact** by the attendee’s **email** (from Cvent).
  - If none is found, we **create** a contact with: **email**, **first name**, **last name** from Cvent.
  - The contact is then linked to the attendee record (see below).

- **Attendee record (custom object)**
  - We look up a HubSpot **attendee** by **Cvent Attendee ID** (`cvent_attendee_id`).
  - If none is found, we **create** an attendee with properties from Cvent (and from the current order/transaction). We set **`cvent_attendee_id`** so the same Cvent attendee is always matched in future syncs.
  - If a contact was just created, we associate the contact to the attendee. If the contact already existed, we still ensure the contact–attendee association exists.

- **Attendee properties** (updated on each sync step)  
  The following are pushed from Cvent/order data (where available):
  - **attendee_name**, **first_name**, **last_name**, **email**
  - **company_name**, **country**, **job_title**, **phone_number**, **linkedin_url**
  - **cvent_attendee_id**, **cvent_admission_item** (from the **current transaction’s** admission item, not only the “current” Cvent profile)
  - **cvent_reg_date**, **cvent_reg_status**, **cvent_registration_type**
  - **cvent_reference_id**, **cvent_confirmation_number**, **cvent_invoice_number**
  - **cvent_cancelled**, **cvent_amount_due**
  - **how_did_you_hear**, **primary_organization_type**, **special_requirements**

So: **one Cvent attendee** → **one HubSpot contact** (by email) and **one HubSpot attendee** (by Cvent Attendee ID). Both are created only when missing; afterwards we update attendee properties and associations.

---

## 2. How attendees are associated to events, festivals and sponsors

### 2.1 Events (with a label)

Attendees are associated to **HubSpot events** with a **label** that describes their role for that event (e.g. Paying Delegate, Sponsor Executive, Speaker - Non Sponsor). Which events we use and which label we use come from:

1. **Events from the admission item**
   - Each Cvent order line has an **admission item** (e.g. “Single event ticket”, “All access ticket”). Each HubSpot **event** has a property **`cvent_admission_item_ids`** (a comma-separated list of Cvent admission item IDs).
   - We find all HubSpot events whose `cvent_admission_item_ids` contains the **admission item ID for the current transaction**. The attendee is associated to those events with a **base label** (see below).

2. **Events (and festivals) from EXEC/CLIENT discount codes**
   - If the order has a discount code that matches the pattern **`{something}EXEC{eventOrFestivalCode}`** or **`{something}CLIENT{eventOrFestivalCode}`**, we:
     - Treat the attendee as **Sponsor Executive** or **Sponsor Client** for that code.
     - Resolve **event or festival** from the code’s suffix: we first search for a HubSpot **festival** by **full name** = that suffix; if found, we add that festival and all events linked to that festival. If no festival is found, we search for a HubSpot **event** by **event_code** = that suffix and add that event.
   - So discount codes can add **extra events** (and festivals) and set the **sponsor** label for those events. Sponsor-specific rules (which event gets “Sponsor” vs “Paying Delegate”) are described in section 4.

**Base label** (before sponsor/speaker overrides) is decided from Cvent **registration type** only:

- If registration type is in the **Paying Delegate** list (e.g. Law Firm, Investment Bank, Research Institution/University, Small and Midsize Enterprise, …) → **Paying Delegate**.
- If registration type is **Dealmakers Guest** → **Dealmakers Guest**.
- If registration type/path indicates **Speaker - Sponsor** or **Speaker - Non Sponsor** → the corresponding **Speaker** label.
- Otherwise → **Paying Delegate**.

Then:

- **Sponsor Executive / Sponsor Client:** If an EXEC/CLIENT discount code was used, we look up the **sponsor** in HubSpot (see section 5) and use HubSpot **sponsor–event associations** to decide, per event, whether the attendee keeps the **Sponsor Executive/Client** label or gets **Paying Delegate** (when they paid for that transaction and the event is not one the sponsor is linked to).
- **Speaker question:** For speakers with a multi-event (e.g. all-access) ticket, we use the answer to “Which event are you participating as a speaker?”: “Both” → speaker on both events; one event name → that event gets the speaker label, the other event(s) get **Dealmakers Guest** or **Paying Delegate** (if they paid to upgrade).

**Result:** For each sync step we have a list of **event associations**: event ID, event name, and **label**. We create/update **attendee → event** associations in HubSpot with that **label** (association type). We skip creating an association if the attendee is already associated to that event with the same label.

### 2.2 Festivals

- Festivals are added when we resolve an **EXEC/CLIENT** discount code whose suffix matches a HubSpot **festival** by **full_name** (see above).
- We associate the attendee to each such **festival** (attendee → festival). No label is used on the association. We skip if already associated.

### 2.3 Sponsors

- Sponsors are determined **only** from **EXEC/CLIENT discount codes** (see section 5). We do **not** derive sponsor from registration type alone.
- For each such code we look up a **HubSpot sponsor** by the fields **`exec_discount_code`** or **`client_discount_code`** (depending on whether the code is EXEC or CLIENT). If a sponsor is found, we associate the **attendee → sponsor**. We skip if already associated.

So: **event** and **festival** associations come from admission item + discount codes; **sponsor** associations come only from discount codes, using the sponsor’s discount-code fields.

---

## 3. When we create deals

Deals are created only when **all** of the following are true:

1. **Registration status** (from Cvent) is **Accepted**.
2. **Amount** for the order/transaction is **positive** (we use the amount paid when present, otherwise amount ordered).
3. **Reference ID** (from Cvent) does **not** contain **“DelSale”** (so we exclude certain test/sale types).

If any of these fails, **no deal** is created or updated for that step. Deals are always associated to the **contact**, not directly to the attendee.

---

## 4. How we decide which event(s) a deal is for and how revenue is split

Logic depends on the **scenario** we detect for that attendee/order.

### 4.1 Standard (single or multiple events, first-time or simple case)

- We have one or more **event associations** for this step (from admission item and/or discount codes).
- We create **one deal per event**.
- **Deal name:** `{Attendee full name} - {Event name}`.
- **Revenue split:** Total order amount is split **evenly** across the events.  
  Example: 2 events, £1,000 total → one deal for Event A with amount £500, one deal for Event B with amount £500.

### 4.2 Paying delegate “upgrade” (already had one event, now added second)

- **When:** The attendee **already exists** in HubSpot, we now have **2 events** for this step, **at least 2 transactions** in the journey, and the attendee is **not** a speaker or sponsor type.
- **Interpretation:** They already had a deal for one event; this step adds the second event (e.g. all-access).
- **Actions:**
  - **Update** the **existing** deal for the **first** event: set its amount to **half of the total** (total of all transactions used for the journey).
  - **Create** a **new** deal for the **second** event with amount = **half of the total**.
- So revenue is split **50/50** between the two events. Deal names remain `{Name} - {Event A}` and `{Name} - {Event B}`.

### 4.3 Speaker upgrade (speaker added second event, possibly paid)

- **When:** The attendee is a **speaker** type, we have **2 events**, **at least 2 transactions**, and we’ve applied the speaker-question logic (so we know which event they speak at and which is “other”).
- **Deal:** We create **one new deal** only for the **event they paid to attend** in this step (the “upgrade” transaction). The amount is the **current transaction’s** amount (the last in the list, so phantom transactions don’t affect it). We do **not** create a second deal for the event they’re speaking at when that was free.
- **Event association:** The event they speak at keeps the **Speaker** label; the other event gets **Paying Delegate** if they paid to upgrade, otherwise **Dealmakers Guest**.

### 4.4 Sponsor upgrade (sponsor exec/client paid to add another event)

- **When:** The attendee is **Sponsor Executive** or **Sponsor Client** (from an EXEC/CLIENT discount code), we have **2 events**, **at least 2 transactions**, and the **current transaction amount is positive**.
- **Which event the deal is for:** We use HubSpot **sponsor–event associations** to know which event the sponsor is linked to. The deal is created **only for the event the sponsor is NOT linked to** (the one they “paid” to attend as a paying delegate).
- **Amount:** The **current transaction’s** amount (again, the last in the list).
- **Event labels:** On the event the sponsor **is** linked to → **Sponsor Executive/Client**. On the event the sponsor is **not** linked to and they paid → **Paying Delegate**; if they didn’t pay, that event still gets the sponsor label (complimentary).

So in all cases we decide the **event(s)** for deals from the **event associations** we computed for that step; revenue split is either **equal across events** (standard and paying-delegate upgrade) or **one deal per relevant event** with the appropriate amount (speaker/sponsor upgrade).

---

## 5. How we associate sponsors and what fields we use

- **When we look for a sponsor:** Only when the order has a discount code that matches the **EXEC/CLIENT** pattern:  
  `{anything}EXEC{eventOrFestivalCode}` or `{anything}CLIENT{eventOrFestivalCode}`.

- **HubSpot fields used:**
  - For **EXEC** codes we search the **Sponsors** object for a record whose property **`exec_discount_code`** **contains** the **full** discount code (e.g. `ACME_EXEC_EU26`). The field can hold multiple codes (comma/space separated); we use a “contains token” match.
  - For **CLIENT** codes we do the same using **`client_discount_code`**.

- **What we do with the sponsor:**
  - We **associate the attendee to that sponsor** (attendee → sponsor). We do **not** store the discount code on the attendee as the source of truth; the link is “attendee ↔ sponsor”.
  - We use the same sponsor (and HubSpot’s **sponsor → event** associations) to decide, per event, whether the attendee gets **Sponsor Executive/Client** or **Paying Delegate** (see sections 2.1 and 4.4).

- **Finding “sponsor’s events”:** We call HubSpot’s **associations API** for the sponsor: “sponsor → events”. That returns event IDs. We then apply an internal rule so that we know which of the attendee’s events are “sponsor-linked” and which are not (and therefore get Paying Delegate when they paid).

So: **Sponsor association is driven only by EXEC/CLIENT discount codes**, and we use the sponsor properties **`exec_discount_code`** and **`client_discount_code`** to find the correct sponsor record.

---

## 6. Phantom transactions

- **Definition:** A **phantom** transaction is one that has the **same amount** and the **same admission item ID** as an **earlier** transaction in the same attendee’s order history (for that Cvent event).
- **Effect:** Phantom transactions are **ignored** for sync: we do **not** create or update contacts, attendees, associations, or deals for that step. They are treated as duplicates (e.g. Cvent amendments or duplicate rows).
- So only the **first** occurrence of a given (amount, admission item) pair is processed; later occurrences are skipped.

---

## 7. Summary table

| What                | Source / rule |
|---------------------|----------------|
| **Contact**         | Look up by email; create if missing (first name, last name, email). |
| **Attendee**        | Look up by Cvent Attendee ID; create if missing; update properties each step. |
| **Event associations** | Admission item → HubSpot events via `cvent_admission_item_ids`; EXEC/CLIENT codes can add events (and set sponsor label). Label from registration type + sponsor/speaker rules. |
| **Festival associations** | From EXEC/CLIENT discount code suffix matched to festival **full_name**. |
| **Sponsor associations** | From EXEC/CLIENT discount codes only; search sponsor by **`exec_discount_code`** or **`client_discount_code`** (full code). |
| **Deals**           | Only when: Accepted, amount &gt; 0, reference ID without “DelSale”. Associated to **contact**. |
| **Deal event / split** | Standard: one deal per event, revenue split equally. Paying delegate upgrade: 50/50. Speaker/sponsor upgrade: one deal for the “paid” event with that transaction’s amount. |
| **Phantom**         | Same (amount, admission item) as earlier transaction → step ignored. |

If you want this tailored to your event names, object IDs, or deal stages, that can be added as a short “Configuration” section.
