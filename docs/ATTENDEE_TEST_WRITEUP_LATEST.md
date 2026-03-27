## Dry-run association + deal logic test (updated)

Event ID (Cvent): `aff9433d-2778-4dae-b917-f3c3858d7731`

This run uses the **updated Paying Delegate list**: registration types such as Research Institution/University, Small and Midsize Enterprise, and the other configured types are now explicitly in the list, so they receive the Paying Delegate label **without** a warning. Sponsor Executive and Sponsor Client remain driven by EXEC/CLIENT discount codes and are not in the Paying Delegate list (they get sponsor labels when the code matches).

Tested attendees: 7

### Todd DeLuca (todd@gmail.com)
Cvent Attendee ID: `5040d5d3-bd4f-4d15-b9f7-7bf2c80e13bf`
Registration type/path: `Sponsor Executive` / `Sponsor Client or Executive Path`
Base association label: `Paying Delegate`
Discount code(s) detected: TODEXECEU26
Speaker answer (if asked): `Both Events`
Admission item id (from Cvent): `edba3040-5a95-401f-90ba-aa51d64b83d2`

Transactions and outcomes:
1) Step 1 (date: 16/03/2026 22:18):
   Amount ordered/paid: 0 / 0; paid numeric: 0.0
   Admission item: ALL ACCESS TICKET: 3rd Annual LF Dealmakers Europe & 3rd Annual IP Dealmakers Europe (id: `edba3040-5a95-401f-90ba-aa51d64b83d2`)
   Association label outcome: 3rd Annual LF Dealmakers Europe -> Sponsor Executive; 3rd Annual IP Dealmakers Europe -> Sponsor Executive
   Deal outcome: No deal created (deal conditions not met)
   Why this label outcome: Registration type not in explicit lists; defaulted to Paying Delegate (Sponsor Executive). EXEC/CLIENT discount code matched sponsor(s). Paid this step: False. Sponsor-linked event IDs: ['39987075650']. Event 3rd Annual LF Dealmakers Europe -> Sponsor (in sponsor-linked set). Event 3rd Annual IP Dealmakers Europe -> Sponsor (in sponsor-linked set). Warnings: Registration type "Sponsor Executive" is not in the Paying Delegate list; defaulting to Paying Delegate.

---

### Vanessa Bonn (vaness@bonn.com)
Cvent Attendee ID: `423885f3-57fc-413f-822f-8f6a5c1f096e`
Registration type/path: `Research Institution/University` / `General Attendee Path`
Base association label: `Paying Delegate`
Discount code(s) detected: none
Admission item id (from Cvent): `b778bd71-299f-45e8-924f-219d4a15229a`

Transactions and outcomes:
1) Step 1 (date: 16/03/2026 22:32):
   Amount ordered/paid: 2,034 / 2,034; paid numeric: 2034.0
   Admission item: SINGLE EVENT TICKET: 3rd Annual LF Dealmakers Europe (id: `b778bd71-299f-45e8-924f-219d4a15229a`)
   Association label outcome: 3rd Annual LF Dealmakers Europe -> Paying Delegate
   Deal outcome: Deal: standard; create 3rd Annual LF Dealmakers Europe (£2034.0)
   Why this label outcome: Base registration type in Paying Delegate list (Research Institution/University).

---

### Kathleen Dixon (kat@bestmom.com)
Cvent Attendee ID: `d4c6a7af-50e4-4610-80c4-a401da2a0abe`
Registration type/path: `Speaker - Non Sponsor` / `Speaker Path - Internal`
Base association label: `Speaker - Non Sponsor`
Discount code(s) detected: LFSPEAKER
Speaker answer (if asked): `3rd Annual LF Dealmakers Europe`
Admission item id (from Cvent): `edba3040-5a95-401f-90ba-aa51d64b83d2`

Transactions and outcomes:
1) Step 1 (date: 16/03/2026 22:32):
   Amount ordered/paid: 0 / 0; paid numeric: 0.0
   Admission item: SINGLE EVENT TICKET: 3rd Annual LF Dealmakers Europe (id: `b778bd71-299f-45e8-924f-219d4a15229a`)
   Association label outcome: 3rd Annual LF Dealmakers Europe -> Speaker - Non Sponsor
   Deal outcome: No deal created (deal conditions not met)
   Why this label outcome: Base type: Speaker - Non Sponsor (reg type/path match).

1) Step 2 (date: 16/03/2026 22:33): phantom transaction ignored
   Amount ordered/paid: 0 / 0; admission item: `b778bd71-299f-45e8-924f-219d4a15229a`

1) Step 3 (date: 16/03/2026 22:33):
   Amount ordered/paid: 0 / 0; paid numeric: 0.0
   Admission item: ALL ACCESS TICKET: 3rd Annual LF Dealmakers Europe & 3rd Annual IP Dealmakers Europe (id: `edba3040-5a95-401f-90ba-aa51d64b83d2`)
   Association label outcome: 3rd Annual LF Dealmakers Europe -> Speaker - Non Sponsor; 3rd Annual IP Dealmakers Europe -> Dealmakers Guest
   Deal outcome: No deal created (deal conditions not met)
   Why this label outcome: Base type: Speaker - Non Sponsor (reg type/path match).

---

### Alyssa Hart (alyssa.dixon@globalbankingmarkets.com)
Cvent Attendee ID: `ed673cfd-616d-4c8f-8491-fdeef9a77108`
Registration type/path: `Speaker - Non Sponsor` / `Speaker Path - Internal`
Base association label: `Speaker - Non Sponsor`
Discount code(s) detected: IPSPEAKER
Speaker answer (if asked): `3rd Annual IP Dealmakers Europe`
Admission item id (from Cvent): `edba3040-5a95-401f-90ba-aa51d64b83d2`

Transactions and outcomes:
1) Step 1 (date: 16/03/2026 22:02):
   Amount ordered/paid: 0 / 0; paid numeric: 0.0
   Admission item: SINGLE EVENT TICKET: 3rd Annual IP Dealmakers Europe (id: `8543c6d8-d195-4491-ba84-64d09d76432e`)
   Association label outcome: 3rd Annual IP Dealmakers Europe -> Speaker - Non Sponsor
   Deal outcome: No deal created (deal conditions not met)
   Why this label outcome: Base type: Speaker - Non Sponsor (reg type/path match).

1) Step 2 (date: 16/03/2026 22:07): phantom transaction ignored
   Amount ordered/paid: 0 / 0; admission item: `8543c6d8-d195-4491-ba84-64d09d76432e`

1) Step 3 (date: 16/03/2026 22:35):
   Amount ordered/paid: 360 / 360; paid numeric: 360.0
   Admission item: ALL ACCESS TICKET: 3rd Annual LF Dealmakers Europe & 3rd Annual IP Dealmakers Europe (id: `edba3040-5a95-401f-90ba-aa51d64b83d2`)
   Association label outcome: 3rd Annual LF Dealmakers Europe -> Paying Delegate; 3rd Annual IP Dealmakers Europe -> Speaker - Non Sponsor
   Deal outcome: Deal: speaker_upgrade; create 3rd Annual IP Dealmakers Europe (£360.0)
   Why this label outcome: Base type: Speaker - Non Sponsor (reg type/path match).

1) Step 4 (date: 16/03/2026 22:35): phantom transaction ignored
   Amount ordered/paid: 0 / 0; admission item: `8543c6d8-d195-4491-ba84-64d09d76432e`

---

### Alison Wiedmann (aly@gmail.com)
Cvent Attendee ID: `c6c5db6f-edb5-409a-a2be-f8966111963d`
Registration type/path: `Sponsor Executive` / `Sponsor Client or Executive Path`
Base association label: `Paying Delegate`
Discount code(s) detected: ALYEXECIPEU26
Speaker answer (if asked): `3rd Annual IP Dealmakers Europe`
Admission item id (from Cvent): `edba3040-5a95-401f-90ba-aa51d64b83d2`

Transactions and outcomes:
1) Step 1 (date: 16/03/2026 22:18):
   Amount ordered/paid: 0 / 0; paid numeric: 0.0
   Admission item: SINGLE EVENT TICKET: 3rd Annual IP Dealmakers Europe (id: `8543c6d8-d195-4491-ba84-64d09d76432e`)
   Association label outcome: 3rd Annual IP Dealmakers Europe -> Sponsor Executive
   Deal outcome: No deal created (deal conditions not met)
   Why this label outcome: Registration type not in explicit lists; defaulted to Paying Delegate (Sponsor Executive). EXEC/CLIENT discount code matched sponsor(s). Paid this step: False. Sponsor-linked event IDs: []. Event 3rd Annual IP Dealmakers Europe -> Sponsor (in sponsor-linked set). Warnings: Registration type "Sponsor Executive" is not in the Paying Delegate list; defaulting to Paying Delegate.

1) Step 2 (date: 16/03/2026 22:36):
   Amount ordered/paid: 0 / 0; paid numeric: 0.0
   Admission item: ALL ACCESS TICKET: 3rd Annual LF Dealmakers Europe & 3rd Annual IP Dealmakers Europe (id: `edba3040-5a95-401f-90ba-aa51d64b83d2`)
   Association label outcome: 3rd Annual LF Dealmakers Europe -> Sponsor Executive; 3rd Annual IP Dealmakers Europe -> Sponsor Executive
   Deal outcome: No deal created (deal conditions not met)
   Why this label outcome: Registration type not in explicit lists; defaulted to Paying Delegate (Sponsor Executive). EXEC/CLIENT discount code matched sponsor(s). Paid this step: False. Sponsor-linked event IDs: ['39987075650']. Event 3rd Annual LF Dealmakers Europe -> Sponsor (in sponsor-linked set). Event 3rd Annual IP Dealmakers Europe -> Sponsor (in sponsor-linked set). Warnings: Registration type "Sponsor Executive" is not in the Paying Delegate list; defaulting to Paying Delegate.

1) Step 3 (date: 16/03/2026 22:36): phantom transaction ignored
   Amount ordered/paid: 0 / 0; admission item: `8543c6d8-d195-4491-ba84-64d09d76432e`

---

### Ryan Zwick (ryry@zwick.org)
Cvent Attendee ID: `d1fe9665-b8e3-42b3-acd4-3d7e4092507f`
Registration type/path: `Sponsor Client` / `Sponsor Client or Executive Path`
Base association label: `Paying Delegate`
Discount code(s) detected: ALYCLIENTIPEU26
Speaker answer (if asked): `3rd Annual IP Dealmakers Europe`
Admission item id (from Cvent): `edba3040-5a95-401f-90ba-aa51d64b83d2`

Transactions and outcomes:
1) Step 1 (date: 16/03/2026 22:40):
   Amount ordered/paid: 0 / 0; paid numeric: 0.0
   Admission item: SINGLE EVENT TICKET: 3rd Annual IP Dealmakers Europe (id: `8543c6d8-d195-4491-ba84-64d09d76432e`)
   Association label outcome: 3rd Annual IP Dealmakers Europe -> Sponsor Client
   Deal outcome: No deal created (deal conditions not met)
   Why this label outcome: Registration type not in explicit lists; defaulted to Paying Delegate (Sponsor Client). EXEC/CLIENT discount code matched sponsor(s). Paid this step: False. Sponsor-linked event IDs: []. Event 3rd Annual IP Dealmakers Europe -> Sponsor (in sponsor-linked set). Warnings: Registration type "Sponsor Client" is not in the Paying Delegate list; defaulting to Paying Delegate.

1) Step 2 (date: 16/03/2026 22:42):
   Amount ordered/paid: 360 / 360; paid numeric: 360.0
   Admission item: ALL ACCESS TICKET: 3rd Annual LF Dealmakers Europe & 3rd Annual IP Dealmakers Europe (id: `edba3040-5a95-401f-90ba-aa51d64b83d2`)
   Association label outcome: 3rd Annual LF Dealmakers Europe -> Paying Delegate; 3rd Annual IP Dealmakers Europe -> Sponsor Client
   Deal outcome: Deal: sponsor_upgrade; create 3rd Annual LF Dealmakers Europe (£360.0)
   Why this label outcome: Registration type not in explicit lists; defaulted to Paying Delegate (Sponsor Client). EXEC/CLIENT discount code matched sponsor(s). Paid this step: True. Sponsor-linked event IDs: ['39987075650']. Event 3rd Annual IP Dealmakers Europe -> Sponsor (in sponsor-linked set). Warnings: Registration type "Sponsor Client" is not in the Paying Delegate list; defaulting to Paying Delegate.

1) Step 3 (date: 16/03/2026 22:42): phantom transaction ignored
   Amount ordered/paid: 0 / 0; admission item: `8543c6d8-d195-4491-ba84-64d09d76432e`

---

### Evan Tracy (evan@tracyindustries.com)
Cvent Attendee ID: `5e33d759-fbd5-4f64-ab60-e2a5df288efa`
Registration type/path: `Small and Midsize Enterprise` / `General Attendee Path`
Base association label: `Paying Delegate`
Discount code(s) detected: none
Admission item id (from Cvent): `edba3040-5a95-401f-90ba-aa51d64b83d2`

Transactions and outcomes:
1) Step 1 (date: 16/03/2026 22:47):
   Amount ordered/paid: 2,429 / 2,429; paid numeric: 2429.0
   Admission item: SINGLE EVENT TICKET: 3rd Annual IP Dealmakers Europe (id: `8543c6d8-d195-4491-ba84-64d09d76432e`)
   Association label outcome: 3rd Annual IP Dealmakers Europe -> Paying Delegate
   Deal outcome: Deal: standard; create 3rd Annual IP Dealmakers Europe (£2429.0)
   Why this label outcome: Base registration type in Paying Delegate list (Small and Midsize Enterprise).

1) Step 2 (date: 16/03/2026 22:48):
   Amount ordered/paid: 2,514 / 2,514; paid numeric: 2514.0
   Admission item: ALL ACCESS TICKET: 3rd Annual LF Dealmakers Europe & 3rd Annual IP Dealmakers Europe (id: `edba3040-5a95-401f-90ba-aa51d64b83d2`)
   Association label outcome: 3rd Annual LF Dealmakers Europe -> Paying Delegate; 3rd Annual IP Dealmakers Europe -> Paying Delegate
   Deal outcome: Deal: paying_delegate_upgrade; update_existing 3rd Annual LF Dealmakers Europe (£2471.5), create 3rd Annual IP Dealmakers Europe (£2471.5)
   Why this label outcome: Base registration type in Paying Delegate list (Small and Midsize Enterprise).

---
