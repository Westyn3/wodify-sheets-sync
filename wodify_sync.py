# wodify_sync.py

import pygsheets
import time
from datetime import datetime

# === CONFIG ===
SHEET_NAME = "LHN Client + Coach Weekly"

COACH_SHEETS = [
    "Coach: Olivia Hill",
    "Coach: Meghan Lindsay",
    "Coach: Beth Winiger",
    "Coach: Brittany Burris",
    "Coach: Megan Argueta",
    "Coach: Leah Davis",
    "Coach: Amber Partin",
]

SYNC_QUEUE_SHEET = "Sync Queue"

COACH_PAY = {
    "Coach: Olivia Hill": "$250.00",
    "Coach: Meghan Lindsay": "$125.00",
    "Coach: Beth Winiger": "$125.00",
    "Coach: Brittany Burris": "$125.00",
    "Coach: Megan Argueta": "$125.00",
    "Coach: Leah Davis": "$125.00",
    "Coach: Amber Partin": "125.00",
}

# === UTILS ===
def retry(fn, *args, retries=3, delay=2):
    for attempt in range(retries):
        try:
            return fn(*args)
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(delay * (attempt + 1))


# === AUTH ===
gc = pygsheets.authorize(service_file="credentials.json")
sheet = gc.open(SHEET_NAME)

# === LOAD SYNC QUEUE ===
sync_wks = sheet.worksheet_by_title(SYNC_QUEUE_SHEET)
sync_rows = retry(sync_wks.get_all_records)
headers = sync_wks.get_row(1)

if "Synced" not in headers:
    raise ValueError("❌ 'Synced' column not found in Sync Queue")

SYNCED_COL_IDX = headers.index("Synced") + 1

print(f"\n📥 Sync Queue entries: {len(sync_rows)}")

# === CACHE COACH SHEETS (CRITICAL FIX) ===
coach_worksheets = {}
coach_data = {}
client_to_coach = {}

for coach in COACH_SHEETS:
    wks = sheet.worksheet_by_title(coach)
    coach_worksheets[coach] = wks
    rows = retry(wks.get_all_records)
    coach_data[coach] = rows

    for r in rows:
        name = r.get("Client Name", "").strip()
        if name:
            client_to_coach[name.lower()] = coach

# === PROCESS SYNC QUEUE ===
added_count = 0
removed_count = 0
synced_count = 0

for idx, row in enumerate(sync_rows, start=2):
    print(f"\n🔎 Processing row {idx}: {row}")

    if row.get("Synced", "").strip() == "✅":
        print("⏭️ Already synced")
        continue

    full_name = row.get("Full Name", "").strip()
    new_coach = row.get("New Tag", "").strip()

    if not full_name or not new_coach.startswith("Coach: "):
        print("⚠️ Missing name or coach tag")
        continue

    old_coach = client_to_coach.get(full_name.lower())

    # === IF ALREADY IN CORRECT SHEET ===
    if old_coach == new_coach:
        print(f"🔁 {full_name} already in {new_coach}. Checking duplicates...")

        for coach, rows in coach_data.items():
            if coach == new_coach:
                continue

            filtered = [
                r for r in rows
                if r.get("Client Name", "").strip().lower() != full_name.lower()
            ]

            if len(filtered) != len(rows):
                retry(
                    coach_worksheets[coach].update_values,
                    "A2",
                    [[r["Assigned Coach"], r["Client Name"], r["Coach's Pay Rate"]] for r in filtered]
                )
                coach_data[coach] = filtered
                removed_count += 1
                print(f"🧹 Removed duplicate from {coach}")

        retry(sync_wks.update_value, (idx, SYNCED_COL_IDX), "✅")
        synced_count += 1
        continue

    # === ADD TO NEW COACH ===
    try:
        pay = COACH_PAY.get(new_coach, "$100.00")
        new_row = [new_coach, full_name, pay]

        wks = coach_worksheets[new_coach]
        existing = coach_data[new_coach]
        insert_row = len(existing) + 2  # after header

        retry(wks.update_values, f"A{insert_row}:C{insert_row}", [new_row])

        coach_data[new_coach].append({
            "Assigned Coach": new_coach,
            "Client Name": full_name,
            "Coach's Pay Rate": pay
        })

        client_to_coach[full_name.lower()] = new_coach
        added_count += 1
        print(f"✅ Added {full_name} to {new_coach}")

    except Exception as e:
        print(f"❌ Failed to add {full_name}: {e}")
        continue

    # === REMOVE FROM OLD COACH ===
    if old_coach:
        rows = coach_data[old_coach]
        filtered = [
            r for r in rows
            if r.get("Client Name", "").strip().lower() != full_name.lower()
        ]

        if len(filtered) != len(rows):
            retry(
                coach_worksheets[old_coach].update_values,
                "A2",
                [[r["Assigned Coach"], r["Client Name"], r["Coach's Pay Rate"]] for r in filtered]
            )
            coach_data[old_coach] = filtered
            removed_count += 1
            print(f"❌ Removed {full_name} from {old_coach}")

    # === MARK SYNCED ===
    retry(sync_wks.update_value, (idx, SYNCED_COL_IDX), "✅")
    synced_count += 1
    print(f"📝 Marked synced")

# === SUMMARY ===
print(
    f"\n📊 Summary\n"
    f"➕ Added: {added_count}\n"
    f"🧹 Removed: {removed_count}\n"
    f"✅ Synced: {synced_count}\n"
)
