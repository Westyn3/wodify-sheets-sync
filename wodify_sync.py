# wodify_sync.py

import pygsheets
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
]
SYNC_QUEUE_SHEET = "Sync Queue"

COACH_PAY = {
    "Coach: Olivia Hill": "$250.00",
    "Coach: Meghan Lindsay": "$125.00",
    "Coach: Beth Winiger": "$125.00",
    "Coach: Brittany Burris": "$125.00",
    "Coach: Megan Argueta": "$125.00",
    "Coach: Leah Davis": "$125.00",
}

# === 1. AUTHENTICATE TO GOOGLE SHEETS ===
gc = pygsheets.authorize(service_file="credentials.json")
sheet = gc.open(SHEET_NAME)

# === 2. LOAD SYNC QUEUE ===
sync_wks = sheet.worksheet_by_title(SYNC_QUEUE_SHEET)
sync_data = sync_wks.get_all_records()
print(f"\n📥 Sync Queue entries: {len(sync_data)}")

# === 3. PROCESS EACH SYNC ENTRY ===
added_count = 0
removed_count = 0
synced_count = 0

for idx, row in enumerate(sync_data, start=2):  # start=2 to skip header
    print(f"\n🔎 Processing row {idx}: {row}")

    # ✅ Skip if already synced
    if row.get("Synced", "").strip() == "✅":
        print(f"⏭️ Skipping row {idx}: Already synced ✅")
        continue

    full_name = row.get("Full Name", "").strip()
    new_tag = row.get("New Tag", "").strip()
    timestamp = row.get("Timestamp", "")

    if not full_name or not new_tag.startswith("Coach: "):
        print(f"⚠️ Skipping row {idx}: Missing full name or coach tag")
        continue

    new_coach = new_tag

    # 🔁 Refresh existing_clients from all coach sheets
    existing_clients = {}
    for coach_sheet in COACH_SHEETS:
        wks = sheet.worksheet_by_title(coach_sheet)
        rows = wks.get_all_records()
        for r in rows:
            name = r.get("Client Name", "").strip()
            if name:
                existing_clients[name.lower()] = coach_sheet

    old_coach = existing_clients.get(full_name.lower())

    # === Check if already correctly placed
    if old_coach == new_coach:
        print(f"🔁 {full_name} is already in {new_coach}, but checking for duplicates in other sheets...")

        # Check for duplicates in wrong sheets
        for coach_sheet in COACH_SHEETS:
            if coach_sheet != new_coach:
                wks = sheet.worksheet_by_title(coach_sheet)
                rows = wks.get_all_records()
                filtered = [r for r in rows if r.get("Client Name", "").strip().lower() != full_name.lower()]
                if len(filtered) != len(rows):
                    wks.clear()
                    wks.update_values("A1", [["Assigned Coach", "Client Name", "Coach's Pay Rate"]] +
                                      [[r["Assigned Coach"], r["Client Name"], r["Coach's Pay Rate"]] for r in filtered])
                    print(f"🧹 Removed duplicate {full_name} from {coach_sheet}")
                    removed_count += 1
        # Still mark as synced
        sync_wks.update_value(f"E{idx}", "✅")
        print(f"📝 Marked row {idx} as synced for {full_name}")
        synced_count += 1
        continue

    # === Add to new coach sheet
    try:
        pay = COACH_PAY.get(new_coach, "$100.00")
        new_row = [new_coach, full_name, pay]
        new_wks = sheet.worksheet_by_title(new_coach)

        # Find first empty row
        existing_values = new_wks.get_all_values(include_tailing_empty_rows=False)
        insert_row_index = len(existing_values) + 1
        new_wks.update_values(f"A{insert_row_index}:C{insert_row_index}", [new_row])

        print(f"✅ Added {full_name} to {new_coach}")
        added_count += 1
    except Exception as e:
        print(f"❌ Failed to add {full_name} to {new_coach}: {e}")
        continue

    # === Remove from old coach sheet
    if old_coach:
        try:
            old_wks = sheet.worksheet_by_title(old_coach)
            old_data = old_wks.get_all_records()
            filtered = [r for r in old_data if r.get("Client Name", "").strip().lower() != full_name.lower()]
            if len(filtered) != len(old_data):
                old_wks.clear()
                old_wks.update_values("A1", [["Assigned Coach", "Client Name", "Coach's Pay Rate"]] +
                                      [[r["Assigned Coach"], r["Client Name"], r["Coach's Pay Rate"]] for r in filtered])
                print(f"❌ Removed {full_name} from {old_coach}")
                removed_count += 1
        except Exception as e:
            print(f"⚠️ Failed to remove from {old_coach}: {e}")

    # === Mark sync row as completed
    sync_wks.update_value(f"E{idx}", "✅")
    print(f"📝 Marked row {idx} as synced for {full_name}")
    synced_count += 1

print(
    f"\n📊 Summary:\n"
    f"👥 Clients added to new coach: {added_count}\n"
    f"🧹 Duplicates removed from wrong coach sheets: {removed_count}\n"
    f"📝 Sync Queue rows marked complete: {synced_count}\n\n"
    )

