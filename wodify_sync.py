import pygsheets
import requests
from datetime import datetime

# === CONFIG ===
SHEET_NAME = "LHN Client + Coach Weekly"
COACH_SHEETS = [
    "Coach: Olivia Hill",
    "Coach: Meghan Lindsay",
    "Coach: Beth Winiger",
    "Coach: Brittany Burris",
    "Coach: Megan Argueta"
]
SYNC_QUEUE_SHEET = "Sync Queue"

COACH_PAY = {
    "Coach: Olivia Hill": "$200.00",
    "Coach: Meghan Lindsay": "$100.00",
    "Coach: Beth Winiger": "$100.00",
    "Coach: Brittany Burris": "$100.00",
    "Coach: Megan Argueta": "$100.00"
}

# === 1. AUTHENTICATE TO GOOGLE SHEETS ===
gc = pygsheets.authorize(service_file='credentials.json')
sheet = gc.open(SHEET_NAME)

# === 2. LOAD SYNC QUEUE ===
sync_wks = sheet.worksheet_by_title(SYNC_QUEUE_SHEET)
sync_data = sync_wks.get_all_records()
print(f"\n📥 Sync Queue entries: {len(sync_data)}")

# === SUMMARY COUNTERS ===
added_count = 0
removed_count = 0
synced_count = 0

# === 3. PROCESS EACH SYNC ENTRY ===
for idx, row in enumerate(sync_data, start=2):  # start=2 to skip header
    print(f"\n🔎 Processing row {idx}: {row}")

    full_name = row.get("Full Name", "").strip()
    new_tag = row.get("New Tag", "").strip()
    timestamp = row.get("Timestamp", "")

    if not full_name or not new_tag.startswith("Coach: "):
        print(f"⏭️ Skipping row {idx}: Invalid name or tag ➝ name: '{full_name}', tag: '{new_tag}'")
        continue

    full_name_normalized = full_name.lower()
    new_coach = new_tag

    # 🔁 Rebuild existing_clients to reflect real-time updates
    existing_clients = {}
    for coach_sheet in COACH_SHEETS:
        wks = sheet.worksheet_by_title(coach_sheet)
        rows = wks.get_all_records()
        for r in rows:
            name = r.get("Client Name", "").strip()
            if name:
                existing_clients[name.lower()] = coach_sheet

    old_coach = existing_clients.get(full_name_normalized)

    if old_coach == new_coach:
        print(f"🔁 {full_name} is already in {new_coach}, but checking for duplicates in other sheets...")

        for coach in COACH_SHEETS:
            if coach == new_coach:
                continue
            wks = sheet.worksheet_by_title(coach)
            values = wks.get_all_values()
            for row_idx, row_vals in enumerate(values[1:], start=2):
                if row_vals[1].strip().lower() == full_name_normalized:
                    wks.delete_rows(row_idx)
                    removed_count += 1
                    print(f"🧹 Removed duplicate {full_name} from {coach}")
                    break

        sync_wks.update_value(f"E{idx}", "✅")
        synced_count += 1
        print(f"📝 Marked row {idx} as synced for {full_name}")
        continue

    # 🎯 Get coach pay
    if new_coach in COACH_PAY:
        pay = COACH_PAY[new_coach]
    else:
        print(f"⚠️ Warning: {new_coach} not found in COACH_PAY — using default $100.00")
        pay = "$100.00"

    if not pay.startswith("$"):
        try:
            pay = f"${float(pay):.2f}"
        except ValueError:
            pay = "$100.00"
            print(f"⚠️ Invalid pay format for {new_coach}, defaulting to $100.00")

    # ✅ Add to new coach sheet
    new_row = [new_coach, full_name, pay]
    try:
        fresh_sheet = gc.open(SHEET_NAME)
        fresh_wks = fresh_sheet.worksheet_by_title(new_coach)
        existing_values = fresh_wks.get_all_values()
        real_rows = [r for r in existing_values if any(cell.strip() for cell in r)]
        next_empty_row = len(real_rows) + 1

        if next_empty_row > fresh_wks.rows:
            fresh_wks.rows = next_empty_row + 100

        fresh_wks.update_row(next_empty_row, new_row)
        added_count += 1
        print(f"✅ Added {full_name} to {new_coach} (Row {next_empty_row})")
    except Exception as e:
        print(f"❌ Failed to add {full_name} to {new_coach}: {e}")
        continue

    # ❌ Remove from old coach sheet
    if old_coach:
        old_wks = sheet.worksheet_by_title(old_coach)
        old_values = old_wks.get_all_values()
        for row_idx, row_vals in enumerate(old_values[1:], start=2):
            if row_vals[1].strip().lower() == full_name_normalized:
                old_wks.delete_rows(row_idx)
                removed_count += 1
                print(f"❌ Removed {full_name} from {old_coach}")
                break
        else:
            print(f"⚠️ Could not find {full_name} in {old_coach} to remove.")

    # ✅ Mark row as synced
    sync_wks.update_value(f"E{idx}", "✅")
    synced_count += 1
    print(f"📝 Marked row {idx} as synced for {full_name}")

# === SUMMARY ===
print("\n📊 Summary:")
print(f"👥 Clients added to new coach: {added_count}")
print(f"🧹 Duplicates removed from wrong coach sheets: {removed_count}")
print(f"📝 Sync Queue rows marked complete: {synced_count}")
print("\n✅ Wodify sync complete with coach tag reconciliation.")
