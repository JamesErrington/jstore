import time

entries = [
    { "key": "name", "value": "James Errington" },
    { "key": "address", "value": "St Albans, United Kingdom" },
    { "key": "age", "value": "26" },
]

for entry in entries:
    timestamp = round(time.time_ns() / 1000)
    with open(f"./data/{timestamp}.wal", "wb") as file:
        file.write(len(entry["key"]).to_bytes(8, "little"))
        file.write(bytes(entry["key"], "utf-8"))
        file.write(len(entry["value"]).to_bytes(8, "little"))
        file.write(bytes(entry["value"], "utf-8"))
        file.write(timestamp.to_bytes(8, "little"))

    time.sleep(1.5)
