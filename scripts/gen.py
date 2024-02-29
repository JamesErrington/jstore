#! /usr/bin/python3

import json
import random
import time

def load_words():
  with open('./scripts/words_dictionary.json') as json_file:
     words = json.load(json_file)
  return words

def random_word(words):
  word = random.choice(list(words))
  return word

def time_micro():
    return round(time.time_ns() / 1000)

def write_entry(file, key, value):
    file.write(len(key).to_bytes(8, "little"))
    file.write(bytes(key, "utf-8"))
    file.write(len(value).to_bytes(8, "little"))
    file.write(bytes(value, "utf-8"))
    file.write(time_micro().to_bytes(8, "little"))

    return len(key) + len(value)

words = load_words()

entries = {
    "name": "James Errington",
    "country": "United Kingdom",
}

timestamp = time_micro()
with open(f"./data/{timestamp}.wal", "wb") as file:
    size = 0

    for key, value in entries.items():
        size += write_entry(file, key, value)

    while size < 1024:
        key = random_word(words)
        value = random_word(words)
        size += write_entry(file, key, value)

    print(f"Wrote {size} bytes")
