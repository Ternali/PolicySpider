import json

if __name__ == "__main__":
    record_dict = {
        "newest_policy_title": None
    }
    record_json = json.dumps(record_dict, ensure_ascii=False, indent=4)
    with open("record.json", "w", encoding="utf-8") as f:
        f.write(record_json)