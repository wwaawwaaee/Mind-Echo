import re
import json
from pathlib import Path
import pandas as pd


def parse_dialogue(path: Path):
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    # 时间：取第一行
    first_line = lines[0].strip() if lines else ""
    m = re.search(
        r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日\s*(上午|下午)?\s*(\d{1,2})\s*:\s*(\d{2})",
        first_line,
    )
    visit_time = None
    if m:
        y, mo, d, ap, hh, mm = m.groups()
        hh = int(hh)
        if ap == "下午" and hh < 12:
            hh += 12
        visit_time = f"{y}-{int(mo):02d}-{int(d):02d} {hh:02d}:{mm}"

    # 关键词
    keywords = []
    km = re.search(r"关键词:\s*\n(.+?)\n\n", text, flags=re.S)
    if km:
        kw_line = km.group(1).strip()
        keywords = [k.strip(" ，、") for k in re.split(r"[、，]", kw_line) if k.strip()]

    # 对话内容
    start = text.find("【D】")
    if start == -1:
        start = text.find("【P】")
    content = text[start:].strip() if start != -1 else text.strip()

    return {
        "visit_time_raw": first_line,
        "visit_time": visit_time,
        "keywords": keywords,
        "content": content,
    }


def parse_scales(score_path: Path, ids):
    df = pd.read_excel(score_path)
    rows = df[df["序号"].isin(ids)]

    results = []
    for _, row in rows.iterrows():
        g_items = []
        for i in range(1, 8):
            col = next(c for c in df.columns if c.startswith(f"G{i}. 在过去2个星期"))
            g_items.append(int(row[col]))
        g_total = sum(g_items)

        p_items = []
        for i in range(1, 10):
            col = next(c for c in df.columns if c.startswith(f"P{i}. 在过去2个星期"))
            p_items.append(int(row[col]))
        p_total = sum(p_items)

        results.append(
            {
                "respondent_role": "self",
                "GAD-7": {"items": g_items, "total": g_total},
                "PHQ-9": {"items": p_items, "total": p_total},
            }
        )

    return results


def build_patient_json(dialogue_path: Path, score_path: Path):
    stem = dialogue_path.stem
    m = re.match(r"^(\d+(?:，\d+)*)\s*(.+)$", stem)
    if not m:
        raise ValueError(f"文件名不符合约定格式: {stem}")
    ids = [int(x) for x in m.group(1).split("，")]
    name = m.group(2)

    dialogue = parse_dialogue(dialogue_path)
    scales = parse_scales(score_path, ids)

    data = {
        "dataset_meta": {
            "schema_version": "0.1",
            "patient_centered": True,
        },
        "patients": [
            {
                "patient_id": f"P-{ids[0]:06d}",
                "name": name,
                "age_group": "adult",
                "visits": [
                    {
                        "visit_id": f"V-{ids[0]:06d}-1",
                        "visit_time": dialogue["visit_time"],
                        "visit_time_raw": dialogue["visit_time_raw"],
                        "dialogue": {
                            "source_file": dialogue_path.name,
                            "keywords": dialogue["keywords"],
                            "content": dialogue["content"],
                        },
                        "scales": scales,
                    }
                ],
            }
        ],
    }

    return data


def main():
    dialogue_path = Path("raw_data/dialogues/41江凤敏.txt")
    score_path = Path("raw_data/diagram/score.csv")

    data = build_patient_json(dialogue_path, score_path)

    out_path = Path("processed_data/output/patient_41.json")
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote in {out_path}")


if __name__ == "__main__":
    main()
