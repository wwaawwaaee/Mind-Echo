import re
from pathlib import Path
from collections import Counter

ROOT = Path("processed_data/anonymized_dialogues")
LABEL_DOCTOR = "[医生]"
LABEL_PATIENT = "[患者]"
LABEL_FAMILY = "[患者家属]"

ROLE_LINE_PATTERNS = [
    (re.compile(r"^\s*医生\s*[：:]?\s*$"), LABEL_DOCTOR),
    (re.compile(r"^\s*患者\s*[：:]?\s*$"), LABEL_PATIENT),
    (re.compile(r"^\s*家属\d*\s*[：:]?\s*$"), LABEL_FAMILY),
]

META_PREFIXES = (
    "关键词",
    "关键字",
    "文字记录",
    "文本记录",
    "对话记录",
)


def normalize_bold_brackets(line: str):
    m = re.match(r"^\s*【([^】]+)】\s*[：:]?\s*(.*)$", line)
    if not m:
        return None

    raw_role = m.group(1).strip()
    text = m.group(2).strip()

    role = None
    if raw_role.startswith("D"):
        role = LABEL_DOCTOR
    elif raw_role.startswith("P"):
        if any(k in raw_role for k in ("家长", "家属", "爸爸", "妈妈", "妻子", "丈夫", "父母")):
            role = LABEL_FAMILY
        elif "患者" in raw_role:
            role = LABEL_PATIENT
        elif "孩子" in raw_role:
            role = LABEL_FAMILY
        else:
            role = LABEL_PATIENT
    if role is None:
        return None

    return f"{role}：{text}" if text else f"{role}："


def looks_meta(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    if s.startswith(META_PREFIXES):
        return True
    if re.search(r"\d{4}年\s*\d{1,2}月\s*\d{1,2}日", s):
        return True
    return False


def extract_age_from_stem(stem: str):
    m = re.search(r"(\d{1,3})\s*岁", stem)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def is_patient_side_explicit(line: str) -> bool:
    s = line.strip()
    return bool(re.match(r"^(你好[，, ]*医生|医生[，, ：:]|请问医生|医生你好)", s))


def choose_non_doctor_label(age, line: str):
    s = line.strip()
    family_cues = ("我家孩子", "孩子", "我儿子", "我女儿", "我孙子", "我孙女", "他妈妈", "他爸爸", "家里")
    if any(c in s for c in family_cues):
        return LABEL_FAMILY
    if age is not None and age < 18:
        return LABEL_FAMILY
    return LABEL_PATIENT


def should_skip_line_for_heuristic(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    if looks_meta(s):
        return True
    if s.endswith(":") or s.endswith("："):
        return True
    return False


def apply_explicit_mapping(lines):
    out = []
    current_role = None
    changed = False
    explicit_hits = 0

    for line in lines:
        bracket = normalize_bold_brackets(line)
        if bracket is not None:
            out.append(bracket)
            changed = changed or (line != bracket)
            explicit_hits += 1
            continue

        role_hit = None
        for pattern, role_label in ROLE_LINE_PATTERNS:
            if pattern.match(line):
                role_hit = role_label
                break

        if role_hit:
            current_role = role_hit
            explicit_hits += 1
            continue

        stripped = line.strip()
        if current_role and stripped:
            new_line = f"{current_role}：{stripped}"
            out.append(new_line)
            changed = changed or (new_line != line)
            continue

        out.append(line)

    return out, changed, explicit_hits


def apply_heuristic_mapping(lines, age):
    out = []
    changed = False

    dialogue_started = False
    first_dialogue_idx = None
    for i, line in enumerate(lines):
        if should_skip_line_for_heuristic(line):
            continue
        first_dialogue_idx = i
        dialogue_started = True
        break

    if not dialogue_started:
        return lines[:], False, 0

    first_line = lines[first_dialogue_idx].strip()
    non_doctor_label = choose_non_doctor_label(age, first_line)
    current_role = non_doctor_label if is_patient_side_explicit(first_line) else LABEL_DOCTOR

    heuristic_count = 0
    for idx, line in enumerate(lines):
        if idx < first_dialogue_idx:
            out.append(line)
            continue

        if should_skip_line_for_heuristic(line):
            out.append(line)
            continue

        s = line.strip()
        tagged = f"{current_role}：{s}"
        out.append(tagged)
        changed = True
        heuristic_count += 1

        if current_role == LABEL_DOCTOR:
            current_role = non_doctor_label
        else:
            current_role = LABEL_DOCTOR

    return out, changed, heuristic_count


def process_file(path: Path, stats: Counter):
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    explicit_out, explicit_changed, explicit_hits = apply_explicit_mapping(lines)

    used_heuristic = False
    heuristic_count = 0
    final_lines = explicit_out

    # If no explicit speaker markers were found, fallback to heuristic tagging.
    if explicit_hits == 0:
        age = extract_age_from_stem(path.stem)
        heuristic_out, h_changed, heuristic_count = apply_heuristic_mapping(explicit_out, age)
        final_lines = heuristic_out
        explicit_changed = explicit_changed or h_changed
        used_heuristic = heuristic_count > 0

    if explicit_changed:
        output = "\n".join(final_lines)
        if text.endswith("\n"):
            output += "\n"
        path.write_text(output, encoding="utf-8")
        stats["files_changed"] += 1

    stats["files_total"] += 1
    if explicit_hits > 0:
        stats["files_explicit"] += 1
    if used_heuristic:
        stats["files_heuristic"] += 1
    stats["explicit_turns"] += explicit_hits
    stats["heuristic_turns"] += heuristic_count



def main():
    stats = Counter()
    files = sorted(ROOT.rglob("*.txt"))
    for f in files:
        process_file(f, stats)

    print("Speaker labeling completed.")
    print(f"files_total={stats['files_total']}")
    print(f"files_changed={stats['files_changed']}")
    print(f"files_explicit={stats['files_explicit']}")
    print(f"files_heuristic={stats['files_heuristic']}")
    print(f"explicit_turns={stats['explicit_turns']}")
    print(f"heuristic_turns={stats['heuristic_turns']}")


if __name__ == "__main__":
    main()
