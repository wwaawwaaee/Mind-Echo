import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


NAME_PLACEHOLDER = "[NAME]"
ORG_PLACEHOLDER = "[ORG]"

TARGET_DIRS = [
    Path("raw_data/dialogues/hezhaoheng"),
    Path("raw_data/dialogues/zhouxiaoyv"),
]


@dataclass
class NerEntity:
    start: int
    end: int
    label: str


def build_ner_detector(model_name: str, chunk_chars: int) -> Callable[[str], list[NerEntity]]:
    from transformers import pipeline

    nlp = pipeline(
        task="token-classification",
        model=model_name,
        tokenizer=model_name,
        aggregation_strategy="simple",
    )

    def to_entities(raw_items: list[dict], offset: int) -> list[NerEntity]:
        entities: list[NerEntity] = []
        for item in raw_items:
            label = str(item.get("entity_group") or item.get("entity") or "").upper()
            start = int(item.get("start", -1))
            end = int(item.get("end", -1))
            if start < 0 or end <= start:
                continue
            if "PER" in label or "PERSON" in label or "NAME" in label:
                entities.append(NerEntity(start=start + offset, end=end + offset, label="NAME"))
            elif "ORG" in label:
                entities.append(NerEntity(start=start + offset, end=end + offset, label="ORG"))
        return entities

    def detect(text: str) -> list[NerEntity]:
        if not text:
            return []
        if len(text) <= chunk_chars:
            return to_entities(nlp(text), offset=0)

        entities: list[NerEntity] = []
        start = 0
        while start < len(text):
            end = min(start + chunk_chars, len(text))
            chunk = text[start:end]
            entities.extend(to_entities(nlp(chunk), offset=start))
            start = end
        return entities

    return detect


def replace_spans(text: str, entities: list[NerEntity]) -> str:
    if not entities:
        return text
    out = text
    for ent in sorted(entities, key=lambda x: x.start, reverse=True):
        placeholder = NAME_PLACEHOLDER if ent.label == "NAME" else ORG_PLACEHOLDER
        out = out[: ent.start] + placeholder + out[ent.end :]
    return out


def anonymize_filename_stem(stem: str, detect_ner: Callable[[str], list[NerEntity]]) -> str:
    entities = detect_ner(stem)
    return replace_spans(stem, entities)


def anonymize_text_full(
    text: str,
    detect_ner: Callable[[str], list[NerEntity]],
) -> str:
    lines = text.splitlines(keepends=True)
    if not lines:
        return text

    for idx, line in enumerate(lines):
        if line.strip():
            lines[idx] = replace_spans(line, detect_ner(line))
    return "".join(lines)


def process_file(
    file_path: Path,
    detect_ner: Callable[[str], list[NerEntity]],
    source_root: Path,
    output_root: Path,
    dry_run: bool,
) -> tuple[bool, bool, Path]:
    new_stem = anonymize_filename_stem(file_path.stem, detect_ner)
    renamed = new_stem != file_path.stem

    original = file_path.read_text(encoding="utf-8")
    updated = anonymize_text_full(
        original,
        detect_ner=detect_ner,
    )
    content_changed = updated != original

    rel_path = file_path.relative_to(source_root)
    out_dir = output_root / rel_path.parent
    out_path = out_dir / f"{new_stem}{file_path.suffix}"

    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path.write_text(updated, encoding="utf-8")

    return renamed, content_changed, out_path


def collect_target_files() -> list[Path]:
    files: list[Path] = []
    for folder in TARGET_DIRS:
        if folder.exists():
            files.extend(sorted(folder.rglob("*.txt")))
    return sorted(files)


def main() -> None:
    source_root = Path("raw_data/dialogues")
    parser = argparse.ArgumentParser(description="NER-only anonymization for NAME/ORG in filename and full text.")
    parser.add_argument(
        "--ner-model",
        type=str,
        required=True,
        help="Transformers token-classification model name/path for NER (PERSON/ORG)",
    )
    parser.add_argument(
        "--chunk-chars",
        type=int,
        default=400,
        help="Split long text into chunks for NER to avoid model max length overflow",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("processed_data/anonymized_dialogues"),
        help="Write anonymized files to this directory and keep original data unchanged",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    detect_ner = build_ner_detector(args.ner_model, args.chunk_chars)

    print(f"[INFO] Target folders: {', '.join(str(p) for p in TARGET_DIRS)}")
    print(f"[INFO] NER model: {args.ner_model}")
    print(f"[INFO] Chunk chars: {args.chunk_chars}")
    print(f"[INFO] Output root: {args.output_root}")

    files = collect_target_files()
    renamed_count = 0
    changed_count = 0
    output_count = 0

    for path in files:
        renamed, changed, out_path = process_file(
            file_path=path,
            detect_ner=detect_ner,
            source_root=source_root,
            output_root=args.output_root,
            dry_run=args.dry_run,
        )
        renamed_count += int(renamed)
        changed_count += int(changed)
        output_count += 1
        if args.dry_run:
            print(f"[DRY RUN] {path} -> {out_path}")

    mode = "DRY RUN" if args.dry_run else "APPLY"
    print(f"[{mode}] scanned files: {len(files)}")
    print(f"[{mode}] renamed files: {renamed_count}")
    print(f"[{mode}] content updates: {changed_count}")
    print(f"[{mode}] output files: {output_count}")


if __name__ == "__main__":
    main()
