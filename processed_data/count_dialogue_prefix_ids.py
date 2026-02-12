import re
from collections import Counter, defaultdict
from pathlib import Path


TARGET_DIRS = [
    Path("raw_data/dialogues/hezhaoheng"),
    Path("raw_data/dialogues/zhouxiaoyv"),
]


def extract_prefix_ids(stem: str) -> list[int]:
    match = re.match(r"^\s*(\d+(?:\s*[，,、]\s*\d+)*)", stem)
    if not match:
        return []
    raw = match.group(1)
    return [int(x) for x in re.split(r"\s*[，,、]\s*", raw) if x.strip()]


def scan_dir(folder: Path) -> dict:
    files = sorted(folder.glob("*.txt"))
    id_to_files = defaultdict(list)
    missing_prefix_files = []
    all_ids = []

    for file in files:
        ids = extract_prefix_ids(file.stem)
        if not ids:
            missing_prefix_files.append(file.name)
            continue
        all_ids.extend(ids)
        for id_value in ids:
            id_to_files[id_value].append(file.name)

    counter = Counter(all_ids)
    duplicates = {k: v for k, v in counter.items() if v > 1}

    return {
        "folder": folder.as_posix(),
        "file_count": len(files),
        "parsed_file_count": len(files) - len(missing_prefix_files),
        "missing_prefix_files": missing_prefix_files,
        "total_prefix_count": len(all_ids),
        "unique_prefix_count": len(counter),
        "unique_prefix_ids": sorted(counter.keys()),
        "duplicates": duplicates,
        "id_to_files": {k: id_to_files[k] for k in sorted(id_to_files.keys())},
    }


def print_report(stats: dict) -> None:
    print(f"目录: {stats['folder']}")
    print(f"文件总数: {stats['file_count']}")
    print(f"成功解析前缀文件数: {stats['parsed_file_count']}")
    print(f"前缀编号总数(含重复): {stats['total_prefix_count']}")
    print(f"前缀编号去重数: {stats['unique_prefix_count']}")

    if stats["unique_prefix_ids"]:
        print("前缀编号列表(去重后):")
        print(", ".join(str(x) for x in stats["unique_prefix_ids"]))
    else:
        print("前缀编号列表(去重后): 无")

    if stats["duplicates"]:
        print("重复出现的编号及次数:")
        for id_value in sorted(stats["duplicates"].keys()):
            print(f"  {id_value}: {stats['duplicates'][id_value]}")
    else:
        print("重复出现的编号及次数: 无")

    if stats["missing_prefix_files"]:
        print("未匹配到前缀编号的文件:")
        for name in stats["missing_prefix_files"]:
            print(f"  {name}")
    else:
        print("未匹配到前缀编号的文件: 无")
    print("-" * 60)


def print_overall(all_stats: list[dict]) -> None:
    all_ids = []
    for stats in all_stats:
        all_ids.extend(stats["unique_prefix_ids"])
    all_unique = sorted(set(all_ids))
    print("总体汇总")
    print(f"两个目录合并后去重编号数: {len(all_unique)}")
    print("合并去重编号列表:")
    print(", ".join(str(x) for x in all_unique))


def main() -> None:
    stats_list = [scan_dir(folder) for folder in TARGET_DIRS]
    for stats in stats_list:
        print_report(stats)
    print_overall(stats_list)


if __name__ == "__main__":
    main()
