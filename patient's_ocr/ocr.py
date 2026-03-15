import base64
import requests
import time
import io
from pathlib import Path
from PIL import Image

# --- 配置参数 ---
API_URL = "https://63d8zcb5y64d48bc.aistudio-app.com/layout-parsing"
TOKEN = "004060330e3babe56c55470800f72a82240a9b98"

# 路径配置
INPUT_DIR = Path("D:/vscode-project/git-project/Mind-Echo/raw_data/photo")
OUTPUT_DIR = Path("./ocr_results")

# 技术参数
MAX_PIXEL = 2000     # 限制图片最大长边，防止服务器500错误
RETRY_COUNT = 3      # 失败自动重试次数
TIMEOUT = 120        # 单次请求超时时间(秒)

def compress_image_to_base64(img_path):
    """
    读取并压缩图片，确保尺寸在服务器处理范围内
    """
    with Image.open(img_path) as img:
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # 等比例缩放
        w, h = img.size
        if max(w, h) > MAX_PIXEL:
            scale = MAX_PIXEL / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), Image.Resampling.LANCZOS)
        
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=85)
        return base64.b64encode(buffer.getvalue()).decode("ascii")

def process_image(img_path):
    """
    执行单张图片的 OCR 请求
    """
    for attempt in range(RETRY_COUNT):
        try:
            file_data = compress_image_to_base64(img_path)
            
            headers = {
                "Authorization": f"token {TOKEN}",
                "Content-Type": "application/json"
            }
            payload = {
                "file": file_data,
                "fileType": 1,
                "useDocOrientationClassify": True,
                "useDocUnwarping": False,  # 关闭此项以提高成功率
                "useChartRecognition": False
            }

            response = requests.post(API_URL, json=payload, headers=headers, timeout=TIMEOUT)
            
            if response.status_code == 200:
                result = response.json().get("result", {})
                markdown_text = "\n".join([res["markdown"]["text"] for res in result.get("layoutParsingResults", [])])
                return markdown_text, 200
            
            elif response.status_code == 500:
                if attempt < RETRY_COUNT - 1:
                    time.sleep(2)
                    continue
                return None, 500
            else:
                return None, response.status_code

        except Exception as e:
            if attempt < RETRY_COUNT - 1:
                time.sleep(2)
                continue
            return None, str(e)
    return None, "FAILED_AFTER_RETRIES"

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    image_files = [f for f in INPUT_DIR.iterdir() if f.suffix.lower() in {'.jpg', '.jpeg', '.png', '.bmp'}]
    total_files = len(image_files)
    
    print(f"[INFO] Found {total_files} images in {INPUT_DIR.absolute()}")
    print(f"[INFO] Output directory: {OUTPUT_DIR.absolute()}")
    print("-" * 60)

    success_count = 0
    skip_count = 0

    for index, img_path in enumerate(image_files):
        output_path = OUTPUT_DIR / f"{img_path.stem}.md"
        progress = f"[{index + 1}/{total_files}]"

        # 断点续传逻辑：检查文件是否已存在
        if output_path.exists():
            print(f"{progress} [SKIP] {img_path.name} (Result already exists)")
            skip_count += 1
            continue

        print(f"{progress} [PROCESSING] {img_path.name}...", end="\r")
        
        text, status = process_image(img_path)
        
        if text:
            output_path.write_text(text, encoding="utf-8")
            print(f"{progress} [SUCCESS] {img_path.name}")
            success_count += 1
        else:
            print(f"{progress} [ERROR] {img_path.name} (Status: {status})")

        # 控制请求频率
        time.sleep(0.5)

    print("-" * 60)
    print(f"[SUMMARY] Total: {total_files} | Processed: {success_count} | Skipped: {skip_count} | Failed: {total_files - success_count - skip_count}")

if __name__ == "__main__":
    main()