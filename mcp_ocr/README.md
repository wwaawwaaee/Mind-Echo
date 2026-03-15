# mcp_ocr

Use a layout-parsing API to extract text from images/PDFs and output structured JSON with fixed fields.

## Configuration
Fill in API URL and TOKEN in:

`mcp_ocr/paddleocr_config.py`

Example placeholders are already included in that file.

## Usage
Single image:

```bash
python mcp_ocr/run_ocr.py --input path\to\image.jpg --output path\to\output.json
```

Directory:

```bash
python mcp_ocr/run_ocr.py --input path\to\images --output path\to\out_dir
```

Recursive scan:

```bash
python mcp_ocr/run_ocr.py --input path\to\images --output path\to\out_dir --recursive
```

Save markdown and images:

```bash
python mcp_ocr/run_ocr.py --input path\to\image.jpg --output path\to\out_dir --assets-dir path\to\assets
```

## Output JSON schema

```json
{
  "元数据": {
    "姓名": "",
    "性别": "",
    "年龄": "",
    "记录日期": ""
  },
  "病例数据": {
    "主诉": "",
    "现病史": "",
    "既往史": "",
    "体格检查": "",
    "初步判断": "",
    "处理": ""
  }
}
```
