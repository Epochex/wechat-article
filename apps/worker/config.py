ROOT = r"C:\Users\lin\Desktop\鼠鼠学习\wechat-article"

EXPORTER_BASE = "http://localhost:3000"
AUTH_KEY = "82ba9ed470dd43088e9c821d5b6f4a29"

DOWNLOAD_FORMAT = "markdown"

RAW_DIR = ROOT + r"\data\raw"
EXPORTS_DIR = ROOT + r"\data\exports"
NEWSLETTER_DIR = ROOT + r"\data\newsletter"


TIME_WINDOW_DAYS = 7

SOURCES = [
    {"keyword": "36氪", "begin": 0, "size": 30},
    {"keyword": "机器之心", "begin": 0, "size": 20},
    {"keyword": "量子位", "begin": 0, "size": 20},
    {"keyword": "AI科技评论", "begin": 0, "size": 20},
    {"keyword": "甲子光年", "begin": 0, "size": 10},
]

INCLUDE_TERMS = [
    "AI", "人工智能", "大模型", "LLM", "Agent", "RAG", "MCP",
    "推理", "训练", "蒸馏", "对齐", "多模态", "视频生成", "图像生成",
    "Copilot", "Claude", "Gemini", "OpenAI", "DeepSeek", "Anthropic",
    "企业", "落地", "应用", "产品", "SaaS", "工作流", "自动化",
    "安全", "风控", "合规", "隐私",
    "芯片", "GPU", "算力", "推理加速", "端侧", "边缘",
]

EXCLUDE_TERMS = [
    "情感", "星座", "鸡汤", "段子", "八卦", "娱乐", "影视", "综艺",
    "育儿", "美食", "旅游", "夜读",
    "招聘", "作者招聘", "编辑作者招聘",
]

TARGET_TOTAL_MIN = 8
TARGET_TOTAL_MAX = 12

CATEGORY_QUOTA = {
    "大模型竞技场": (2, 3),
    "AI产品探新": (2, 3),
    "企业AI前沿与AI原生企业": (2, 3),
    "先行者观点": (2, 3),
}

OPENING_STYLE = "v2"
NEWSLETTER_TITLE = "AI Newsletter"

# 新增：抓取去重缓存（跨运行持久化）
FETCH_CACHE_PATH = EXPORTS_DIR + r"\fetched_urls.json"

# 新增：下载节流（秒）。减少无意义请求量，降低触发风控概率
DOWNLOAD_SLEEP_SECONDS = 1.2