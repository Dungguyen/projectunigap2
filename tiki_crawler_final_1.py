import asyncio
import json
import logging
import random
import re
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import aiofiles
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).parent

# Đổi tên file Excel tại đây nếu bạn chạy file khác
INPUT_FILE = BASE_DIR / "products-200001-400000.xlsx"

DATASET_NAME = INPUT_FILE.stem

OUTPUT_DIR = BASE_DIR / "tiki_products" / DATASET_NAME
ERROR_DIR = BASE_DIR / "tiki_errors" / DATASET_NAME
CHECKPOINT_DIR = BASE_DIR / "tiki_checkpoints" / DATASET_NAME
LOG_DIR = BASE_DIR / "logs" / DATASET_NAME

API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
NETWORK_CHECK_URL = "https://api.tiki.vn"

BATCH_SIZE = 1000
START_CONCURRENCY = 15    # concurrency khởi điểm
MIN_CONCURRENCY = 3       # không giảm xuống dưới mức này dù bị rate-limit liên tục
MAX_CONCURRENCY = 50      # trần trên cùng, dù đang chạy ổn cũng không vượt quá
GROW_EVERY = 25           # cứ N request thành công liên tiếp thì tăng concurrency +1
RETRY = 5
TIMEOUT = 20
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=TIMEOUT)  # phải dùng ClientTimeout, không dùng số int trực tiếp
JITTER_MIN = 0.1   # giãn cách ngẫu nhiên giữa các request (giây), tránh bắn đồng loạt
JITTER_MAX = 0.6


# ============================================================
# LOGGING
# ============================================================

def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "crawl.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


# ============================================================
# CLEAN DESCRIPTION
# ============================================================

def normalize_description(html_text: Optional[str]) -> str:
    """Xóa HTML, script/style, ký tự thừa và chuẩn hóa khoảng trắng."""
    if not html_text:
        return ""

    soup = BeautifulSoup(str(html_text), "html.parser")

    for tag in soup(["script", "style"]):
        tag.decompose()

    text = soup.get_text(separator=" ")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


# ============================================================
# EXTRACT IMAGES
# ============================================================

def extract_images(product: Dict[str, Any]) -> List[str]:
    """Lấy danh sách URL ảnh từ field images của API Tiki."""
    images_url = []
    images = product.get("images", [])

    if isinstance(images, list):
        for img in images:
            if isinstance(img, dict) and img.get("base_url"):
                images_url.append(img["base_url"])

    return images_url


# ============================================================
# READ EXCEL
# ============================================================

def read_product_ids() -> Tuple[List[str], int, int]:
    """Đọc product id từ Excel, bỏ rỗng và bỏ trùng."""
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file: {INPUT_FILE}\n"
            f"Hãy đặt file Excel cùng thư mục với file Python này."
        )

    df = pd.read_excel(INPUT_FILE, usecols=["id"])
    before_count = len(df)

    product_ids = (
        df["id"]
        .dropna()
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .drop_duplicates()
        .tolist()
    )

    after_count = len(product_ids)
    removed_count = before_count - after_count

    return product_ids, before_count, removed_count


# ============================================================
# JSON UTILITIES
# ============================================================

async def save_json(file_path: Path, data: Any) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)

    async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=2))


def load_json_sync(file_path: Path, default: Any) -> Any:
    if not file_path.exists():
        return default

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


async def append_json_list(file_path: Path, item: Dict[str, Any]) -> None:
    """
    Lưu checkpoint ngay sau khi xử lý xong 1 ID.
    Cách này giúp nếu mất điện/tắt máy thì chạy lại không bị mất tiến trình.
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    data = load_json_sync(file_path, [])
    data.append(item)

    await save_json(file_path, data)


def get_done_ids_from_checkpoint(checkpoint_file: Path) -> Set[str]:
    checkpoint_data = load_json_sync(checkpoint_file, [])

    done_ids = set()

    for item in checkpoint_data:
        product_id = item.get("id")
        if product_id is not None:
            done_ids.add(str(product_id))

    return done_ids


def load_checkpoint_results(checkpoint_file: Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    checkpoint_data = load_json_sync(checkpoint_file, [])

    products = []
    errors = []

    for item in checkpoint_data:
        if item.get("success"):
            products.append(item["data"])
        else:
            errors.append(item["error"])

    return products, errors


# ============================================================
# ADAPTIVE CONCURRENCY LIMITER
# ============================================================

class AdaptiveLimiter:
    """
    Thay cho asyncio.Semaphore cố định.

    Cơ chế giống TCP congestion control:
    - Bình thường: mỗi N request thành công liên tiếp -> tăng dần concurrency (+1),
      để tận dụng tối đa băng thông Tiki cho phép.
    - Khi phát hiện dấu hiệu rate-limit (body rỗng / 429) -> giảm mạnh concurrency
      (giảm một nửa) và tạm nghỉ (cooldown) trước khi cho request mới chạy tiếp.

    Nhờ vậy chương trình luôn tự dò ra mức concurrency cao nhất mà Tiki chấp nhận,
    thay vì phải đoán và cố định một con số (quá cao -> lỗi hàng loạt,
    quá thấp -> chạy chậm không cần thiết).
    """

    def __init__(self, start: int, min_limit: int, max_limit: int, grow_every: int = 20):
        self._limit = start
        self._min_limit = min_limit
        self._max_limit = max_limit
        self._grow_every = grow_every

        self._active = 0
        self._success_streak = 0
        self._cooldown_until = 0.0

        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    async def acquire(self) -> None:
        async with self._condition:
            while True:
                now = time.monotonic()

                if now < self._cooldown_until:
                    # đang trong thời gian "hạ nhiệt" sau khi bị rate-limit
                    await asyncio.sleep(self._cooldown_until - now)
                    continue

                if self._active < self._limit:
                    self._active += 1
                    return

                await self._condition.wait()

    async def release(self) -> None:
        async with self._condition:
            self._active -= 1
            self._condition.notify_all()

    async def report_success(self) -> None:
        async with self._condition:
            self._success_streak += 1

            if self._success_streak >= self._grow_every and self._limit < self._max_limit:
                self._limit += 1
                self._success_streak = 0
                logging.info(f"[AdaptiveLimiter] Ổn định, tăng concurrency lên {self._limit}")

            self._condition.notify_all()

    async def report_rate_limited(self, cooldown_seconds: float = 8.0) -> None:
        async with self._condition:
            new_limit = max(self._min_limit, self._limit // 2)

            if new_limit != self._limit:
                logging.warning(
                    f"[AdaptiveLimiter] Nghi ngờ bị rate-limit, "
                    f"giảm concurrency {self._limit} -> {new_limit}, "
                    f"cooldown {cooldown_seconds}s"
                )

            self._limit = new_limit
            self._success_streak = 0
            self._cooldown_until = time.monotonic() + cooldown_seconds
            self._condition.notify_all()

    def acquire_cm(self):
        """Trả về async context manager để dùng: async with limiter.acquire_cm(): ..."""
        return _LimiterContext(self)


class _LimiterContext:
    def __init__(self, limiter: AdaptiveLimiter):
        self._limiter = limiter

    async def __aenter__(self):
        await self._limiter.acquire()
        return self._limiter

    async def __aexit__(self, exc_type, exc, tb):
        await self._limiter.release()



async def wait_for_network() -> None:
    """Nếu mất mạng thì chờ đến khi có mạng lại rồi mới tiếp tục."""
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=5)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(NETWORK_CHECK_URL) as response:
                    if response.status < 500:
                        logging.info("Đã có mạng, tiếp tục crawl.")
                        return

        except Exception:
            logging.warning("Mất kết nối mạng... chờ 10 giây rồi thử lại.")

        await asyncio.sleep(10)


# ============================================================
# FETCH PRODUCT
# ============================================================

def build_product(data: Dict[str, Any]) -> Dict[str, Any]:
    """Chuẩn hóa dữ liệu sản phẩm lấy từ API."""
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "url_key": data.get("url_key"),
        "price": data.get("price"),
        "description": normalize_description(data.get("description")),
        "images_url": extract_images(data),
    }


async def fetch_product(
    session: aiohttp.ClientSession,
    product_id: str,
    limiter: AdaptiveLimiter
) -> Dict[str, Any]:
    """
    Gọi API Tiki để lấy chi tiết sản phẩm.

    Điểm quan trọng:
    - Không dùng response.json() trực tiếp.
    - Luôn đọc response.text() rồi json.loads(text).
    - Nhờ vậy tránh lỗi:
      Attempt to decode JSON with unexpected mimetype: text/html
    """
    url = API_URL.format(product_id)

    async with limiter.acquire_cm():
        await asyncio.sleep(random.uniform(JITTER_MIN, JITTER_MAX))

        for attempt in range(1, RETRY + 1):
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                    status = response.status
                    content_type = response.headers.get("Content-Type", "")
                    text = await response.text()

                    # 404: sản phẩm không tồn tại, không cần retry
                    if status == 404:
                        return {
                            "id": product_id,
                            "success": False,
                            "error": {
                                "id": product_id,
                                "status": 404,
                                "reason": "Product not found",
                                "content_type": content_type,
                                "url": url
                            }
                        }

                    # Các lỗi tạm thời: retry
                    if status in [429, 500, 502, 503, 504]:
                        logging.warning(
                            f"ID {product_id} gặp HTTP {status}, retry {attempt}/{RETRY}"
                        )
                        if status == 429:
                            await limiter.report_rate_limited()
                        await asyncio.sleep(attempt * 2)
                        continue

                    # Các status khác 200
                    if status != 200:
                        return {
                            "id": product_id,
                            "success": False,
                            "error": {
                                "id": product_id,
                                "status": status,
                                "reason": f"HTTP error {status}",
                                "content_type": content_type,
                                "body_preview": text[:500],
                                "url": url
                            }
                        }

                    # Status 200 nhưng body có thể không phải JSON
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError as e:
                        logging.warning(
                            f"ID {product_id} JSONDecodeError: {e}. Retry {attempt}/{RETRY} | "
                            f"status={status} content_type='{content_type}' "
                            f"body_len={len(text)} body_preview={text[:200]!r}"
                        )

                        # Status 200 nhưng body rỗng/không parse được thường là dấu hiệu
                        # bị rate-limit/anti-bot chứ không phải lỗi mạng thông thường.
                        await limiter.report_rate_limited()

                        if attempt < RETRY:
                            await asyncio.sleep(attempt * 3 + random.uniform(0, 2))
                            continue

                        return {
                            "id": product_id,
                            "success": False,
                            "error": {
                                "id": product_id,
                                "status": "invalid_json",
                                "reason": "Response status 200 nhưng body không phải JSON",
                                "content_type": content_type,
                                "body_preview": text[:500],
                                "url": url
                            }
                        }

                    # Nếu JSON là thông báo lỗi hoặc không có id/name thì vẫn ghi lỗi rõ ràng
                    if not isinstance(data, dict):
                        return {
                            "id": product_id,
                            "success": False,
                            "error": {
                                "id": product_id,
                                "status": "invalid_json_structure",
                                "reason": "JSON trả về không phải object",
                                "content_type": content_type,
                                "body_preview": text[:500],
                                "url": url
                            }
                        }

                    # Một số API có thể trả JSON nhưng báo lỗi trong body
                    if data.get("code") in [404, "404"] or data.get("status") in [404, "404"]:
                        return {
                            "id": product_id,
                            "success": False,
                            "error": {
                                "id": product_id,
                                "status": 404,
                                "reason": "Product not found in JSON body",
                                "content_type": content_type,
                                "body_preview": text[:500],
                                "url": url
                            }
                        }

                    product = build_product(data)

                    await limiter.report_success()

                    return {
                        "id": product_id,
                        "success": True,
                        "data": product
                    }

            except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, asyncio.TimeoutError) as e:
                logging.warning(
                    f"ID {product_id} mất mạng/timeout (attempt {attempt}/{RETRY}): "
                    f"{type(e).__name__}: {e}"
                )

                if attempt == RETRY:
                    return {
                        "id": product_id,
                        "success": False,
                        "error": {
                            "id": product_id,
                            "status": "exception",
                            "reason": f"{type(e).__name__}: {e}",
                            "url": url
                        }
                    }

                await wait_for_network()
                continue

            except Exception as e:
                # Log traceback đầy đủ ra file log để biết lỗi THẬT nằm ở đâu
                # (str(e) đôi khi chỉ là message của 1 exception nội bộ bên trong
                # aiohttp/ssl, không phản ánh đúng nguyên nhân gốc).
                logging.error(
                    f"ID {product_id} lỗi exception ở attempt {attempt}/{RETRY}: "
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )

                if attempt == RETRY:
                    reason = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
                    return {
                        "id": product_id,
                        "success": False,
                        "error": {
                            "id": product_id,
                            "status": "exception",
                            "reason": reason,
                            "url": url
                        }
                    }

                await asyncio.sleep(attempt)

        return {
            "id": product_id,
            "success": False,
            "error": {
                "id": product_id,
                "status": "retry_failed",
                "reason": "Request failed after retries",
                "url": url
            }
        }


# ============================================================
# BATCH CRAWLER
# ============================================================

async def crawl_batch(product_ids: List[str], batch_index: int) -> Tuple[int, int]:
    """
    Crawl 1 batch.
    Nếu có checkpoint thì chỉ crawl các ID chưa xử lý.
    """
    output_file = OUTPUT_DIR / f"products_{batch_index:04d}.json"
    error_file = ERROR_DIR / f"errors_{batch_index:04d}.json"
    checkpoint_file = CHECKPOINT_DIR / f"checkpoint_{batch_index:04d}.json"

    done_ids = get_done_ids_from_checkpoint(checkpoint_file)
    remaining_ids = [pid for pid in product_ids if str(pid) not in done_ids]

    if done_ids:
        logging.info(
            f"Batch {batch_index:04d}: checkpoint có {len(done_ids)} ID, "
            f"còn {len(remaining_ids)} ID cần crawl."
        )

    if not remaining_ids:
        products, errors = load_checkpoint_results(checkpoint_file)

        await save_json(output_file, products)
        if errors:
            await save_json(error_file, errors)

        logging.info(
            f"Batch {batch_index:04d} hoàn tất từ checkpoint | "
            f"Success: {len(products)} | Errors: {len(errors)}"
        )

        return len(products), len(errors)

    limiter = AdaptiveLimiter(
        start=START_CONCURRENCY,
        min_limit=MIN_CONCURRENCY,
        max_limit=MAX_CONCURRENCY,
        grow_every=GROW_EVERY,
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://tiki.vn/",
    }

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(
        headers=headers,
        connector=connector,
        timeout=timeout
    ) as session:
        tasks = [
            fetch_product(session, product_id, limiter)
            for product_id in remaining_ids
        ]

        for task in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc=f"Batch {batch_index:04d}",
        ):
            result = await task

            # Checkpoint quan trọng:
            # Xử lý xong ID nào thì ghi ngay ID đó.
            await append_json_list(checkpoint_file, result)

    products, errors = load_checkpoint_results(checkpoint_file)

    await save_json(output_file, products)

    if errors:
        await save_json(error_file, errors)

    logging.info(
        f"Batch {batch_index:04d} done | "
        f"Success: {len(products)} | Errors: {len(errors)}"
    )

    return len(products), len(errors)


# ============================================================
# MAIN
# ============================================================

async def main() -> None:
    start_time = time.time()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    setup_logging()

    logging.info("===== START CRAWL =====")
    logging.info(f"Input file       : {INPUT_FILE}")
    logging.info(f"Dataset name     : {DATASET_NAME}")
    logging.info(f"Output dir       : {OUTPUT_DIR}")
    logging.info(f"Error dir        : {ERROR_DIR}")
    logging.info(f"Checkpoint dir   : {CHECKPOINT_DIR}")

    product_ids, original_count, removed_count = read_product_ids()

    total_products = len(product_ids)
    total_batches = (total_products + BATCH_SIZE - 1) // BATCH_SIZE

    total_success = 0
    total_error = 0

    logging.info(f"Số ID ban đầu trong Excel       : {original_count}")
    logging.info(f"Số ID bị trùng/rỗng đã loại     : {removed_count}")
    logging.info(f"Số ID sau khi làm sạch          : {total_products}")
    logging.info(f"Batch size                      : {BATCH_SIZE}")
    logging.info(f"Tổng số batch                   : {total_batches}")
    logging.info(f"Concurrency (start/min/max)     : {START_CONCURRENCY}/{MIN_CONCURRENCY}/{MAX_CONCURRENCY}")

    for i in range(total_batches):
        start = i * BATCH_SIZE
        end = start + BATCH_SIZE

        batch_ids = product_ids[start:end]
        batch_index = i + 1

        output_file = OUTPUT_DIR / f"products_{batch_index:04d}.json"
        error_file = ERROR_DIR / f"errors_{batch_index:04d}.json"

        if output_file.exists():
            existing_products = load_json_sync(output_file, [])
            existing_errors = load_json_sync(error_file, [])

            total_success += len(existing_products)
            total_error += len(existing_errors)

            logging.info(
                f"Skip batch {batch_index:04d}: output đã tồn tại | "
                f"Success: {len(existing_products)} | Errors: {len(existing_errors)}"
            )
            continue

        success, error = await crawl_batch(batch_ids, batch_index)

        total_success += success
        total_error += error

    elapsed = time.time() - start_time
    elapsed_text = time.strftime("%H:%M:%S", time.gmtime(elapsed))

    summary = {
        "input_file": str(INPUT_FILE),
        "dataset_name": DATASET_NAME,
        "original_id_count": original_count,
        "removed_duplicate_or_empty_count": removed_count,
        "clean_id_count": total_products,
        "success_count": total_success,
        "error_count": total_error,
        "total_batches": total_batches,
        "batch_size": BATCH_SIZE,
        "concurrency_start": START_CONCURRENCY,
        "concurrency_min": MIN_CONCURRENCY,
        "concurrency_max": MAX_CONCURRENCY,
        "elapsed_time": elapsed_text,
        "output_dir": str(OUTPUT_DIR),
        "error_dir": str(ERROR_DIR),
        "checkpoint_dir": str(CHECKPOINT_DIR),
    }

    await save_json(BASE_DIR / f"summary_{DATASET_NAME}.json", summary)

    print("\n==============================")
    print("CRAWL FINISHED")
    print("==============================")
    print(f"File Excel ban đầu              : {INPUT_FILE.name}")
    print(f"Tổng ID ban đầu                 : {original_count}")
    print(f"ID bị trùng/rỗng đã loại        : {removed_count}")
    print(f"Tổng ID sau khi làm sạch        : {total_products}")
    print(f"Lấy thành công                  : {total_success}")
    print(f"ID lỗi / Không có dữ liệu       : {total_error}")
    print(f"Tổng số batch                   : {total_batches}")
    print(f"Thời gian chạy                  : {elapsed_text}")
    print(f"Thư mục sản phẩm                : {OUTPUT_DIR}")
    print(f"Thư mục lỗi                     : {ERROR_DIR}")
    print(f"Thư mục checkpoint              : {CHECKPOINT_DIR}")
    print(f"File summary                    : {BASE_DIR / f'summary_{DATASET_NAME}.json'}")

    logging.info("===== CRAWL FINISHED =====")


if __name__ == "__main__":
    asyncio.run(main())
