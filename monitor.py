#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AOV Event Monitor - dùng chung cho HAI loại lịch sự kiện.

Một file script, một secret EVENT_URL (danh sách URL phân tách bằng dấu phẩy).
Script TỰ PHÂN LOẠI URL theo tên miền và chỉ xử lý nhóm được giao qua MONITOR_GROUP:

  - MONITOR_GROUP=moba  -> chỉ xử lý các URL có host khớp MOBA_HOST_SUFFIX
                           (mặc định *.moba.garena.vn). Dùng cho workflow chạy
                           Thứ 3 & Thứ 6 theo CỬA SỔ giờ (xem WINDOW_END_VN).
  - MONITOR_GROUP=other -> xử lý mọi URL KHÔNG thuộc nhóm moba (vd *.lienquan.garena.vn).
                           Dùng cho workflow quét rải đều cả ngày (một lượt rồi thoát).
  - MONITOR_GROUP=all   -> xử lý toàn bộ (mặc định, tương thích cũ).

VÌ SAO AN TOÀN (không lặp lại lỗi khiến tài khoản bị cấm):
  - KHÔNG còn vòng lặp 5.5h và KHÔNG còn "fleet" gọi gh run list / gh run cancel.
  - Nhóm moba: một lượt mỗi ngày Thứ 3/Thứ 6, khởi động TRƯỚC cửa sổ lúc máy chủ ít
    tải rồi giữ máy xuyên suốt cửa sổ; bắt được là thoát ngay. Tối đa ~2h/lượt.
  - Nhóm other: mỗi lượt chỉ quét MỘT lần (HTTP GET nhẹ) rồi thoát, ~1-2 phút.

GHI HISTORY AN TOÀN KHI HAI WORKFLOW CHẠY SONG SONG:
  - history.json được commit theo từng sự kiện, mỗi lần đều reset về bản remote mới
    nhất rồi áp thay đổi và push lại (có thử lại) -> không xung đột git.
"""

import os
import re
import time
import json
import random
import shutil
import hashlib
import subprocess
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests

# --- CẤU HÌNH CHUNG ---
# EVENT_URL có thể chứa NHIỀU URL, phân tách bằng XUỐNG DÒNG, dấu phẩy hoặc chấm phẩy.
# Khuyến nghị: mỗi URL một dòng (an toàn nhất vì URL ingame rất dài và chứa nhiều ký tự đặc biệt).
URL_RAW = os.getenv('EVENT_URL', '')
ALL_URLS = [u.strip() for u in re.split(r'[\n,;]+', URL_RAW) if u.strip()]
TG_TOKEN = os.getenv('TELEGRAM_TOKEN')
TG_ID = os.getenv('TELEGRAM_CHAT_ID')
RUN_ID = os.getenv('GITHUB_RUN_NUMBER', '0')
GH_TOKEN = os.getenv('GH_TOKEN')
WORKFLOW_FILE = os.getenv('WORKFLOW_FILE', 'main.yml')
AUTO_DISABLE = os.getenv('AUTO_DISABLE', 'false').lower() == 'true'

# --- PHÂN LOẠI NHÓM ---
MONITOR_GROUP = os.getenv('MONITOR_GROUP', 'all').lower()
MOBA_HOST_SUFFIX = os.getenv('MOBA_HOST_SUFFIX', 'moba.garena.vn').lower()

# --- CHẾ ĐỘ CHỐNG TRÙNG ---
#   once  -> mỗi URL chỉ đóng gói MỘT lần (phù hợp khi mỗi sự kiện là một URL khác nhau).
#   daily -> đóng gói lại mỗi NGÀY URL đó mở (phù hợp khi cùng một URL mở lại định kỳ).
CAPTURE_MODE = os.getenv('CAPTURE_MODE', 'once').lower()

# --- CẤU HÌNH CỬA SỔ (chỉ dùng cho nhóm moba) ---
WINDOW_END_VN = os.getenv('WINDOW_END_VN', '')           # HH:MM giờ VN; trống = không giới hạn theo giờ
SCAN_INTERVAL_SECONDS = int(os.getenv('SCAN_INTERVAL_SECONDS', '60'))
SCAN_JITTER_SECONDS = int(os.getenv('SCAN_JITTER_SECONDS', '15'))
MAX_RUNTIME_MINUTES = int(os.getenv('MAX_RUNTIME_MINUTES', '0'))  # 0 = quét đơn (1 lượt rồi thoát)

LOG_FILE = "history.json"
REQUEST_TIMEOUT = 20
USER_AGENT = ("Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36")

MAINTENANCE_KEYWORDS = [
    "under maintainance", "maintainance", "maintenance",
    "bảo trì", "quay lại sau", "nâng cấp", "đang cập nhật",
]

_browser_ready = False


def get_vn_now():
    return datetime.now(timezone.utc) + timedelta(hours=7)


def url_group(url):
    host = urlparse(url).netloc.lower()
    if host == MOBA_HOST_SUFFIX or host.endswith('.' + MOBA_HOST_SUFFIX):
        return 'moba'
    return 'other'


def sanitize_url(url):
    """
    BẢO MẬT: loại bỏ TOÀN BỘ query string + fragment trước khi ghi vào history.json
    (sẽ bị commit lên repo). Các sự kiện ingame nhét token/sig/seq/itopencodeparam/nickname
    vào query -> tuyệt đối không được lưu phần đó. Chỉ giữ scheme://host/path để dễ nhận biết.
    """
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path}"


# Danh sách làm việc thực tế của lượt chạy này (đã lọc theo nhóm)
if MONITOR_GROUP in ('moba', 'other'):
    URL_LIST = [u for u in ALL_URLS if url_group(u) == MONITOR_GROUP]
else:
    URL_LIST = ALL_URLS


def get_event_id(url):
    parsed = urlparse(url)
    domain = parsed.netloc.split('.')[0]
    path_code = ([p for p in parsed.path.split('/') if p] or ['event'])[0]
    suffix = hashlib.md5(url.encode()).hexdigest()[:4]
    return f"{domain}-{path_code}-{suffix}"


def occurrence_key(url):
    """Khóa chống trùng trong history.json."""
    ev_id = get_event_id(url)
    if CAPTURE_MODE == 'daily':
        return f"{ev_id}@{get_vn_now().strftime('%Y-%m-%d')}"
    return ev_id


def is_fake_200(html_content):
    """Phát hiện trang trả về 200 nhưng thực chất là trang bảo trì / rỗng."""
    if not html_content or len(html_content) < 800:
        return True
    low = html_content.lower()
    return any(k in low for k in MAINTENANCE_KEYWORDS)


def load_history():
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def commit_archived_event(occ_key, record):
    """
    Lưu MỘT sự kiện đã đóng gói vào history.json, an toàn kể cả khi workflow nhóm
    khác cùng ghi: mỗi lần thử đều reset về bản remote mới nhất rồi áp thay đổi và push.
    """
    subprocess.run(["git", "config", "user.name", "AOV-Monitor-Bot"], check=False)
    subprocess.run(["git", "config", "user.email",
                    "bot@users.noreply.github.com"], check=False)
    for _ in range(4):
        subprocess.run(["git", "fetch", "origin", "main"], check=False, capture_output=True)
        subprocess.run(["git", "reset", "--hard", "origin/main"], check=False, capture_output=True)
        hist = load_history()
        if hist.get(occ_key, {}).get("archived"):
            return hist  # đã có trên remote
        hist[occ_key] = record
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(hist, f, indent=4, ensure_ascii=False)
        subprocess.run(["git", "add", LOG_FILE], check=False)
        commit = subprocess.run(
            ["git", "commit", "-m", f"Run #{RUN_ID}: Luu {occ_key}"],
            capture_output=True,
        )
        if commit.returncode != 0:
            return hist  # không có gì để commit
        push = subprocess.run(["git", "push", "origin", "HEAD:main"], capture_output=True)
        if push.returncode == 0:
            return hist
        time.sleep(random.uniform(1.0, 3.0))
    print("Cảnh báo: không push được history.json sau nhiều lần thử.")
    return load_history()


def notify_telegram(text):
    if not (TG_TOKEN and TG_ID):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_ID, "text": text},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception:
        pass


def ensure_browser():
    """Tải Chromium cho Playwright; chỉ chạy 1 lần và chỉ khi cần lưu trữ."""
    global _browser_ready
    if _browser_ready:
        return
    print("Cài đặt trình duyệt Chromium cho Playwright...")
    subprocess.run(["python", "-m", "playwright", "install", "chromium"], check=True)
    _browser_ready = True


def quick_check(url):
    """Kiểm tra nhanh bằng HTTP GET: trang đã thực sự mở (200 + nội dung thật) chưa."""
    try:
        res = requests.get(
            url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        live = (res.status_code == 200) and (not is_fake_200(res.text))
        if not live:
            n = len(res.text or "")
            if res.status_code != 200:
                reason = f"HTTP {res.status_code}"
            elif n < 800:
                reason = f"nội dung quá ngắn ({n} bytes)"
            else:
                reason = "khớp từ khóa bảo trì"
            print(f"  -> chưa mở (lý do: {reason})")
        return live
    except Exception as e:
        print(f"  Lỗi kết nối: {e}")
        return False


def archive_event(url, ev_id):
    """Mở trang bằng trình duyệt thật, lưu DOM + tài nguyên + ảnh chụp, gửi Telegram."""
    try:
        ensure_browser()
        from playwright.sync_api import sync_playwright

        if os.path.exists(ev_id):
            shutil.rmtree(ev_id, ignore_errors=True)
        os.makedirs(ev_id, exist_ok=True)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 375, 'height': 812},
                is_mobile=True,
                user_agent=USER_AGENT,
            )
            page = context.new_page()
            counter = [0]

            def handle_res(res):
                try:
                    u = res.url
                    if any(x in u for x in ["google", "analytics", "facebook"]):
                        return
                    counter[0] += 1
                    ct = res.headers.get("content-type", "").lower()
                    clean = (urlparse(u).path.split('/')[-1] or "index").split('?')[0]
                    name = f"{counter[0]:03d}_{clean}"
                    if "javascript" in ct and not name.endswith(".js"):
                        name += ".js"
                    elif "css" in ct and not name.endswith(".css"):
                        name += ".css"
                    elif "json" in ct and not name.endswith(".json"):
                        name += ".json"
                    body = res.body()
                    path = os.path.join(ev_id, name)
                    if "json" in ct:
                        try:
                            parsed = json.loads(body.decode("utf-8"))
                            with open(path, "w", encoding="utf-8") as f:
                                json.dump(parsed, f, indent=4, ensure_ascii=False)
                        except Exception:
                            with open(path, "wb") as f:
                                f.write(body)
                    else:
                        with open(path, "wb") as f:
                            f.write(body)
                except Exception:
                    pass

            page.on("response", handle_res)
            page.goto(url, wait_until="networkidle", timeout=90000)
            time.sleep(8)

            if is_fake_200(page.content()):
                browser.close()
                shutil.rmtree(ev_id, ignore_errors=True)
                return "MAINTENANCE"

            page.screenshot(path=f"{ev_id}.png", full_page=True)
            with open(os.path.join(ev_id, "000_DOM.html"), "w", encoding="utf-8") as f:
                f.write(page.content())
            browser.close()

        shutil.make_archive(ev_id, 'zip', root_dir=ev_id)

        caption = (
            f"Đã lưu trữ sự kiện: {ev_id}\n"
            f"Nhóm: {url_group(url)}\n"
            f"Thời gian: {get_vn_now().strftime('%H:%M:%S %d/%m/%Y')}\n"
            f"Run #{RUN_ID}"
        )
        if TG_TOKEN and TG_ID:
            with open(f"{ev_id}.png", 'rb') as f:
                requests.post(
                    f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto",
                    data={"chat_id": TG_ID, "caption": caption},
                    files={'photo': f}, timeout=60,
                )
            with open(f"{ev_id}.zip", 'rb') as f:
                requests.post(
                    f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument",
                    data={"chat_id": TG_ID},
                    files={'document': f}, timeout=120,
                )

        shutil.rmtree(ev_id, ignore_errors=True)
        for fp in [f"{ev_id}.png", f"{ev_id}.zip"]:
            if os.path.exists(fp):
                os.remove(fp)
        return True
    except Exception as e:
        print(f"Lỗi lưu trữ {ev_id}: {e}")
        return False


def disable_workflow():
    """Tắt CHÍNH workflow này (WORKFLOW_FILE) khi AUTO_DISABLE bật và đã đóng gói xong."""
    print(f"Đã đóng gói xong nhóm '{MONITOR_GROUP}'. Đang tắt workflow {WORKFLOW_FILE}...")
    notify_telegram(
        f"Hoàn tất nhóm '{MONITOR_GROUP}': đã lưu trữ xong các sự kiện cần theo dõi.\n"
        f"Workflow {WORKFLOW_FILE} đã tự tắt.\n"
        "Bật lại thủ công (tab Actions > Enable workflow) khi cần."
    )
    if not GH_TOKEN:
        print("Không có GH_TOKEN, bỏ qua việc tắt workflow.")
        return
    env = {**os.environ, "GH_TOKEN": GH_TOKEN}
    subprocess.run(["gh", "workflow", "disable", WORKFLOW_FILE], env=env, check=False)


def scan_once(history):
    """Quét MỘT lượt qua các URL (đã lọc nhóm) chưa lưu trữ. Trả về history đã cập nhật."""
    pending = [u for u in URL_LIST
               if not history.get(occurrence_key(u), {}).get("archived")]
    for url in pending:
        ev_id = get_event_id(url)
        occ = occurrence_key(url)
        print(f"[{get_vn_now().strftime('%H:%M:%S')}] Kiểm tra: {ev_id}")
        if quick_check(url):
            print("  Phát hiện sự kiện đang mở. Bắt đầu lưu trữ...")
            result = archive_event(url, ev_id)
            if result is True:
                record = {
                    "status": 200, "archived": True,
                    "url": sanitize_url(url),  # BẢO MẬT: không lưu token trong query
                    "group": url_group(url),
                    "time": get_vn_now().strftime('%Y-%m-%d %H:%M:%S'),
                    "by_run": RUN_ID,
                }
                history = commit_archived_event(occ, record)
                print(f"  Đã lưu trữ xong: {occ}")
            elif result == "MAINTENANCE":
                print("  Trang đang bảo trì (fake 200). Sẽ thử lại ở lượt quét sau.")
            else:
                print("  Lưu trữ thất bại. Sẽ thử lại ở lượt quét sau.")
        else:
            print("  Chưa mở hoặc đang bảo trì.")
        if len(pending) > 1:
            time.sleep(random.uniform(2, 5))
    return history


def compute_window_end_ts(start_ts):
    """Thời điểm (epoch) dừng quét: gần nhất giữa WINDOW_END_VN (giờ VN) và MAX_RUNTIME."""
    candidates = []
    if MAX_RUNTIME_MINUTES and MAX_RUNTIME_MINUTES > 0:
        candidates.append(start_ts + MAX_RUNTIME_MINUTES * 60)
    raw = (WINDOW_END_VN or "").strip()
    if raw:
        try:
            hh, mm = raw.split(':')
            now_vn = get_vn_now()
            end_vn = now_vn.replace(hour=int(hh), minute=int(mm),
                                    second=0, microsecond=0)
            secs = (end_vn - now_vn).total_seconds()
            if secs > 0:
                candidates.append(start_ts + secs)
            else:
                print("Cửa sổ hôm nay đã qua; chỉ dùng MAX_RUNTIME_MINUTES nếu được bật.")
        except Exception:
            print(f"WINDOW_END_VN không hợp lệ ('{WINDOW_END_VN}'); bỏ qua giới hạn cửa sổ.")
    return min(candidates) if candidates else None


def main():
    print(f"AOV Monitor - Run #{RUN_ID} - group={MONITOR_GROUP} - "
          f"{get_vn_now().strftime('%H:%M:%S %d/%m/%Y')} (VN) - "
          f"capture={CAPTURE_MODE}, window_end={WINDOW_END_VN or 'none'}, "
          f"interval={SCAN_INTERVAL_SECONDS}s, max_runtime={MAX_RUNTIME_MINUTES}m")

    if not URL_LIST:
        print(f"Không có URL nào thuộc nhóm '{MONITOR_GROUP}' trong EVENT_URL. Thoát.")
        return

    history = load_history()
    start_ts = time.time()
    deadline_ts = compute_window_end_ts(start_ts)

    while True:
        remaining = [u for u in URL_LIST
                     if not history.get(occurrence_key(u), {}).get("archived")]
        if not remaining:
            print("Đã đóng gói xong các sự kiện cần theo dõi trong lượt này.")
            if AUTO_DISABLE:
                disable_workflow()
            return

        history = scan_once(history)

        remaining = [u for u in URL_LIST
                     if not history.get(occurrence_key(u), {}).get("archived")]
        if not remaining:
            print("Đã đóng gói xong các sự kiện cần theo dõi trong lượt này.")
            if AUTO_DISABLE:
                disable_workflow()
            return

        # Còn thời gian để quét tiếp trong lượt chạy này không?
        if deadline_ts is None:
            # Không đặt giới hạn nào -> chế độ quét đơn: thoát sau một lượt, chờ cron kế tiếp.
            print("Chế độ quét đơn. Kết thúc lượt, chờ lượt cron kế tiếp.")
            return

        left = deadline_ts - time.time()
        if left <= 0:
            print("Đã hết cửa sổ theo dõi. Kết thúc, chờ lượt cron kế tiếp.")
            return
        wait = SCAN_INTERVAL_SECONDS + random.uniform(0, SCAN_JITTER_SECONDS)
        if wait >= left:
            print("Không còn đủ thời gian cho một chu kỳ quét nữa. Kết thúc.")
            return
        print(f"[*] Nghỉ {int(wait)}s rồi quét lại (còn ~{int(left / 60)} phút trong cửa sổ)...")
        time.sleep(wait)


if __name__ == "__main__":
    main()
