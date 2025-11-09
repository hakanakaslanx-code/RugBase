"""Tkinter panel for managing Google Sheets synchronisation."""
from __future__ import annotations

import queue
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

from core import sheets_sync
from core.sync_service import SyncService
from core.sheets_sync import SheetsSyncError
from settings import (
    DEFAULT_CREDENTIALS_PATH,
    DEFAULT_SERVICE_ACCOUNT_EMAIL,
    DEFAULT_SPREADSHEET_ID,
    DEFAULT_WORKSHEET_TITLE,
    GoogleSyncSettings,
    load_google_sync_settings,
    save_google_sync_settings,
)


class _BackgroundWorker:
    """Process synchronisation tasks on a single background thread."""

    def __init__(
        self, callback: Callable[[str, str, Optional[Exception]], None]
    ) -> None:
        self._callback = callback
        self._queue: "queue.Queue[tuple[str, Callable[[], None]]]" = queue.Queue()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, label: str, task: Callable[[], None]) -> None:
        self._queue.put((label, task))

    def shutdown(self) -> None:
        self._stop_event.set()
        self._queue.put(("__STOP__", lambda: None))
        self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                label, task = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if label == "__STOP__":
                break
            try:
                task()
            except Exception as exc:  # pragma: no cover - defensive logging
                self._callback("error", label, exc)
            else:
                self._callback("done", label, None)
            finally:
                self._queue.task_done()


class SyncPanel(ttk.Frame):
    """Composite widget for Google Sheets synchronisation."""

    AUTO_SYNC_INTERVAL = 300  # seconds

    def __init__(self, master: tk.Misc) -> None:
        super().__init__(master, padding=12)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self._settings = load_google_sync_settings()
        self._service = SyncService(
            log_callback=self._post_log,
            conflict_resolver=self._resolve_conflict,
        )
        self._worker = _BackgroundWorker(self._on_worker_event)
        self._busy_task: Optional[str] = None
        self._connection_ready = False
        self._auto_sync_enabled = False
        self._auto_job: Optional[str] = None

        self._build_ui()
        self._update_credentials_state()
        self._update_button_states()

        if sheets_sync.is_api_available() and self._credentials_exist():
            self.after(750, self._initial_test_connection)
        elif not sheets_sync.is_api_available():
            self._append_log(
                "google-api-python-client bulunamadı. Senkronizasyon özellikleri devre dışı bırakıldı."
            )

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        self.spreadsheet_var = tk.StringVar(value=self._settings.spreadsheet_id)
        self.worksheet_var = tk.StringVar(
            value=self._settings.worksheet_title or DEFAULT_WORKSHEET_TITLE
        )
        self.service_email_var = tk.StringVar(
            value=self._settings.service_account_email or DEFAULT_SERVICE_ACCOUNT_EMAIL
        )
        self.credentials_path_var = tk.StringVar(value=self._settings.credential_path)

        form = ttk.LabelFrame(self, text="Google Sheets Ayarları", padding=12)
        form.grid(row=0, column=0, sticky="nsew")
        form.columnconfigure(1, weight=1)

        ttk.Label(form, text="Sheet ID").grid(row=0, column=0, sticky=tk.W, pady=(0, 6))
        self.spreadsheet_entry = ttk.Entry(form, textvariable=self.spreadsheet_var)
        self.spreadsheet_entry.grid(row=0, column=1, sticky="ew", pady=(0, 6))
        self.spreadsheet_entry.bind("<FocusOut>", self._on_spreadsheet_focus_out)

        ttk.Label(form, text="Servis hesabı e-postası").grid(
            row=1, column=0, sticky=tk.W, pady=6
        )
        ttk.Entry(form, textvariable=self.service_email_var).grid(
            row=1, column=1, sticky="ew", pady=6
        )

        ttk.Label(form, text="Çalışma sayfası adı").grid(row=2, column=0, sticky=tk.W, pady=6)
        self.worksheet_entry = ttk.Entry(form, textvariable=self.worksheet_var)
        self.worksheet_entry.grid(row=2, column=1, sticky="ew", pady=6)
        self.worksheet_entry.bind("<FocusOut>", self._on_worksheet_focus_out)

        ttk.Label(form, text="Service Account JSON").grid(row=3, column=0, sticky=tk.W, pady=6)
        credentials_frame = ttk.Frame(form)
        credentials_frame.grid(row=3, column=1, sticky="ew", pady=6)
        credentials_frame.columnconfigure(1, weight=1)

        self.choose_button = ttk.Button(
            credentials_frame,
            text="Service Account JSON seç",
            command=self._on_choose_credentials,
        )
        self.choose_button.grid(row=0, column=0, padx=(0, 6))
        self.credentials_status_var = tk.StringVar()
        ttk.Label(
            credentials_frame,
            textvariable=self.credentials_status_var,
            foreground="#555555",
        ).grid(row=0, column=1, sticky="w")

        button_frame = ttk.Frame(form)
        button_frame.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        button_frame.columnconfigure(3, weight=1)

        self.test_button = ttk.Button(
            button_frame, text="Bağlantıyı Test Et", command=self._on_test_connection
        )
        self.test_button.grid(row=0, column=0, padx=(0, 6))

        self.pull_button = ttk.Button(
            button_frame, text="Çek (Pull)", command=self._on_pull
        )
        self.pull_button.grid(row=0, column=1, padx=(0, 6))

        self.push_button = ttk.Button(
            button_frame, text="Gönder (Push)", command=self._on_push
        )
        self.push_button.grid(row=0, column=2, padx=(0, 6))

        self.auto_button = ttk.Button(
            button_frame, text="Eşitle (Auto Sync)", command=self._on_toggle_auto_sync
        )
        self.auto_button.grid(row=0, column=3, sticky=tk.W)

        self.metadata_hint_var = tk.StringVar()
        ttk.Label(
            form,
            textvariable=self.metadata_hint_var,
            foreground="#0b5394",
            wraplength=460,
            justify=tk.LEFT,
        ).grid(row=5, column=0, columnspan=2, sticky="w", pady=(8, 0))

        log_frame = ttk.LabelFrame(self, text="Durum Günlüğü", padding=12)
        log_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_widget = scrolledtext.ScrolledText(
            log_frame, width=80, height=16, state="disabled"
        )
        self.log_widget.grid(row=0, column=0, sticky="nsew")

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------
    def _on_spreadsheet_focus_out(self, _event: tk.Event) -> None:
        parsed = sheets_sync.parse_spreadsheet_id(self.spreadsheet_var.get())
        if parsed and parsed != self.spreadsheet_var.get():
            self.spreadsheet_var.set(parsed)
        self._persist_settings()

    def _on_worksheet_focus_out(self, _event: tk.Event) -> None:
        title = (self.worksheet_var.get() or "").strip()
        if not title:
            title = DEFAULT_WORKSHEET_TITLE
        if title != self.worksheet_var.get():
            self.worksheet_var.set(title)
        self._persist_settings()

    def _on_choose_credentials(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Kimlik dosyasını seçin",
            filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")],
        )
        if not file_path:
            return

        target_path = Path(DEFAULT_CREDENTIALS_PATH)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(file_path, target_path)
        except OSError as exc:
            messagebox.showerror(
                "Kimlik Dosyası",
                f"Kimlik dosyası kopyalanamadı: {exc}",
                parent=self.winfo_toplevel(),
            )
            return

        self.credentials_path_var.set(str(target_path))
        self._settings.credential_path = str(target_path)
        self._persist_settings()
        self._update_credentials_state()
        self._append_log(f"Kimlik dosyası {target_path} konumuna kopyalandı.")
        self._update_button_states()

    def _on_test_connection(self) -> None:
        if not self._credentials_exist():
            messagebox.showinfo(
                "Bağlantı", "Lütfen önce kimlik dosyasını seçin.", parent=self.winfo_toplevel()
            )
            return
        self._enqueue_task("connection", self._task_test_connection)

    def _on_pull(self) -> None:
        self._enqueue_task("pull", self._task_pull)

    def _on_push(self) -> None:
        self._enqueue_task("push", self._task_push)

    def _on_toggle_auto_sync(self) -> None:
        if not self._auto_sync_enabled:
            if not self._connection_ready:
                messagebox.showinfo(
                    "Auto Sync", "Bağlantı testi tamamlanmadan otomatik eşitleme başlatılamaz.",
                    parent=self.winfo_toplevel(),
                )
                return
            self._auto_sync_enabled = True
            self.auto_button.config(text="Auto Sync'i Durdur")
            self._append_log(
                "Otomatik eşitleme başlatıldı. Döngü aralığı 5 dakikadır."
            )
            self._schedule_auto_sync(immediate=True)
        else:
            self._auto_sync_enabled = False
            self.auto_button.config(text="Eşitle (Auto Sync)")
            if self._auto_job is not None:
                try:
                    self.after_cancel(self._auto_job)
                except Exception:  # pragma: no cover - Tk variations
                    pass
                self._auto_job = None
            self._append_log("Otomatik eşitleme durduruldu.")
        self._update_button_states()

    # ------------------------------------------------------------------
    # Worker tasks
    # ------------------------------------------------------------------
    def _enqueue_task(self, label: str, task_callable) -> None:
        if self._busy_task:
            if label == "autosync":
                self._schedule_auto_sync()
            else:
                self._append_log("Devam eden bir görev tamamlanana kadar bekleyin.")
            return
        self._busy_task = label
        self._update_button_states()

        self._worker.submit(label, task_callable)

    def _task_test_connection(self) -> None:
        try:
            settings = self._collect_settings()
            report = self._service.test_connection(settings)
        except SheetsSyncError as exc:
            self._post_log(f"Bağlantı testi başarısız: {exc}")
            self._connection_ready = False
            self._post_metadata_hint("")
        else:
            for key in ("imports", "values_get", "roundtrip"):
                if report.get(key):
                    self._post_log(report[key])
            extra_keys = [
                key for key in report.keys() if key not in {"imports", "values_get", "roundtrip"}
            ]
            for key in sorted(extra_keys):
                self._post_log(report[key])
            self._post_log("Bağlantı testi başarılı.")
            self._connection_ready = True
            hint = self._compute_metadata_hint(settings)
            self._post_metadata_hint(hint)

    def _task_pull(self) -> None:
        settings = self._collect_settings()
        try:
            stats = self._service.pull(settings)
        except SheetsSyncError as exc:
            self._post_log(f"Çekme başarısız: {exc}")
            self._connection_ready = False
            self._post_metadata_hint("")
        else:
            self._post_log(
                "Çekme tamamlandı: "
                f"{stats['inserted']} yeni, {stats['updated']} güncelleme, {stats['conflicts']} çakışma."
            )
            self._connection_ready = True
            hint = self._compute_metadata_hint(settings)
            self._post_metadata_hint(hint)

    def _task_push(self) -> None:
        settings = self._collect_settings()
        try:
            stats = self._service.push(settings)
        except SheetsSyncError as exc:
            self._post_log(f"Gönderme başarısız: {exc}")
            self._connection_ready = False
            self._post_metadata_hint("")
        else:
            self._post_log(
                "Gönderme tamamlandı: "
                f"{stats['inserted']} yeni, {stats['updated']} güncelleme, {stats['conflicts']} çakışma."
            )
            self._connection_ready = True
            hint = self._compute_metadata_hint(settings)
            self._post_metadata_hint(hint)

    def _task_auto_sync(self) -> None:
        settings = self._collect_settings()
        try:
            summary = self._service.autosync(settings)
        except SheetsSyncError as exc:
            self._post_log(f"Otomatik eşitleme başarısız: {exc}")
            self._connection_ready = False
            self._auto_sync_enabled = False
            self._post_metadata_hint("")
        else:
            pull = summary.get("pull", {})
            push = summary.get("push", {})
            self._post_log(
                "Otomatik eşitleme tamamlandı: "
                f"Pull ({pull.get('inserted', 0)} yeni, {pull.get('updated', 0)} güncelleme); "
                f"Push ({push.get('inserted', 0)} yeni, {push.get('updated', 0)} güncelleme)."
            )
            self._connection_ready = True
            hint = self._compute_metadata_hint(settings)
            self._post_metadata_hint(hint)

    # ------------------------------------------------------------------
    # Worker callbacks and scheduling
    # ------------------------------------------------------------------
    def _on_worker_event(self, status: str, label: str, error: Optional[Exception]) -> None:
        if status == "error" and error is not None:
            self.after(0, self._handle_task_error, label, error)
        else:
            self.after(0, self._handle_task_complete)

    def _handle_task_complete(self) -> None:
        self._busy_task = None
        self._update_button_states()
        if self._auto_sync_enabled and not self._auto_job:
            self._schedule_auto_sync()

    def _handle_task_error(self, label: str, error: Exception) -> None:
        self._busy_task = None
        self._append_log(f"{label} görevi hata verdi: {error}")
        self._update_button_states()
        if self._auto_sync_enabled:
            self._auto_sync_enabled = False
            self.auto_button.config(text="Eşitle (Auto Sync)")
            self._append_log("Otomatik eşitleme hatadan dolayı durduruldu.")

    def _schedule_auto_sync(self, immediate: bool = False) -> None:
        if not self._auto_sync_enabled:
            return
        delay = 1000 if immediate else self.AUTO_SYNC_INTERVAL * 1000
        self._auto_job = self.after(delay, self._start_auto_sync_cycle)

    def _start_auto_sync_cycle(self) -> None:
        if not self._auto_sync_enabled:
            return
        self._auto_job = None
        self._enqueue_task("autosync", self._task_auto_sync)

    # ------------------------------------------------------------------
    # Logging and conflict resolution
    # ------------------------------------------------------------------
    def _post_log(self, message: str) -> None:
        self.after(0, self._append_log, message)

    def _append_log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_widget.configure(state="normal")
        self.log_widget.insert("end", f"[{timestamp}] {message}\n")
        self.log_widget.see("end")
        self.log_widget.configure(state="disabled")

    def _resolve_conflict(
        self, local: Dict[str, object], remote: Dict[str, object]
    ) -> str:
        result: Dict[str, str] = {}
        event = threading.Event()

        def _prompt() -> None:
            local_info = (
                f"Yerel -> rug_no: {local.get('rug_no', '')}, sku: {local.get('sku', '')}, "
                f"qty: {local.get('qty', '')}, price: {local.get('price', '')}"
            )
            remote_info = (
                f"Sheets -> rug_no: {remote.get('rug_no', '')}, sku: {remote.get('sku', '')}, "
                f"qty: {remote.get('qty', '')}, price: {remote.get('price', '')}"
            )
            response = messagebox.askyesnocancel(
                "Veri Çakışması",
                (
                    "Aynı kayıt hem yerelde hem de Sheets üzerinde değişmiş görünüyor.\n\n"
                    f"{local_info}\n{remote_info}\n\n"
                    "Sheets verisini kabul etmek için 'Evet', yerel veriyi korumak için 'Hayır',"
                    " işlemi atlamak için 'Vazgeç' seçin."
                ),
                parent=self.winfo_toplevel(),
            )
            if response is True:
                result["decision"] = "remote"
            elif response is False:
                result["decision"] = "local"
            else:
                result["decision"] = "skip"
            event.set()

        self.after(0, _prompt)
        event.wait()
        return result.get("decision", "skip")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _credentials_exist(self) -> bool:
        path = self.credentials_path_var.get() or DEFAULT_CREDENTIALS_PATH
        return Path(path).exists()

    def _collect_settings(self) -> GoogleSyncSettings:
        spreadsheet_id = sheets_sync.parse_spreadsheet_id(self.spreadsheet_var.get())
        if spreadsheet_id != self.spreadsheet_var.get():
            self.spreadsheet_var.set(spreadsheet_id)
        worksheet = (self.worksheet_var.get() or DEFAULT_WORKSHEET_TITLE).strip()
        if worksheet != self.worksheet_var.get():
            self.worksheet_var.set(worksheet)
        settings = GoogleSyncSettings(
            spreadsheet_id=spreadsheet_id or DEFAULT_SPREADSHEET_ID,
            credential_path=self.credentials_path_var.get() or DEFAULT_CREDENTIALS_PATH,
            service_account_email=self.service_email_var.get() or DEFAULT_SERVICE_ACCOUNT_EMAIL,
            worksheet_title=worksheet or DEFAULT_WORKSHEET_TITLE,
        )
        save_google_sync_settings(settings)
        self._settings = settings
        return settings

    def _persist_settings(self) -> None:
        settings = GoogleSyncSettings(
            spreadsheet_id=self.spreadsheet_var.get() or DEFAULT_SPREADSHEET_ID,
            credential_path=self.credentials_path_var.get() or DEFAULT_CREDENTIALS_PATH,
            service_account_email=self.service_email_var.get() or DEFAULT_SERVICE_ACCOUNT_EMAIL,
            worksheet_title=(self.worksheet_var.get() or DEFAULT_WORKSHEET_TITLE) or DEFAULT_WORKSHEET_TITLE,
        )
        save_google_sync_settings(settings)
        self._settings = settings

    @staticmethod
    def _parse_iso_timestamp(value: str) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            if value.endswith("Z"):
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    return None
        return None

    def _determine_metadata_hint(
        self, local: Dict[str, str], remote: Dict[str, str]
    ) -> str:
        if not local and not remote:
            return ""
        if not remote:
            return "Yerel veritabanı daha yeni görünüyor. Gönder (Push) önerilir."
        if not local:
            return "Google Sheets verisi daha yeni görünüyor. Çek (Pull) önerilir."

        local_time = self._parse_iso_timestamp(local.get("mtime", ""))
        remote_time = self._parse_iso_timestamp(remote.get("mtime", ""))

        if local_time and remote_time:
            if local_time > remote_time:
                return "Yerel veritabanı daha yeni görünüyor. Gönder (Push) önerilir."
            if remote_time > local_time:
                return "Google Sheets verisi daha yeni görünüyor. Çek (Pull) önerilir."
        elif local_time and not remote_time:
            return "Yerel veritabanı daha yeni görünüyor. Gönder (Push) önerilir."
        elif remote_time and not local_time:
            return "Google Sheets verisi daha yeni görünüyor. Çek (Pull) önerilir."

        if (local.get("sha256") or "") != (remote.get("sha256") or ""):
            return (
                "Yerel ve Sheets verileri farklı görünüyor. Çek (Pull) veya Gönder (Push) "
                "işlemini gözden geçirin."
            )
        return ""

    def _compute_metadata_hint(self, settings: GoogleSyncSettings) -> str:
        local = self._service.get_local_metadata()
        try:
            remote = self._service.get_remote_metadata(settings)
        except SheetsSyncError as exc:
            self._post_log(f"Metadata okunamadı: {exc}")
            return ""
        return self._determine_metadata_hint(local, remote)

    def _post_metadata_hint(self, message: str) -> None:
        def _update() -> None:
            self.metadata_hint_var.set(message)

        self.after(0, _update)

    def _update_credentials_state(self) -> None:
        path = self.credentials_path_var.get()
        if not path:
            path = DEFAULT_CREDENTIALS_PATH
        exists = Path(path).exists()
        display_path = Path(path)
        status = "Bulundu" if exists else "Eksik"
        self.credentials_status_var.set(f"{display_path} ({status})")

    def _update_button_states(self) -> None:
        api_available = sheets_sync.is_api_available()
        credentials_ready = self._credentials_exist()
        ready = api_available and credentials_ready and not self._busy_task

        self.test_button.config(state="normal" if credentials_ready and api_available else "disabled")
        pull_state = "normal" if ready and self._connection_ready else "disabled"
        push_state = pull_state
        auto_state = "normal" if ready and self._connection_ready else "disabled"

        self.pull_button.config(state=pull_state)
        self.push_button.config(state=push_state)
        self.auto_button.config(state=auto_state)
        if self._auto_sync_enabled and auto_state == "disabled":
            self._auto_sync_enabled = False
            self.auto_button.config(text="Eşitle (Auto Sync)")
        else:
            self.auto_button.config(
                text="Auto Sync'i Durdur" if self._auto_sync_enabled else "Eşitle (Auto Sync)"
            )

    def _initial_test_connection(self) -> None:
        self._on_test_connection()

    def shutdown(self) -> None:
        if self._auto_job is not None:
            try:
                self.after_cancel(self._auto_job)
            except Exception:  # pragma: no cover - Tk variations
                pass
            self._auto_job = None
        self._auto_sync_enabled = False
        self._worker.shutdown()


__all__ = ["SyncPanel"]
