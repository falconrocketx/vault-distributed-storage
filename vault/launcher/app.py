import os
import sys
import shutil
import subprocess
import webbrowser
import threading
from pathlib import Path
from typing import List, Optional

try:
    import customtkinter as ctk
    CTK_AVAILABLE = True
except ImportError:
    import tkinter as ctk
    CTK_AVAILABLE = False

from vault.config import (
    DATA_DIR,
    METADATA_DB_PATH,
    COORDINATOR_PORT,
    BASE_NODE_PORT,
    DEFAULT_NODE_COUNT,
    DEFAULT_REPLICATION_FACTOR,
    PROJECT_ROOT,
    ensure_data_directories
)
from vault.utils import get_lan_ip

class VaultLauncherApp:
    def __init__(self):
        self.lan_ip = get_lan_ip()
        self.coordinator_proc: Optional[subprocess.Popen] = None
        self.node_procs: List[subprocess.Popen] = []
        self.node_count = DEFAULT_NODE_COUNT
        self.replication_factor = DEFAULT_REPLICATION_FACTOR

        if CTK_AVAILABLE:
            ctk.set_appearance_mode("dark")
            ctk.set_default_color_theme("blue")
            self.root = ctk.CTk()
        else:
            import tkinter as tk
            self.root = tk.Tk()

        self.root.title("Vault Distributed Object Storage - Desktop Launcher")
        self.root.geometry("780x680")
        self.root.minsize(720, 620)

        ico_path = PROJECT_ROOT / "vault.ico"
        if ico_path.exists():
            try:
                self.root.iconbitmap(str(ico_path))
            except Exception:
                pass

        self._build_ui()
        self._start_status_checker()

    def _build_ui(self):
        # Header banner
        header_frame = ctk.CTkFrame(self.root, corner_radius=12, fg_color="#131c2e")
        header_frame.pack(fill="x", padx=16, pady=(16, 10))

        title_label = ctk.CTkLabel(
            header_frame,
            text="VAULT DISTRIBUTED STORAGE",
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color="#6366f1"
        )
        title_label.pack(anchor="w", padx=16, pady=(12, 2))

        sub_label = ctk.CTkLabel(
            header_frame,
            text="Multi-node fault-tolerant storage topology with quorum consensus & auto-healing",
            font=ctk.CTkFont(size=12),
            text_color="#94a3b8"
        )
        sub_label.pack(anchor="w", padx=16, pady=(0, 12))

        # Config Card: Cluster Sizer & Replication
        cfg_frame = ctk.CTkFrame(self.root, corner_radius=12, fg_color="#0f172a")
        cfg_frame.pack(fill="x", padx=16, pady=8)

        cfg_title = ctk.CTkLabel(
            cfg_frame,
            text="1. Cluster Topology Configuration",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#f8fafc"
        )
        cfg_title.pack(anchor="w", padx=16, pady=(12, 8))

        row_cfg = ctk.CTkFrame(cfg_frame, fg_color="transparent")
        row_cfg.pack(fill="x", padx=16, pady=(0, 12))

        # Node Sizer
        sizer_col = ctk.CTkFrame(row_cfg, fg_color="#1e293b", corner_radius=10)
        sizer_col.pack(side="left", fill="both", expand=True, padx=(0, 8), pady=4)

        self.sizer_label = ctk.CTkLabel(
            sizer_col,
            text=f"Storage Nodes (N): {self.node_count}",
            font=ctk.CTkFont(size=12, weight="bold")
        )
        self.sizer_label.pack(anchor="w", padx=12, pady=(10, 4))

        self.node_slider = ctk.CTkSlider(
            sizer_col,
            from_=2,
            to=5,
            number_of_steps=3,
            command=self._on_node_count_change
        )
        self.node_slider.set(self.node_count)
        self.node_slider.pack(fill="x", padx=12, pady=(4, 12))

        # Replication Factor
        rep_col = ctk.CTkFrame(row_cfg, fg_color="#1e293b", corner_radius=10)
        rep_col.pack(side="right", fill="both", expand=True, padx=(8, 0), pady=4)

        self.rep_label = ctk.CTkLabel(
            rep_col,
            text=f"Replication Factor (R): {self.replication_factor}",
            font=ctk.CTkFont(size=12, weight="bold")
        )
        self.rep_label.pack(anchor="w", padx=12, pady=(10, 4))

        self.rep_slider = ctk.CTkSlider(
            rep_col,
            from_=1,
            to=self.node_count,
            number_of_steps=self.node_count - 1,
            command=self._on_rep_change
        )
        self.rep_slider.set(self.replication_factor)
        self.rep_slider.pack(fill="x", padx=12, pady=(4, 12))

        # Lifecycle Controls Card
        ctrl_frame = ctk.CTkFrame(self.root, corner_radius=12, fg_color="#0f172a")
        ctrl_frame.pack(fill="x", padx=16, pady=8)

        ctrl_title = ctk.CTkLabel(
            ctrl_frame,
            text="2. Cluster Lifecycle Controls",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#f8fafc"
        )
        ctrl_title.pack(anchor="w", padx=16, pady=(12, 8))

        btn_row = ctk.CTkFrame(ctrl_frame, fg_color="transparent")
        btn_row.pack(fill="x", padx=16, pady=(0, 12))

        self.btn_start = ctk.CTkButton(
            btn_row,
            text="▶ Start Cluster",
            fg_color="#10b981",
            hover_color="#059669",
            text_color="#ffffff",
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self.start_cluster,
            height=36
        )
        self.btn_start.pack(side="left", fill="x", expand=True, padx=(0, 6))

        self.btn_stop = ctk.CTkButton(
            btn_row,
            text="⏹ Stop Cluster",
            fg_color="#ef4444",
            hover_color="#dc2626",
            text_color="#ffffff",
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self.stop_cluster,
            state="disabled",
            height=36
        )
        self.btn_stop.pack(side="left", fill="x", expand=True, padx=6)

        self.btn_purge = ctk.CTkButton(
            btn_row,
            text="🗑 Purge Data",
            fg_color="#475569",
            hover_color="#334155",
            text_color="#ffffff",
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self.purge_data,
            height=36
        )
        self.btn_purge.pack(side="right", fill="x", expand=True, padx=(6, 0))

        # Status Banner
        self.status_banner = ctk.CTkLabel(
            ctrl_frame,
            text="Status: CLUSTER INACTIVE",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#94a3b8"
        )
        self.status_banner.pack(anchor="w", padx=16, pady=(0, 10))

        # Interactive URLs Box
        url_frame = ctk.CTkFrame(self.root, corner_radius=12, fg_color="#0f172a")
        url_frame.pack(fill="x", padx=16, pady=8)

        url_title = ctk.CTkLabel(
            url_frame,
            text="3. Accessible Web Interfaces",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#f8fafc"
        )
        url_title.pack(anchor="w", padx=16, pady=(12, 6))

        # Client Portal row
        client_row = ctk.CTkFrame(url_frame, fg_color="#1e293b", corner_radius=10)
        client_row.pack(fill="x", padx=16, pady=4)

        client_info = ctk.CTkLabel(
            client_row,
            text="Client Portal:\nhttp://localhost:8000/client\nhttp://" + self.lan_ip + ":8000/client",
            justify="left",
            font=ctk.CTkFont(size=11, family="Courier"),
            text_color="#38bdf8"
        )
        client_info.pack(side="left", padx=12, pady=8)

        client_actions = ctk.CTkFrame(client_row, fg_color="transparent")
        client_actions.pack(side="right", padx=12, pady=8)

        ctk.CTkButton(
            client_actions,
            text="Open",
            width=65,
            command=lambda: webbrowser.open(f"http://localhost:{COORDINATOR_PORT}/client")
        ).pack(side="left", padx=4)

        ctk.CTkButton(
            client_actions,
            text="Copy",
            width=65,
            fg_color="#334155",
            hover_color="#1e293b",
            command=lambda: self._copy_to_clipboard(f"http://{self.lan_ip}:{COORDINATOR_PORT}/client")
        ).pack(side="left", padx=4)

        # Admin Portal row
        admin_row = ctk.CTkFrame(url_frame, fg_color="#1e293b", corner_radius=10)
        admin_row.pack(fill="x", padx=16, pady=4)

        admin_info = ctk.CTkLabel(
            admin_row,
            text="Admin & Chaos Console:\nhttp://localhost:8000/admin\nhttp://" + self.lan_ip + ":8000/admin",
            justify="left",
            font=ctk.CTkFont(size=11, family="Courier"),
            text_color="#fbbf24"
        )
        admin_info.pack(side="left", padx=12, pady=8)

        admin_actions = ctk.CTkFrame(admin_row, fg_color="transparent")
        admin_actions.pack(side="right", padx=12, pady=8)

        ctk.CTkButton(
            admin_actions,
            text="Open",
            width=65,
            command=lambda: webbrowser.open(f"http://localhost:{COORDINATOR_PORT}/admin")
        ).pack(side="left", padx=4)

        ctk.CTkButton(
            admin_actions,
            text="Copy",
            width=65,
            fg_color="#334155",
            hover_color="#1e293b",
            command=lambda: self._copy_to_clipboard(f"http://{self.lan_ip}:{COORDINATOR_PORT}/admin")
        ).pack(side="left", padx=4)

        # Footer log text / info
        self.footer_label = ctk.CTkLabel(
            self.root,
            text=f"Detected Host IPv4: {self.lan_ip} | Storage Dir: {DATA_DIR}",
            font=ctk.CTkFont(size=11),
            text_color="#64748b"
        )
        self.footer_label.pack(side="bottom", pady=10)

    def _copy_to_clipboard(self, text: str):
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.status_banner.configure(
            text=f"Copied '{text}' to clipboard!",
            text_color="#38bdf8"
        )

    def _on_node_count_change(self, val):
        self.node_count = int(round(val))
        self.sizer_label.configure(text=f"Storage Nodes (N): {self.node_count}")
        # Adjust replication slider bounds
        if self.replication_factor > self.node_count:
            self.replication_factor = self.node_count
        self.rep_slider.configure(to=self.node_count, number_of_steps=self.node_count - 1)
        self.rep_slider.set(self.replication_factor)
        self.rep_label.configure(text=f"Replication Factor (R): {self.replication_factor}")

    def _on_rep_change(self, val):
        self.replication_factor = int(round(val))
        self.rep_label.configure(text=f"Replication Factor (R): {self.replication_factor}")

    def start_cluster(self):
        if self.is_running():
            return

        ensure_data_directories(self.node_count)
        self.node_procs = []

        python_exe = sys.executable

        # 1. Start Storage Nodes
        for i in range(1, self.node_count + 1):
            port = BASE_NODE_PORT + i - 1
            cmd = [
                python_exe,
                str(PROJECT_ROOT / "run_node.py"),
                "--node-id", str(i),
                "--port", str(port)
            ]
            proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            self.node_procs.append(proc)

        # 2. Start Coordinator
        coord_cmd = [
            python_exe,
            str(PROJECT_ROOT / "run_coordinator.py"),
            "--port", str(COORDINATOR_PORT),
            "--nodes", str(self.node_count),
            "--replication", str(self.replication_factor)
        ]
        self.coordinator_proc = subprocess.Popen(
            coord_cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        self.btn_start.configure(state="disabled")
        self.btn_stop.configure(state="normal")
        self.node_slider.configure(state="disabled")
        self.rep_slider.configure(state="disabled")
        self.status_banner.configure(
            text=f"Status: CLUSTER ACTIVE ({self.node_count} nodes running, R={self.replication_factor})",
            text_color="#34d399"
        )

    def stop_cluster(self):
        if self.coordinator_proc:
            try:
                self.coordinator_proc.terminate()
            except Exception:
                pass
            self.coordinator_proc = None

        for p in self.node_procs:
            try:
                p.terminate()
            except Exception:
                pass
        self.node_procs = []

        self.btn_start.configure(state="normal")
        self.btn_stop.configure(state="disabled")
        self.node_slider.configure(state="normal")
        self.rep_slider.configure(state="normal")
        self.status_banner.configure(
            text="Status: CLUSTER STOPPED",
            text_color="#f87171"
        )

    def purge_data(self):
        if self.is_running():
            self.status_banner.configure(
                text="Cannot purge while cluster is active. Please stop cluster first.",
                text_color="#fbbf24"
            )
            return

        if DATA_DIR.exists():
            shutil.rmtree(DATA_DIR, ignore_errors=True)

        ensure_data_directories(self.node_count)
        self.status_banner.configure(
            text="All ./data/ chunks and metadata.db purged successfully.",
            text_color="#38bdf8"
        )

    def is_running(self) -> bool:
        return self.coordinator_proc is not None and self.coordinator_proc.poll() is None

    def _start_status_checker(self):
        def check_loop():
            if self.coordinator_proc:
                if self.coordinator_proc.poll() is not None:
                    # Coordinator exited unexpectedly
                    self.stop_cluster()
                    self.status_banner.configure(
                        text="Status: ERROR - Coordinator exited unexpectedly",
                        text_color="#f43f5e"
                    )
            self.root.after(1000, self._start_status_checker)

        self.root.after(1000, check_loop)

    def run(self):
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()

    def _on_close(self):
        self.stop_cluster()
        self.root.destroy()
