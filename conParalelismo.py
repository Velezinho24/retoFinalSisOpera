import sys
import os
import gzip
import shutil
import time
import tempfile
import tarfile
from pathlib import Path
from dask import delayed, compute
from PyQt5.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout,
    QFileDialog, QTextEdit, QLabel, QListWidget, QHBoxLayout
)
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class BackupApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("🗃️ Backup Paralelo por Archivo (GZIP + Dask)")
        self.resize(600, 500)
        self.selected_folders = []

        layout = QVBoxLayout()
        button_layout = QHBoxLayout()

        self.select_button = QPushButton("➕ Seleccionar carpetas")
        self.select_button.clicked.connect(self.select_folders)
        button_layout.addWidget(self.select_button)

        self.remove_button = QPushButton("❌ Eliminar carpeta")
        self.remove_button.clicked.connect(self.remove_selected_folder)
        button_layout.addWidget(self.remove_button)

        layout.addLayout(button_layout)

        self.folder_list = QListWidget()
        layout.addWidget(QLabel("📁 Carpetas seleccionadas:"))
        layout.addWidget(self.folder_list)

        self.backup_button = QPushButton("📦 Iniciar Backup Paralelo")
        self.backup_button.clicked.connect(self.start_backup)
        layout.addWidget(self.backup_button)

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        layout.addWidget(QLabel("📜 Log:"))
        layout.addWidget(self.log_output)

        self.setLayout(layout)

        self.gauth = GoogleAuth()
        self.drive = None
        self.authenticate_drive()

    def authenticate_drive(self):
        try:
            self.gauth.LocalWebserverAuth()
            self.drive = GoogleDrive(self.gauth)
            self.log_output.append("✅ Google Drive autenticado.")
        except Exception as e:
            self.log_output.append(f"❌ Error autenticando Drive: {e}")

    def select_folders(self):
        folder = QFileDialog.getExistingDirectory(self, "Seleccionar carpeta", "", QFileDialog.ShowDirsOnly)
        if folder and folder not in self.selected_folders:
            self.selected_folders.append(folder)
            self.folder_list.addItem(folder)
            self.log_output.append(f"📁 Añadida: {folder}")
        elif folder:
            self.log_output.append("⚠️ Ya fue añadida.")

    def remove_selected_folder(self):
        selected = self.folder_list.selectedItems()
        if not selected:
            self.log_output.append("⚠️ Nada seleccionado.")
            return
        for item in selected:
            folder = item.text()
            self.selected_folders.remove(folder)
            self.folder_list.takeItem(self.folder_list.row(item))
            self.log_output.append(f"🗑️ Eliminada: {folder}")

    def start_backup(self):
        if not self.selected_folders:
            self.log_output.append("⚠️ No hay carpetas.")
            return

        output_file, _ = QFileDialog.getSaveFileName(self, "Guardar backup final", "backup_final.tar", "Backup (*.tar)")
        if not output_file:
            self.log_output.append("❌ Cancelado.")
            return

        self.log_output.append("🚀 Iniciando compresión GZIP por archivo (paralelo)...")

        start_time = time.time()
        with tempfile.TemporaryDirectory() as temp_dir:
            gzip_tasks = []
            original_size = 0

            for folder in self.selected_folders:
                for dirpath, _, filenames in os.walk(folder):
                    for file in filenames:
                        full_path = os.path.join(dirpath, file)
                        rel_path = os.path.relpath(full_path, folder)
                        dest_path = os.path.join(temp_dir, Path(folder).name, rel_path + ".gz")
                        original_size += os.path.getsize(full_path)
                        gzip_tasks.append(
                            delayed(self.compress_file_gzip)(full_path, dest_path, level=6)
                        )

            compute(*gzip_tasks)
            self.log_output.append("📦 Archivos comprimidos. Empaquetando en TAR...")

            self.create_tar(temp_dir, output_file)
            elapsed = time.time() - start_time
            compressed_size = os.path.getsize(output_file)
            ratio = compressed_size / original_size if original_size else 0

            self.log_output.append(f"✅ Backup creado: {output_file}")
            self.log_output.append(f"⏱️ Tiempo total: {elapsed:.2f} s")
            self.log_output.append(f"📁 Tamaño original: {original_size/1024/1024:.2f} MB")
            self.log_output.append(f"📦 Tamaño comprimido: {compressed_size/1024/1024:.2f} MB")
            self.log_output.append(f"📉 Tasa de compresión: {ratio:.2f}")
            self.log_output.append(f"🚀 Velocidad: {original_size/1024/1024/elapsed:.2f} MB/s")

        if os.path.exists(output_file):
            self.upload_to_drive(output_file)

    def compress_file_gzip(self, src, dst, level=6):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(src, 'rb') as f_in, gzip.open(dst, 'wb', compresslevel=level) as f_out:
            shutil.copyfileobj(f_in, f_out)

    def create_tar(self, folder_path, tar_output):
        with tarfile.open(tar_output, "w") as tar:
            tar.add(folder_path, arcname="backup_gzipped")

    def upload_to_drive(self, filepath):
        try:
            self.log_output.append("☁️ Subiendo a Google Drive...")
            start = time.time()
            gfile = self.drive.CreateFile({'title': os.path.basename(filepath)})
            gfile.SetContentFile(filepath)
            gfile.Upload()
            duration = time.time() - start
            self.log_output.append(f"✅ Subido en {duration:.2f} s")
        except Exception as e:
            self.log_output.append(f"❌ Error al subir: {e}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BackupApp()
    window.show()
    sys.exit(app.exec_())
