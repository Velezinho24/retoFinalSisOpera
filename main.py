import sys
import os
import tarfile
from dask import delayed
from PyQt5.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout,
    QFileDialog, QTextEdit, QLabel, QListWidget, QHBoxLayout
)
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from datetime import datetime


class BackupApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("🗃️ Sistema de Backup Seguro con BZIP2 + Google Drive")
        self.resize(600, 500)

        self.selected_folders = []

        # Layout principal
        layout = QVBoxLayout()

        # Botones de control
        button_layout = QHBoxLayout()

        self.select_button = QPushButton("➕ Seleccionar carpetas")
        self.select_button.clicked.connect(self.select_folders)
        button_layout.addWidget(self.select_button)

        self.remove_button = QPushButton("❌ Eliminar carpeta seleccionada")
        self.remove_button.clicked.connect(self.remove_selected_folder)
        button_layout.addWidget(self.remove_button)

        layout.addLayout(button_layout)

        # Lista visual de carpetas seleccionadas
        self.folder_list = QListWidget()
        layout.addWidget(QLabel("📁 Carpetas seleccionadas:"))
        layout.addWidget(self.folder_list)

        # Botón para iniciar backup
        self.backup_button = QPushButton("📦 Iniciar Backup")
        self.backup_button.clicked.connect(self.start_backup)
        layout.addWidget(self.backup_button)

        # Log de salida
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        layout.addWidget(QLabel("📜 Log:"))
        layout.addWidget(self.log_output)

        self.setLayout(layout)

        # Inicializar Google Drive
        self.gauth = GoogleAuth()
        self.drive = None
        self.authenticate_drive()

    def authenticate_drive(self):
        try:
            self.gauth.LocalWebserverAuth()
            self.drive = GoogleDrive(self.gauth)
            self.log_output.append("✅ Autenticado con Google Drive correctamente.")
        except Exception as e:
            self.log_output.append(f"❌ Error autenticando con Google Drive: {e}")

    def select_folders(self):
        folder = QFileDialog.getExistingDirectory(
            self, "Seleccionar carpeta", "", QFileDialog.ShowDirsOnly
        )
        if folder and folder not in self.selected_folders:
            self.selected_folders.append(folder)
            self.folder_list.addItem(folder)
            self.log_output.append(f"📁 Carpeta añadida: {folder}")
        elif folder:
            self.log_output.append(f"⚠️ La carpeta ya fue añadida.")

    def remove_selected_folder(self):
        selected_items = self.folder_list.selectedItems()
        if not selected_items:
            self.log_output.append("⚠️ No hay carpeta seleccionada para eliminar.")
            return

        for item in selected_items:
            folder_path = item.text()
            self.selected_folders.remove(folder_path)
            self.folder_list.takeItem(self.folder_list.row(item))
            self.log_output.append(f"🗑️ Carpeta eliminada: {folder_path}")

    def start_backup(self):
        if not self.selected_folders:
            self.log_output.append("⚠️ No hay carpetas seleccionadas.")
            return

        output_file, _ = QFileDialog.getSaveFileName(
            self, "Guardar archivo de backup", "respaldo_final.tar.bz2", "Backup (*.tar.bz2)"
        )
        if not output_file:
            self.log_output.append("❌ Operación cancelada.")
            return

        self.log_output.append("🚀 Iniciando backup...")

        task = delayed(self.create_backup)(self.selected_folders, output_file)
        result = task.compute()

        self.log_output.append(result)

        # ✅ Subir a Google Drive
        if os.path.exists(output_file):
            self.upload_to_drive(output_file)

    def create_backup(self, folders, output_path):
        try:
            with tarfile.open(output_path, "w:bz2") as tar:
                for folder in folders:
                    folder_name = os.path.basename(folder)
                    tar.add(folder, arcname=folder_name)

            return f"✅ Backup creado correctamente en: {output_path}"
        except Exception as e:
            return f"❌ Error durante el backup: {str(e)}"

    def upload_to_drive(self, filepath):
        try:
            file_name = os.path.basename(filepath)
            gfile = self.drive.CreateFile({'title': file_name})
            gfile.SetContentFile(filepath)
            gfile.Upload()
            self.log_output.append(f"☁️ Archivo subido exitosamente a Google Drive: {file_name}")
        except Exception as e:
            self.log_output.append(f"❌ Error al subir a Google Drive: {e}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BackupApp()
    window.show()
    sys.exit(app.exec_())
