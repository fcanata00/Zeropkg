# zeropkg1.0/plugins/unpacker_tar.py
import os
import tarfile
from . import register_plugin

class TarUnpacker:
    def unpack(self, tarball, dest_dir):
        if not os.path.exists(tarball):
            raise FileNotFoundError(f"Tarball não encontrado: {tarball}")

        os.makedirs(dest_dir, exist_ok=True)

        with tarfile.open(tarball, "r:*") as tar:
            tar.extractall(path=dest_dir)

        # Retorna o diretório raiz extraído
        top_level = None
        with tarfile.open(tarball, "r:*") as tar:
            top_level = tar.getmembers()[0].name.split("/")[0]

        return os.path.join(dest_dir, top_level)

# auto-registro
register_plugin("unpacker", "tar", TarUnpacker())
