# zeropkg1.0/plugins/fetcher_http.py
import os
import urllib.request
from . import register_plugin

class HTTPFetcher:
    def fetch(self, url, dest_dir):
        filename = os.path.basename(url)
        dest_path = os.path.join(dest_dir, filename)
        urllib.request.urlretrieve(url, dest_path)
        return dest_path

# auto-registro
register_plugin("fetcher", "http", HTTPFetcher())
