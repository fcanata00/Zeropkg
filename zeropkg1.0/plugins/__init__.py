# zeropkg1.0/plugins/__init__.py
import importlib
import pkgutil

PLUGINS = {}

def register_plugin(kind, name, plugin_class):
    """Registra um plugin em uma categoria (kind) com um nome único"""
    if kind not in PLUGINS:
        PLUGINS[kind] = {}
    PLUGINS[kind][name] = plugin_class

def get_plugin(kind, name):
    """Obtém plugin por categoria e nome"""
    return PLUGINS.get(kind, {}).get(name)

def list_plugins():
    """Lista todos os plugins registrados"""
    return PLUGINS

# carregamento automático
for loader, module_name, ispkg in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{module_name}")
