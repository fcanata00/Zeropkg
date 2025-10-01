# plugins/deps.py
import os
import tomllib

RECIPES_DIR = "recipes"

def load_all_recipes():
    recipes = {}
    for fname in os.listdir(RECIPES_DIR):
        if fname.endswith(".toml"):
            path = os.path.join(RECIPES_DIR, fname)
            with open(path, "rb") as f:
                data = tomllib.load(f)
                name = data["package"]["name"]
                recipes[name] = data
    return recipes

def get_dependents(target_pkg, recipes=None):
    if recipes is None:
        recipes = load_all_recipes()

    dependents = []
    for pkg, recipe in recipes.items():
        deps = recipe.get("dependencies", {})
        runtime = deps.get("runtime", [])
        build = deps.get("build", [])
        all_deps = set(runtime + build)
        if target_pkg in all_deps:
            dependents.append(pkg)
    return dependents
