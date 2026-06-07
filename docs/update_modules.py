from pathlib import Path

CWD = Path(__file__).parent
BASE_DIR = (CWD / "..").resolve()

RST_TEMPLATE = """%s

"""

def scan_modules(name):
    doc_path = CWD / "api" / f"{name}.rst"
    mod_path = BASE_DIR / "duct" / name

    modules = []
    for node in mod_path.rglob("**/*.py"):
        if node.is_file() and node.name != "__init__.py":
            filepath = node.relative_to(BASE_DIR)
            module = str(filepath.parents[0]/filepath.stem).replace('/', '.')
            modules.append(module)

    modules.sort()
    print(modules)
    with open(doc_path, "w+t") as f:
        f.write(f"duct.{name}\n")
        f.write("**************\n\n")

        for module in modules:
            f.write(module + '\n')
            f.write("===================================\n\n")
            f.write(f".. automodule:: {module}\n")
            f.write("   :members:\n")
            f.write("   :show-inheritance:\n\n")

if __name__ == "__main__":
    scan_modules("sources")
    scan_modules("outputs")
    scan_modules("protocol")