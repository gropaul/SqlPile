


def run_all():
    # execute all scripts in this directory that start with "sec_"
    import os
    import importlib.util
    current_dir = os.path.dirname(__file__)
    for filename in os.listdir(current_dir):
        if filename.startswith("sec_") and filename.endswith(".py"):
            print(f"Running {filename}...")
            module_name = filename[:-3]
            module_path = os.path.join(current_dir, filename)
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            if hasattr(module, 'main'):
                module.main()
            else:
                print(f"No main() function found in {filename}.")

if __name__ == "__main__":
    run_all()