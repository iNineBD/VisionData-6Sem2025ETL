import os
import sys

from dotenv import load_dotenv


def get_env_path():
    if getattr(sys, "frozen", False):
        base_path = sys._MEIPASS
    else:
        base_path = os.getcwd()
        fallback = os.path.dirname(os.path.abspath(__file__))

        if not os.path.exists(os.path.join(base_path, "development.env")):
            base_path = fallback

    return os.path.join(base_path, "development.env")


def load_default_env():
    if "ENV_FILE" not in os.environ:
        load_dotenv(get_env_path())
    else:
        env_file = os.getenv("ENV_FILE")
        env_file_path = os.path.join(env_file)
        load_dotenv(env_file_path)


def get_boolean_from_env(env_var_name):
    env_var_str = os.getenv(env_var_name)

    if env_var_str is None:
        return None
    return env_var_str.lower() in ["true", "yes", "1"]
