import argparse
from dataclasses import dataclass

@dataclass
class Args:
    port: int = 6379
    dir: str | None = None
    dbfilename: str | None = None

def parse_args() -> Args:
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--port", type=int, required=False)
    argparser.add_argument("--dir", type=str, required=False)
    argparser.add_argument("--dbfilename", type=str, required=False)
    namespace = Args()
    return argparser.parse_args(namespace=namespace)