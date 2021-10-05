import argparse
import json

from .schema import infer_schema

def console_main():
    parser = argparse.ArgumentParser(description='json-tools')
    parser.add_argument(
        "-a", "--action", type=str, required=True, help="Specify action",
        choices=["test"]
    )
    args = parser.parse_args()
    if args.action == "test":
        console_test(args)
    return


def console_test(args):
    schema = infer_schema([
        {"x": 1,   "info": [{"x": 1}]},
        {"y": 2.0, "info": []},

    ])

    print(
        json.dumps(
            schema, 
            indent=4, 
            separators=(',', ':',) 
        )
    )
