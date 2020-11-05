import sys
import traceback
import pkgutil, inspect
import cmapBQ


def import_from(module, name):
    module = __import__(module, fromlist=[name])
    return getattr(module, name)


def run_tool(toolname, *argv):
    tool = import_from(".".join(["cmapBQ", "tools", toolname]), "main")
    tool(*argv)


def print_help():
    print("cmapBQ: Toolkit for interacting with Google BigQuery and CMAP datasets\n")
    print("Avaliable tools:")
    print_tools()


def print_tools():
    tool_list = [
        name
        for _, name, _ in pkgutil.iter_modules(
            [inspect.getabsfile(cmapBQ).replace("__init__.py", "tools")]
        )
    ]
    print("Tools:")
    for tool in tool_list:
        print(tool)


def main(argv=None):
    if len(argv) < 2:
        print_help()
        sys.exit(0)

    if argv[1] == "help":
        print_help()
    else:
        try:
            run_tool(argv[1], argv[2:])
        except ModuleNotFoundError as exc:
            traceback.print_exc()
            print(exc)
            print("No tool {} found".format(sys.argv[1]))
            print_tools()
            exit(1)
            print("Unknown command: {}, try cmapBQ help".format(argv[1]))
