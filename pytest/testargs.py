import argparse

def add_fn(arg1, arg2, arg3):
    return arg1 + arg2 + arg3

def main():
    parser = argparse.ArgumentParser(description="Process three string arguments.")
    parser.add_argument('arg1', type=str, help='First string argument')
    parser.add_argument('arg2', type=str, help='Second string argument')
    parser.add_argument('arg3', type=str, help='Third string argument')

    args = parser.parse_args()

    try:
        result = add_fn(args.arg1, args.arg2, args.arg3)
        print(f"Result: {result}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()