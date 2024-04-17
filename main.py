
import sys

def print_hi(name):

    print(f'Hello, {name}')



if __name__ == '__main__':
    print_hi('World!')
    for path in sys.path:
        print(path)


