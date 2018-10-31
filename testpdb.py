import pdb, traceback, sys

def lalala():
    a = []
    print(a[0])

if __name__ == '__main__':
    try:
        lalala()
    except:
        extype, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)