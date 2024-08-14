import random, time, pickle, stash, tempfile, pathlib


def mkobj(length, hashable):
    options = str, bytes, int, float, bool, tuple, frozenset
    if not hashable:
        options += bytearray, list, set, dict
    T = random.choice(options)
    if T is int:
        return random.randint(-100, 100)
    elif T is float:
        return random.normalvariate(0, 100)
    elif T is bool:
        return random.choice([True, False])
    elif T is str:
        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=random.randrange(0, 1000)))
    elif T in (bytes, bytearray):
        return T(random.choices(range(256), k=1000))
    elif T in (tuple, list):
        return T(mkobj(length-1, hashable) for i in range(length))
    elif T in (set, frozenset):
        return T(mkobj(length-1, True) for i in range(length))
    elif T is dict:
        return T((mkobj(length-1, True), mkobj(length-1, False)) for i in range(length))
    else:
        raise Exception


def get_test_object():
    pkl = 'time.pkl'
    try:
        with open(pkl, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        print('generating test object ...')
        random.seed(0)
        smax = b''
        for i in range(10):
            obj = mkobj(12, False)
            s = pickle.dumps(obj)
            print(f'{i}. {len(s)}')
            if len(s) > len(smax):
                smax = s
        with open(pkl, 'wb') as f:
            f.write(smax)
        return pickle.loads(smax)


def run_test(db, obj):

    print(db)

    T0 = time.perf_counter()
    t0 = time.process_time()
    h = db.dumps(obj)
    dT = time.perf_counter() - T0
    dt = time.process_time() - t0
    print(f'dumped in {dT:.2f}s ({100*dt/dT:.0f}% CPU)')

    T0 = time.perf_counter()
    t0 = time.process_time()
    obj_ = db.loads(h)
    dT = time.perf_counter() - T0
    dt = time.process_time() - t0
    print(f'loaded in {dT:.2f}s ({100*dt/dT:.0f}% CPU)')

    assert obj == obj_


def run_all_tests():

    obj = get_test_object()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)

        run_test(pickle, obj)
        if hasattr(stash, 'PyDB'):
            run_test(stash.PyDB({}), obj)
        if hasattr(stash, 'RAM'):
            run_test(stash.RAM(), obj)
        if hasattr(stash, 'FsDB'):
            run_test(stash.FsDB(tmpdir/'disk'), obj)
        if hasattr(stash, 'Sled'):
            run_test(stash.Sled(tmpdir/'sled'), obj)
        if hasattr(stash, 'Iroh'):
            run_test(stash.Iroh('/home/gertjan/.local/share/iroh'), obj)


if __name__ == '__main__':
    run_all_tests()
