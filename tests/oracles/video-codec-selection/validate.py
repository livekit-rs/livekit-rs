#!/usr/bin/env python3
"""Strict independent literal ACK checker, including schema and corruption controls."""
import argparse
import copy
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
from matrix import CASE_COUNT, expected, schema

HERE = Path(__file__).resolve().parent


def require(ok, message):
    if not ok:
        raise ValueError(message)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, 'duplicate key: ' + key)
        result[key] = value
    return result


def parse(raw):
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite ' + value)))


def exact(actual, wanted, path='$'):
    require(type(actual) is type(wanted), path + ': type mismatch')
    if isinstance(wanted, dict):
        require(actual.keys() == wanted.keys(), path + ': missing/unknown keys')
        for k, v in wanted.items():
            exact(actual[k], v, path + '.' + k)
    elif isinstance(wanted, list):
        require(len(actual) == len(wanted), path + ': length mismatch')
        for i, (a, w) in enumerate(zip(actual, wanted)):
            exact(a, w, path + '[' + str(i) + ']')
    else:
        require(actual == wanted, path + ': ' + repr(actual) + ' != ' + repr(wanted))


def load(raw):
    rows = [parse(line) for line in raw.splitlines()]
    exact(rows, expected())
    return rows


def self_test():
    rows = expected()
    load('\n'.join(json.dumps(r) for r in rows))
    rejected = 0
    def reject(value):
        nonlocal rejected
        try:
            load(value)
        except (ValueError, TypeError):
            rejected += 1
        else:
            raise AssertionError('accepted corruption')
    def encoded(value):
        return '\n'.join(json.dumps(r) for r in value)
    # Every scalar in every row is load-bearing. Test type and value corruption,
    # every object missing/unknown key, and every array deletion/duplication/order.
    def visit(value, path):
        def modified(replacement):
            mutant = copy.deepcopy(rows)
            target = mutant
            for key in path[:-1]:
                target = target[key]
            if path:
                target[path[-1]] = replacement
            else:
                mutant = replacement
            reject(encoded(mutant))
        if isinstance(value, dict):
            modified(None)
            modified([])
            modified(dict(value, unexpected=True))
            for key in value:
                smaller = dict(value); del smaller[key]; modified(smaller)
                visit(value[key], path + [key])
        elif isinstance(value, list):
            modified(value + [None])
            if value:
                modified(value[1:])
                if value != list(reversed(value)):
                    modified(list(reversed(value)))
                for i, child in enumerate(value):
                    visit(child, path + [i])
        else:
            if type(value) is bool:
                modified(not value); modified(int(value))
            elif type(value) is int:
                modified(value + 1); modified(float(value))
            else:
                modified(value + '!'); modified(None)
    visit(rows, [])
    raw = encoded(rows)
    reject(raw.replace('"schema_version": 2', '"schema_version": 2, "schema_version": 2', 1))
    reject(raw + '\n' + json.dumps(rows[0]))
    # Regression-specific wrong choice: use originating unsupported layers.
    for index in (0, 2, 13, 15):
        mutant = copy.deepcopy(rows)
        mutant[index]['output']['track']['codecs'][0]['layers'] = mutant[index]['input']['request']['simulcast_codecs'][0]['layers']
        reject(encoded(mutant))
    return rejected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    wanted = expected()
    exact(parse((HERE / 'cases.json').read_bytes()), [{k: v for k, v in r.items() if k != 'output'} for r in wanted])
    exact(parse((HERE / 'schema.json').read_bytes()), schema())
    result = {'cases': len(wanted), 'schema_exact': True}
    if args.self_test:
        result['negative_controls_rejected'] = self_test()
    if args.input:
        raw = args.input.read_bytes()
        result['observed_rows'] = len(load(raw))
        require(args.input.read_bytes() == raw, 'raw output source modified')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
