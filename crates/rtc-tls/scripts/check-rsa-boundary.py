#!/usr/bin/env python3
"""Fail closed if TLS RSA reaches the affected rsa-crate private API.

This source boundary check complements compilation/interoperability and review;
it does not claim to prove constant-time machine code.
"""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]


def problems(sign: str, verify: str) -> list[str]:
    errors = []
    for name, source in [('rsa_sign.rs', sign), ('rsa_verify.rs', verify)]:
        production = source.split('#[cfg(test)]', 1)[0]
        # This DER borrowed ASN.1 structure is a parser, not rsa::RsaPrivateKey.
        production = production.replace('rsa::pkcs1::RsaPrivateKey::from_der', 'RSA_DER_PARSER')
        for pattern in [r'\bRsaPrivateKey\b', r'\brsa_decrypt\b', r'\brsa::hazmat\b',
                        r'\.(?:decrypt|decrypt_blinded|decrypt_in_place)\s*\(',
                        r'rsa::(?:pss|pkcs1v15)::(?:Blinded)?SigningKey',
                        r'\b(?:dp|dq|qinv)\s*\(']:
            if re.search(pattern, production):
                errors.append(f'{name}: forbidden private/CRT/decryption API {pattern}')
    compact = re.sub(r'\s+', '', sign)
    for required in ['blinded.pow(&key.d)', 'getrandom::fill',
                     'private_signature_with_rng', 'public_check!=*m',
                     'random[0]&=0xff>>(key.bytes*8-key.n_bits)', 'Zeroizing<BoxedUint>']:
        if required not in compact:
            errors.append(f'rsa_sign.rs: required invariant absent: {required}')
    if re.search(r'key\.d\.(?:bits|bits_vartime)|pow_bounded_exp\(&key\.d', compact):
        errors.append('rsa_sign.rs: private exponent must use full public precision')
    return errors


def main() -> int:
    sign = (ROOT / 'src/rsa_sign.rs').read_text()
    verify = (ROOT / 'src/rsa_verify.rs').read_text()
    errors = problems(sign, verify)
    for path in (ROOT / 'src').rglob('*.rs'):
        if path.name in {'rsa_sign.rs', 'rsa_verify.rs'}:
            continue
        production = path.read_text().split('#[cfg(test)]', 1)[0]
        if re.search(r'\brsa::', production):
            errors.append(f'{path.relative_to(ROOT)}: unreviewed rsa-crate call site')
    if '--self-test' in sys.argv:
        for mutation in [sign + '\nrsa::RsaPrivateKey::from_components();',
                         sign + '\nkey.decrypt(input);',
                         sign.replace('blinded.pow(&key.d)', 'blinded.pow_bounded_exp(&key.d, 8)')]:
            # Put mutation before the test boundary to exercise production scanning.
            mutated = mutation.replace('#[cfg(test)]', '// test boundary')
            if not problems(mutated, verify):
                errors.append('source boundary regression failed to reject mutation')
    for error in errors:
        print(error, file=sys.stderr)
    if not errors:
        print('PASS: RSA crate public-only; independent signing adapter invariants retained')
    return bool(errors)


if __name__ == '__main__':
    sys.exit(main())
