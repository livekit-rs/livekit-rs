/** Cases from the unmodified installed official protobuf descriptors. */
import * as protocol from '@livekit/protocol';

function scalarValue(field, second) {
  let value;
  switch (field.T) {
    case 9: value = second ? 'second' : 'first'; break; // ScalarType.STRING
    case 12: value = second ? 'Yg==' : 'YQ=='; break; // BYTES, protojson base64
    case 8: value = second; break; // BOOL
    case 3: case 4: case 6: case 16: case 18: value = second ? '2' : '1'; break;
    default: value = second ? 2 : 1; break;
  }
  return JSON.stringify(field.repeated ? [value] : value);
}
function findScalar(type, aliases, parents = [], depth = 0) {
  if (depth > 8) throw new Error(`descriptor recursion limit: ${type.typeName}`);
  const fields = type.fields.list();
  const scalar = fields.find(f => f.kind === 'scalar' && (!aliases || f.name !== f.jsonName));
  if (scalar) return { parents, field: scalar };
  for (const field of fields) {
    if (field.kind !== 'message' || field.repeated) continue;
    const found = findScalar(field.T, aliases, [...parents, field], depth + 1);
    if (found) return found;
  }
  return undefined;
}
function wrap(path, body) {
  for (const field of [...path].reverse()) body = `{${JSON.stringify(field.jsonName)}:${body}}`;
  return body;
}
export function duplicateJsonCases(contract) {
  const type = protocol[contract.request];
  if (!type?.fields) throw new Error(`missing official request descriptor: ${contract.request}`);
  const found = findScalar(type, false);
  if (!found) throw new Error(`no scalar path in official descriptor: ${contract.request}`);
  const { parents, field } = found;
  const cases = [{ name: 'duplicate-known-scalar', expectMalformed: true,
    fieldPath: [...parents.map(f => f.name), field.name],
    body: wrap(parents, `{${JSON.stringify(field.jsonName)}:${scalarValue(field, false)},${JSON.stringify(field.jsonName)}:${scalarValue(field, true)}}`) }];
  const alias = findScalar(type, true);
  if (alias) {
    for (const reverse of [false, true]) {
      const names = reverse ? [alias.field.jsonName, alias.field.name] : [alias.field.name, alias.field.jsonName];
      cases.push({ name: reverse ? 'duplicate-camel-snake' : 'duplicate-snake-camel', expectMalformed: true,
        fieldPath: [...alias.parents.map(f => f.name), alias.field.name],
        body: wrap(alias.parents, `{${JSON.stringify(names[0])}:${scalarValue(alias.field, false)},${JSON.stringify(names[1])}:${scalarValue(alias.field, true)}}`) });
    }
  }
  // Compare the actual oracle response. Unknown-key duplication must not be
  // assigned known-key behavior just because their JSON spelling repeats.
  cases.push({ name: 'duplicate-unknown-field', expectMalformed: false,
    body: '{"f02_unknown_future_field":1,"f02_unknown_future_field":{"nested":true}} trailing' });
  return cases;
}
