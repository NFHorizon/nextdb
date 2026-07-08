import assert from "node:assert/strict"
import { readFile, writeFile } from "node:fs/promises"

const MAGIC = Buffer.from("NDBW")
const V1_HEADER_BYTES = 12
const V2_HEADER_BYTES = 16
const VERSION_V1 = 1
const VERSION_V2 = 2
const ENCODING_JSON = 1
const ENCODING_POSTCARD = 2

export async function corruptWalPayloadString(path, original, replacement) {
  const bytes = await readFile(path)
  const updated = rewriteWalPayloadString(bytes, original, replacement)
  await writeFile(path, updated)
}

export async function walFileContainsString(path, needle) {
  const bytes = await readFile(path)
  if (isFramedWal(bytes)) {
    return decodeWalPayloads(bytes).some((payload) => payload.includes(needle))
  }
  return bytes.toString("utf8").includes(needle)
}

function rewriteWalPayloadString(bytes, original, replacement) {
  if (!isFramedWal(bytes)) {
    const text = bytes.toString("utf8")
    assert(text.includes(original), `legacy WAL payload does not include ${original}`)
    assert.equal(text.includes(replacement), false)
    return Buffer.from(text.replace(original, replacement), "utf8")
  }

  const out = []
  let offset = 0
  let replaced = false
  while (offset < bytes.length) {
    const { headerBytes, encoding } = assertFrameHeader(bytes, offset)
    const len = bytes.readUInt32BE(offset + 8)
    const start = offset + headerBytes
    const end = start + len
    assert(end <= bytes.length, "truncated framed WAL payload")
    const payload = Buffer.from(bytes.subarray(start, end))
    const originalBytes = Buffer.from(original, "utf8")
    const replacementBytes = Buffer.from(replacement, "utf8")
    if (!replaced) {
      const index = payload.indexOf(originalBytes)
      if (index !== -1) {
        assert.equal(payload.indexOf(replacementBytes), -1)
        assert.equal(
          replacementBytes.length,
          originalBytes.length,
          "framed WAL byte replacement must preserve payload length",
        )
        replacementBytes.copy(payload, index)
        replaced = true
      }
    }
    const header = Buffer.alloc(V1_HEADER_BYTES)
    MAGIC.copy(header, 0)
    header.writeUInt16BE(VERSION_V1, 4)
    header.writeUInt16BE(encoding, 6)
    header.writeUInt32BE(payload.length, 8)
    out.push(header, payload)
    offset = end
  }
  assert(replaced, `framed WAL payload does not include ${original}`)
  return Buffer.concat(out)
}

function decodeWalPayloads(bytes) {
  const payloads = []
  let offset = 0
  while (offset < bytes.length) {
    const { headerBytes } = assertFrameHeader(bytes, offset)
    const len = bytes.readUInt32BE(offset + 8)
    const start = offset + headerBytes
    const end = start + len
    assert(end <= bytes.length, "truncated framed WAL payload")
    payloads.push(bytes.subarray(start, end).toString("utf8"))
    offset = end
  }
  return payloads
}

function isFramedWal(bytes) {
  return bytes.length >= MAGIC.length && bytes.subarray(0, MAGIC.length).equals(MAGIC)
}

function assertFrameHeader(bytes, offset) {
  assert(bytes.length - offset >= V1_HEADER_BYTES, "truncated framed WAL header")
  assert(bytes.subarray(offset, offset + MAGIC.length).equals(MAGIC), "invalid framed WAL magic")
  const version = bytes.readUInt16BE(offset + 4)
  assert(version === VERSION_V1 || version === VERSION_V2, `unsupported framed WAL version ${version}`)
  const encoding = bytes.readUInt16BE(offset + 6)
  assert(
    encoding === ENCODING_JSON || encoding === ENCODING_POSTCARD,
    `unsupported framed WAL encoding ${encoding}`,
  )
  const headerBytes = version === VERSION_V2 ? V2_HEADER_BYTES : V1_HEADER_BYTES
  assert(bytes.length - offset >= headerBytes, "truncated framed WAL header")
  return { headerBytes, version, encoding }
}
