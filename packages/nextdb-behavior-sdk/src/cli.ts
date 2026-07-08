#!/usr/bin/env node
import { spawn } from "node:child_process"
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { createRequire } from "node:module"
import { tmpdir } from "node:os"
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path"
import { validateManifest } from "./index.js"

const require = createRequire(import.meta.url)

type PackOptions = {
  manifest: string
  wasm?: string
  out: string
}

type CompileOptions = PackOptions & {
  entry: string
  optimize: boolean
}

async function main(argv: string[]): Promise<void> {
  const [command, ...rest] = argv
  if (command === "pack") {
    await pack(parsePackOptions(rest))
    return
  }
  if (command === "compile") {
    await compile(parseCompileOptions(rest))
    return
  }

  usage()
  if (command) {
    throw new Error(`unknown command ${command}`)
  }
  process.exitCode = 1
}

async function pack(options: PackOptions): Promise<void> {
  const manifestPath = resolve(options.manifest)
  const manifest = validateManifest(JSON.parse(await readFile(manifestPath, "utf8")))
  const manifestDir = dirname(manifestPath)
  const sourceWasm = resolve(
    options.wasm ??
      (isAbsolute(manifest.modulePath) ? manifest.modulePath : join(manifestDir, manifest.modulePath)),
  )
  const moduleName = basename(manifest.modulePath)
  if (!moduleName.endsWith(".wasm")) {
    throw new Error("manifest.modulePath must point to a .wasm file")
  }

  const outDir = resolve(options.out)
  await mkdir(outDir, { recursive: true })
  await copyFile(sourceWasm, join(outDir, moduleName))
  await writeManifest(outDir, manifest, moduleName)
  printResult(manifest, outDir, moduleName)
}

async function compile(options: CompileOptions): Promise<void> {
  const manifestPath = resolve(options.manifest)
  const manifest = validateManifest(JSON.parse(await readFile(manifestPath, "utf8")))
  if (manifest.abiEncoding === "postcardTypedSchema") {
    throw new Error("compile currently emits string-based handlers; use pack for precompiled postcardTypedSchema Wasm")
  }
  const moduleName = basename(manifest.modulePath)
  if (!moduleName.endsWith(".wasm")) {
    throw new Error("manifest.modulePath must point to a .wasm file")
  }

  const outDir = resolve(options.out)
  await mkdir(outDir, { recursive: true })
  const tempDir = await mkdtemp(join(tmpdir(), "nextdb-behavior-"))
  const wrapperPath = join(tempDir, "nextdb-entry.ts")
  const wasmPath = join(outDir, moduleName)

  try {
    await writeFile(wrapperPath, wrapperSource(wrapperPath, resolve(options.entry), manifest.abiEncoding))
    await runAsc([
      wrapperPath,
      "--outFile",
      wasmPath,
      "--exportRuntime",
      "--runtime",
      "stub",
      ...(options.optimize ? ["--optimize"] : []),
    ])
  } finally {
    await rm(tempDir, { recursive: true, force: true })
  }

  await writeManifest(outDir, manifest, moduleName)
  printResult(manifest, outDir, moduleName)
}

async function writeManifest(
  outDir: string,
  manifest: ReturnType<typeof validateManifest>,
  moduleName: string,
): Promise<void> {
  await writeFile(
    join(outDir, "nextdb.behavior.json"),
    `${JSON.stringify(
      {
        ...manifest,
        modulePath: moduleName,
      },
      null,
      2,
    )}\n`,
  )
}

function printResult(
  manifest: ReturnType<typeof validateManifest>,
  outDir: string,
  moduleName: string,
): void {
  console.log(
    JSON.stringify(
      {
        name: manifest.name,
        version: manifest.version,
        outDir,
        modulePath: moduleName,
        ...(manifest.abiEncoding === undefined ? {} : { abiEncoding: manifest.abiEncoding }),
        mutations: manifest.mutations,
        ...(manifest.reads === undefined ? {} : { reads: manifest.reads }),
        ...(manifest.recordScopes === undefined ? {} : { recordScopes: manifest.recordScopes }),
        ...(manifest.objectScopes === undefined ? {} : { objectScopes: manifest.objectScopes }),
        ...(manifest.realtimeScopes === undefined ? {} : { realtimeScopes: manifest.realtimeScopes }),
        ...(manifest.connectionScopes === undefined ? {} : { connectionScopes: manifest.connectionScopes }),
        ...(manifest.userScopes === undefined ? {} : { userScopes: manifest.userScopes }),
        ...(manifest.eventScopes === undefined ? {} : { eventScopes: manifest.eventScopes }),
        commands: manifest.commands ?? [],
      },
      null,
      2,
    ),
  )
}

function wrapperSource(
  wrapperPath: string,
  entryPath: string,
  abiEncoding: ReturnType<typeof validateManifest>["abiEncoding"],
): string {
  let importPath = relative(dirname(wrapperPath), entryPath).split(sep).join("/")
  if (!importPath.startsWith(".")) {
    importPath = `./${importPath}`
  }
  importPath = importPath.replace(/\.ts$/, "")
  if (abiEncoding === "postcard") {
    return postcardJsonWrapperSource(importPath)
  }
  return jsonWrapperSource(importPath)
}

function jsonWrapperSource(importPath: string): string {
  return `import { handle } from "${importPath}";

let __nextdbInput = new ArrayBuffer(0);
let __nextdbOutput = new ArrayBuffer(0);

export function alloc(len: i32): usize {
  __nextdbInput = new ArrayBuffer(len);
  return changetype<usize>(__nextdbInput);
}

export function dealloc(_ptr: usize, _len: i32): void {}

export function invoke(ptr: usize, len: i32): u64 {
  const request = String.UTF8.decodeUnsafe(ptr, len, false);
  const response = handle(request);
  __nextdbOutput = String.UTF8.encode(response, false);
  const outputPtr = changetype<usize>(__nextdbOutput);
  const outputLen = __nextdbOutput.byteLength;
  return (<u64>outputPtr << 32) | <u64>outputLen;
}

export function handle_message(ptr: usize, len: i32): u64 {
  return invoke(ptr, len);
}
`
}

function postcardJsonWrapperSource(importPath: string): string {
  return `import { handle } from "${importPath}";

let __nextdbInput = new ArrayBuffer(0);
let __nextdbOutput = new ArrayBuffer(0);

export function alloc(len: i32): usize {
  __nextdbInput = new ArrayBuffer(len);
  return changetype<usize>(__nextdbInput);
}

export function dealloc(_ptr: usize, _len: i32): void {}

export function invoke(ptr: usize, len: i32): u64 {
  const request = decodePostcardJsonFrame(ptr, len);
  const response = handle(request);
  __nextdbOutput = encodePostcardJsonFrame(response);
  const outputPtr = changetype<usize>(__nextdbOutput);
  const outputLen = __nextdbOutput.byteLength;
  return (<u64>outputPtr << 32) | <u64>outputLen;
}

export function handle_message(ptr: usize, len: i32): u64 {
  return invoke(ptr, len);
}

function decodePostcardJsonFrame(ptr: usize, len: i32): string {
  let offset: i32 = 0;
  const encoding = readVarUInt(ptr, len, offset);
  offset = encoding.nextOffset;
  if (encoding.value != 0) {
    return errorOutput("unsupported postcard behavior payload encoding");
  }
  const payloadLen = readVarUInt(ptr, len, offset);
  offset = payloadLen.nextOffset;
  if (payloadLen.value < 0 || offset + payloadLen.value > len) {
    return errorOutput("invalid postcard behavior payload length");
  }
  return String.UTF8.decodeUnsafe(ptr + <usize>offset, payloadLen.value, false);
}

function encodePostcardJsonFrame(json: string): ArrayBuffer {
  const payload = String.UTF8.encode(json, false);
  const payloadLen = payload.byteLength;
  const headerLen = encodedVarUIntLength(0) + encodedVarUIntLength(payloadLen);
  const out = new ArrayBuffer(headerLen + payloadLen);
  let offset = writeVarUInt(out, 0, 0);
  offset = writeVarUInt(out, offset, payloadLen);
  memory.copy(changetype<usize>(out) + <usize>offset, changetype<usize>(payload), payloadLen);
  return out;
}

function errorOutput(message: string): string {
  return "{\\"commands\\":[],\\"result\\":{\\"error\\":" + jsonString(message) + "}}";
}

function jsonString(value: string): string {
  let out = "\\"";
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code == 34) {
      out += "\\\\\\\"";
    } else if (code == 92) {
      out += "\\\\\\\\";
    } else if (code == 10) {
      out += "\\\\n";
    } else if (code == 13) {
      out += "\\\\r";
    } else if (code == 9) {
      out += "\\\\t";
    } else {
      out += String.fromCharCode(code);
    }
  }
  return out + "\\"";
}

class VarUInt {
  value: i32;
  nextOffset: i32;

  constructor(value: i32, nextOffset: i32) {
    this.value = value;
    this.nextOffset = nextOffset;
  }
}

function readVarUInt(ptr: usize, len: i32, startOffset: i32): VarUInt {
  let result: i32 = 0;
  let shift: i32 = 0;
  let offset = startOffset;
  while (offset < len && shift < 35) {
    const byte = load<u8>(ptr + <usize>offset);
    result |= <i32>(byte & 0x7f) << shift;
    offset += 1;
    if ((byte & 0x80) == 0) {
      return new VarUInt(result, offset);
    }
    shift += 7;
  }
  return new VarUInt(-1, len);
}

function encodedVarUIntLength(value: i32): i32 {
  let remaining = value;
  let length = 1;
  while (remaining >= 0x80) {
    remaining >>= 7;
    length += 1;
  }
  return length;
}

function writeVarUInt(buffer: ArrayBuffer, offset: i32, value: i32): i32 {
  let remaining = value;
  let currentOffset = offset;
  while (remaining >= 0x80) {
    store<u8>(changetype<usize>(buffer) + <usize>currentOffset, <u8>((remaining & 0x7f) | 0x80));
    remaining >>= 7;
    currentOffset += 1;
  }
  store<u8>(changetype<usize>(buffer) + <usize>currentOffset, <u8>remaining);
  return currentOffset + 1;
}
`
}

async function runAsc(args: string[]): Promise<void> {
  const ascPath = require.resolve("assemblyscript/bin/asc.js")
  await new Promise<void>((resolvePromise, reject) => {
    const child = spawn(process.execPath, [ascPath, ...args], {
      stdio: "inherit",
    })
    child.on("error", reject)
    child.on("exit", (code) => {
      if (code === 0) {
        resolvePromise()
      } else {
        reject(new Error(`asc failed with exit code ${code}`))
      }
    })
  })
}

function parsePackOptions(args: string[]): PackOptions {
  const options: Partial<PackOptions> = {}
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]
    const value = args[index + 1]
    if (!value || value.startsWith("--")) {
      throw new Error(`${arg} requires a value`)
    }
    if (arg === "--manifest") {
      options.manifest = value
    } else if (arg === "--wasm") {
      options.wasm = value
    } else if (arg === "--out") {
      options.out = value
    } else {
      throw new Error(`unknown option ${arg}`)
    }
    index += 1
  }

  if (!options.manifest || !options.out) {
    throw new Error("pack requires --manifest and --out")
  }
  return options as PackOptions
}

function parseCompileOptions(args: string[]): CompileOptions {
  const options: Partial<CompileOptions> = {
    optimize: true,
  }
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]
    if (arg === "--debug") {
      options.optimize = false
      continue
    }
    const value = args[index + 1]
    if (!value || value.startsWith("--")) {
      throw new Error(`${arg} requires a value`)
    }
    if (arg === "--manifest") {
      options.manifest = value
    } else if (arg === "--entry") {
      options.entry = value
    } else if (arg === "--out") {
      options.out = value
    } else {
      throw new Error(`unknown option ${arg}`)
    }
    index += 1
  }

  if (!options.manifest || !options.entry || !options.out) {
    throw new Error("compile requires --manifest, --entry, and --out")
  }
  return options as CompileOptions
}

function usage(): void {
  console.error(`Usage:
  nextdb-behavior pack --manifest nextdb.behavior.json --wasm behavior.wasm --out data/behaviors/name
  nextdb-behavior compile --manifest nextdb.behavior.json --entry src/index.ts --out data/behaviors/name

If --wasm is omitted, manifest.modulePath is resolved relative to the manifest file.
The compile command expects the entry module to export handle(requestJson: string): string.`)
}

main(process.argv.slice(2)).catch((error) => {
  console.error(error instanceof Error ? error.message : String(error))
  process.exitCode = 1
})
