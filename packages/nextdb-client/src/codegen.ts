#!/usr/bin/env node
import { mkdir, writeFile } from "node:fs/promises"
import { dirname, resolve } from "node:path"

interface CodegenOptions {
  endpoint: string
  out?: string
  token?: string
  adminToken?: string
  print: boolean
}

interface SchemaTypescriptResponse {
  typescript: string
}

const DEFAULT_ENDPOINT = "http://127.0.0.1:3188"

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2))
  if (!options.print && options.out === undefined) {
    throw new Error("missing output target: pass --out <file> or --print")
  }

  const typescript = await fetchSchemaTypescript(options)

  if (options.print) {
    process.stdout.write(typescript)
    if (!typescript.endsWith("\n")) {
      process.stdout.write("\n")
    }
  }

  if (options.out !== undefined) {
    const outPath = resolve(options.out)
    await mkdir(dirname(outPath), { recursive: true })
    await writeFile(outPath, typescript)
    process.stderr.write(`generated ${outPath}\n`)
  }
}

function parseArgs(args: string[]): CodegenOptions {
  const options: CodegenOptions = {
    endpoint: process.env.NEXTDB_ENDPOINT ?? DEFAULT_ENDPOINT,
    token: process.env.NEXTDB_CLIENT_TOKEN,
    adminToken: process.env.NEXTDB_ADMIN_TOKEN,
    print: false,
  }

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]
    switch (arg) {
      case "--endpoint":
        options.endpoint = requireValue(args, ++index, "--endpoint")
        break
      case "--out":
        options.out = requireValue(args, ++index, "--out")
        break
      case "--token":
        options.token = requireValue(args, ++index, "--token")
        break
      case "--admin-token":
        options.adminToken = requireValue(args, ++index, "--admin-token")
        break
      case "--print":
        options.print = true
        break
      case "--help":
      case "-h":
        printHelp()
        process.exit(0)
        break
      default:
        throw new Error(`unknown argument: ${arg}`)
    }
  }

  options.endpoint = options.endpoint.replace(/\/$/, "")
  return options
}

function requireValue(args: string[], index: number, flag: string): string {
  const value = args[index]
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${flag} requires a value`)
  }
  return value
}

async function fetchSchemaTypescript(options: CodegenOptions): Promise<string> {
  const headers: Record<string, string> = {}
  if (options.token !== undefined) {
    headers.authorization = `Bearer ${options.token}`
    headers["x-nextdb-client-token"] = options.token
  }
  if (options.adminToken !== undefined) {
    headers["x-nextdb-admin-token"] = options.adminToken
  }

  const response = await fetch(`${options.endpoint}/v1/schema/typescript`, { headers })
  if (!response.ok) {
    const errorBody = await response.text().catch(() => "")
    throw new Error(`schema codegen failed with ${response.status}: ${errorBody || response.statusText}`)
  }

  const payload = (await response.json()) as Partial<SchemaTypescriptResponse>
  if (typeof payload.typescript !== "string") {
    throw new Error("schema codegen response is missing `typescript`")
  }
  return payload.typescript
}

function printHelp(): void {
  process.stdout.write(`nextdb-codegen

Generate TypeScript schema bindings from a running NextDB server.

Options:
  --endpoint <url>     NextDB endpoint. Defaults to NEXTDB_ENDPOINT or ${DEFAULT_ENDPOINT}
  --out <file>         Write generated TypeScript to a file
  --print              Print generated TypeScript to stdout
  --token <token>      Client token. Defaults to NEXTDB_CLIENT_TOKEN
  --admin-token <tok>  Admin token. Defaults to NEXTDB_ADMIN_TOKEN
  -h, --help           Show this help
`)
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error)
  process.stderr.write(`nextdb-codegen: ${message}\n`)
  process.exit(1)
})
