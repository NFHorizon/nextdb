import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { chromium } from "playwright"

const root = resolve(new URL("..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const behaviorCli = resolve(root, "packages/nextdb-behavior-sdk/dist/cli.js")
const behaviorExample = resolve(root, "examples/behaviors/echo-ts")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-admin-ui-"))
const dataDir = join(tempRoot, "data")
const behaviorOut = join(dataDir, "behaviors", "echo-ts")
const screenshotDir = join(tempRoot, "screenshots")
const server = {
  url: "http://127.0.0.1:3401",
  addr: "127.0.0.1:3401",
  dataDir,
}
const realtimeFixture = {
  channelId: "admin-ui-call",
  userId: "admin-ui-user",
}
const auditFixture = {
  roomId: "admin-ui-trace-room",
  messageId: "admin-ui-trace-message",
  mutationId: "admin-ui-trace-room-upsert",
}
const admin = {
  url: "http://127.0.0.1:5174",
  port: "5174",
}

let serverChild
let adminChild
let browser

try {
  await mkdir(dataDir, { recursive: true })
  await mkdir(behaviorOut, { recursive: true })
  await mkdir(screenshotDir, { recursive: true })
  await run(process.execPath, [
    behaviorCli,
    "compile",
    "--manifest",
    join(behaviorExample, "nextdb.behavior.json"),
    "--entry",
    join(behaviorExample, "src/index.ts"),
    "--out",
    behaviorOut,
  ])

  serverChild = spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: server.addr,
      NEXTDB_DATA_DIR: server.dataDir,
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHealth(server.url)
  await postJson(`${server.url}/v1/realtime/channels/${encodeURIComponent(realtimeFixture.channelId)}/join`, {
    userId: realtimeFixture.userId,
    metadata: { role: "operator" },
  })
  await postJson(`${server.url}/v1/realtime/channels/${encodeURIComponent(realtimeFixture.channelId)}/state`, {
    fromUserId: realtimeFixture.userId,
    expectedVersion: 0,
    state: { phase: "admin-smoke", tick: 1 },
  })
  await postJson(`${server.url}/v1/records/rooms/${encodeURIComponent(auditFixture.roomId)}`, {
    value: {
      id: auditFixture.roomId,
      title: "Admin UI Trace Room",
    },
    clientMutationId: auditFixture.mutationId,
  })
  await postJson(`${server.url}/v1/records/rooms/${encodeURIComponent(auditFixture.roomId)}/messages/${encodeURIComponent(auditFixture.messageId)}`, {
    value: {
      id: auditFixture.messageId,
      roomId: auditFixture.roomId,
      senderId: "admin-ui-user",
      body: "Admin UI nested activation message",
      attachments: [],
      createdAtMs: Date.now(),
      path: `tables/rooms/${auditFixture.roomId}/messages/${auditFixture.messageId}`,
    },
    clientMutationId: "admin-ui-trace-message-upsert",
  })

  adminChild = spawn("npm", ["run", "dev", "-w", "@nextdb/admin", "--", "--port", admin.port, "--strictPort"], {
    cwd: root,
    env: process.env,
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHttp(admin.url)

  browser = await chromium.launch({ headless: true })
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
  const consoleIssues = []
  page.on("console", (message) => {
    if (["error", "warning"].includes(message.type())) {
      consoleIssues.push(`${message.type()}: ${message.text()}`)
    }
  })
  page.on("pageerror", (error) => {
    consoleIssues.push(`pageerror: ${error.message}`)
  })
  await page.addInitScript((endpoint) => {
    localStorage.setItem("nextdb-admin:endpoint", endpoint)
    localStorage.removeItem("nextdb-admin:token")
  }, server.url)

  await page.goto(admin.url, { waitUntil: "networkidle" })
  await expectBodyContains(page, "NextDB Admin")
  await expectBodyContains(page, "Ready")
  await expectBodyContains(page, "R:ok W:ok RT:ok")
  await expectBodyContains(page, "Read ready")
  await expectBodyContains(page, "Write ready")
  await expectBodyContains(page, "Realtime ready")
  await expectBodyContains(page, "Readiness checks")
  await page.getByRole("button", { name: "Drain runtime" }).click()
  await expectBodyContains(page, "Draining")
  await expectBodyContains(page, "R:ok W:no RT:no")
  await expectBodyContains(page, "runtimeDrain")
  await page.getByRole("button", { name: "Resume runtime" }).click()
  await expectBodyContains(page, "Ready")
  await expectBodyContains(page, "R:ok W:ok RT:ok")
  await expectBodyContains(page, "Data Explorer")
  await expectBodyContains(page, "Schema Registry")
  await expectBodyContains(page, "Schema WAL")
  await expectBodyContains(page, "Realtime transport")
  await expectBodyContains(page, "HTTP JSONL")
  await assertSelectOption(page, "admin-realtime-transport", "websocket")
  await expectBodyContains(page, "WAL repair")
  await expectBodyContains(page, "Object repair")
  await expectBodyContains(page, "nextdb.realtime.v1")
  await expectBodyContains(page, "JSONL gateway")
  await expectBodyContains(page, "/v1/connect/jsonl")
  await expectBodyContains(page, "Default transport")
  await expectBodyContains(page, "Admin active")
  await expectBodyContains(page, "Configured transport")
  await page.getByTestId("admin-realtime-transport").selectOption("jsonl")
  await page.getByRole("button", { name: "Connect", exact: true }).click()
  await expectBodyContains(page, "jsonl / custom")
  await page.getByTestId("admin-realtime-transport").selectOption("websocket")
  await page.getByRole("button", { name: "Connect", exact: true }).click()
  await expectBodyContains(page, "websocket / webSocket")
  await expectBodyContains(page, realtimeFixture.channelId)
  await expectBodyContains(page, "Runtime Snapshot")
  await expectBodyContains(page, "Parent key")
  await expectBodyContains(page, "Nested")
  await page.getByTestId("runtime-record-parent-key").fill(auditFixture.roomId)
  await page.getByTestId("runtime-record-nested").fill("messages")
  await page.getByTestId("runtime-record-order").selectOption("schema")
  await page.getByTestId("runtime-record-activate").click()
  await expectBodyContains(page, "Activate runtime records")
  await expectBodyContains(page, "records in rooms.messages")
  await expectBodyContains(page, "Restore Chain")
  await expectBodyContains(page, "Coverage")
  await expectBodyContains(page, "Hot records")
  await expectBodyContains(page, "history v1")
  await expectBodyContains(page, "echo-ts")
  await expectBodyContains(page, "behavior-object-*")
  await expectBodyContains(page, "behavior.channel.*")
  await assertNoFrameworkOverlay(page)
  await assertNoPageHorizontalOverflow(page)

  await page.getByRole("button", { name: "Refresh all" }).click()
  await expectBodyContains(page, "Refresh health")
  await expectBodyContains(page, "history v1")
  await page.getByTestId("audit-mode").selectOption("trace")
  await page.getByTestId("audit-trace-kind").selectOption("record")
  await page.getByTestId("audit-trace-table").fill("rooms")
  await page.getByTestId("audit-trace-id").fill(auditFixture.roomId)
  await page.getByRole("button", { name: "Trace Entity" }).click()
  await expectBodyContains(page, "Trace target")
  await expectBodyContains(page, auditFixture.roomId)
  await expectBodyContains(page, "recordUpserted")
  await page.getByTestId("audit-mode").selectOption("replay")
  await page.getByRole("button", { name: "Replay Entity" }).click()
  await expectBodyContains(page, "Replay exists")
  await expectBodyContains(page, "Admin UI Trace Room")
  await page.getByTestId(`realtime-channel-${realtimeFixture.channelId}`).click()
  await expectInputValueContains(page, "realtime-state-json", "admin-smoke")
  await expectBodyContains(page, "nextdb-admin")
  await postJson(`${server.url}/v1/realtime/channels/${encodeURIComponent(realtimeFixture.channelId)}/broadcast`, {
    fromUserId: realtimeFixture.userId,
    kind: "adminPing",
    payload: { nonce: "admin-ui-recent-event" },
    includeSelf: false,
  })
  await expectBodyContains(page, "1 local events")
  await page.getByTestId("realtime-event-kind").fill("adminUiBroadcast")
  await page.getByTestId("realtime-event-payload").fill(JSON.stringify({ nonce: "admin-ui-broadcast-button" }, null, 2))
  await page.getByRole("button", { name: "Broadcast Event" }).click()
  await expectBodyContains(page, "Realtime broadcast")
  await expectBodyContains(page, "2 local events")
  await page.getByTestId("realtime-signal-user").fill("nextdb-admin")
  await page.getByTestId("realtime-signal-kind").fill("adminUiSignal")
  await page.getByTestId("realtime-signal-payload").fill(JSON.stringify({ nonce: "admin-ui-signal-button" }, null, 2))
  await page.getByRole("button", { name: "Signal User" }).click()
  await expectBodyContains(page, "Realtime signal")
  await expectBodyContains(page, "sessions 1")
  await expectBodyContains(page, "1 signals")
  await page.getByTestId("realtime-state-json").fill(JSON.stringify({ phase: "admin-updated", tick: 2 }, null, 2))
  await page.getByRole("button", { name: "Update State" }).click()
  await expectBodyContains(page, "v2")
  await expectInputValueContains(page, "realtime-state-json", "admin-updated")

  await page.getByRole("button", { name: "Snapshot" }).click()
  await expectBodyContains(page, "hot records")

  const behaviorRoomId = `admin-ui-behavior-${Date.now()}`
  await page.getByTestId("behavior-select").selectOption("echo-ts")
  await page.getByTestId("behavior-mutation-select").selectOption("echo.send")
  await page.getByTestId("behavior-user").fill("admin-ui-behavior-user")
  await page.getByTestId("behavior-input-roomId").fill(behaviorRoomId)
  await page.getByTestId("behavior-input-body").fill("admin browser behavior invoke")
  await page.getByTestId("behavior-invoke-submit").click()
  await expectBodyContains(page, "Behavior invoke")
  await expectBodyContains(page, "3 commits")
  await expectBodyContains(page, "recordUpserted")
  await expectBodyContains(page, "objectCommitted")
  await expectBodyContains(page, "messageCreated")
  await page.getByTestId("audit-mode").selectOption("trace")
  await page.getByTestId("audit-trace-kind").selectOption("record")
  await page.getByTestId("audit-trace-table").fill("rooms")
  await page.getByTestId("audit-trace-id").fill(behaviorRoomId)
  await page.getByRole("button", { name: "Trace Entity" }).click()
  await expectBodyContains(page, behaviorRoomId)
  await expectBodyContains(page, "recordUpserted")

  await page.getByRole("button", { name: "Export Manifest" }).click()
  await expectBodyContains(page, "Export manifest")
  await expectBodyContains(page, "history v1")

  await page.getByRole("button", { name: "Export Bundle" }).click()
  await expectBodyContains(page, "Export bundle")
  await expectBodyContains(page, "history v1")

  const backupFixtureRoomId = `admin-ui-backup-${Date.now()}`
  await postJson(`${server.url}/v1/records/rooms/${encodeURIComponent(backupFixtureRoomId)}`, {
    value: {
      id: backupFixtureRoomId,
      title: "Admin UI Backup Room",
    },
    clientMutationId: `${backupFixtureRoomId}-upsert`,
  })
  await page.getByRole("button", { name: "Run Backup" }).click()
  await expectBodyContains(page, "Run export backup")
  await expectBodyContains(page, "Backup run")
  await expectBodyContains(page, "chain ok")
  await page.getByRole("button", { name: "List Backups" }).click()
  await expectBodyContains(page, "List backup runs")
  await expectBodyContains(page, "1 runs")
  await page.getByRole("button", { name: "Backup Policy" }).click()
  await expectBodyContains(page, "Get backup policy")
  await page.getByRole("button", { name: "Save Policy" }).click()
  await expectBodyContains(page, "Save backup policy")
  await expectBodyContains(page, "keep 8")
  await page.getByRole("button", { name: "Run Policy" }).click()
  await expectBodyContains(page, "Run backup policy")
  await expectBodyContains(page, "Backup catalog")
  await page.getByRole("button", { name: "Plan Retention" }).click()
  await expectBodyContains(page, "Plan backup retention")
  await expectBodyContains(page, "candidate runs")
  await page.getByRole("button", { name: "List Bundles" }).click()
  await expectBodyContains(page, "List export bundles")
  await page.getByRole("button", { name: "Verify Bundle" }).click()
  await expectBodyContains(page, "Verify export bundle")
  await expectBodyContains(page, "Bundle verify")

  await page.getByTestId("schema-dry-run").click()
  await expectBodyContains(page, "validated v")

  const desktopScreenshot = join(screenshotDir, "admin-desktop.png")
  await page.screenshot({ path: desktopScreenshot, fullPage: false })

  await page.setViewportSize({ width: 390, height: 844 })
  await page.reload({ waitUntil: "networkidle" })
  await expectBodyContains(page, "NextDB Admin")
  await expectBodyContains(page, "Ready")
  await assertNoFrameworkOverlay(page)
  await assertNoPageHorizontalOverflow(page)
  const mobileScreenshot = join(screenshotDir, "admin-mobile.png")
  await page.screenshot({ path: mobileScreenshot, fullPage: false })

  assert.deepEqual(consoleIssues, [])
  console.log(`admin ui smoke ok`)
  console.log(`screenshots captured`)
} finally {
  if (browser) {
    await browser.close()
  }
  if (adminChild) {
    await stopProcess(adminChild)
  }
  if (serverChild) {
    await stopProcess(serverChild)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function waitForHealth(baseUrl) {
  await waitFor(async () => {
    try {
      const response = await fetch(`${baseUrl}/v1/health`)
      return response.ok
    } catch {
      return false
    }
  }, `health ${baseUrl}`)
}

async function waitForHttp(url) {
  await waitFor(async () => {
    try {
      const response = await fetch(url)
      return response.ok
    } catch {
      return false
    }
  }, `http ${url}`)
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    throw new Error(`${url} failed with ${response.status}: ${await response.text()}`)
  }
  return response.json()
}

async function run(command, args) {
  const child = spawn(command, args, {
    cwd: root,
    stdio: ["ignore", "ignore", "inherit"],
  })
  const code = await new Promise((resolve) => child.once("exit", resolve))
  if (code !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with ${code}`)
  }
}

async function waitFor(predicate, label) {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for ${label}`)
}

async function expectBodyContains(page, text) {
  await page.waitForFunction(
    (expected) => document.body?.innerText.includes(expected),
    text,
    { timeout: 5_000 },
  )
}

async function expectInputValueContains(page, testId, text) {
  await page.waitForFunction(
    ({ testId, text }) => {
      const element = document.querySelector(`[data-testid="${testId}"]`)
      return element instanceof HTMLTextAreaElement || element instanceof HTMLInputElement
        ? element.value.includes(text)
        : false
    },
    { testId, text },
    { timeout: 5_000 },
  )
}

async function assertSelectOption(page, testId, value) {
  const current = await page.getByTestId(testId).evaluate((element) =>
    element instanceof HTMLSelectElement ? element.value : undefined,
  )
  assert.equal(current, value)
}

async function assertNoFrameworkOverlay(page) {
  const bodyText = await page.locator("body").innerText()
  assert.equal(bodyText.includes("Internal server error"), false)
  assert.equal(bodyText.includes("Failed to fetch dynamically imported module"), false)
  assert.equal(bodyText.includes("[vite]"), false)
}

async function assertNoPageHorizontalOverflow(page) {
  const overflow = await page.evaluate(() => ({
    scrollWidth: document.documentElement.scrollWidth,
    clientWidth: document.documentElement.clientWidth,
    bodyScrollWidth: document.body.scrollWidth,
    bodyClientWidth: document.body.clientWidth,
  }))
  assert(
    overflow.scrollWidth <= overflow.clientWidth + 2,
    `page has horizontal overflow: ${JSON.stringify(overflow)}`,
  )
}

async function stopProcess(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return
  }
  child.kill("SIGINT")
  await new Promise((resolve) => child.once("exit", resolve))
}
