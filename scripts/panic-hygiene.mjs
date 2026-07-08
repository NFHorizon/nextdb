#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();
const targets = [
  'crates/nextdb-server/src/wal.rs',
  'crates/nextdb-server/src/record_store.rs',
  'crates/nextdb-server/src/schema.rs',
];

const panicCallPattern = /\.(unwrap|expect)\s*\(/;

function braceDelta(line) {
  let delta = 0;
  let inString = false;
  let escaped = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (!inString && char === '/' && next === '/') {
      break;
    }
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === '\\') {
        escaped = true;
      } else if (char === '"') {
        inString = false;
      }
      continue;
    }
    if (char === '"') {
      inString = true;
    } else if (char === '{') {
      delta += 1;
    } else if (char === '}') {
      delta -= 1;
    }
  }

  return delta;
}

const violations = [];

for (const target of targets) {
  const absolutePath = path.join(repoRoot, target);
  const lines = fs.readFileSync(absolutePath, 'utf8').split(/\r?\n/);
  let pendingTestAttribute = false;
  let testModuleDepth = 0;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const trimmed = line.trim();

    if (testModuleDepth > 0) {
      testModuleDepth += braceDelta(line);
      continue;
    }

    if (trimmed === '#[cfg(test)]') {
      pendingTestAttribute = true;
      continue;
    }
    if (pendingTestAttribute && trimmed.startsWith('#')) {
      continue;
    }
    if (pendingTestAttribute && /^\s*mod\s+tests\s*\{/.test(line)) {
      pendingTestAttribute = false;
      testModuleDepth = braceDelta(line);
      continue;
    }
    pendingTestAttribute = false;

    if (panicCallPattern.test(line)) {
      violations.push(`${target}:${index + 1}: ${trimmed}`);
    }
  }
}

if (violations.length > 0) {
  console.error('panic hygiene failed: unwrap/expect in non-test safety-critical code');
  for (const violation of violations) {
    console.error(violation);
  }
  process.exit(1);
}

console.log('panic hygiene ok');
