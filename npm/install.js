#!/usr/bin/env node
"use strict";

const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const https = require("https");
const os = require("os");
const zlib = require("zlib");

const RELEASE_VERSION = "v0.1.0";
const REPO = "mplusm/thinkinglanguage";
const NATIVE_DIR = path.join(__dirname, "native");
const BIN_PATH = path.join(NATIVE_DIR, os.platform() === "win32" ? "tl.exe" : "tl");

function getPlatformTarget() {
  const platform = os.platform();
  const arch = os.arch();

  if (platform === "linux" && arch === "x64") {
    return "x86_64-unknown-linux-gnu";
  }
  if (platform === "darwin" && arch === "arm64") {
    return "aarch64-apple-darwin";
  }
  if (platform === "win32" && arch === "x64") {
    return "x86_64-pc-windows-msvc";
  }

  throw new Error(
    `Unsupported platform: ${platform}-${arch}. ` +
      `Download manually from https://github.com/${REPO}/releases`
  );
}

function fetch(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "User-Agent": "thinkinglanguage-npm" } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return fetch(res.headers.location).then(resolve, reject);
        }
        if (res.statusCode !== 200) {
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => resolve(Buffer.concat(chunks)));
        res.on("error", reject);
      })
      .on("error", reject);
  });
}

function extractTarGz(buffer, destDir, binaryName) {
  // tar.gz: gzip layer then POSIX tar (512-byte header blocks)
  const tar = zlib.gunzipSync(buffer);
  let offset = 0;
  while (offset < tar.length) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((b) => b === 0)) break;

    const name = header.toString("utf8", 0, 100).replace(/\0.*/g, "");
    const sizeStr = header.toString("utf8", 124, 136).replace(/\0.*/g, "").trim();
    const size = parseInt(sizeStr, 8) || 0;
    const dataStart = offset + 512;

    if (name === binaryName || name.endsWith(`/${binaryName}`)) {
      const data = tar.subarray(dataStart, dataStart + size);
      const dest = path.join(destDir, binaryName);
      fs.writeFileSync(dest, data);
      fs.chmodSync(dest, 0o755);
      return true;
    }

    offset = dataStart + Math.ceil(size / 512) * 512;
  }
  return false;
}

function extractZip(buffer, destDir, binaryName) {
  // Find the file in the zip by scanning for local file headers
  let offset = 0;
  while (offset < buffer.length - 4) {
    // Local file header signature: 0x04034b50
    if (
      buffer[offset] === 0x50 &&
      buffer[offset + 1] === 0x4b &&
      buffer[offset + 2] === 0x03 &&
      buffer[offset + 3] === 0x04
    ) {
      const nameLen = buffer.readUInt16LE(offset + 26);
      const extraLen = buffer.readUInt16LE(offset + 28);
      const compSize = buffer.readUInt32LE(offset + 18);
      const uncompSize = buffer.readUInt32LE(offset + 22);
      const compressionMethod = buffer.readUInt16LE(offset + 8);
      const name = buffer.toString("utf8", offset + 30, offset + 30 + nameLen);
      const dataStart = offset + 30 + nameLen + extraLen;

      if (name === binaryName || name.endsWith(`/${binaryName}`) || name.endsWith(`\\${binaryName}`)) {
        let data;
        if (compressionMethod === 0) {
          data = buffer.subarray(dataStart, dataStart + uncompSize);
        } else if (compressionMethod === 8) {
          data = zlib.inflateRawSync(buffer.subarray(dataStart, dataStart + compSize));
        } else {
          throw new Error(`Unsupported zip compression method: ${compressionMethod}`);
        }
        const dest = path.join(destDir, binaryName);
        fs.writeFileSync(dest, data);
        fs.chmodSync(dest, 0o755);
        return true;
      }

      offset = dataStart + compSize;
    } else {
      offset++;
    }
  }
  return false;
}

async function main() {
  if (fs.existsSync(BIN_PATH)) {
    console.log(`tl binary already exists at ${BIN_PATH}`);
    return;
  }

  const target = getPlatformTarget();
  const isWindows = os.platform() === "win32";
  const ext = isWindows ? "zip" : "tar.gz";
  const archive = `tl-${target}.${ext}`;
  const url = `https://github.com/${REPO}/releases/download/${RELEASE_VERSION}/${archive}`;

  console.log(`Downloading tl ${RELEASE_VERSION} for ${target}...`);
  const buffer = await fetch(url);

  fs.mkdirSync(NATIVE_DIR, { recursive: true });

  const binaryName = isWindows ? "tl.exe" : "tl";

  console.log("Extracting...");
  let found;
  if (isWindows) {
    found = extractZip(buffer, NATIVE_DIR, binaryName);
  } else {
    found = extractTarGz(buffer, NATIVE_DIR, binaryName);
  }

  if (!found) {
    throw new Error(`Could not find ${binaryName} in downloaded archive`);
  }

  console.log(`Installed tl to ${BIN_PATH}`);
}

main().catch((err) => {
  console.error(`Failed to install tl: ${err.message}`);
  process.exit(1);
});
