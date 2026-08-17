import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const [packageName, outputArgument] = process.argv.slice(2);
if (!packageName || !outputArgument) {
  throw new Error("usage: prepare-package.mjs PACKAGE_NAME OUTPUT_DIRECTORY");
}

const root = resolve(import.meta.dirname, "..");
const output = resolve(outputArgument);
const manifest = JSON.parse(await readFile(resolve(root, "package.json"), "utf8"));
manifest.name = packageName;
delete manifest.devDependencies;
delete manifest.scripts;
manifest.files = ["dist/src", "README.md"];

await rm(output, { recursive: true, force: true });
await mkdir(resolve(output, "dist"), { recursive: true });
await cp(resolve(root, "dist/src"), resolve(output, "dist/src"), { recursive: true });
await cp(resolve(root, "README.md"), resolve(output, "README.md"));
await writeFile(resolve(output, "package.json"), `${JSON.stringify(manifest, null, 2)}\n`);
