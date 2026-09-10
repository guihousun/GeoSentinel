import path from "node:path";
import { readdirSync, readFileSync, existsSync } from "node:fs";

// The product ships its own skill library instead of relying on a filesystem
// skill root: the release snapshot is frozen, so the library travels with the
// app, and the registry entry carries the skill directory as `resourceBase` so
// the model can read references/ and scripts/ with the fenced read tools.
const NAME = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const FRONTMATTER = /^---\r?\n([\s\S]*?)\r?\n---[ \t]*\r?\n?/;

function unquote(value) {
  const text = value.trim();
  if (text.length >= 2 && ((text.startsWith('"') && text.endsWith('"')) || (text.startsWith("'") && text.endsWith("'"))))
    return text.slice(1, -1);
  return text;
}

/** Read the three routing fields the registry needs from a SKILL.md frontmatter block. */
export function parseSkillFile(text, file) {
  const match = FRONTMATTER.exec(text);
  if (!match) throw new Error(`skill file has no frontmatter: ${file}`);
  const fields = {};
  const lines = match[1].split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (/^\s*$/.test(line) || /^\s*#/.test(line)) continue;
    const separator = line.indexOf(":");
    if (separator < 1) continue;
    const key = line.slice(0, separator).trim();
    let value = line.slice(separator + 1);
    // Block scalars (`key: >` / `key: |`) fold the following indented lines.
    if (/^\s*[|>][-+]?[0-9]*\s*$/.test(value)) {
      const block = [];
      while (index + 1 < lines.length && (lines[index + 1].trim() === "" || /^\s+\S/.test(lines[index + 1]))) {
        block.push(lines[index + 1].trim());
        index += 1;
      }
      value = block.join(" ").trim();
    } else if (value.trim() === "") {
      // A nested mapping (for example `metadata:`) is not a routing field.
      continue;
    } else value = unquote(value);
    if (["name", "description", "whenToUse", "when-to-use"].includes(key)) fields[key] = value;
  }
  const name = fields.name;
  const description = fields.description;
  if (typeof name !== "string" || !NAME.test(name)) throw new Error(`skill has an invalid name: ${file}`);
  if (typeof description !== "string" || description.trim() === "") throw new Error(`skill has no description: ${file}`);
  return {
    name,
    description: description.trim(),
    whenToUse: (fields.whenToUse ?? fields["when-to-use"])?.trim() || undefined,
    content: text.slice(match[0].length).trim(),
  };
}

/** Load every `<root>/<name>/SKILL.md` bundle, failing loudly on a malformed entry. */
export function loadSkills(root) {
  if (!existsSync(root)) return [];
  const skills = [];
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const directory = path.join(root, entry.name);
    const file = path.join(directory, "SKILL.md");
    if (!existsSync(file)) continue;
    const parsed = parseSkillFile(readFileSync(file, "utf8"), file);
    if (parsed.name !== entry.name) throw new Error(`skill directory and name disagree: ${entry.name} != ${parsed.name}`);
    skills.push({
      name: parsed.name,
      description: parsed.description,
      ...(parsed.whenToUse ? { whenToUse: parsed.whenToUse } : {}),
      content: parsed.content,
      source: "custom",
      path: file,
      resourceBase: { kind: "directory", path: directory },
    });
  }
  return skills.sort((left, right) => left.name.localeCompare(right.name));
}

/**
 * Register the product library into the global skill layer once the registry
 * exists, keeping only the skills the caller's filter accepts.
 * @returns a `sync()` that re-registers the filtered set; used when the
 *   administrator changes the ordinary-user capability policy.
 */
export function registerProductSkills(ctx, root, filter = () => true) {
  let scope = null;
  const disposers = [];
  const sync = () => {
    if (scope === null) return;
    for (const dispose of disposers.splice(0)) dispose();
    for (const skill of loadSkills(root))
      if (filter(skill)) disposers.push(scope.skills.register(skill));
  };
  ctx.inject(["skills"], (inner) => {
    scope = inner;
    inner.effect(() => () => { for (const dispose of disposers.splice(0)) dispose(); });
    sync();
  });
  return sync;
}
