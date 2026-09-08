import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { parse } from "acorn";

const require = createRequire(import.meta.url);

// Read only the published theme data. Do not execute the upstream bundle's
// DOM observers, wallpaper importers or shared, unauthenticated settings API.
export async function dreamSkinTheme() {
  const source = await readFile(require.resolve("dsh-dream-skin/client"), "utf8");
  const pending = [parse(source, { ecmaVersion: "latest", sourceType: "script" })];
  let skins;
  while (pending.length) {
    const node = pending.pop();
    if (node.type === "VariableDeclarator" && node.id.name === "SKINS") skins = literal(node.init);
    for (const value of Object.values(node)) {
      if (Array.isArray(value)) {
        for (const child of value) if (child?.type) pending.push(child);
      } else if (value?.type) pending.push(value);
    }
  }
  const theme = skins?.find((item) => item.id === "midnight");
  if (!theme || theme.colorScheme !== "dark") throw new Error("Pinned Dream Skin midnight theme is unavailable");
  return { ...theme, id: "geosentinel-midnight", tokens: {
    ...theme.tokens,
    "--dsw-alias-label-tertiary": "#a4a4b2",
    "--dsw-alias-border-l2": "#626274",
  } };
}

function literal(node) {
  if (node.type === "Literal" && typeof node.value === "string") return node.value;
  if (node.type === "ArrayExpression") return node.elements.map(literal);
  if (node.type === "ObjectExpression") return Object.fromEntries(node.properties.map((property) => {
    if (property.type !== "Property" || property.computed || property.method || property.kind !== "init") throw new Error("Non-static Dream Skin theme data");
    return [property.key.name ?? property.key.value, literal(property.value)];
  }));
  throw new Error("Non-static Dream Skin theme data");
}
