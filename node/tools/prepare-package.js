import { copyFileSync, cpSync } from "node:fs";

// Rust and Node use the same Lua sources. Bundle a copy for npm consumers.
cpSync(new URL("../../scripts/", import.meta.url), new URL("../scripts/", import.meta.url), {
    recursive: true,
});
copyFileSync(new URL("../../LICENSE", import.meta.url), new URL("../LICENSE", import.meta.url));
