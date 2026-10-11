import { defineConfig } from "vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import { nitro } from "nitro/vite";
import viteReact from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import tsConfigPaths from "vite-tsconfig-paths";

export default defineConfig({
  plugins: [
    tsConfigPaths({ projects: ["./tsconfig.json"] }),
    tailwindcss(),
    tanstackStart({
      // Use src/server.ts as the SSR entry (see src/server.ts).
      server: { entry: "server" },
    }),
    nitro({ preset: "cloudflare-module" }),
    viteReact(),
  ],
  // Pre-bundle UI deps up front so a mid-session re-optimization can't load two React copies.
  optimizeDeps: {
    include: [
      "@radix-ui/react-tooltip",
      "@radix-ui/react-alert-dialog",
      "@radix-ui/react-tabs",
      "@radix-ui/react-switch",
      "@radix-ui/react-checkbox",
      "cmdk",
      "recharts",
      "sonner",
      "date-fns",
    ],
  },
});
