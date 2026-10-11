import { useEffect, useState, type FormEvent } from "react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { api } from "@/api/endpoints";
import { ApiError } from "@/api/client";
import { setSession, useSession } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export const Route = createFileRoute("/login")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Sign in — ForgeQueue Console" },
      { name: "description", content: "Sign in to the ForgeQueue operational console." },
    ],
  }),
  component: LoginPage,
});

function LoginPage() {
  const navigate = useNavigate();
  const session = useSession();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (session) navigate({ to: "/", replace: true });
  }, [session, navigate]);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const next = await api.login(username.trim(), password);
      setSession(next);
      navigate({ to: "/", replace: true });
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Unable to reach the API server");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-4">
      <form onSubmit={onSubmit} className="w-full max-w-sm space-y-4">
        <div className="space-y-1">
          <h1 className="text-lg font-semibold tracking-tight">ForgeQueue</h1>
          <p className="text-xs text-muted-foreground">Sign in to the operational console.</p>
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="username" className="text-xs">
            Username
          </Label>
          <Input
            id="username"
            autoComplete="username"
            autoFocus
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            className="h-8 text-xs"
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="password" className="text-xs">
            Password
          </Label>
          <Input
            id="password"
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="h-8 text-xs"
          />
        </div>
        {error && (
          <div className="rounded-sm border tone-danger px-2 py-1.5 text-xs" role="alert">
            {error}
          </div>
        )}
        <Button type="submit" className="h-8 w-full text-xs" disabled={busy || !username || !password}>
          {busy ? "Signing in…" : "Sign in"}
        </Button>
      </form>
    </div>
  );
}
