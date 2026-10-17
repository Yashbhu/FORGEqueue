import { useEffect, useState, type FormEvent } from "react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { AlertCircle, ArrowRight, Eye, EyeOff, KeyRound, Loader2, Lock, User } from "lucide-react";
import { api, type AuthConfig } from "@/api/endpoints";
import { ApiError, getApiBase } from "@/api/client";
import { setSession, useSession } from "@/lib/auth";
import { getPrefs } from "@/lib/prefs";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";

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

function LogoMark({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 16 16" className={className} aria-hidden>
      <rect x="1" y="2" width="4" height="12" fill="currentColor" />
      <rect x="6" y="5" width="4" height="9" fill="currentColor" opacity=".7" />
      <rect x="11" y="8" width="4" height="6" fill="currentColor" opacity=".4" />
    </svg>
  );
}

function Field({
  id,
  label,
  icon: Icon,
  children,
}: {
  id: string;
  label: string;
  icon: typeof User;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1.5">
      <label
        htmlFor={id}
        className="text-[10px] font-medium uppercase tracking-[0.12em] text-muted-foreground"
      >
        {label}
      </label>
      <div className="relative">
        <Icon
          className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground"
          aria-hidden
        />
        {children}
      </div>
    </div>
  );
}

function ErrorNote({ message }: { message: string }) {
  return (
    <div
      role="alert"
      className="flex items-start gap-2 rounded-sm border tone-danger px-2.5 py-2 text-xs"
    >
      <AlertCircle className="mt-px size-3.5 shrink-0" aria-hidden />
      <span className="leading-4">{message}</span>
    </div>
  );
}

const inputClass =
  "h-9 w-full rounded-sm border border-input bg-background/60 pl-8 pr-3 text-sm text-foreground shadow-none outline-none transition-colors placeholder:text-muted-foreground/60 focus-visible:border-ring focus-visible:ring-1 focus-visible:ring-ring/40 disabled:opacity-60";

function loginReturnTarget() {
  return `${window.location.origin}/login`;
}

function LoginPage() {
  const navigate = useNavigate();
  const session = useSession();
  const [config, setConfig] = useState<AuthConfig | null>(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [exchanging, setExchanging] = useState(false);

  const apiBase = getPrefs().apiBase;
  const env = apiBase.startsWith("/") ? "local-dev" : apiBase.replace(/^https?:\/\//, "");
  const version = import.meta.env["VITE_APP_VERSION"] || "dev";

  useEffect(() => {
    if (session) navigate({ to: "/", replace: true });
  }, [session, navigate]);

  useEffect(() => {
    let active = true;
    api
      .authConfig()
      .then((c) => active && setConfig(c))
      .catch(() => active && setConfig({ password: true, oidc: false }));
    return () => {
      active = false;
    };
  }, []);

  // Complete an SSO redirect: exchange the one-time code for a session.
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");
    const ssoError = params.get("error");
    if (!code && !ssoError) return;

    const clean = () => {
      const url = new URL(window.location.href);
      url.searchParams.delete("code");
      url.searchParams.delete("error");
      window.history.replaceState({}, "", url.toString());
    };

    if (ssoError) {
      setError(
        ssoError === "access_denied"
          ? "Sign-in was cancelled."
          : "Sign-in failed. Please try again.",
      );
      clean();
      return;
    }

    setExchanging(true);
    api
      .exchangeOidc(code as string)
      .then((next) => {
        setSession(next);
        clean();
        navigate({ to: "/", replace: true });
      })
      .catch((err) => {
        setError(err instanceof ApiError ? err.message : "Sign-in failed. Please try again.");
        clean();
        setExchanging(false);
      });
  }, [navigate]);

  function startSSO() {
    const target = `${getApiBase()}/auth/oidc/login?redirect=${encodeURIComponent(loginReturnTarget())}`;
    window.location.href = target;
  }

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (busy) return;
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

  const showOIDC = !!config?.oidc;
  const passwordEnabled = config ? config.password : true;
  const provider = config?.provider || "single sign-on";

  return (
    <div className="relative flex min-h-screen items-center justify-center overflow-hidden bg-background px-4 py-10">
      {/* Ambient background: faint grid + radial glow behind the card. */}
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 opacity-[0.55]"
        style={{
          backgroundImage:
            "linear-gradient(to right, var(--color-border) 1px, transparent 1px), linear-gradient(to bottom, var(--color-border) 1px, transparent 1px)",
          backgroundSize: "44px 44px",
          maskImage: "radial-gradient(ellipse 70% 60% at 50% 40%, #000 0%, transparent 75%)",
          WebkitMaskImage: "radial-gradient(ellipse 70% 60% at 50% 40%, #000 0%, transparent 75%)",
        }}
      />
      <div
        aria-hidden
        className="pointer-events-none absolute left-1/2 top-1/2 size-[520px] -translate-x-1/2 -translate-y-1/2 rounded-full opacity-[0.07] blur-3xl"
        style={{ background: "radial-gradient(circle, var(--color-primary), transparent 70%)" }}
      />

      <div className="relative w-full max-w-[380px]">
        <div className="mb-4 flex items-center justify-center gap-2.5">
          <div className="flex size-8 items-center justify-center rounded-sm border bg-card text-primary shadow-sm">
            <LogoMark className="size-4" />
          </div>
          <div className="leading-none">
            <div className="text-sm font-semibold tracking-tight">ForgeQueue</div>
            <div className="mt-0.5 text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
              Operational Console
            </div>
          </div>
        </div>

        <div className="overflow-hidden rounded-md border bg-card/95 shadow-2xl backdrop-blur-sm">
          <div className="h-0.5 w-full bg-gradient-to-r from-primary/0 via-primary/60 to-primary/0" />

          {exchanging ? (
            <div className="flex flex-col items-center gap-3 px-5 py-12 text-center">
              <Loader2 className="size-5 animate-spin text-primary" aria-hidden />
              <p className="text-xs text-muted-foreground">Completing sign in…</p>
            </div>
          ) : (
            <div className="space-y-3.5 p-5">
              <div className="space-y-0.5">
                <h1 className="text-sm font-semibold tracking-tight">Sign in</h1>
                <p className="text-xs text-muted-foreground">
                  {showOIDC && !passwordEnabled
                    ? `Continue with ${provider} to access the console.`
                    : "Authenticate to manage queues, workers and jobs."}
                </p>
              </div>

              {error && <ErrorNote message={error} />}

              {showOIDC && (
                <Button
                  type="button"
                  onClick={startSSO}
                  disabled={busy}
                  className="group h-9 w-full gap-2 text-xs font-medium"
                >
                  <KeyRound className="size-3.5" />
                  Continue with {provider}
                  <ArrowRight className="size-3.5 transition-transform group-hover:translate-x-0.5" />
                </Button>
              )}

              {showOIDC && passwordEnabled && (
                <div className="flex items-center gap-3 py-0.5">
                  <span className="h-px flex-1 bg-border" />
                  <span className="text-[10px] uppercase tracking-[0.14em] text-muted-foreground/70">
                    or
                  </span>
                  <span className="h-px flex-1 bg-border" />
                </div>
              )}

              {passwordEnabled && (
                <form onSubmit={onSubmit} className="space-y-3.5">
                  <Field id="username" label="Username" icon={User}>
                    <input
                      id="username"
                      name="username"
                      autoComplete="username"
                      autoFocus
                      spellCheck={false}
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                      placeholder="admin"
                      className={inputClass}
                    />
                  </Field>

                  <Field id="password" label="Password" icon={Lock}>
                    <input
                      id="password"
                      name="password"
                      type={showPassword ? "text" : "password"}
                      autoComplete="current-password"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      placeholder="••••••••••••"
                      className={cn(inputClass, "pr-9")}
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword((s) => !s)}
                      aria-label={showPassword ? "Hide password" : "Show password"}
                      className="absolute right-1.5 top-1/2 flex size-6 -translate-y-1/2 items-center justify-center rounded-sm text-muted-foreground transition-colors hover:text-foreground"
                    >
                      {showPassword ? (
                        <EyeOff className="size-3.5" />
                      ) : (
                        <Eye className="size-3.5" />
                      )}
                    </button>
                  </Field>

                  <Button
                    type="submit"
                    variant={showOIDC ? "outline" : "default"}
                    disabled={busy || !username || !password}
                    className="group h-9 w-full gap-2 text-xs font-medium"
                  >
                    {busy ? (
                      <>
                        <Loader2 className="size-3.5 animate-spin" /> Signing in…
                      </>
                    ) : (
                      <>
                        Sign in
                        <ArrowRight className="size-3.5 transition-transform group-hover:translate-x-0.5" />
                      </>
                    )}
                  </Button>
                </form>
              )}
            </div>
          )}

          <div className="flex items-center justify-between border-t bg-muted/30 px-3 py-2 font-mono text-[10px] text-muted-foreground">
            <span className="truncate" title={apiBase}>
              {env}
            </span>
            <span>{version}</span>
          </div>
        </div>

        <p className="mt-3 text-center text-[10px] leading-4 text-muted-foreground/70">
          {showOIDC
            ? `You'll be redirected to ${provider} to sign in. Access is scoped by role.`
            : "Credentials are configured on the API server. Access is scoped by role."}
        </p>
      </div>
    </div>
  );
}
