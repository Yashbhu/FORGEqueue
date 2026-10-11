import { Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { setPrefs, usePrefs } from "@/lib/prefs";

export function Toolbar({ children }: { children: React.ReactNode }) {
  return <div className="flex flex-wrap items-center gap-1.5 border-b px-2 py-1.5">{children}</div>;
}

export function SearchInput({
  value,
  onChange,
  placeholder,
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder: string;
}) {
  return (
    <label className="flex h-7 items-center gap-1.5 rounded-sm border bg-background px-2 text-xs focus-within:outline focus-within:outline-ring">
      <Search className="size-3 text-muted-foreground" />
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-48 bg-transparent font-mono outline-none placeholder:font-sans placeholder:text-muted-foreground"
      />
    </label>
  );
}

export function FilterSelect({
  value,
  onChange,
  options,
  label,
}: {
  value: string;
  onChange: (v: string) => void;
  options: string[];
  label: string;
}) {
  return (
    <select
      aria-label={label}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="h-7 rounded-sm border bg-background px-1.5 text-xs"
    >
      <option value="">{label}: all</option>
      {options.map((o) => (
        <option key={o} value={o}>
          {o}
        </option>
      ))}
    </select>
  );
}

export function DensityToggle() {
  const { density } = usePrefs();
  return (
    <Button
      variant="outline"
      size="sm"
      className="h-7 text-xs"
      onClick={() => setPrefs({ density: density === "compact" ? "comfortable" : "compact" })}
    >
      {density === "compact" ? "Comfortable" : "Compact"}
    </Button>
  );
}
