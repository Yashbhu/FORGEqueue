import { useState, type ReactNode } from "react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { buttonVariants } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface Props {
  trigger: ReactNode;
  title: string;
  description: ReactNode;
  confirmLabel: string;
  destructive?: boolean;
  /** Require typing this string before confirming (for irreversible bulk ops). */
  typeToConfirm?: string;
  onConfirm: () => void;
}

export function ConfirmDialog({
  trigger,
  title,
  description,
  confirmLabel,
  destructive,
  typeToConfirm,
  onConfirm,
}: Props) {
  const [typed, setTyped] = useState("");
  return (
    <AlertDialog onOpenChange={() => setTyped("")}>
      <AlertDialogTrigger asChild>{trigger}</AlertDialogTrigger>
      <AlertDialogContent className="max-w-md">
        <AlertDialogHeader>
          <AlertDialogTitle className="text-sm">{title}</AlertDialogTitle>
          <AlertDialogDescription className="text-xs">{description}</AlertDialogDescription>
        </AlertDialogHeader>
        {typeToConfirm && (
          <label className="space-y-1 text-xs">
            <span className="text-muted-foreground">
              Type <code className="font-mono text-foreground">{typeToConfirm}</code> to confirm
            </span>
            <input
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              className="w-full rounded-sm border bg-background px-2 py-1 font-mono text-xs focus-visible:outline focus-visible:outline-ring"
            />
          </label>
        )}
        <AlertDialogFooter>
          <AlertDialogCancel autoFocus className="h-7 text-xs">
            Cancel
          </AlertDialogCancel>
          <AlertDialogAction
            disabled={!!typeToConfirm && typed !== typeToConfirm}
            onClick={onConfirm}
            className={cn(
              "h-7 text-xs",
              destructive && buttonVariants({ variant: "destructive" }),
              destructive && "h-7",
            )}
          >
            {confirmLabel}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
